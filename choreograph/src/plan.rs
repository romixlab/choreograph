//! What runs next, where, and why the rest waits.
//!
//! A [`Planner`] looks at the model and the current state and returns a [`Plan`]: a batch of starts the driver
//! applies all together ([`State::start`]), reservations for jobs that wait, and a typed [`WaitReason`] for every
//! queued job that does not start. Planners are pure: the same model and state give the same plan.
//!
//! [`FirstFit`] is the baseline: list scheduling in arrival order, each job on the first assignment that fits,
//! no reservations. It never deadlocks (a start takes all its needs at once) and never blocks the queue behind a
//! job that does not fit, but a large job can starve behind a stream of small ones, which is what aging and
//! reservations in better planners are for.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::model::{JobId, KindId, Model, NodeId, ResourceId};
use crate::state::{Assignment, JobStatus, State};
use crate::time::Time;

/// One job to start, on these resources.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Start {
    pub job: JobId,
    pub assignment: Assignment,
}

/// A promise to start a waiting job at a time, on these resources (backfilling must not delay it).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reservation {
    pub job: JobId,
    pub at: Time,
    pub assignment: Assignment,
}

/// Why a queued job does not start now.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "why", rename_all = "snake_case")]
pub enum WaitReason {
    /// Its release time (earliest start) is later.
    Release { at: Time },
    /// A job it comes after is not done yet.
    Dep { job: JobId },
    /// A job it comes after failed or was cancelled; it cannot run unless that one is redone.
    DepFailed { job: JobId },
    /// Another member of its group cannot start yet.
    Group { member: JobId },
    /// A resource it could use is held; `free_at` is the estimated time it frees, when known.
    Resource {
        need: usize,
        resource: ResourceId,
        held_by: Vec<JobId>,
        free_at: Option<Time>,
    },
    /// Not enough of a kind free on any matching resource (several holders, or capacity split up).
    Capacity {
        need: usize,
        kind: KindId,
        amount: u64,
        most_free: u64,
    },
    /// Needs tagged to run on one node cannot all be met on one node now.
    SameNode { tag: String },
    /// No resource can ever satisfy this need: a configuration error.
    NoMatch { need: usize },
    /// It would fit, but would delay a reservation held for a more important job.
    ReservedFor { job: JobId },
    /// A safety interlock refuses it.
    Interlock { rule: String },
    /// Waits for an approval.
    Approval,
}

/// The planner's answer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    /// Starts to apply now, all together.
    pub starts: Vec<Start>,
    /// Reservations for waiting jobs.
    pub reservations: Vec<Reservation>,
    /// Why each queued job that does not start waits.
    pub waits: BTreeMap<JobId, WaitReason>,
    /// The planner wants to be asked again at this time even if nothing else happens (a release time).
    pub wake_at: Option<Time>,
}

impl Plan {
    /// The starts as the batch [`State::start`] takes.
    pub fn batch(&self) -> Vec<(JobId, Assignment)> {
        self.starts
            .iter()
            .map(|s| (s.job, s.assignment.clone()))
            .collect()
    }
}

/// Decides what starts next. Implementations are deterministic.
pub trait Planner {
    fn plan(&mut self, model: &Model, state: &State) -> Plan;
}

/// Finds an assignment for job `j` given free amounts per resource: each need gets one matching resource with
/// enough free, needs sharing a `same_node` tag land on one node, and two needs on the same resource add up.
/// Candidates are tried in the order `rank` returns them (lower first, ties by resource id); `None` when nothing
/// fits. Exhaustive over candidates (backtracking), so a job with few needs is cheap and the answer is exact.
pub fn find_assignment(
    model: &Model,
    j: JobId,
    free: &[u64],
    rank: &dyn Fn(usize, ResourceId) -> i64,
) -> Option<Assignment> {
    let job = model.job(j);
    let cands: Vec<Vec<ResourceId>> = job
        .needs
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let mut c: Vec<ResourceId> = model
                .candidates(n)
                .into_iter()
                .filter(|r| free[r.index()] >= n.amount)
                .collect();
            c.sort_by_key(|r| (rank(i, *r), *r));
            c
        })
        .collect();
    let mut free = free.to_vec();
    let mut chosen = Vec::with_capacity(job.needs.len());
    let mut tags: BTreeMap<&str, NodeId> = BTreeMap::new();
    if search(model, j, 0, &cands, &mut free, &mut chosen, &mut tags) {
        Some(Assignment(chosen))
    } else {
        None
    }
}

fn search<'m>(
    model: &'m Model,
    j: JobId,
    i: usize,
    cands: &[Vec<ResourceId>],
    free: &mut [u64],
    chosen: &mut Vec<ResourceId>,
    tags: &mut BTreeMap<&'m str, NodeId>,
) -> bool {
    let needs = &model.job(j).needs;
    if i == needs.len() {
        return true;
    }
    let need = &needs[i];
    for &r in &cands[i] {
        if free[r.index()] < need.amount {
            continue;
        }
        let node = model.resource(r).node;
        let tag = need.same_node.as_deref();
        let mut new_tag = false;
        if let Some(t) = tag {
            match tags.get(t) {
                Some(n) if *n != node => continue,
                Some(_) => {}
                None => {
                    tags.insert(t, node);
                    new_tag = true;
                }
            }
        }
        free[r.index()] -= need.amount;
        chosen.push(r);
        if search(model, j, i + 1, cands, free, chosen, tags) {
            return true;
        }
        chosen.pop();
        free[r.index()] += need.amount;
        if new_tag && let Some(t) = tag {
            tags.remove(t);
        }
    }
    false
}

/// Why job `j` (queued, past its release, dependencies done) does not fit with these free amounts.
pub fn capacity_reason(model: &Model, state: &State, j: JobId, free: &[u64]) -> WaitReason {
    let job = model.job(j);
    for (i, need) in job.needs.iter().enumerate() {
        let cands = model.candidates(need);
        if cands.is_empty() {
            return WaitReason::NoMatch { need: i };
        }
        if cands.iter().any(|r| free[r.index()] >= need.amount) {
            continue;
        }
        // Prefer naming a single resource whose holders, once gone, would make it fit.
        if let Some(&r) = cands
            .iter()
            .find(|r| model.resource(**r).capacity >= need.amount && state.used(**r) > 0)
        {
            return WaitReason::Resource {
                need: i,
                resource: r,
                held_by: state.holders(r).map(|(h, _)| h).collect(),
                free_at: None,
            };
        }
        return WaitReason::Capacity {
            need: i,
            kind: need.kind,
            amount: need.amount,
            most_free: cands.iter().map(|r| free[r.index()]).max().unwrap_or(0),
        };
    }
    // Every need fits on its own: the same_node tags are what fails.
    let tag = job
        .needs
        .iter()
        .find_map(|n| n.same_node.clone())
        .unwrap_or_default();
    WaitReason::SameNode { tag }
}

/// Why job `j` cannot start for reasons other than resources (release time, dependencies), if any.
pub fn precondition_reason(model: &Model, state: &State, j: JobId) -> Option<WaitReason> {
    let job = model.job(j);
    for d in &job.after {
        match state.status(*d) {
            JobStatus::Done { outcome, .. } if outcome.satisfies_deps() => {}
            JobStatus::Done { .. } => return Some(WaitReason::DepFailed { job: *d }),
            _ => return Some(WaitReason::Dep { job: *d }),
        }
    }
    let at = job.earliest_start();
    if state.now() < at {
        return Some(WaitReason::Release { at });
    }
    None
}

/// The baseline planner: arrival order (ties by id), first assignment that fits, no reservations.
#[derive(Clone, Debug, Default)]
pub struct FirstFit;

impl Planner for FirstFit {
    fn plan(&mut self, model: &Model, state: &State) -> Plan {
        let mut plan = Plan::default();
        let mut free = state.free_all(model);
        let mut order: Vec<JobId> = state.queued().collect();
        order.sort_by_key(|j| (model.job(*j).arrival, *j));
        let mut decided: BTreeSet<JobId> = BTreeSet::new();
        for j in order {
            if decided.contains(&j) {
                continue;
            }
            if let Some(why) = precondition_reason(model, state, j) {
                if let WaitReason::Release { at } = why {
                    plan.wake_at = Some(plan.wake_at.map_or(at, |w| w.min(at)));
                }
                plan.waits.insert(j, why);
                decided.insert(j);
                continue;
            }
            // A group starts as one: every queued member must be ready and all must fit together.
            let members: Vec<JobId> = match model.job(j).group {
                Some(g) => model
                    .group(g)
                    .members
                    .iter()
                    .copied()
                    .filter(|m| matches!(state.status(*m), JobStatus::Queued))
                    .collect(),
                None => vec![j],
            };
            let mut tentative = free.clone();
            let mut starts = Vec::new();
            let mut blocked: Option<(JobId, WaitReason)> = None;
            for &m in &members {
                if m != j
                    && let Some(why) = precondition_reason(model, state, m)
                {
                    blocked = Some((m, why));
                    break;
                }
                match find_assignment(model, m, &tentative, &|_, _| 0) {
                    Some(a) => {
                        for (need, r) in model.job(m).needs.iter().zip(&a.0) {
                            tentative[r.index()] -= need.amount;
                        }
                        starts.push(Start {
                            job: m,
                            assignment: a,
                        });
                    }
                    None => {
                        blocked = Some((m, capacity_reason(model, state, m, &tentative)));
                        break;
                    }
                }
            }
            match blocked {
                None => {
                    free = tentative;
                    plan.starts.extend(starts);
                }
                Some((who, why)) => {
                    for &m in &members {
                        let reason = if m == who {
                            why.clone()
                        } else {
                            WaitReason::Group { member: who }
                        };
                        plan.waits.insert(m, reason);
                    }
                }
            }
            decided.extend(members);
        }
        plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Job, Labels, ModelBuilder, Unit};

    #[test]
    fn first_fit_skips_what_does_not_fit() {
        let mut b = ModelBuilder::new();
        let ram = b.kind("ram", Unit::Count);
        let n = b.node("n", Labels::new());
        let r = b.pool("ram", ram, n, 10);
        let big = b.job(Job::new("big").needs(ram, 8));
        let huge = b.job(Job::new("huge").needs(ram, 10).arrival(Time(1)));
        let small = b.job(Job::new("small").needs(ram, 2).arrival(Time(2)));
        let m = b.build().unwrap();
        let mut s = State::new(&m, Time(5));
        for j in [big, huge, small] {
            s.arrive(j).unwrap();
        }
        let p = FirstFit.plan(&m, &s);
        let started: Vec<JobId> = p.starts.iter().map(|x| x.job).collect();
        assert_eq!(started, vec![big, small], "no head-of-line blocking");
        assert!(matches!(
            p.waits[&huge],
            WaitReason::Capacity { most_free: 2, .. }
        ));
        s.start(&m, &p.batch()).unwrap();
        assert_eq!(s.used(r), 10);
        let p2 = FirstFit.plan(&m, &s);
        assert!(p2.starts.is_empty());
        assert!(matches!(p2.waits[&huge], WaitReason::Resource { .. }));
    }

    #[test]
    fn same_node_backtracks() {
        let mut b = ModelBuilder::new();
        let ram = b.kind("ram", Unit::Count);
        let probe = b.kind("probe", Unit::Count);
        let n1 = b.node("n1", Labels::new());
        let n2 = b.node("n2", Labels::new());
        b.pool("n1-ram", ram, n1, 10);
        let r2 = b.pool("n2-ram", ram, n2, 10);
        let p2 = b.pool("n2-probe", probe, n2, 1);
        let mut j = Job::new("j").needs(ram, 4).needs(probe, 1);
        j.needs[0].same_node = Some("x".into());
        j.needs[1].same_node = Some("x".into());
        let j = b.job(j);
        let m = b.build().unwrap();
        let free = vec![10, 10, 1];
        let a = find_assignment(&m, j, &free, &|_, _| 0).unwrap();
        assert_eq!(
            a.0,
            vec![r2, p2],
            "n1's RAM is tried first, then backtracked"
        );
    }

    #[test]
    fn release_and_deps_are_reasons() {
        let mut b = ModelBuilder::new();
        let a = b.job(Job::new("a"));
        let mut later = Job::new("later").after(a);
        later.release = Some(Time(100));
        let l = b.job(later);
        let m = b.build().unwrap();
        let mut s = State::new(&m, Time::ZERO);
        s.arrive(a).unwrap();
        s.arrive(l).unwrap();
        let p = FirstFit.plan(&m, &s);
        assert_eq!(p.waits[&l], WaitReason::Dep { job: a });
        s.start(&m, &p.batch()).unwrap();
        for _ in 0..3 {
            s.advance(&m, a).unwrap();
        }
        let p = FirstFit.plan(&m, &s);
        assert_eq!(p.waits[&l], WaitReason::Release { at: Time(100) });
        assert_eq!(p.wake_at, Some(Time(100)));
    }
}
