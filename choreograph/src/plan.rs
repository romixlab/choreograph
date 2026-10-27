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

use crate::model::{JobId, KindId, Model, Need, NodeId, ResourceId};
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
    /// Every resource that could take this need is on a node that is down.
    NodesDown { need: usize },
    /// A resource that would take this need has spent a refilling quota (see [`quota`](crate::quota)): the
    /// window that holds it longest, what it has used of its limit, and when every spent window of it has
    /// refilled (`None`: not known).
    Quota {
        need: usize,
        resource: ResourceId,
        window: String,
        used: u64,
        limit: u64,
        reopens: Option<Time>,
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
    /// Running jobs whose holders are asked to give way (pause or release; see
    /// [`Preempt::Ask`](crate::model::Preempt::Ask)), each with the job it would make room for.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub give_way: BTreeMap<JobId, JobId>,
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

/// Finds an assignment for job `j` given free amounts per resource, in the first of its shapes that fits (they
/// go from larger to smaller, so the largest that fits): each need gets one matching resource with enough free,
/// needs sharing a `same_node` tag land on one node, and two needs on the same resource add up. Candidates are
/// tried in the order `rank` returns them (lower first, ties by resource id); `None` when no shape fits.
/// Exhaustive over candidates (backtracking), so a job with few needs is cheap and the answer is exact.
pub fn find_assignment(
    model: &Model,
    j: JobId,
    free: &[u64],
    rank: &dyn Fn(usize, ResourceId) -> i64,
) -> Option<Assignment> {
    (0..model.job(j).shape_count()).find_map(|k| find_shaped(model, j, k, free, rank))
}

/// [`find_assignment`] in shape `k` only.
pub fn find_shaped(
    model: &Model,
    j: JobId,
    k: usize,
    free: &[u64],
    rank: &dyn Fn(usize, ResourceId) -> i64,
) -> Option<Assignment> {
    let needs = model.job(j).shape(k)?;
    let cands: Vec<Vec<ResourceId>> = needs
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
    let mut chosen = Vec::with_capacity(needs.len());
    let mut tags: BTreeMap<&str, NodeId> = BTreeMap::new();
    if search(model, needs, 0, &cands, &mut free, &mut chosen, &mut tags) {
        Some(Assignment::shaped(k, chosen))
    } else {
        None
    }
}

/// Asks to be woken when a waiting job may start anyway (see [`State::start_anyway_at`]).
pub fn wake_for_start_anyway(model: &Model, state: &State, plan: &mut Plan) {
    let now = state.now();
    for j in plan.waits.keys() {
        if let Some(t) = state.start_anyway_at(model, *j).filter(|t| *t > now)
            && matches!(state.status(*j), JobStatus::Queued)
        {
            plan.wake_at = Some(plan.wake_at.map_or(t, |w| w.min(t)));
        }
    }
}

/// Where a queued job that may start anyway ([`State::may_start_anyway`]) goes: its smallest shape, the least
/// over capacity, on the resources it is pinned to, as if they were empty (a node that is down takes nothing).
/// `None` for any other job, or when even that does not fit.
pub fn anyway_assignment(model: &Model, state: &State, j: JobId) -> Option<Assignment> {
    let job = model.job(j);
    if !state.may_start_anyway(model, j) || job.group.is_some() || !job.pinned(job.smallest()) {
        return None;
    }
    let empty: Vec<u64> = model
        .resource_ids()
        .map(|r| {
            let res = model.resource(r);
            if state.node_up(res.node) {
                res.capacity
            } else {
                0
            }
        })
        .collect();
    find_shaped(model, j, job.smallest(), &empty, &|_, r| {
        -(state.free(model, r) as i64)
    })
}

fn search<'m>(
    model: &Model,
    needs: &'m [Need],
    i: usize,
    cands: &[Vec<ResourceId>],
    free: &mut [u64],
    chosen: &mut Vec<ResourceId>,
    tags: &mut BTreeMap<&'m str, NodeId>,
) -> bool {
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
        if search(model, needs, i + 1, cands, free, chosen, tags) {
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

/// Why job `j` (queued, past its release, dependencies done) does not fit with these free amounts: the reason
/// its smallest shape does not fit (a `need` index is into that shape, `Job::needs` when it has one).
pub fn capacity_reason(model: &Model, state: &State, j: JobId, free: &[u64]) -> WaitReason {
    let job = model.job(j);
    let needs = job.shape(job.smallest()).unwrap_or_default();
    for (i, need) in needs.iter().enumerate() {
        let cands = model.candidates(need);
        if cands.is_empty() {
            return WaitReason::NoMatch { need: i };
        }
        if cands.iter().any(|r| free[r.index()] >= need.amount) {
            continue;
        }
        if cands
            .iter()
            .all(|r| !state.node_up(model.resource(*r).node))
        {
            return WaitReason::NodesDown { need: i };
        }
        if let Some(why) = quota_reason(model, state, i, &cands, need.amount) {
            return why;
        }
        // Prefer naming a single resource whose holders, once gone, would make it fit.
        if let Some(&r) = cands
            .iter()
            .find(|r| state.capacity(model, **r) >= need.amount && state.used(**r) > 0)
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
    let tag = needs
        .iter()
        .find_map(|n| n.same_node.clone())
        .unwrap_or_default();
    WaitReason::SameNode { tag }
}

/// The quota reason for need `i` when a spent quota is what keeps it off a resource that would otherwise take
/// it: the one that reopens first (an unknown reset last, ties by id).
fn quota_reason(
    model: &Model,
    state: &State,
    i: usize,
    cands: &[ResourceId],
    amount: u64,
) -> Option<WaitReason> {
    cands
        .iter()
        .filter_map(|r| {
            let s = state.spent(*r)?;
            let open = state
                .open_capacity(model, *r)
                .saturating_sub(state.used(*r));
            (open >= amount).then_some((s.reopens.is_none(), s.reopens, *r, s))
        })
        .min_by_key(|(none, at, r, _)| (*none, *at, *r))
        .map(|(_, _, r, s)| WaitReason::Quota {
            need: i,
            resource: r,
            window: s.window.name.clone(),
            used: s.window.used,
            limit: s.window.limit,
            reopens: s.reopens,
        })
}

/// The earliest time after now at which a spent quota refills, for a planner's `wake_at`.
pub fn next_refill(state: &State) -> Option<Time> {
    state
        .quotas()
        .filter_map(|(_, q)| q.spent_at(state.now())?.reopens)
        .min()
}

/// What a paused job that asked to resume takes back: (need index, resource, amount) for every need it gave
/// up, in need order; empty for any other job. The resources are the ones it ran on.
pub fn resume_needs(model: &Model, state: &State, j: JobId) -> Vec<(usize, ResourceId, u64)> {
    let JobStatus::Running {
        assignment,
        paused: Some(p),
        ..
    } = state.status(j)
    else {
        return Vec::new();
    };
    if !p.resume {
        return Vec::new();
    }
    model
        .job(j)
        .needs_for(assignment)
        .iter()
        .zip(&assignment.resources)
        .zip(&p.held)
        .enumerate()
        .filter(|(_, (_, h))| !**h)
        .map(|(i, ((n, r), _))| (i, *r, n.amount))
        .collect()
}

/// Why a resume does not fit with these free amounts (the first need it gave back that does not fit, held by
/// whom), `None` when it fits.
pub fn resume_reason(model: &Model, state: &State, j: JobId, free: &[u64]) -> Option<WaitReason> {
    let mut left = free.to_vec();
    for (i, r, amount) in resume_needs(model, state, j) {
        if left[r.index()] < amount {
            if !state.node_up(model.resource(r).node) {
                return Some(WaitReason::NodesDown { need: i });
            }
            if let Some(why) = quota_reason(model, state, i, &[r], amount) {
                return Some(why);
            }
            return Some(WaitReason::Resource {
                need: i,
                resource: r,
                held_by: state.holders(r).map(|(h, _)| h).collect(),
                free_at: None,
            });
        }
        left[r.index()] -= amount;
    }
    None
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
        // Resumes first: their holders are already at work and come back to what they gave up.
        for j in state.resuming() {
            match resume_reason(model, state, j, &free) {
                Some(why) => {
                    plan.waits.insert(j, why);
                }
                None => {
                    for (_, r, amount) in resume_needs(model, state, j) {
                        free[r.index()] -= amount;
                    }
                    if let JobStatus::Running { assignment, .. } = state.status(j) {
                        plan.starts.push(Start {
                            job: j,
                            assignment: assignment.clone(),
                        });
                    }
                }
            }
        }
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
                        for (need, r) in model.job(m).needs_for(&a).iter().zip(&a.resources) {
                            tentative[r.index()] -= need.amount;
                        }
                        starts.push(Start {
                            job: m,
                            assignment: a,
                        });
                    }
                    None => match anyway_assignment(model, state, m) {
                        Some(a) => {
                            for (need, r) in model.job(m).needs_for(&a).iter().zip(&a.resources) {
                                let f = &mut tentative[r.index()];
                                *f = f.saturating_sub(need.amount);
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
                    },
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
        wake_for_start_anyway(model, state, &mut plan);
        if !plan.waits.is_empty()
            && let Some(t) = next_refill(state)
        {
            plan.wake_at = Some(plan.wake_at.map_or(t, |w| w.min(t)));
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
            a.resources,
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
