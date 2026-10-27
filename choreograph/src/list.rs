//! The list scheduler: the main planner.
//!
//! At every decision point it walks the queued jobs by effective priority and starts each one that fits, all its
//! needs at once (a group as one), on the best of the assignments that fit. A job that does not fit is skipped,
//! never blocking the ones behind it, and gets a typed [`WaitReason`]; the highest blocked job (EASY backfilling)
//! or every blocked job (conservative backfilling) gets a [`Reservation`] at its earliest feasible start, which
//! later jobs may not delay. [`ListScheduler::forecast`] runs the same rule forward on the estimates and gives
//! every queued job an ETA.
//!
//! **Effective priority**, in thousandths of a class ([`PRIORITY_SCALE`]):
//!
//! π_j(t) = max(class_j + (t − e_j)/A, U·clamp(1 − s_j(t)/H, 0, 1)),  s_j(t) = d_j − t − p90_j
//!
//! with e_j the job's earliest start (arrival or release), A the waiting time per class of aging
//! ([`Policy::aging_per_class`], 1/α), d_j the due date, H the deadline horizon and U the class reached at zero
//! slack ([`Policy::deadline_class`]). Aging bounds starvation: a job g classes below a fresh arrival overtakes
//! it after g·A. Near the due date the urgency orders jobs by least slack (EDF for equal durations). Ties go to
//! the shorter p50 (SPT), then the earlier arrival, then the lower id.
//!
//! **Placement**: the assignments that fit are found per node (and without a preference), then the one on the
//! fewest nodes ([`Policy::locality`]) with the best score on the job's dominant resource
//! ([`Policy::placement`]) wins; ties by resource ids.
//!
//! **Reservations and backfilling** work on a profile of free capacity over time: running jobs release their
//! resources at their estimated end ([`Policy::reserve_with`], p90 by default), jobs started in this round hold
//! theirs until theirs. A job may start now only if it fits for its whole estimated duration next to every
//! reservation, so with durations within their estimates a backfilled job never delays a reservation.
//!
//! ```
//! use choreograph::{ListScheduler, Planner, Policy, State, Time};
//!
//! let model = choreograph::jobset::from_toml(r#"
//!     [[node]]
//!     name = "bench"
//!     [[kind]]
//!     name = "scope"
//!     [[resource]]
//!     name = "scope-1"
//!     kind = "scope"
//!     node = "bench"
//!     [[job]]
//!     name = "soak"
//!     needs = [{ kind = "scope" }]
//!     run = { p50 = "20m", p90 = "30m" }
//!     [[job]]
//!     name = "smoke"
//!     needs = [{ kind = "scope" }]
//!     run = "1m"
//!     class = 1
//! "#).unwrap();
//! let policy = Policy::from_toml(r#"backfill = "conservative""#).unwrap();
//! let mut planner = ListScheduler::new(policy);
//! let mut state = State::new(&model, Time::ZERO);
//! for j in model.job_ids() {
//!     state.arrive(j).unwrap();
//! }
//! let plan = planner.plan(&model, &state);
//! let smoke = model.job_by_name("smoke").unwrap();
//! let soak = model.job_by_name("soak").unwrap();
//! assert_eq!(plan.starts[0].job, smoke); // the higher class goes first
//! assert_eq!(plan.reservations[0].job, soak); // soak is promised the scope after it
//! let eta = planner.forecast(&model, &state).etas[&soak];
//! assert_eq!(eta.p50.unwrap().finish, Time::ZERO + "21m".parse().unwrap());
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::Excluded;

use serde::{Deserialize, Serialize};

use crate::model::{Estimate, JobId, Model, Need, NodeId, Phase, Preempt, ResourceId};
use crate::plan::{
    Plan, Planner, Reservation, Start, WaitReason, capacity_reason, find_assignment,
    precondition_reason, resume_needs, resume_reason,
};
use crate::policy::{Backfill, Placement, Policy, Quantile};
use crate::state::{Assignment, JobStatus, State};
use crate::time::{Dur, Time};

/// Effective priorities are in units of 1/1000 of a class: class 2 is 2000.
pub const PRIORITY_SCALE: i64 = 1000;

/// The list scheduler with its policy.
#[derive(Clone, Debug, Default)]
pub struct ListScheduler {
    pub policy: Policy,
}

/// A projected start and finish.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    pub start: Time,
    pub finish: Time,
}

/// A job's projected start and finish if every duration takes its median (`p50`) or its 90th percentile (`p90`);
/// `None` where it does not start in that projection (a failed dependency, a dependency not yet arrived).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Eta {
    pub p50: Option<Span>,
    pub p90: Option<Span>,
}

/// The forward plan: an ETA for every queued and running job.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Forecast {
    pub etas: BTreeMap<JobId, Eta>,
}

/// One planned placement: the job, its resources and how long it holds them.
type Placed = Vec<(JobId, Assignment, Dur)>;

impl ListScheduler {
    pub fn new(policy: Policy) -> ListScheduler {
        ListScheduler { policy }
    }

    /// The class a job with a due date reaches at zero slack.
    fn deadline_class(&self, model: &Model) -> i32 {
        self.policy
            .deadline_class
            .unwrap_or_else(|| model.jobs().iter().map(|j| j.class).max().unwrap_or(0))
    }

    /// The class plus aging, in [`PRIORITY_SCALE`] units.
    pub fn aged_priority(&self, model: &Model, now: Time, j: JobId) -> i64 {
        let job = model.job(j);
        let base = i128::from(job.class) * i128::from(PRIORITY_SCALE);
        let aging = match self.policy.aging_per_class {
            Some(step) => {
                let waited = (now - job.earliest_start()).as_ms().max(0);
                i128::from(waited) * i128::from(PRIORITY_SCALE) / i128::from(step.as_ms().max(1))
            }
            None => 0,
        };
        clamp_i64(base + aging)
    }

    /// The deadline urgency in [`PRIORITY_SCALE`] units, `None` without a due date or while the slack is at least
    /// the horizon.
    pub fn urgency(&self, model: &Model, now: Time, j: JobId) -> Option<i64> {
        self.urgency_with(model, now, j, &|| self.deadline_class(model))
    }

    /// The urgency with the deadline class asked for only when needed (it scans the model's jobs).
    fn urgency_with(
        &self,
        model: &Model,
        now: Time,
        j: JobId,
        top: &dyn Fn() -> i32,
    ) -> Option<i64> {
        let job = model.job(j);
        let due = job.due?;
        let slack = i128::from(due.ms()) - i128::from(now.ms()) - i128::from(remaining_p90(job));
        let h = i128::from(self.policy.deadline_horizon.as_ms().max(1));
        if slack >= h {
            return None;
        }
        let into = (h - slack).min(h);
        let top = i128::from(top()) * i128::from(PRIORITY_SCALE);
        Some(clamp_i64(top * into / h))
    }

    /// The effective priority π_j(t) in [`PRIORITY_SCALE`] units: the larger of the aged class and the deadline
    /// urgency.
    pub fn priority(&self, model: &Model, now: Time, j: JobId) -> i64 {
        self.priority_with(model, now, j, &|| self.deadline_class(model))
    }

    fn priority_with(&self, model: &Model, now: Time, j: JobId, top: &dyn Fn() -> i32) -> i64 {
        let aged = self.aged_priority(model, now, j);
        self.urgency_with(model, now, j, top)
            .map_or(aged, |u| aged.max(u))
    }

    /// The queued jobs in dispatch order: effective priority, then shortest p50, then arrival, then id.
    pub fn order(&self, model: &Model, state: &State) -> Vec<JobId> {
        let now = state.now();
        // Computed once per call, and only if some job is within its deadline horizon.
        let top = std::cell::OnceCell::new();
        let top = || *top.get_or_init(|| self.deadline_class(model));
        let mut keyed: Vec<_> = state
            .queued()
            .map(|j| {
                let job = model.job(j);
                (
                    std::cmp::Reverse(self.priority_with(model, now, j, &top)),
                    job.phases.total_p50(),
                    job.arrival,
                    j,
                )
            })
            .collect();
        keyed.sort();
        keyed.into_iter().map(|k| k.3).collect()
    }

    /// The forward plan: this rule run on the estimates from the current state, once with every duration at its
    /// p50 and once at its p90. Jobs not yet arrived are not part of it.
    pub fn forecast(&self, model: &Model, state: &State) -> Forecast {
        let mut f = Forecast::default();
        for (q, p50) in [(Quantile::P50, true), (Quantile::P90, false)] {
            for (j, span) in self.run_forward(model, state, q) {
                let eta = f.etas.entry(j).or_default();
                if p50 {
                    eta.p50 = Some(span);
                } else {
                    eta.p90 = Some(span);
                }
            }
        }
        for j in state.queued().chain(state.running()) {
            f.etas.entry(j).or_default();
        }
        f
    }

    /// A run without randomness: plan, start, jump to the next estimated phase end (or release time), advance,
    /// repeat.
    fn run_forward(&self, model: &Model, state: &State, q: Quantile) -> BTreeMap<JobId, Span> {
        let phase_dur = |j: JobId, p: Phase| q.of(model.job(j).phases.get(p));
        let mut s = state.clone();
        let mut planner = self.clone();
        let mut starts: BTreeMap<JobId, Time> = BTreeMap::new();
        // The estimated end of each running job's current phase.
        let mut phase_end: BTreeMap<JobId, Time> = BTreeMap::new();
        for j in state.running() {
            if let JobStatus::Running { started, .. } = state.status(j) {
                starts.insert(j, *started);
                // A paused job has no phase end until it resumes.
                if let Some(end) = self.phase_end(model, state, j, q) {
                    phase_end.insert(j, end);
                }
            }
        }
        let mut out = BTreeMap::new();
        // Every round starts jobs, ends phases or moves time to a release: bounded by the job count.
        for _ in 0..8 * (model.jobs().len() + 1) {
            let plan = planner.plan(model, &s);
            let now = s.now();
            if s.start(model, &plan.batch()).is_err() {
                debug_assert!(false, "a plan the state refuses");
                break;
            }
            for st in &plan.starts {
                if starts.contains_key(&st.job) {
                    // A resume: it goes on in its phase.
                    if let Some(end) = self.phase_end(model, &s, st.job, q) {
                        phase_end.insert(st.job, end);
                    }
                    continue;
                }
                starts.insert(st.job, now);
                phase_end.insert(st.job, now + phase_dur(st.job, Phase::Setup));
            }
            let wake = plan.wake_at.filter(|w| *w > now);
            let Some(next) = phase_end.values().copied().chain(wake).min() else {
                break;
            };
            if s.set_now(next).is_err() {
                break;
            }
            // Phases that end now, including zero-length ones that follow them.
            while let Some((&j, _)) = phase_end.iter().find(|(_, e)| **e <= next) {
                match s.advance(model, j) {
                    Ok(JobStatus::Running { phase, .. }) => {
                        let p = *phase;
                        phase_end.insert(j, next + phase_dur(j, p));
                    }
                    _ => {
                        phase_end.remove(&j);
                        out.insert(
                            j,
                            Span {
                                start: starts[&j],
                                finish: next,
                            },
                        );
                    }
                }
            }
        }
        out
    }

    /// When a running job is estimated to end at a quantile; `None` while it is paused (it no longer claims
    /// any future time: what it keeps stays held, with no end). A bounded job: its current phase at its
    /// estimate (or now, if overrun), then the later phases. An open-ended job outside cleanup: from its age,
    /// at least [`Policy::open_floor`] from now.
    pub fn est_end(&self, model: &Model, state: &State, j: JobId, q: Quantile) -> Option<Time> {
        if state.is_paused(j) {
            return None;
        }
        self.end_if_running(model, state, j, q)
    }

    /// [`est_end`](ListScheduler::est_end) as if the job were not paused (a resume from now on).
    fn end_if_running(&self, model: &Model, state: &State, j: JobId, q: Quantile) -> Option<Time> {
        let JobStatus::Running {
            phase,
            phase_since,
            started,
            ..
        } = state.status(j)
        else {
            return None;
        };
        let job = model.job(j);
        let now = state.now();
        if job.open && *phase != Phase::Cleanup {
            let p = &job.phases;
            let (p50, p90) = (p.total_p50(), p.total_p90());
            let age = now - *started;
            let qv = match q {
                Quantile::P50 => 0.5,
                Quantile::P90 => 0.9,
            };
            let t = crate::theory::lognormal_conditional_quantile(
                p50.as_ms() as f64,
                p90.as_ms() as f64,
                age.as_ms() as f64,
                qv,
            )
            .map_or(q.of(Estimate { p50, p90 }), |t| Dur(t.round() as i64));
            return Some((*started + t).max(now + self.policy.open_floor));
        }
        let phases = &job.phases;
        let mut end = (*phase_since + q.of(phases.get(*phase))).max(now);
        for p in Phase::ALL.iter().filter(|p| **p > *phase) {
            end += q.of(phases.get(*p));
        }
        Some(end)
    }

    /// When a running job's current phase is estimated to end: its end less the later phases.
    fn phase_end(&self, model: &Model, state: &State, j: JobId, q: Quantile) -> Option<Time> {
        let end = self.est_end(model, state, j, q)?;
        let JobStatus::Running { phase, .. } = state.status(j) else {
            return None;
        };
        let later: Dur = Phase::ALL
            .iter()
            .filter(|p| **p > *phase)
            .map(|p| q.of(model.job(j).phases.get(*p)))
            .sum();
        Some((end - later).max(state.now()))
    }

    /// Paused jobs that asked to resume, before the queue: each takes back what it gave up on the resources it
    /// ran on, if that fits for its estimated rest next to this round's starts; else it waits (with a
    /// reservation as the backfilling mode allows, like a queued job).
    fn plan_resumes(
        &self,
        model: &Model,
        state: &State,
        busy: &mut Profile,
        full: &mut Profile,
        plan: &mut Plan,
    ) {
        let now = state.now();
        let q = self.policy.reserve_with;
        let mut resuming: Vec<JobId> = state.resuming().collect();
        resuming.sort_by_key(|j| (std::cmp::Reverse(self.priority(model, now, *j)), *j));
        for j in resuming {
            let parts = resume_needs(model, state, j);
            let end = self
                .end_if_running(model, state, j, q)
                .unwrap_or(now)
                .max(now + Dur(1));
            let JobStatus::Running { assignment, .. } = state.status(j) else {
                continue;
            };
            let fits = |p: &Profile, at: Time| {
                let free = p.window(at, at + (end - now));
                let mut want: BTreeMap<ResourceId, u64> = BTreeMap::new();
                for (_, r, a) in &parts {
                    *want.entry(*r).or_default() += a;
                }
                want.iter().all(|(r, a)| free[r.index()] >= *a)
            };
            if fits(full, now) {
                busy.hold_parts(&parts, now, end);
                full.hold_parts(&parts, now, end);
                plan.starts.push(Start {
                    job: j,
                    assignment: assignment.clone(),
                });
                continue;
            }
            let mut why = resume_reason(model, state, j, &busy.window(now, now + Dur(1)))
                .unwrap_or(WaitReason::Resource {
                    need: parts[0].0,
                    resource: parts[0].1,
                    held_by: state.holders(parts[0].1).map(|(h, _)| h).collect(),
                    free_at: None,
                });
            if let WaitReason::Resource {
                need,
                resource,
                free_at,
                ..
            } = &mut why
            {
                let amount = model.job(j).needs[*need].amount;
                *free_at = busy.earliest_free(*resource, amount, now);
            }
            plan.waits.insert(j, why);
            let reserve = match self.policy.backfill {
                Backfill::Off => false,
                Backfill::Easy => plan.reservations.is_empty(),
                Backfill::Conservative => true,
            };
            if reserve
                && let Some(at) = full
                    .times()
                    .into_iter()
                    .filter(|t| *t > now)
                    .find(|t| fits(full, *t))
            {
                let d = end - now;
                full.hold_parts(&parts, at, at + d);
                plan.reservations.push(Reservation {
                    job: j,
                    at,
                    assignment: assignment.clone(),
                });
            }
        }
    }

    /// The first job (in priority order) that waits for capacity and could start now if some running
    /// [`Preempt::Ask`] jobs at least [`Policy::preempt_gap`] classes lower gave way: their holders are
    /// asked, the lowest class first, then the youngest, as few as make it fit. One such job per plan.
    fn ask_give_way(&self, model: &Model, state: &State, busy: &Profile, plan: &mut Plan) {
        let Some(gap) = self.policy.preempt_gap else {
            return;
        };
        let now = state.now();
        let q = self.policy.reserve_with;
        let mut victims: Vec<(i32, std::cmp::Reverse<Time>, JobId)> = state
            .running()
            .filter(|v| model.job(*v).preempt == Preempt::Ask && !state.is_paused(*v))
            .filter_map(|v| match state.status(v) {
                JobStatus::Running { phase, started, .. } if *phase != Phase::Cleanup => {
                    Some((model.job(v).class, std::cmp::Reverse(*started), v))
                }
                _ => None,
            })
            .collect();
        if victims.is_empty() {
            return;
        }
        victims.sort();
        for j in self.order(model, state) {
            if !matches!(
                plan.waits.get(&j),
                Some(
                    WaitReason::Resource { .. }
                        | WaitReason::Capacity { .. }
                        | WaitReason::SameNode { .. }
                )
            ) || model.job(j).group.is_some()
            {
                continue;
            }
            let class = model.job(j).class;
            let mut p = busy.clone();
            let mut asked = Vec::new();
            for &(vclass, _, v) in &victims {
                if vclass > class - gap {
                    break;
                }
                let (Some(end), JobStatus::Running { assignment, .. }) =
                    (self.est_end(model, state, v, q), state.status(v))
                else {
                    continue;
                };
                let end = end.max(now + Dur(1));
                for (need, r) in model.job(v).needs.iter().zip(&assignment.0) {
                    p.free_early(*r, end, need.amount);
                }
                asked.push(v);
                if self.place(model, &p, &[j], now).is_some() {
                    for v in asked {
                        plan.give_way.insert(v, j);
                    }
                    return;
                }
            }
        }
    }

    /// The best assignment for job `j` with these free amounts, or `None`.
    fn choose(&self, model: &Model, j: JobId, free: &[u64]) -> Option<Assignment> {
        let job = model.job(j);
        let fit = |i: usize, r: ResourceId| self.need_score(model, &job.needs[i], r, free);
        let mut found: Vec<Assignment> = Vec::new();
        let mut add = |a: Option<Assignment>| {
            if let Some(a) = a
                && !found.contains(&a)
            {
                found.push(a);
            }
        };
        add(find_assignment(model, j, free, &fit));
        if self.policy.locality {
            let nodes: BTreeSet<NodeId> = job
                .needs
                .iter()
                .flat_map(|n| model.candidates(n))
                .map(|r| model.resource(r).node)
                .collect();
            for n in nodes {
                let rank = |i: usize, r: ResourceId| {
                    let away = i64::from(model.resource(r).node != n);
                    away * (1 << 40) + fit(i, r)
                };
                add(find_assignment(model, j, free, &rank));
            }
        }
        found.into_iter().min_by_key(|a| {
            let spans = if self.policy.locality {
                a.0.iter()
                    .map(|r| model.resource(*r).node)
                    .collect::<BTreeSet<_>>()
                    .len()
            } else {
                0
            };
            (spans, self.assignment_score(model, j, a, free), a.0.clone())
        })
    }

    /// Lower is better: the free share left on `r` after the need (best fit), or the need's share of `r`.
    fn need_score(&self, model: &Model, need: &Need, r: ResourceId, free: &[u64]) -> i64 {
        let cap = u128::from(model.resource(r).capacity.max(1));
        let x = match self.policy.placement {
            Placement::BestFit => free[r.index()].saturating_sub(need.amount),
            Placement::LeastShare => need.amount,
        };
        (u128::from(x) * 1_000_000 / cap) as i64
    }

    /// The score of a whole assignment: the need score on the job's dominant resource (its largest share).
    fn assignment_score(&self, model: &Model, j: JobId, a: &Assignment, free: &[u64]) -> i64 {
        let needs = &model.job(j).needs;
        let share = |(n, r): &(&Need, &ResourceId)| {
            u128::from(n.amount) * 1_000_000 / u128::from(model.resource(**r).capacity.max(1))
        };
        needs
            .iter()
            .zip(&a.0)
            .enumerate()
            .max_by_key(|(i, nr)| (share(nr), std::cmp::Reverse(*i)))
            .map_or(0, |(_, (n, r))| self.need_score(model, n, *r, free))
    }

    /// Places every member at time `t` on the profile, each for its estimated duration; all or none.
    fn place(
        &self,
        model: &Model,
        profile: &Profile,
        members: &[JobId],
        t: Time,
    ) -> Option<Placed> {
        let mut p = profile.clone();
        let mut placed = Vec::with_capacity(members.len());
        for &m in members {
            let d = hold_dur(model, m, self.policy.reserve_with);
            let free = p.window(t, t + d);
            let a = self.choose(model, m, &free)?;
            p.hold(model, m, &a, t, t + d);
            placed.push((m, a, d));
        }
        Some(placed)
    }

    /// The earliest time from `from` on at which every member fits on the profile.
    fn earliest(
        &self,
        model: &Model,
        profile: &Profile,
        members: &[JobId],
        from: Time,
    ) -> Option<(Time, Placed)> {
        std::iter::once(from)
            .chain(profile.times().into_iter().filter(|t| *t > from))
            .find_map(|t| self.place(model, profile, members, t).map(|p| (t, p)))
    }

    /// The reservation that keeps `members` from starting now: the first (in priority order) without which they
    /// would fit, else the first.
    fn blocking_reservation(
        &self,
        model: &Model,
        busy: &Profile,
        reservations: &[Reservation],
        members: &[JobId],
        now: Time,
    ) -> JobId {
        let q = self.policy.reserve_with;
        let blocker = reservations.iter().enumerate().find(|(i, _)| {
            let mut p = busy.clone();
            for (k, r) in reservations.iter().enumerate() {
                if k != *i && r.job != reservations[*i].job {
                    p.hold(
                        model,
                        r.job,
                        &r.assignment,
                        r.at,
                        r.at + hold_dur(model, r.job, q),
                    );
                }
            }
            self.place(model, &p, members, now).is_some()
        });
        blocker
            .map(|(_, r)| r)
            .or(reservations.first())
            .map(|r| r.job)
            .expect("a start blocked only by reservations means there is one")
    }

    /// Why a group (or a single job) that fits nowhere now waits: the first member that does not fit gets the
    /// capacity reason (with the estimated free time of a held resource), the others name it.
    fn blocked_reason(
        &self,
        model: &Model,
        state: &State,
        busy: &Profile,
        members: &[JobId],
    ) -> (JobId, WaitReason) {
        let now = state.now();
        let mut free = busy.window(now, now + Dur(1));
        for &m in members {
            match self.choose(model, m, &free) {
                Some(a) => {
                    for (need, r) in model.job(m).needs.iter().zip(&a.0) {
                        free[r.index()] -= need.amount;
                    }
                }
                None => {
                    let mut why = capacity_reason(model, state, m, &free);
                    if let WaitReason::Resource {
                        need,
                        resource,
                        free_at,
                        ..
                    } = &mut why
                    {
                        let amount = model.job(m).needs[*need].amount;
                        *free_at = busy.earliest_free(*resource, amount, now);
                    }
                    return (m, why);
                }
            }
        }
        // Each fits on its own at this instant but not together over their durations.
        let m = members[0];
        (m, capacity_reason(model, state, m, &free))
    }
}

impl Planner for ListScheduler {
    fn plan(&mut self, model: &Model, state: &State) -> Plan {
        let now = state.now();
        let q = self.policy.reserve_with;
        let mut plan = Plan::default();
        // `busy`: running jobs and this round's starts; `full`: also the reservations.
        let mut busy = Profile::new(
            model
                .resource_ids()
                .map(|r| state.headroom(model, r))
                .collect(),
        );
        for j in state.running() {
            if let (Some(end), JobStatus::Running { assignment, .. }) =
                (self.est_end(model, state, j, q), state.status(j))
            {
                // Still running at `now`, so it frees at the earliest just after.
                let end = end.max(now + Dur(1));
                for (need, r) in model.job(j).needs.iter().zip(&assignment.0) {
                    busy.release_at(*r, end, need.amount);
                }
            }
        }
        let mut full = busy.clone();
        self.plan_resumes(model, state, &mut busy, &mut full, &mut plan);
        let mut decided: BTreeSet<JobId> = BTreeSet::new();
        for j in self.order(model, state) {
            if decided.contains(&j) {
                continue;
            }
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
            decided.extend(&members);
            let pre = members
                .iter()
                .find_map(|m| precondition_reason(model, state, *m).map(|w| (*m, w)));
            if let Some((who, why)) = pre {
                if let WaitReason::Release { at } = why {
                    plan.wake_at = Some(plan.wake_at.map_or(at, |w| w.min(at)));
                }
                set_waits(&mut plan, &members, who, why);
                continue;
            }
            if let Some(placed) = self.place(model, &full, &members, now) {
                for (m, a, d) in placed {
                    busy.hold(model, m, &a, now, now + d);
                    full.hold(model, m, &a, now, now + d);
                    plan.starts.push(Start {
                        job: m,
                        assignment: a,
                    });
                }
                continue;
            }
            let (who, why) = match self.place(model, &busy, &members, now) {
                Some(_) => (
                    j,
                    WaitReason::ReservedFor {
                        job: self.blocking_reservation(
                            model,
                            &busy,
                            &plan.reservations,
                            &members,
                            now,
                        ),
                    },
                ),
                None => self.blocked_reason(model, state, &busy, &members),
            };
            set_waits(&mut plan, &members, who, why);
            let reserve = match self.policy.backfill {
                Backfill::Off => false,
                Backfill::Easy => plan.reservations.is_empty(),
                Backfill::Conservative => true,
            };
            if reserve && let Some((at, placed)) = self.earliest(model, &full, &members, now) {
                for (m, a, d) in placed {
                    full.hold(model, m, &a, at, at + d);
                    plan.reservations.push(Reservation {
                        job: m,
                        at,
                        assignment: a,
                    });
                }
            }
        }
        self.ask_give_way(model, state, &busy, &mut plan);
        plan
    }
}

fn set_waits(plan: &mut Plan, members: &[JobId], who: JobId, why: WaitReason) {
    for &m in members {
        let reason = if m == who {
            why.clone()
        } else {
            WaitReason::Group { member: who }
        };
        plan.waits.insert(m, reason);
    }
}

/// The whole job's estimated duration at a quantile.
fn total(model: &Model, j: JobId, q: Quantile) -> Dur {
    let p = &model.job(j).phases;
    Phase::ALL.iter().map(|ph| q.of(p.get(*ph))).sum()
}

/// How long a start holds its resources in a plan: at least 1 ms, so a zero estimate still occupies an instant.
fn hold_dur(model: &Model, j: JobId, q: Quantile) -> Dur {
    total(model, j, q).max(Dur(1))
}

/// The p90 work a queued job still has, in ms.
fn remaining_p90(job: &crate::model::Job) -> i64 {
    job.phases.total_p90().as_ms()
}

fn clamp_i64(x: i128) -> i64 {
    x.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

/// Free capacity per resource over time, from now on: the free amount now plus changes at later times.
#[derive(Clone, Debug)]
struct Profile {
    free: Vec<i64>,
    deltas: Vec<BTreeMap<Time, i64>>,
}

impl Profile {
    /// From the headroom of each resource now (negative where a capacity dropped below what is held).
    fn new(free: Vec<i64>) -> Profile {
        let n = free.len();
        Profile {
            free,
            deltas: vec![BTreeMap::new(); n],
        }
    }

    fn release_at(&mut self, r: ResourceId, t: Time, amount: u64) {
        *self.deltas[r.index()].entry(t).or_default() += amount as i64;
    }

    /// Job `j` holds its assignment over [from, until).
    fn hold(&mut self, model: &Model, j: JobId, a: &Assignment, from: Time, until: Time) {
        for (need, r) in model.job(j).needs.iter().zip(&a.0) {
            let d = &mut self.deltas[r.index()];
            *d.entry(from).or_default() -= need.amount as i64;
            *d.entry(until).or_default() += need.amount as i64;
        }
    }

    /// Parts of a job's needs, (need index, resource, amount), held over [from, until).
    fn hold_parts(&mut self, parts: &[(usize, ResourceId, u64)], from: Time, until: Time) {
        for (_, r, amount) in parts {
            let d = &mut self.deltas[r.index()];
            *d.entry(from).or_default() -= *amount as i64;
            *d.entry(until).or_default() += *amount as i64;
        }
    }

    /// An amount planned to free at `end` frees now instead.
    fn free_early(&mut self, r: ResourceId, end: Time, amount: u64) {
        self.free[r.index()] += amount as i64;
        *self.deltas[r.index()].entry(end).or_default() -= amount as i64;
    }

    fn at(&self, r: usize, t: Time) -> i64 {
        self.free[r] + self.deltas[r].range(..=t).map(|(_, d)| d).sum::<i64>()
    }

    /// The least free amount of every resource over [a, b).
    fn window(&self, a: Time, b: Time) -> Vec<u64> {
        (0..self.free.len())
            .map(|r| {
                let mut f = self.at(r, a);
                let mut min = f;
                for (_, d) in self.deltas[r].range((Excluded(a), Excluded(b))) {
                    f += d;
                    min = min.min(f);
                }
                min.max(0) as u64
            })
            .collect()
    }

    /// Every time the profile changes.
    fn times(&self) -> BTreeSet<Time> {
        self.deltas.iter().flat_map(|d| d.keys().copied()).collect()
    }

    /// The first time from `from` on at which `r` has `amount` free.
    fn earliest_free(&self, r: ResourceId, amount: u64, from: Time) -> Option<Time> {
        let r = r.index();
        std::iter::once(from)
            .chain(
                self.deltas[r]
                    .range((Excluded(from), Excluded(Time::MAX)))
                    .map(|(t, _)| *t),
            )
            .find(|t| self.at(r, *t) >= amount as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Estimate, Job, KindId, Labels, ModelBuilder, Unit};
    use crate::theory::aging_overtake;

    fn est(p50: Dur, p90: Dur) -> Estimate {
        Estimate { p50, p90 }
    }

    fn fixed(d: Dur) -> Estimate {
        Estimate::fixed(d)
    }

    /// One node with one pool of `cap` slots.
    fn pool(cap: u64) -> (ModelBuilder, KindId, ResourceId) {
        let mut b = ModelBuilder::new();
        let k = b.kind("slot", Unit::Count);
        let n = b.node("n", Labels::new());
        let r = b.pool("slots", k, n, cap);
        (b, k, r)
    }

    fn queued(m: &Model, now: Time, jobs: &[JobId]) -> State {
        let mut s = State::new(m, now);
        for j in jobs {
            s.arrive(*j).unwrap();
        }
        s
    }

    fn started(p: &Plan) -> Vec<JobId> {
        p.starts.iter().map(|s| s.job).collect()
    }

    fn with(policy: Policy) -> ListScheduler {
        ListScheduler::new(policy)
    }

    #[test]
    fn aging_overtakes_after_gap_over_alpha() {
        let (mut b, k, _) = pool(1);
        let low = b.job(
            Job::new("low")
                .needs(k, 1)
                .class(0)
                .run(fixed(Dur::mins(1))),
        );
        let high = b.job(
            Job::new("high")
                .needs(k, 1)
                .class(2)
                .run(fixed(Dur::mins(1))),
        );
        let m = b.build().unwrap();
        let step = Dur::hours(1);
        let ls = with(Policy {
            aging_per_class: Some(step),
            ..Policy::default()
        });
        // The overtake time from the theory module: gap of 2 classes at α = 1 class per hour.
        let w = Dur((aging_overtake(2.0, 1.0) * step.as_ms() as f64) as i64);
        assert_eq!(w, Dur::hours(2));
        // `high` arrives fresh at the decision time; `low` has waited since 0.
        let at = |now: Time| {
            let mut b2 = ModelBuilder::new();
            let k2 = b2.kind("slot", Unit::Count);
            let n = b2.node("n", Labels::new());
            b2.pool("slots", k2, n, 1);
            b2.job(m.job(low).clone());
            b2.job(m.job(high).clone().arrival(now));
            let m2 = b2.build().unwrap();
            let s = queued(&m2, now, &[low, high]);
            (ls.order(&m2, &s), with(ls.policy.clone()).plan(&m2, &s))
        };
        let (order, plan) = at(Time::ZERO + w - Dur(1));
        assert_eq!(
            order,
            vec![high, low],
            "just before 2h the fresh high job leads"
        );
        assert_eq!(started(&plan), vec![high]);
        let (order, plan) = at(Time::ZERO + w);
        assert_eq!(
            order,
            vec![low, high],
            "from 2h on the low job leads (equal π, earlier arrival)"
        );
        assert_eq!(started(&plan), vec![low]);
        // Without aging the low job never overtakes.
        let no_aging = with(Policy {
            aging_per_class: None,
            ..Policy::default()
        });
        let s = queued(&m, Time::ZERO + Dur::hours(100), &[low, high]);
        assert_eq!(no_aging.order(&m, &s), vec![high, low]);
    }

    #[test]
    fn deadline_urgency_orders_by_slack_near_due_dates() {
        let (mut b, k, _) = pool(1);
        let p = fixed(Dur::mins(10));
        // Arrival order is the reverse of the due order, so arrival cannot explain the result.
        let late = b.job(
            Job::new("due-40m")
                .needs(k, 1)
                .run(p)
                .due(Time(Dur::mins(40).as_ms())),
        );
        let mid = b.job(
            Job::new("due-25m")
                .needs(k, 1)
                .run(p)
                .due(Time(Dur::mins(25).as_ms()))
                .arrival(Time(1)),
        );
        let soon = b.job(
            Job::new("due-20m")
                .needs(k, 1)
                .run(p)
                .due(Time(Dur::mins(20).as_ms()))
                .arrival(Time(2)),
        );
        let other = b.job(
            Job::new("class-1")
                .needs(k, 1)
                .run(p)
                .class(1)
                .arrival(Time(3)),
        );
        let m = b.build().unwrap();
        let ls = with(Policy {
            aging_per_class: None,
            deadline_horizon: Dur::mins(30),
            deadline_class: Some(2),
            ..Policy::default()
        });
        let s = queued(&m, Time::ZERO, &[late, mid, soon, other]);
        // Slacks 30m, 15m, 10m: `due-40m` is at the horizon (no urgency yet, class 0).
        assert_eq!(ls.urgency(&m, Time::ZERO, late), None);
        assert_eq!(ls.priority(&m, Time::ZERO, mid), 1000, "half way: class 1");
        assert_eq!(ls.priority(&m, Time::ZERO, soon), 1333);
        assert_eq!(ls.order(&m, &s), vec![soon, mid, other, late]);
        // At zero slack a job reaches the deadline class, and stays there when overdue.
        assert_eq!(ls.priority(&m, Time(Dur::mins(10).as_ms()), soon), 2000);
        assert_eq!(ls.priority(&m, Time(Dur::hours(5).as_ms()), soon), 2000);
        // The default deadline class is the model's highest.
        assert_eq!(ListScheduler::default().deadline_class(&m), 1);
    }

    #[test]
    fn ties_go_to_shorter_p50_then_arrival_then_id() {
        let (mut b, k, _) = pool(1);
        let long = b.job(
            Job::new("long")
                .needs(k, 1)
                .run(est(Dur::mins(9), Dur::mins(10))),
        );
        let short = b.job(
            Job::new("short")
                .needs(k, 1)
                .run(est(Dur::mins(2), Dur::mins(20)))
                .arrival(Time(5)),
        );
        let twin_late = b.job(
            Job::new("twin-late")
                .needs(k, 1)
                .run(fixed(Dur::mins(2)))
                .arrival(Time(7)),
        );
        let twin_a = b.job(
            Job::new("twin-a")
                .needs(k, 1)
                .run(fixed(Dur::mins(2)))
                .arrival(Time(6)),
        );
        let twin_b = b.job(
            Job::new("twin-b")
                .needs(k, 1)
                .run(fixed(Dur::mins(2)))
                .arrival(Time(6)),
        );
        let m = b.build().unwrap();
        let ls = with(Policy {
            aging_per_class: None,
            ..Policy::default()
        });
        let s = queued(&m, Time(10), &[long, short, twin_late, twin_a, twin_b]);
        assert_eq!(
            ls.order(&m, &s),
            vec![short, twin_a, twin_b, twin_late, long]
        );
    }

    /// 12 slots; `running` holds 4 until 10m. `head` (class 1) needs 10: it waits for 10m and gets the
    /// reservation. Three class-0 jobs could start now.
    fn backfill_case() -> (Model, State, [JobId; 5]) {
        let (mut b, k, _) = pool(12);
        let running = b.job(Job::new("running").needs(k, 4).run(fixed(Dur::mins(10))));
        let head = b.job(
            Job::new("head")
                .needs(k, 10)
                .class(1)
                .run(est(Dur::mins(5), Dur::mins(10))),
        );
        let short = b.job(
            Job::new("short")
                .needs(k, 2)
                .run(est(Dur::mins(3), Dur::mins(5))),
        );
        let long2 = b.job(Job::new("long-2").needs(k, 2).run(fixed(Dur::hours(1))));
        let long3 = b.job(Job::new("long-3").needs(k, 3).run(fixed(Dur::hours(1))));
        let m = b.build().unwrap();
        let mut s = queued(&m, Time::ZERO, &[running]);
        let r = m.resource_by_name("slots").unwrap();
        s.start(&m, &[(running, Assignment(vec![r]))]).unwrap();
        s.advance(&m, running).unwrap(); // setup (0s) done, now in run
        for j in [head, short, long2, long3] {
            s.arrive(j).unwrap();
        }
        (m, s, [running, head, short, long2, long3])
    }

    #[test]
    fn easy_backfill_starts_what_does_not_delay_the_reservation() {
        let (m, s, [running, head, short, long2, long3]) = backfill_case();
        let mut ls = ListScheduler::default();
        let p = ls.plan(&m, &s);
        let ten = Time(Dur::mins(10).as_ms());
        assert_eq!(p.reservations.len(), 1);
        assert_eq!((p.reservations[0].job, p.reservations[0].at), (head, ten));
        match &p.waits[&head] {
            WaitReason::Resource {
                held_by, free_at, ..
            } => {
                assert_eq!(held_by, &vec![running]);
                assert_eq!(*free_at, Some(ten), "the holder's p90 end");
            }
            w => panic!("head waits for {w:?}"),
        }
        // `short` ends (p90) before 10m; `long-2` overlaps but leaves the reservation its 10 slots.
        assert_eq!(started(&p), vec![short, long2], "no head-of-line blocking");
        assert_eq!(p.waits[&long3], WaitReason::ReservedFor { job: head });
        s.clone().start(&m, &p.batch()).unwrap();
    }

    #[test]
    fn backfill_never_delays_the_reservation_at_p90() {
        let (m, s, [_, head, _, _, long3]) = backfill_case();
        for backfill in [Backfill::Easy, Backfill::Conservative] {
            let mut ls = with(Policy {
                backfill,
                ..Policy::default()
            });
            let reserved = ls.plan(&m, &s).reservations[0].at;
            // Every duration at its p90, the worst case reservations plan for: the head starts on time.
            let f = ls.forecast(&m, &s);
            assert_eq!(f.etas[&head].p90.unwrap().start, reserved, "{backfill:?}");
            assert!(f.etas[&head].p50.unwrap().start <= reserved);
            assert!(f.etas[&long3].p90.unwrap().start >= reserved);
        }
        // Without reservations `long-3` starts now and the head waits for it: an hour late.
        let mut off = with(Policy {
            backfill: Backfill::Off,
            ..Policy::default()
        });
        assert!(started(&off.plan(&m, &s)).contains(&long3));
        let f = off.forecast(&m, &s);
        assert_eq!(
            f.etas[&head].p90.unwrap().start,
            Time(Dur::hours(1).as_ms())
        );
    }

    #[test]
    fn conservative_protects_every_reservation() {
        // 10 slots; `running` holds 6 until 10m. b (class 2) needs 6, c (class 1) needs 8, d (class 0) needs 4
        // for 25m: it fits now and does not delay b, but would delay c.
        let (mut b, k, r) = pool(10);
        let running = b.job(Job::new("running").needs(k, 6).run(fixed(Dur::mins(10))));
        let jb = b.job(Job::new("b").needs(k, 6).class(2).run(fixed(Dur::mins(10))));
        let jc = b.job(Job::new("c").needs(k, 8).class(1).run(fixed(Dur::mins(10))));
        let jd = b.job(Job::new("d").needs(k, 4).run(fixed(Dur::mins(25))));
        let m = b.build().unwrap();
        let mut s = queued(&m, Time::ZERO, &[running]);
        s.start(&m, &[(running, Assignment(vec![r]))]).unwrap();
        for j in [jb, jc, jd] {
            s.arrive(j).unwrap();
        }
        let mins = |x| Time(Dur::mins(x).as_ms());

        let mut easy = ListScheduler::default();
        let p = easy.plan(&m, &s);
        assert_eq!(started(&p), vec![jd]);
        assert_eq!(
            p.reservations.len(),
            1,
            "EASY reserves for the highest blocked job only"
        );
        let f = easy.forecast(&m, &s);
        assert_eq!(f.etas[&jb].p90.unwrap().start, mins(10));
        assert_eq!(f.etas[&jc].p90.unwrap().start, mins(25), "d pushed c back");

        let mut cons = with(Policy {
            backfill: Backfill::Conservative,
            ..Policy::default()
        });
        let p = cons.plan(&m, &s);
        assert!(p.starts.is_empty());
        let res: Vec<_> = p.reservations.iter().map(|r| (r.job, r.at)).collect();
        assert_eq!(res, vec![(jb, mins(10)), (jc, mins(20)), (jd, mins(30))]);
        assert_eq!(p.waits[&jd], WaitReason::ReservedFor { job: jc });
        let f = cons.forecast(&m, &s);
        assert_eq!(f.etas[&jc].p90.unwrap().start, mins(20));
        assert_eq!(f.etas[&jd].p90.unwrap().start, mins(30));
    }

    #[test]
    fn groups_start_all_or_nothing() {
        let (mut b, k, r) = pool(3);
        let holder = b.job(Job::new("holder").needs(k, 2).run(fixed(Dur::mins(5))));
        let x = b.job(Job::new("x").needs(k, 1).class(1).run(fixed(Dur::mins(1))));
        let y = b.job(Job::new("y").needs(k, 1).class(1).run(fixed(Dur::mins(1))));
        let z = b.job(Job::new("z").needs(k, 1).run(fixed(Dur::mins(2))));
        b.group("xy", vec![x, y]);
        let m = b.build().unwrap();
        let mut s = queued(&m, Time::ZERO, &[holder]);
        s.start(&m, &[(holder, Assignment(vec![r]))]).unwrap();
        for j in [x, y, z] {
            s.arrive(j).unwrap();
        }
        let mut ls = ListScheduler::default();
        let p = ls.plan(&m, &s);
        // One slot free: x alone would fit, the group does not; z (2m) fits before the group's reservation.
        assert_eq!(started(&p), vec![z]);
        assert!(
            matches!(p.waits[&y], WaitReason::Group { member } if member == x)
                || matches!(p.waits[&x], WaitReason::Group { member } if member == y)
        );
        let res: BTreeSet<_> = p.reservations.iter().map(|r| (r.job, r.at)).collect();
        let five = Time(Dur::mins(5).as_ms());
        assert_eq!(res, [(x, five), (y, five)].into());
        s.start(&m, &p.batch()).unwrap();
        s.set_now(five).unwrap();
        for j in [z, holder] {
            while matches!(s.status(j), JobStatus::Running { .. }) {
                s.advance(&m, j).unwrap();
            }
        }
        let p = ls.plan(&m, &s);
        assert_eq!(started(&p), vec![x, y]);
        s.start(&m, &p.batch()).unwrap();
    }

    #[test]
    fn placement_best_fit_least_share_and_locality() {
        let mut b = ModelBuilder::new();
        let ram = b.kind("ram", Unit::Count);
        let probe = b.kind("probe", Unit::Count);
        let na = b.node("a", Labels::new());
        let nb = b.node("b", Labels::new());
        let a_ram = b.pool("a-ram", ram, na, 16);
        let b_ram = b.pool("b-ram", ram, nb, 32);
        let b_probe = b.pool("b-probe", probe, nb, 1);
        let j = b.job(Job::new("j").needs(ram, 12));
        let jp = b.job(Job::new("jp").needs(ram, 12).needs(probe, 1));
        let m = b.build().unwrap();
        let free = vec![16, 32, 1];
        let pick = |policy: Policy, job| with(policy).choose(&m, job, &free).unwrap().0;
        assert_eq!(
            pick(Policy::default(), j),
            vec![a_ram],
            "best fit: the tighter hole"
        );
        let least = Policy {
            placement: Placement::LeastShare,
            ..Policy::default()
        };
        assert_eq!(pick(least, j), vec![b_ram], "least share: the larger node");
        assert_eq!(
            pick(Policy::default(), jp),
            vec![b_ram, b_probe],
            "locality: one node"
        );
        let spread = Policy {
            locality: false,
            ..Policy::default()
        };
        assert_eq!(pick(spread, jp), vec![a_ram, b_probe]);
    }

    #[test]
    fn wait_reasons_and_wake_time() {
        let (mut b, k, _) = pool(4);
        let first = b.job(Job::new("first"));
        let dep = b.job(Job::new("dep").needs(k, 1).after(first));
        let mut later = Job::new("later").needs(k, 1);
        later.release = Some(Time(500));
        let later = b.job(later);
        let m = b.build().unwrap();
        let s = queued(&m, Time::ZERO, &[first, dep, later]);
        let p = ListScheduler::default().plan(&m, &s);
        assert_eq!(started(&p), vec![first]);
        assert_eq!(p.waits[&dep], WaitReason::Dep { job: first });
        assert_eq!(p.waits[&later], WaitReason::Release { at: Time(500) });
        assert_eq!(p.wake_at, Some(Time(500)));
        assert!(
            p.reservations.is_empty(),
            "only jobs blocked by resources get reservations"
        );
    }

    #[test]
    fn forecast_gives_p50_and_p90_etas() {
        let (mut b, k, _) = pool(1);
        let e = est(Dur::mins(5), Dur::mins(10));
        let a = b.job(Job::new("a").needs(k, 1).run(e));
        let c = b.job(Job::new("c").needs(k, 1).run(e).arrival(Time(1)));
        let d = b.job(Job::new("d").needs(k, 1).run(e).arrival(Time(2)).after(a));
        let m = b.build().unwrap();
        let s = queued(&m, Time(10), &[a, c, d]);
        let ls = ListScheduler::default();
        let f = ls.forecast(&m, &s);
        let span = |start: i64, finish: i64| {
            Some(Span {
                start: Time(10 + Dur::mins(start).as_ms()),
                finish: Time(10 + Dur::mins(finish).as_ms()),
            })
        };
        assert_eq!(
            f.etas[&a],
            Eta {
                p50: span(0, 5),
                p90: span(0, 10)
            }
        );
        assert_eq!(
            f.etas[&c],
            Eta {
                p50: span(5, 10),
                p90: span(10, 20)
            }
        );
        assert_eq!(
            f.etas[&d],
            Eta {
                p50: span(10, 15),
                p90: span(20, 30)
            }
        );
        assert_eq!(ls.forecast(&m, &s), f, "deterministic");
    }

    /// A small seeded generator (SplitMix64), so the random cases are the same on every run.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    #[test]
    fn random_cases_keep_reservations_at_p90() {
        for seed in 0..300 {
            let mut rng = Rng(seed);
            let mut b = ModelBuilder::new();
            let cores = b.kind("cores", Unit::Count);
            let probe = b.kind("probe", Unit::Count);
            let nodes: Vec<NodeId> = (0..2)
                .map(|i| b.node(format!("n{i}"), Labels::new()))
                .collect();
            for (i, n) in nodes.iter().enumerate() {
                b.pool(format!("n{i}-cores"), cores, *n, 4 + rng.below(5));
                b.pool(format!("n{i}-probe"), probe, *n, 1);
            }
            let jobs: Vec<JobId> = (0..8)
                .map(|i| {
                    let p50 = Dur::mins(1 + rng.below(20) as i64);
                    let p90 = p50 + Dur::mins(rng.below(20) as i64);
                    let mut j = Job::new(format!("j{i}"))
                        .needs(cores, 1 + rng.below(4))
                        .class(rng.below(3) as i32)
                        .run(est(p50, p90));
                    if rng.below(3) == 0 {
                        j = j.needs(probe, 1);
                    }
                    b.job(j)
                })
                .collect();
            let m = b.build().unwrap();
            let s = queued(&m, Time::ZERO, &jobs);
            for backfill in [Backfill::Easy, Backfill::Conservative] {
                let mut ls = with(Policy {
                    aging_per_class: None,
                    backfill,
                    ..Policy::default()
                });
                let p = ls.plan(&m, &s);
                s.clone().start(&m, &p.batch()).unwrap();
                let f = ls.forecast(&m, &s);
                for r in &p.reservations {
                    let start = f.etas[&r.job].p90.expect("everything runs").start;
                    assert!(
                        start <= r.at,
                        "seed {seed} {backfill:?}: {:?} reserved at {} started at {}",
                        r.job,
                        r.at,
                        start
                    );
                    if backfill == Backfill::Easy {
                        break;
                    }
                }
            }
        }
    }
}
