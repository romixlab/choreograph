//! Runtime bookkeeping: which jobs are queued, running or done, and who holds how much of each resource.
//!
//! The planner reads a [`State`]; a driver (the simulator, later the executor) changes it. Every change is
//! checked: a start that would go over a resource's capacity, skip a dependency or take a job twice is refused
//! with a [`StateError`], and a batch of starts is applied all together or not at all (a group starts as one).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::{JobId, Model, Need, NodeId, Phase, ResourceId};
use crate::time::Time;

/// Which resource satisfies each of a job's needs, in the order of `Job::needs`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Assignment(pub Vec<ResourceId>);

/// How a job ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    /// Ran and passed.
    Ok,
    /// Ran (or set up) and failed; cleanup ran.
    Failed,
    /// Cancelled before or during its run; cleanup ran if it had started.
    Cancelled,
    /// Not run: a valid cached result was reused.
    Cached,
}

impl Outcome {
    /// Whether jobs that come `after` this one may go on.
    pub fn satisfies_deps(self) -> bool {
        matches!(self, Outcome::Ok | Outcome::Cached)
    }
}

/// Where a job is.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Not arrived yet.
    Pending,
    /// In the queue, holding nothing.
    Queued,
    /// Holding its resources, in a phase.
    Running {
        assignment: Assignment,
        phase: Phase,
        /// When the job started (acquired its resources).
        started: Time,
        /// When the current phase started.
        phase_since: Time,
        /// How the job will end once cleanup is done: `Ok` unless it failed or was cancelled.
        ending: Outcome,
    },
    Done {
        at: Time,
        outcome: Outcome,
    },
}

/// Why a change to the state was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StateError {
    #[error("time cannot go back: now {now}, asked {asked}")]
    TimeBack { now: Time, asked: Time },
    #[error("job {0:?} is not queued")]
    NotQueued(JobId),
    #[error("job {0:?} is not running")]
    NotRunning(JobId),
    #[error("job {0:?} has not arrived")]
    NotArrived(JobId),
    #[error("job {job:?} may not start before {at}")]
    TooEarly { job: JobId, at: Time },
    #[error("job {job:?} waits for job {dep:?}")]
    DepNotDone { job: JobId, dep: JobId },
    #[error("job {job:?}: the assignment has {got} resources for {want} needs")]
    Arity { job: JobId, got: usize, want: usize },
    #[error("job {job:?}: need #{need} cannot be met by resource {resource:?}")]
    NoMatch {
        job: JobId,
        need: usize,
        resource: ResourceId,
    },
    #[error("job {job:?}: needs tagged same_node={tag:?} are on different nodes")]
    SameNode { job: JobId, tag: String },
    #[error("resource {resource:?} would be over capacity: {want} wanted, {free} free")]
    OverCapacity {
        resource: ResourceId,
        want: u64,
        free: u64,
    },
    #[error("job {job:?} is in a group whose other members do not start with it")]
    GroupSplit { job: JobId },
    #[error("job {0:?} is started twice in one batch")]
    Twice(JobId),
}

/// The runtime state of a model.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct State {
    now: Time,
    status: Vec<JobStatus>,
    used: Vec<u64>,
    holders: Vec<BTreeMap<JobId, u64>>,
    /// The current state index of each stateful resource.
    res_state: Vec<Option<usize>>,
}

impl State {
    /// Every job `Pending`, every resource free and in its initial state, time at `now`.
    pub fn new(model: &Model, now: Time) -> State {
        State {
            now,
            status: vec![JobStatus::Pending; model.jobs().len()],
            used: vec![0; model.resources().len()],
            holders: vec![BTreeMap::new(); model.resources().len()],
            res_state: model
                .resources()
                .iter()
                .map(|r| r.states.as_ref().map(|s| s.initial))
                .collect(),
        }
    }

    pub fn now(&self) -> Time {
        self.now
    }

    /// Moves time forward (never back).
    pub fn set_now(&mut self, t: Time) -> Result<(), StateError> {
        if t < self.now {
            return Err(StateError::TimeBack {
                now: self.now,
                asked: t,
            });
        }
        self.now = t;
        Ok(())
    }

    pub fn status(&self, j: JobId) -> &JobStatus {
        &self.status[j.index()]
    }

    /// Amount of a resource in use.
    pub fn used(&self, r: ResourceId) -> u64 {
        self.used[r.index()]
    }

    /// Amount of a resource free now.
    pub fn free(&self, model: &Model, r: ResourceId) -> u64 {
        model.resource(r).capacity - self.used[r.index()]
    }

    /// Free amount of every resource, indexed by resource id.
    pub fn free_all(&self, model: &Model) -> Vec<u64> {
        model.resource_ids().map(|r| self.free(model, r)).collect()
    }

    /// Who holds a resource and how much, in job id order.
    pub fn holders(&self, r: ResourceId) -> impl Iterator<Item = (JobId, u64)> + '_ {
        self.holders[r.index()].iter().map(|(j, a)| (*j, *a))
    }

    /// The current state of a stateful resource (index into its `StateSpace::states`).
    pub fn resource_state(&self, r: ResourceId) -> Option<usize> {
        self.res_state[r.index()]
    }

    /// Jobs in a given condition, in id order.
    pub fn queued(&self) -> impl Iterator<Item = JobId> + '_ {
        self.ids_where(|s| matches!(s, JobStatus::Queued))
    }
    pub fn running(&self) -> impl Iterator<Item = JobId> + '_ {
        self.ids_where(|s| matches!(s, JobStatus::Running { .. }))
    }
    pub fn pending(&self) -> impl Iterator<Item = JobId> + '_ {
        self.ids_where(|s| matches!(s, JobStatus::Pending))
    }

    fn ids_where<'a>(
        &'a self,
        f: impl Fn(&JobStatus) -> bool + 'a,
    ) -> impl Iterator<Item = JobId> + 'a {
        self.status
            .iter()
            .enumerate()
            .filter(move |(_, s)| f(s))
            .map(|(i, _)| JobId(i as u32))
    }

    /// Whether every job is done.
    pub fn all_done(&self) -> bool {
        self.status
            .iter()
            .all(|s| matches!(s, JobStatus::Done { .. }))
    }

    /// The outcome of a finished job.
    pub fn outcome(&self, j: JobId) -> Option<Outcome> {
        match &self.status[j.index()] {
            JobStatus::Done { outcome, .. } => Some(*outcome),
            _ => None,
        }
    }

    /// A pending job enters the queue.
    pub fn arrive(&mut self, j: JobId) -> Result<(), StateError> {
        match self.status[j.index()] {
            JobStatus::Pending => {
                self.status[j.index()] = JobStatus::Queued;
                Ok(())
            }
            _ => Err(StateError::NotQueued(j)),
        }
    }

    /// Checks one start against the given free amounts (which the caller may have reduced for other starts in
    /// the same batch) and returns the amount it takes from each resource.
    pub fn check_start(
        &self,
        model: &Model,
        j: JobId,
        a: &Assignment,
        free: &[u64],
    ) -> Result<Vec<(ResourceId, u64)>, StateError> {
        match self.status[j.index()] {
            JobStatus::Queued => {}
            JobStatus::Pending => return Err(StateError::NotArrived(j)),
            _ => return Err(StateError::NotQueued(j)),
        }
        let job = model.job(j);
        if self.now < job.earliest_start() {
            return Err(StateError::TooEarly {
                job: j,
                at: job.earliest_start(),
            });
        }
        for d in &job.after {
            if !self.outcome(*d).is_some_and(Outcome::satisfies_deps) {
                return Err(StateError::DepNotDone { job: j, dep: *d });
            }
        }
        if a.0.len() != job.needs.len() {
            return Err(StateError::Arity {
                job: j,
                got: a.0.len(),
                want: job.needs.len(),
            });
        }
        let mut take: BTreeMap<ResourceId, u64> = BTreeMap::new();
        let mut tags: BTreeMap<&str, NodeId> = BTreeMap::new();
        for (i, (need, r)) in job.needs.iter().zip(&a.0).enumerate() {
            if !model.matches(need, *r) {
                return Err(StateError::NoMatch {
                    job: j,
                    need: i,
                    resource: *r,
                });
            }
            if let Some(tag) = &need.same_node {
                let node = model.resource(*r).node;
                if *tags.entry(tag).or_insert(node) != node {
                    return Err(StateError::SameNode {
                        job: j,
                        tag: tag.clone(),
                    });
                }
            }
            *take.entry(*r).or_default() += need.amount;
        }
        for (r, want) in &take {
            let f = free[r.index()];
            if *want > f {
                return Err(StateError::OverCapacity {
                    resource: *r,
                    want: *want,
                    free: f,
                });
            }
        }
        Ok(take.into_iter().collect())
    }

    /// Starts a batch of jobs at the current time, all or none: every job must be queued, past its earliest
    /// start, with its dependencies done, and all of them together must fit. A job of a group may only start in
    /// a batch with every other queued member of its group.
    pub fn start(
        &mut self,
        model: &Model,
        batch: &[(JobId, Assignment)],
    ) -> Result<(), StateError> {
        let mut free = self.free_all(model);
        let mut takes = Vec::with_capacity(batch.len());
        let mut in_batch = std::collections::BTreeSet::new();
        for (j, _) in batch {
            if !in_batch.insert(*j) {
                return Err(StateError::Twice(*j));
            }
        }
        for (j, a) in batch {
            let take = self.check_start(model, *j, a, &free)?;
            for (r, amount) in &take {
                free[r.index()] -= amount;
            }
            if let Some(g) = model.job(*j).group {
                let split = model.group(g).members.iter().any(|m| {
                    !in_batch.contains(m) && matches!(self.status[m.index()], JobStatus::Queued)
                });
                if split {
                    return Err(StateError::GroupSplit { job: *j });
                }
            }
            takes.push(take);
        }
        for ((j, a), take) in batch.iter().zip(takes) {
            for (r, amount) in take {
                self.used[r.index()] += amount;
                *self.holders[r.index()].entry(*j).or_default() += amount;
            }
            for (need, r) in model.job(*j).needs.iter().zip(&a.0) {
                self.apply_state(model, need, *r);
            }
            self.status[j.index()] = JobStatus::Running {
                assignment: a.clone(),
                phase: Phase::Setup,
                started: self.now,
                phase_since: self.now,
                ending: Outcome::Ok,
            };
        }
        Ok(())
    }

    fn apply_state(&mut self, model: &Model, need: &Need, r: ResourceId) {
        if let (Some(name), Some(sp)) = (&need.state, &model.resource(r).states)
            && let Some(s) = sp.state(name)
        {
            self.res_state[r.index()] = Some(s);
        }
    }

    /// The current phase of a running job is done: setup goes to run, run to cleanup, and the end of cleanup
    /// releases the resources and finishes the job. Returns the new status.
    pub fn advance(&mut self, model: &Model, j: JobId) -> Result<&JobStatus, StateError> {
        let now = self.now;
        let JobStatus::Running { phase, ending, .. } = &mut self.status[j.index()] else {
            return Err(StateError::NotRunning(j));
        };
        let next = match phase {
            Phase::Setup => Some(Phase::Run),
            Phase::Run => Some(Phase::Cleanup),
            Phase::Cleanup => None,
        };
        match next {
            Some(p) => {
                let ending = *ending;
                self.set_phase(j, p, ending, now);
            }
            None => {
                let ending = *ending;
                self.release(model, j);
                self.status[j.index()] = JobStatus::Done {
                    at: now,
                    outcome: ending,
                };
            }
        }
        Ok(&self.status[j.index()])
    }

    /// The running job failed in setup or run: it goes straight to cleanup and will end `Failed`. A failure
    /// during cleanup keeps it in cleanup (cleanup must still finish; the driver retries or takes the resource
    /// out of service).
    pub fn fail(&mut self, j: JobId) -> Result<(), StateError> {
        self.interrupt(j, Outcome::Failed)
    }

    /// Cancels a job: a queued or pending one is done at once, a running one goes to cleanup.
    pub fn cancel(&mut self, j: JobId) -> Result<(), StateError> {
        match self.status[j.index()] {
            JobStatus::Queued | JobStatus::Pending => {
                self.status[j.index()] = JobStatus::Done {
                    at: self.now,
                    outcome: Outcome::Cancelled,
                };
                Ok(())
            }
            _ => self.interrupt(j, Outcome::Cancelled),
        }
    }

    /// A queued job is satisfied by a valid cached result and is done without running.
    pub fn mark_cached(&mut self, j: JobId) -> Result<(), StateError> {
        match self.status[j.index()] {
            JobStatus::Queued => {
                self.status[j.index()] = JobStatus::Done {
                    at: self.now,
                    outcome: Outcome::Cached,
                };
                Ok(())
            }
            _ => Err(StateError::NotQueued(j)),
        }
    }

    fn interrupt(&mut self, j: JobId, why: Outcome) -> Result<(), StateError> {
        let now = self.now;
        let JobStatus::Running { phase, ending, .. } = &mut self.status[j.index()] else {
            return Err(StateError::NotRunning(j));
        };
        if *ending == Outcome::Ok {
            *ending = why;
        }
        if *phase != Phase::Cleanup {
            let ending = *ending;
            self.set_phase(j, Phase::Cleanup, ending, now);
        }
        Ok(())
    }

    fn set_phase(&mut self, j: JobId, p: Phase, ending: Outcome, now: Time) {
        if let JobStatus::Running {
            phase,
            phase_since,
            ending: e,
            ..
        } = &mut self.status[j.index()]
        {
            *phase = p;
            *phase_since = now;
            *e = ending;
        }
    }

    fn release(&mut self, _model: &Model, j: JobId) {
        for (r, holders) in self.holders.iter_mut().enumerate() {
            if let Some(a) = holders.remove(&j) {
                self.used[r] -= a;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Job, Labels, ModelBuilder, Unit};

    fn model() -> (Model, JobId, JobId, JobId, ResourceId) {
        let mut b = ModelBuilder::new();
        let ram = b.kind("ram", Unit::Count);
        let n = b.node("n", Labels::new());
        let r = b.pool("ram", ram, n, 10);
        let a = b.job(Job::new("a").needs(ram, 6));
        let c = b.job(Job::new("b").needs(ram, 6));
        let d = b.job(Job::new("c").needs(ram, 4).after(a));
        (b.build().unwrap(), a, c, d, r)
    }

    #[test]
    fn capacity_is_enforced_for_a_batch() {
        let (m, a, b, _, r) = model();
        let mut s = State::new(&m, Time::ZERO);
        s.arrive(a).unwrap();
        s.arrive(b).unwrap();
        let err = s
            .start(&m, &[(a, Assignment(vec![r])), (b, Assignment(vec![r]))])
            .unwrap_err();
        assert!(matches!(err, StateError::OverCapacity { .. }));
        assert_eq!(s.used(r), 0, "nothing applied");
        s.start(&m, &[(a, Assignment(vec![r]))]).unwrap();
        assert_eq!(s.used(r), 6);
        assert_eq!(s.holders(r).collect::<Vec<_>>(), vec![(a, 6)]);
    }

    #[test]
    fn deps_and_phases() {
        let (m, a, _, c, r) = model();
        let mut s = State::new(&m, Time::ZERO);
        s.arrive(a).unwrap();
        s.arrive(c).unwrap();
        assert!(matches!(
            s.start(&m, &[(c, Assignment(vec![r]))]),
            Err(StateError::DepNotDone { .. })
        ));
        s.start(&m, &[(a, Assignment(vec![r]))]).unwrap();
        s.set_now(Time(10)).unwrap();
        s.advance(&m, a).unwrap(); // run
        s.advance(&m, a).unwrap(); // cleanup
        assert_eq!(s.used(r), 6, "held through cleanup");
        let st = s.advance(&m, a).unwrap().clone();
        assert_eq!(
            st,
            JobStatus::Done {
                at: Time(10),
                outcome: Outcome::Ok
            }
        );
        assert_eq!(s.used(r), 0);
        s.start(&m, &[(c, Assignment(vec![r]))]).unwrap();
    }

    #[test]
    fn failure_goes_to_cleanup_and_blocks_dependents() {
        let (m, a, _, c, r) = model();
        let mut s = State::new(&m, Time::ZERO);
        s.arrive(a).unwrap();
        s.arrive(c).unwrap();
        s.start(&m, &[(a, Assignment(vec![r]))]).unwrap();
        s.fail(a).unwrap();
        assert!(matches!(
            s.status(a),
            JobStatus::Running {
                phase: Phase::Cleanup,
                ending: Outcome::Failed,
                ..
            }
        ));
        s.advance(&m, a).unwrap();
        assert_eq!(s.outcome(a), Some(Outcome::Failed));
        assert_eq!(s.used(r), 0, "cleanup released");
        assert!(s.start(&m, &[(c, Assignment(vec![r]))]).is_err());
    }

    #[test]
    fn time_only_moves_forward() {
        let (m, ..) = model();
        let mut s = State::new(&m, Time(5));
        assert!(s.set_now(Time(4)).is_err());
        s.set_now(Time(6)).unwrap();
    }

    #[test]
    fn group_starts_together() {
        let mut b = ModelBuilder::new();
        let k = b.kind("slot", Unit::Count);
        let n = b.node("n", Labels::new());
        let r = b.pool("slot", k, n, 2);
        let x = b.job(Job::new("x").needs(k, 1));
        let y = b.job(Job::new("y").needs(k, 1));
        b.group("xy", vec![x, y]);
        let m = b.build().unwrap();
        let mut s = State::new(&m, Time::ZERO);
        s.arrive(x).unwrap();
        s.arrive(y).unwrap();
        assert!(matches!(
            s.start(&m, &[(x, Assignment(vec![r]))]),
            Err(StateError::GroupSplit { .. })
        ));
        s.start(&m, &[(x, Assignment(vec![r])), (y, Assignment(vec![r]))])
            .unwrap();
        assert_eq!(s.used(r), 2);
    }
}
