//! Runtime bookkeeping: which jobs are queued, running or done, and who holds how much of each resource.
//!
//! The planner reads a [`State`]; a driver (the simulator, later the executor) changes it. Every change is
//! checked: a start that would go over a resource's capacity, skip a dependency or take a job twice is refused
//! with a [`StateError`], and a batch of starts is applied all together or not at all (a group starts as one).
//!
//! The state also holds what is measured or reported from outside: a resource's current capacity
//! ([`State::set_capacity`]) and whether a node is up ([`State::set_node_up`], [`State::node_lost`]). A capacity
//! that drops below what is held, or a node that goes down, stops new starts there; the jobs running there keep
//! what they hold.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::model::{JobId, Model, Need, NodeId, Phase, Remap, ResourceId};
use crate::time::Time;

/// How a job's demand is met: which of its shapes (see [`Job::shapes`](crate::model::Job::shapes); 0 is
/// `Job::needs`, the largest) and which resource satisfies each need of that shape, in order.
///
/// It serializes as a bare list of resource ids for shape 0, so traces and journals written before shapes
/// existed read the same.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "AssignmentRepr", into = "AssignmentRepr")]
pub struct Assignment {
    pub resources: Vec<ResourceId>,
    pub shape: usize,
}

impl Assignment {
    /// An assignment of the job's first shape (`Job::needs`).
    pub fn new(resources: Vec<ResourceId>) -> Assignment {
        Assignment {
            resources,
            shape: 0,
        }
    }

    /// An assignment of shape `shape`.
    pub fn shaped(shape: usize, resources: Vec<ResourceId>) -> Assignment {
        Assignment { resources, shape }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum AssignmentRepr {
    Plain(Vec<ResourceId>),
    Shaped {
        shape: usize,
        resources: Vec<ResourceId>,
    },
}

impl From<AssignmentRepr> for Assignment {
    fn from(r: AssignmentRepr) -> Assignment {
        match r {
            AssignmentRepr::Plain(resources) => Assignment::new(resources),
            AssignmentRepr::Shaped { shape, resources } => Assignment { resources, shape },
        }
    }
}

impl From<Assignment> for AssignmentRepr {
    fn from(a: Assignment) -> AssignmentRepr {
        if a.shape == 0 {
            AssignmentRepr::Plain(a.resources)
        } else {
            AssignmentRepr::Shaped {
                shape: a.shape,
                resources: a.resources,
            }
        }
    }
}

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
    /// Not run: a valid cached result (a pass) was reused.
    Cached,
    /// Not run: a valid cached result says it fails (a known-bad combination is not run again).
    #[serde(rename = "cached_fail")]
    CachedFail,
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
        /// Set while the job is paused: it gave back what it does not keep.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        paused: Option<Paused>,
        /// It started anyway, past its [`Job::start_anyway_after`](crate::model::Job::start_anyway_after),
        /// counted over the capacity of what it holds.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        over: bool,
    },
    Done {
        at: Time,
        outcome: Outcome,
    },
}

/// A running job's pause (see [`State::pause`]): its lease and place stay, but it holds only the needs whose
/// kind it keeps on pause, and plans no longer count on it ending at any time. A resume asks for the rest back
/// on the same resources; the planner gives it when it fits, as a start of the job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Paused {
    pub since: Time,
    /// Per need, in the order of `Job::needs`: whether it is still held.
    pub held: Vec<bool>,
    /// The holder asked to go on: the job waits for what it gave back.
    pub resume: bool,
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
    #[error("{0:?} is not how a run ends (ok, failed or cancelled)")]
    NotARunOutcome(Outcome),
    #[error("job {0:?} is paused")]
    Paused(JobId),
    #[error("job {0:?} is not paused")]
    NotPaused(JobId),
    #[error("job {0:?} is in cleanup and cannot pause")]
    InCleanup(JobId),
    #[error("job {0:?} resumes only on the resources it ran on")]
    ResumeElsewhere(JobId),
    #[error("job {job:?} has no shape {shape}")]
    NoShape { job: JobId, shape: usize },
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
    /// Index of the queued jobs, so the planner's look at the queue does not scan every job.
    queued: BTreeSet<JobId>,
    /// Index of the running jobs.
    running: BTreeSet<JobId>,
    /// The measured capacity of each resource, `None` while it is the declared one.
    measured: Vec<Option<u64>>,
    /// Whether each node is up.
    up: Vec<bool>,
}

/// What happens to the jobs running on a node that is lost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LostJobs {
    /// They go back to the queue and start again elsewhere (a build, a test run).
    #[default]
    Requeue,
    /// They end `Failed` (work that must not run twice without a look).
    Fail,
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
            queued: BTreeSet::new(),
            running: BTreeSet::new(),
            measured: vec![None; model.resources().len()],
            up: vec![true; model.nodes().len()],
        }
    }

    /// Makes room for the jobs, resources and nodes added to the model since: new jobs are `Pending`, new
    /// resources free in their initial state, new nodes up.
    pub(crate) fn grow(&mut self, model: &Model) {
        self.status.resize(model.jobs().len(), JobStatus::Pending);
        let rs = &model.resources()[self.used.len()..];
        self.res_state
            .extend(rs.iter().map(|r| r.states.as_ref().map(|s| s.initial)));
        let n = model.resources().len();
        self.used.resize(n, 0);
        self.holders.resize(n, BTreeMap::new());
        self.measured.resize(n, None);
        self.up.resize(model.nodes().len(), true);
    }

    /// Which jobs a compaction keeps: every job not done at or before `done_by`, and every job a kept job comes
    /// after (its outcome still decides whether that one may run), in job id order.
    pub fn keep_after_compaction(&self, model: &Model, done_by: Time) -> Vec<bool> {
        let mut keep: Vec<bool> = self
            .status
            .iter()
            .map(|s| !matches!(s, JobStatus::Done { at, .. } if *at <= done_by))
            .collect();
        let mut work: Vec<JobId> = model.job_ids().filter(|j| keep[j.index()]).collect();
        while let Some(j) = work.pop() {
            for d in &model.job(j).after {
                if !keep[d.index()] {
                    keep[d.index()] = true;
                    work.push(*d);
                }
            }
        }
        keep
    }

    /// Follows a compaction of the model: drops the removed jobs (all done) and renumbers the rest.
    pub(crate) fn remap(&mut self, remap: &Remap) {
        let mut i = 0;
        self.status.retain(|_| {
            i += 1;
            remap.job(JobId(i as u32 - 1)).is_some()
        });
        let map = |set: &BTreeSet<JobId>| -> BTreeSet<JobId> {
            set.iter().filter_map(|j| remap.job(*j)).collect()
        };
        self.queued = map(&self.queued);
        self.running = map(&self.running);
        for h in &mut self.holders {
            *h = h
                .iter()
                .filter_map(|(j, a)| remap.job(*j).map(|n| (n, *a)))
                .collect();
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

    /// The capacity of a resource now: 0 while its node is down, else the measured one if set, else the
    /// declared one.
    pub fn capacity(&self, model: &Model, r: ResourceId) -> u64 {
        let res = model.resource(r);
        if !self.up[res.node.index()] {
            return 0;
        }
        self.measured[r.index()].unwrap_or(res.capacity)
    }

    /// The measured capacity of a resource, `None` while the declared one counts.
    pub fn measured(&self, r: ResourceId) -> Option<u64> {
        self.measured[r.index()]
    }

    /// Whether a node is up.
    pub fn node_up(&self, n: NodeId) -> bool {
        self.up[n.index()]
    }

    /// Amount of a resource free now: its capacity now less what is held, 0 when more is held than that.
    pub fn free(&self, model: &Model, r: ResourceId) -> u64 {
        self.capacity(model, r).saturating_sub(self.used[r.index()])
    }

    /// Free amount of every resource, indexed by resource id.
    pub fn free_all(&self, model: &Model) -> Vec<u64> {
        model.resource_ids().map(|r| self.free(model, r)).collect()
    }

    /// Capacity now less what is held, negative when a capacity dropped below what running jobs hold: the
    /// amount stays short until enough of them end.
    pub fn headroom(&self, model: &Model, r: ResourceId) -> i64 {
        let c = i128::from(self.capacity(model, r)) - i128::from(self.used[r.index()]);
        c.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
    }

    /// Sets the capacity of a resource as measured from outside (`None`: back to the declared one). It may drop
    /// below what is held: nothing new starts on the resource until enough is free again, and the jobs that
    /// hold it keep running. Matching still uses the declared capacity, so a need above the measured capacity
    /// waits instead of being refused.
    pub fn set_capacity(&mut self, r: ResourceId, capacity: Option<u64>) {
        self.measured[r.index()] = capacity;
    }

    /// Marks a node up or down. Nothing new starts on a node that is down; jobs running there keep running
    /// and release their resources as usual (a drain). For a node that is gone with its jobs, see
    /// [`State::node_lost`].
    pub fn set_node_up(&mut self, n: NodeId, up: bool) {
        self.up[n.index()] = up;
    }

    /// A node is lost with the jobs running on it: it goes down, and every running job that holds a resource
    /// on it releases all it holds (cleanup cannot run there) and is requeued or ends `Failed`, as `then`
    /// says. A job that was already ending failed or cancelled ends so. Returns the affected jobs in id
    /// order.
    pub fn node_lost(&mut self, model: &Model, n: NodeId, then: LostJobs) -> Vec<JobId> {
        self.up[n.index()] = false;
        let hit: Vec<JobId> = self
            .running
            .iter()
            .copied()
            .filter(|j| match &self.status[j.index()] {
                JobStatus::Running { assignment, .. } => assignment
                    .resources
                    .iter()
                    .any(|r| model.resource(*r).node == n),
                _ => false,
            })
            .collect();
        for &j in &hit {
            self.abandon(model, j, then)
                .expect("a job in the running index is running");
        }
        hit
    }

    /// A running job is given up without its cleanup (its holder is gone: the node was lost, its lease
    /// expired): it releases all it holds at once and goes back to the queue or ends `Failed`, as `then` says.
    /// A job that was already ending failed or cancelled ends so.
    pub fn abandon(&mut self, model: &Model, j: JobId, then: LostJobs) -> Result<(), StateError> {
        let JobStatus::Running { ending, .. } = self.status[j.index()] else {
            return Err(StateError::NotRunning(j));
        };
        self.release(model, j);
        self.running.remove(&j);
        self.status[j.index()] = match (ending, then) {
            (Outcome::Ok, LostJobs::Requeue) => {
                self.queued.insert(j);
                JobStatus::Queued
            }
            (Outcome::Ok, LostJobs::Fail) => JobStatus::Done {
                at: self.now,
                outcome: Outcome::Failed,
            },
            (e, _) => JobStatus::Done {
                at: self.now,
                outcome: e,
            },
        };
        Ok(())
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
        self.queued.iter().copied()
    }
    pub fn running(&self) -> impl Iterator<Item = JobId> + '_ {
        self.running.iter().copied()
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
                self.queued.insert(j);
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
        match &self.status[j.index()] {
            JobStatus::Queued => {}
            JobStatus::Pending => return Err(StateError::NotArrived(j)),
            JobStatus::Running {
                assignment,
                paused: Some(p),
                ..
            } if p.resume => return self.check_resume(model, j, assignment, &p.held, a, free),
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
        let Some(needs) = job.shape(a.shape) else {
            return Err(StateError::NoShape {
                job: j,
                shape: a.shape,
            });
        };
        if a.resources.len() != needs.len() {
            return Err(StateError::Arity {
                job: j,
                got: a.resources.len(),
                want: needs.len(),
            });
        }
        let mut take: BTreeMap<ResourceId, u64> = BTreeMap::new();
        let mut tags: BTreeMap<&str, NodeId> = BTreeMap::new();
        for (i, (need, r)) in needs.iter().zip(&a.resources).enumerate() {
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
        if self.may_start_anyway(model, j) {
            return Ok(take.into_iter().collect());
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

    /// Since when a queued job could start but for resources: the later of its earliest start and the end of
    /// every job it comes after (a job not done yet counts as now).
    pub fn ready_since(&self, model: &Model, j: JobId) -> Time {
        let job = model.job(j);
        job.after
            .iter()
            .map(|d| match &self.status[d.index()] {
                JobStatus::Done { at, .. } => *at,
                _ => self.now,
            })
            .fold(job.earliest_start(), Time::max)
    }

    /// When a queued job may start anyway, over capacity (see [`Job::start_anyway_after`]): `None` for a job
    /// that waits for room.
    ///
    /// [`Job::start_anyway_after`]: crate::model::Job::start_anyway_after
    pub fn start_anyway_at(&self, model: &Model, j: JobId) -> Option<Time> {
        let after = model.job(j).start_anyway_after?;
        Some(self.ready_since(model, j) + after)
    }

    /// Whether a queued job has waited past its [`start_anyway_at`](State::start_anyway_at): it may start
    /// over capacity on what it is pinned to.
    pub fn may_start_anyway(&self, model: &Model, j: JobId) -> bool {
        matches!(self.status[j.index()], JobStatus::Queued)
            && self
                .start_anyway_at(model, j)
                .is_some_and(|t| self.now >= t)
    }

    /// A resume takes back the needs it gave up, on the resources it ran on.
    fn check_resume(
        &self,
        model: &Model,
        j: JobId,
        had: &Assignment,
        held: &[bool],
        a: &Assignment,
        free: &[u64],
    ) -> Result<Vec<(ResourceId, u64)>, StateError> {
        if a != had {
            return Err(StateError::ResumeElsewhere(j));
        }
        let mut take: BTreeMap<ResourceId, u64> = BTreeMap::new();
        for ((need, r), h) in model.job(j).needs_for(a).iter().zip(&a.resources).zip(held) {
            if !h {
                *take.entry(*r).or_default() += need.amount;
            }
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
        let mut anyway = Vec::with_capacity(batch.len());
        for (j, a) in batch {
            let take = self.check_start(model, *j, a, &free)?;
            let mut over = false;
            for (r, amount) in &take {
                let f = &mut free[r.index()];
                over |= *amount > *f;
                *f = f.saturating_sub(*amount);
            }
            anyway.push(over);
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
        for (((j, a), take), over) in batch.iter().zip(takes).zip(anyway) {
            for (r, amount) in take {
                self.used[r.index()] += amount;
                *self.holders[r.index()].entry(*j).or_default() += amount;
            }
            if let JobStatus::Running { paused, .. } = &mut self.status[j.index()] {
                let held = paused.take().map(|p| p.held).unwrap_or_default();
                for ((need, r), h) in model
                    .job(*j)
                    .needs_for(a)
                    .iter()
                    .zip(&a.resources)
                    .zip(held)
                {
                    if !h {
                        self.apply_state(model, need, *r);
                    }
                }
                continue;
            }
            for (need, r) in model.job(*j).needs_for(a).iter().zip(&a.resources) {
                self.apply_state(model, need, *r);
            }
            self.queued.remove(j);
            self.running.insert(*j);
            self.status[j.index()] = JobStatus::Running {
                assignment: a.clone(),
                phase: Phase::Setup,
                started: self.now,
                phase_since: self.now,
                ending: Outcome::Ok,
                paused: None,
                over,
            };
        }
        Ok(())
    }

    /// Pauses a running job: it keeps its place, but gives back every need whose kind is not in its
    /// `pause_keeps`, and its phase does not move until it resumes. Not in cleanup (that must finish).
    pub fn pause(&mut self, model: &Model, j: JobId) -> Result<(), StateError> {
        let now = self.now;
        let JobStatus::Running {
            assignment,
            phase,
            paused,
            ..
        } = &mut self.status[j.index()]
        else {
            return Err(StateError::NotRunning(j));
        };
        if paused.is_some() {
            return Err(StateError::Paused(j));
        }
        if *phase == Phase::Cleanup {
            return Err(StateError::InCleanup(j));
        }
        let job = model.job(j);
        let needs = job.needs_for(assignment);
        let held: Vec<bool> = needs
            .iter()
            .map(|n| job.pause_keeps.contains(&n.kind))
            .collect();
        for ((need, r), h) in needs.iter().zip(&assignment.resources).zip(&held) {
            if !h {
                let ri = r.index();
                self.used[ri] -= need.amount;
                let left = self.holders[ri]
                    .get_mut(&j)
                    .expect("a running job holds its needs");
                *left -= need.amount;
                if *left == 0 {
                    self.holders[ri].remove(&j);
                }
            }
        }
        *paused = Some(Paused {
            since: now,
            held,
            resume: false,
        });
        Ok(())
    }

    /// A paused job asks to go on: it waits, as a resume, for the needs it gave back on the same resources;
    /// the planner starts it when they fit.
    pub fn resume(&mut self, j: JobId) -> Result<(), StateError> {
        match &mut self.status[j.index()] {
            JobStatus::Running {
                paused: Some(p), ..
            } => {
                p.resume = true;
                Ok(())
            }
            JobStatus::Running { .. } => Err(StateError::NotPaused(j)),
            _ => Err(StateError::NotRunning(j)),
        }
    }

    /// Whether a running job is paused.
    pub fn is_paused(&self, j: JobId) -> bool {
        matches!(
            &self.status[j.index()],
            JobStatus::Running {
                paused: Some(_),
                ..
            }
        )
    }

    /// The paused jobs that asked to resume, in id order.
    pub fn resuming(&self) -> impl Iterator<Item = JobId> + '_ {
        self.running.iter().copied().filter(|j| {
            matches!(
                &self.status[j.index()],
                JobStatus::Running {
                    paused: Some(Paused { resume: true, .. }),
                    ..
                }
            )
        })
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
        let JobStatus::Running {
            phase,
            ending,
            paused,
            ..
        } = &mut self.status[j.index()]
        else {
            return Err(StateError::NotRunning(j));
        };
        if paused.is_some() {
            return Err(StateError::Paused(j));
        }
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
                self.running.remove(&j);
                self.status[j.index()] = JobStatus::Done {
                    at: now,
                    outcome: ending,
                };
            }
        }
        Ok(&self.status[j.index()])
    }

    /// A running job ends now, in whatever phase it is, with `outcome` (its holder ran setup, run and cleanup
    /// itself and reports how it went): it releases what it holds. A job already ending failed or cancelled
    /// keeps that outcome. Only `Ok`, `Failed` and `Cancelled` end a run.
    pub fn finish(&mut self, model: &Model, j: JobId, outcome: Outcome) -> Result<(), StateError> {
        if !matches!(outcome, Outcome::Ok | Outcome::Failed | Outcome::Cancelled) {
            return Err(StateError::NotARunOutcome(outcome));
        }
        let JobStatus::Running { ending, .. } = self.status[j.index()] else {
            return Err(StateError::NotRunning(j));
        };
        let outcome = if ending == Outcome::Ok {
            outcome
        } else {
            ending
        };
        self.release(model, j);
        self.running.remove(&j);
        self.status[j.index()] = JobStatus::Done {
            at: self.now,
            outcome,
        };
        Ok(())
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
                self.queued.remove(&j);
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
                self.queued.remove(&j);
                self.status[j.index()] = JobStatus::Done {
                    at: self.now,
                    outcome: Outcome::Cached,
                };
                Ok(())
            }
            _ => Err(StateError::NotQueued(j)),
        }
    }

    /// A queued job is settled by a valid cached failure: it is done, failed, without running.
    pub fn mark_cached_fail(&mut self, j: JobId) -> Result<(), StateError> {
        match self.status[j.index()] {
            JobStatus::Queued => {
                self.queued.remove(&j);
                self.status[j.index()] = JobStatus::Done {
                    at: self.now,
                    outcome: Outcome::CachedFail,
                };
                Ok(())
            }
            _ => Err(StateError::NotQueued(j)),
        }
    }

    fn interrupt(&mut self, j: JobId, why: Outcome) -> Result<(), StateError> {
        let now = self.now;
        let JobStatus::Running {
            phase,
            ending,
            paused,
            ..
        } = &mut self.status[j.index()]
        else {
            return Err(StateError::NotRunning(j));
        };
        if *ending == Outcome::Ok {
            *ending = why;
        }
        // A paused job cleans up with what it kept; it no longer waits for the rest.
        if let Some(p) = paused {
            p.resume = false;
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
            .start(
                &m,
                &[(a, Assignment::new(vec![r])), (b, Assignment::new(vec![r]))],
            )
            .unwrap_err();
        assert!(matches!(err, StateError::OverCapacity { .. }));
        assert_eq!(s.used(r), 0, "nothing applied");
        s.start(&m, &[(a, Assignment::new(vec![r]))]).unwrap();
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
            s.start(&m, &[(c, Assignment::new(vec![r]))]),
            Err(StateError::DepNotDone { .. })
        ));
        s.start(&m, &[(a, Assignment::new(vec![r]))]).unwrap();
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
        s.start(&m, &[(c, Assignment::new(vec![r]))]).unwrap();
    }

    #[test]
    fn failure_goes_to_cleanup_and_blocks_dependents() {
        let (m, a, _, c, r) = model();
        let mut s = State::new(&m, Time::ZERO);
        s.arrive(a).unwrap();
        s.arrive(c).unwrap();
        s.start(&m, &[(a, Assignment::new(vec![r]))]).unwrap();
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
        assert!(s.start(&m, &[(c, Assignment::new(vec![r]))]).is_err());
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
            s.start(&m, &[(x, Assignment::new(vec![r]))]),
            Err(StateError::GroupSplit { .. })
        ));
        s.start(
            &m,
            &[(x, Assignment::new(vec![r])), (y, Assignment::new(vec![r]))],
        )
        .unwrap();
        assert_eq!(s.used(r), 2);
    }
}
