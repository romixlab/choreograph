//! Discrete-event simulation: a model, a planner and random durations, run on simulated time.
//!
//! [`Sim`] keeps a queue of future events (arrivals, phase ends, planner wake-ups) ordered by [`Time`]. Each
//! step takes the next instant, applies every event due then to the [`State`], asks the [`Planner`] what to
//! start, applies its starts and schedules their phase ends with durations drawn from each job's distributions.
//! Everything that happens goes into a [`Trace`]. There is no clock and no IO: a run is a pure function of the
//! model, the planner and the [`SimConfig`].
//!
//! # Randomness and common random numbers
//!
//! Every random draw comes from a ChaCha8 stream picked by the seed, a [`Purpose`] and the job it is for:
//! the duration of job 17's run phase, attempt 0, comes from the same stream whatever the planner does and in
//! whatever order jobs start. Two planners run on one seed therefore see the same world (the same arrivals,
//! durations, failures and flakes), which makes an A/B comparison of policies fair and low in noise.
//!
//! # Order within an instant
//!
//! Events due at the same time are applied phase ends first, then arrivals, then wake-ups, each kind in job id
//! order. After they are applied the planner is asked once; if its starts have phases of zero length, their
//! ends are applied at the same instant and the planner is asked again.
//!
//! # Failures and flakes
//!
//! A job's [`SimHints`](crate::model::SimHints) give the probability that its run phase fails (`p_fail`) or
//! flakes (`p_flake`, a failure that a retry would not repeat). A failed run goes to cleanup and the job ends
//! `Failed`. A flaky run is retried in place, holding its resources, up to [`SimConfig::flake_retries`] times;
//! a retry shows in the trace as a longer run phase and in [`SimStats`]. Jobs that come after a failed or
//! cancelled job are cancelled when [`SimConfig::cancel_blocked`] is set.
//!
//! # Example
//!
//! An M/M/1 queue: Poisson arrivals at 40 per hour on one slot, exponential service with a mean of one minute.
//!
//! ```
//! use choreograph::model::{Job, Labels, ModelBuilder, Unit};
//! use choreograph::sim::{Arrivals, Dist, PhaseDists, SimConfig, Template, Workload, simulate};
//! use choreograph::{Dur, FirstFit, Time};
//!
//! let mut b = ModelBuilder::new();
//! let slot = b.kind("slot", Unit::Count);
//! let node = b.node("pc", Labels::new());
//! b.pool("slot", slot, node, 1);
//! let base = b.build().unwrap();
//!
//! let workload = Workload {
//!     templates: vec![Template {
//!         job: Job::new("gate").needs(slot, 1),
//!         weight: 1,
//!         dists: PhaseDists::run(Dist::Exponential { mean: Dur::mins(1) }),
//!     }],
//!     arrivals: Arrivals::poisson(40.0, 500),
//!     start: Time::ZERO,
//! };
//! let generated = workload.build(&base, 7).unwrap();
//! let cfg = SimConfig { seed: 7, durations: generated.durations, ..SimConfig::default() };
//! let run = simulate(&generated.model, &mut FirstFit, &cfg).unwrap();
//! assert!(run.state.all_done());
//! // Same seed, same trace, bit for bit.
//! assert_eq!(simulate(&generated.model, &mut FirstFit, &cfg).unwrap().trace, run.trace);
//! ```

pub mod dist;
pub mod workload;

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use rand_chacha::ChaCha8Rng;
use rand_core::SeedableRng;
use serde::{Deserialize, Serialize};

pub use dist::{Dist, DistError};
pub use workload::{Arrivals, Generated, PhaseDists, Template, Workload, WorkloadError};

use crate::model::{JobId, Model, Phase};
use crate::plan::{Planner, WaitReason};
use crate::state::{JobStatus, State, StateError};
use crate::time::{Dur, Time};
use crate::trace::{Event, Trace};

/// What a random stream is used for. Each purpose has its own streams, so drawing more of one kind of number
/// (a planner that retries more, a workload with more arrivals) does not shift the others.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Purpose {
    /// Gaps between arrivals of a generated workload.
    Arrivals = 1,
    /// Which template each generated arrival uses.
    Mix = 2,
    /// Phase durations, one stream per job, phase and attempt.
    Durations = 3,
    /// Whether a run fails or flakes, and when, one stream per job and attempt.
    Failures = 4,
}

/// The random stream for a purpose, a job and a sub-index (phase and attempt), from a seed.
///
/// Streams are ChaCha8 keyed by the seed (through [`SeedableRng::seed_from_u64`]) with the stream number
/// `purpose << 56 | sub << 32 | job`; `sub` keeps its low 24 bits. The numbers are the same on every machine.
pub fn stream(seed: u64, purpose: Purpose, job: u32, sub: u32) -> ChaCha8Rng {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    rng.set_stream(((purpose as u64) << 56) | (((sub & 0xFF_FFFF) as u64) << 32) | job as u64);
    rng
}

/// Where in a failing run attempt the failure shows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailPoint {
    /// At the end of the attempt's sampled duration (a test suite that reports at the end).
    #[default]
    End,
    /// At a uniformly drawn point of the attempt (a failure can stop the run early).
    Uniform,
}

/// Settings of a simulation run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SimConfig {
    /// The seed of every random stream.
    pub seed: u64,
    /// The simulated time the run starts at; jobs that arrive earlier arrive then.
    pub start: Time,
    /// Stop at this time even if events remain (`None`: run until no event is left).
    pub until: Option<Time>,
    /// Phase duration distributions per job; phases without one use the job's estimate
    /// ([`Dist::from_estimate`]: fixed when p50 = p90, else lognormal).
    pub durations: BTreeMap<JobId, PhaseDists>,
    /// How many times a flaky run is retried in place before the job fails.
    pub flake_retries: u32,
    /// Where a failing attempt stops.
    pub fail_point: FailPoint,
    /// Cancel queued jobs whose dependency failed or was cancelled, so the run can finish.
    pub cancel_blocked: bool,
    /// Record a `Waiting` event whenever a queued job's wait reason changes.
    pub record_waits: bool,
    /// Give up after this many steps (events applied plus planner calls): a guard against a planner that never
    /// settles.
    pub max_steps: u64,
    /// Free text appended to the trace header's source (the policy's name, a scenario).
    pub label: String,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            seed: 0,
            start: Time::ZERO,
            until: None,
            durations: BTreeMap::new(),
            flake_retries: 0,
            fail_point: FailPoint::End,
            cancel_blocked: true,
            record_waits: true,
            max_steps: 100_000_000,
            label: String::new(),
        }
    }
}

/// Counts from a run.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimStats {
    /// Events applied.
    pub events: u64,
    /// Times the planner was asked.
    pub plans: u64,
    /// Jobs whose run failed (a real failure, or a flake with no retries left).
    pub failed: u64,
    /// Run attempts that flaked.
    pub flakes: u64,
    /// Run attempts repeated after a flake.
    pub retries: u64,
    /// Queued jobs cancelled because a dependency failed.
    pub cancelled: u64,
    /// The run stopped at [`SimConfig::until`] with events left.
    pub stopped_at_until: bool,
    /// Jobs not done at the end (the planner left them waiting with nothing left to happen, or `until`).
    pub unfinished: Vec<JobId>,
}

/// A finished run.
#[derive(Clone, Debug, PartialEq)]
pub struct SimRun {
    pub trace: Trace,
    /// The state at the end.
    pub state: State,
    pub stats: SimStats,
}

/// Why a run stopped with an error.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SimError {
    #[error("at {at}: the planner's starts were refused: {error}")]
    Refused { at: Time, error: StateError },
    #[error("at {at}: {error}")]
    State { at: Time, error: StateError },
    #[error("job {job:?}: {error}")]
    Dist { job: JobId, error: DistError },
    #[error("durations are given for job {0:?}, which is not in the model")]
    UnknownJob(JobId),
    #[error("at {at}: more than {steps} steps; does the planner never settle?")]
    TooManySteps { at: Time, steps: u64 },
}

/// What happens at an instant. The order of the variants is the order within an instant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Ev {
    /// The current phase of the job ends; `fail` for a run phase that fails.
    PhaseEnd {
        fail: bool,
    },
    Arrive,
    Wake,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Item {
    t: Time,
    rank: u8,
    job: JobId,
    seq: u64,
    ev: Ev,
}

/// A simulation in progress; [`simulate`] runs one to the end.
pub struct Sim<'a, P: Planner + ?Sized> {
    model: &'a Model,
    planner: &'a mut P,
    cfg: &'a SimConfig,
    state: State,
    trace: Trace,
    queue: BinaryHeap<Reverse<Item>>,
    seq: u64,
    waits: BTreeMap<JobId, WaitReason>,
    wakes: BTreeSet<Time>,
    stats: SimStats,
    /// Some job ended without satisfying its dependents (for `cancel_blocked`).
    any_failed: bool,
}

impl<'a, P: Planner + ?Sized> Sim<'a, P> {
    /// Sets up a run: every job is scheduled to arrive, nothing has happened yet.
    pub fn new(model: &'a Model, planner: &'a mut P, cfg: &'a SimConfig) -> Result<Self, SimError> {
        for (j, d) in &cfg.durations {
            if j.index() >= model.jobs().len() {
                return Err(SimError::UnknownJob(*j));
            }
            d.check()
                .map_err(|error| SimError::Dist { job: *j, error })?;
        }
        let mut sim = Sim {
            model,
            planner,
            cfg,
            state: State::new(model, cfg.start),
            trace: Trace::new(format!(
                "choreograph-sim {} seed={}{}{}",
                env!("CARGO_PKG_VERSION"),
                cfg.seed,
                if cfg.label.is_empty() { "" } else { " " },
                cfg.label
            )),
            queue: BinaryHeap::new(),
            seq: 0,
            waits: BTreeMap::new(),
            wakes: BTreeSet::new(),
            stats: SimStats::default(),
            any_failed: false,
        };
        for j in model.job_ids() {
            let at = model.job(j).arrival.max(cfg.start);
            sim.push(at, j, Ev::Arrive);
        }
        Ok(sim)
    }

    /// The state now.
    pub fn state(&self) -> &State {
        &self.state
    }

    /// The trace so far.
    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    /// The time of the next event, if any.
    pub fn next_time(&self) -> Option<Time> {
        self.queue.peek().map(|Reverse(i)| i.t)
    }

    /// Runs the next instant: applies every event due then and the planner's starts. Returns `false` when the
    /// run is over (no events left, or the next one is past `until`).
    pub fn step(&mut self) -> Result<bool, SimError> {
        let Some(t) = self.next_time() else {
            return Ok(false);
        };
        if let Some(until) = self.cfg.until
            && t > until
        {
            self.state
                .set_now(until)
                .map_err(|error| SimError::State { at: until, error })?;
            self.stats.stopped_at_until = true;
            self.queue.clear();
            return Ok(false);
        }
        self.state
            .set_now(t)
            .map_err(|error| SimError::State { at: t, error })?;
        loop {
            let mut arrived = false;
            let mut failed_now = false;
            while let Some(Reverse(item)) = self.queue.peek().copied()
                && item.t == t
            {
                self.queue.pop();
                self.count_step(t)?;
                self.stats.events += 1;
                match item.ev {
                    Ev::Arrive => {
                        self.state
                            .arrive(item.job)
                            .map_err(|error| SimError::State { at: t, error })?;
                        self.trace.push(t, Event::Arrived { job: item.job });
                        arrived = true;
                    }
                    Ev::PhaseEnd { fail } => failed_now |= self.phase_end(t, item.job, fail)?,
                    Ev::Wake => {
                        self.wakes.remove(&t);
                    }
                }
            }
            self.any_failed |= failed_now;
            if self.cfg.cancel_blocked && (failed_now || (arrived && self.any_failed)) {
                self.cancel_blocked(t)?;
            }
            self.count_step(t)?;
            self.stats.plans += 1;
            let plan = self.planner.plan(self.model, &self.state);
            if !plan.starts.is_empty() {
                self.state
                    .start(self.model, &plan.batch())
                    .map_err(|error| SimError::Refused { at: t, error })?;
                for s in &plan.starts {
                    self.waits.remove(&s.job);
                    self.trace.push(
                        t,
                        Event::Started {
                            job: s.job,
                            assignment: s.assignment.clone(),
                        },
                    );
                    let d = self.duration(s.job, Phase::Setup, 0);
                    self.push(t + d, s.job, Ev::PhaseEnd { fail: false });
                }
            }
            if self.cfg.record_waits {
                for (j, why) in &plan.waits {
                    if self.waits.get(j) != Some(why) {
                        self.waits.insert(*j, why.clone());
                        self.trace.push(
                            t,
                            Event::Waiting {
                                job: *j,
                                reason: why.clone(),
                            },
                        );
                    }
                }
            }
            if let Some(w) = plan.wake_at
                && w > t
                && self.wakes.insert(w)
            {
                self.push(w, JobId(u32::MAX), Ev::Wake);
            }
            if self.next_time() != Some(t) {
                return Ok(true);
            }
        }
    }

    /// Runs to the end and returns the result.
    pub fn run(mut self) -> Result<SimRun, SimError> {
        while self.step()? {}
        Ok(self.finish())
    }

    /// Ends the run where it is.
    pub fn finish(mut self) -> SimRun {
        self.stats.unfinished = self
            .model
            .job_ids()
            .filter(|j| !matches!(self.state.status(*j), JobStatus::Done { .. }))
            .collect();
        SimRun {
            trace: self.trace,
            state: self.state,
            stats: self.stats,
        }
    }

    fn push(&mut self, t: Time, job: JobId, ev: Ev) {
        let rank = match ev {
            Ev::PhaseEnd { .. } => 0,
            Ev::Arrive => 1,
            Ev::Wake => 2,
        };
        self.seq += 1;
        self.queue.push(Reverse(Item {
            t,
            rank,
            job,
            seq: self.seq,
            ev,
        }));
    }

    fn count_step(&self, t: Time) -> Result<(), SimError> {
        let steps = self.stats.events + self.stats.plans;
        if steps >= self.cfg.max_steps {
            return Err(SimError::TooManySteps { at: t, steps });
        }
        Ok(())
    }

    /// The end of a job's current phase. Returns whether the job finished without satisfying its dependents.
    fn phase_end(&mut self, t: Time, j: JobId, fail: bool) -> Result<bool, SimError> {
        let err = |error| SimError::State { at: t, error };
        let JobStatus::Running { phase, .. } = self.state.status(j) else {
            return Err(err(StateError::NotRunning(j)));
        };
        match phase {
            Phase::Setup => {
                self.state.advance(self.model, j).map_err(err)?;
                self.trace.push(
                    t,
                    Event::Phase {
                        job: j,
                        phase: Phase::Run,
                    },
                );
                let (d, fail) = self.run_attempts(j);
                self.push(t + d, j, Ev::PhaseEnd { fail });
            }
            Phase::Run => {
                if fail {
                    self.state.fail(j).map_err(err)?;
                    self.trace.push(
                        t,
                        Event::Failed {
                            job: j,
                            phase: Phase::Run,
                        },
                    );
                } else {
                    self.state.advance(self.model, j).map_err(err)?;
                }
                self.trace.push(
                    t,
                    Event::Phase {
                        job: j,
                        phase: Phase::Cleanup,
                    },
                );
                let d = self.duration(j, Phase::Cleanup, 0);
                self.push(t + d, j, Ev::PhaseEnd { fail: false });
            }
            Phase::Cleanup => {
                let outcome = match self.state.advance(self.model, j).map_err(err)? {
                    JobStatus::Done { outcome, .. } => *outcome,
                    other => unreachable!("cleanup ended in {other:?}"),
                };
                self.trace.push(t, Event::Finished { job: j, outcome });
                return Ok(!outcome.satisfies_deps());
            }
        }
        Ok(false)
    }

    /// The run phase of job `j` with its flaky retries: the total time and whether it ends in a failure.
    fn run_attempts(&mut self, j: JobId) -> (Dur, bool) {
        let hints = self.model.job(j).sim;
        let mut total = Dur::ZERO;
        for attempt in 0..=self.cfg.flake_retries {
            let d = self.duration(j, Phase::Run, attempt);
            let mut rng = stream(self.cfg.seed, Purpose::Failures, j.0, attempt);
            let fails = dist::unit(&mut rng) < hints.p_fail;
            let flakes = dist::unit(&mut rng) < hints.p_flake;
            let at = dist::unit(&mut rng);
            if !fails && !flakes {
                return (total + d, false);
            }
            total += match self.cfg.fail_point {
                FailPoint::End => d,
                FailPoint::Uniform => Dur(libm::round(d.0 as f64 * at) as i64),
            };
            if fails {
                self.stats.failed += 1;
                return (total, true);
            }
            self.stats.flakes += 1;
            if attempt == self.cfg.flake_retries {
                self.stats.failed += 1;
                return (total, true);
            }
            self.stats.retries += 1;
        }
        unreachable!("the last attempt returns")
    }

    /// A phase duration of job `j`, from its own stream.
    fn duration(&self, j: JobId, phase: Phase, attempt: u32) -> Dur {
        let given = self.cfg.durations.get(&j).and_then(|d| match phase {
            Phase::Setup => d.setup.as_ref(),
            Phase::Run => d.run.as_ref(),
            Phase::Cleanup => d.cleanup.as_ref(),
        });
        let mut rng = stream(
            self.cfg.seed,
            Purpose::Durations,
            j.0,
            ((phase as u32) << 20) | (attempt & 0xF_FFFF),
        );
        match given {
            Some(d) => d.sample(&mut rng),
            None => Dist::from_estimate(self.model.job(j).phases.get(phase)).sample(&mut rng),
        }
    }

    /// Cancels queued jobs that wait on a failed or cancelled job, until none is left.
    fn cancel_blocked(&mut self, t: Time) -> Result<(), SimError> {
        loop {
            let blocked: Vec<JobId> = self
                .state
                .queued()
                .filter(|j| {
                    self.model
                        .job(*j)
                        .after
                        .iter()
                        .any(|d| self.state.outcome(*d).is_some_and(|o| !o.satisfies_deps()))
                })
                .collect();
            if blocked.is_empty() {
                return Ok(());
            }
            for j in blocked {
                self.state
                    .cancel(j)
                    .map_err(|error| SimError::State { at: t, error })?;
                self.waits.remove(&j);
                self.stats.cancelled += 1;
                self.trace.push(t, Event::Cancelled { job: j });
            }
        }
    }
}

/// Runs a simulation to the end.
pub fn simulate<P: Planner + ?Sized>(
    model: &Model,
    planner: &mut P,
    cfg: &SimConfig,
) -> Result<SimRun, SimError> {
    Sim::new(model, planner, cfg)?.run()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Estimate, Job, KindId, Labels, ModelBuilder, Unit};
    use crate::plan::{FirstFit, Plan, Start, find_assignment};
    use crate::state::{Assignment, Outcome};

    fn slots(cap: u64) -> (ModelBuilder, KindId) {
        let mut b = ModelBuilder::new();
        let k = b.kind("slot", Unit::Count);
        let n = b.node("n", Labels::new());
        b.pool("slot", k, n, cap);
        (b, k)
    }

    fn job(name: &str, k: KindId, at: i64, setup: i64, run: i64, cleanup: i64) -> Job {
        let mut j = Job::new(name).needs(k, 1).arrival(Time(at));
        j.phases.setup = Estimate::fixed(Dur(setup));
        j.phases.run = Estimate::fixed(Dur(run));
        j.phases.cleanup = Estimate::fixed(Dur(cleanup));
        j
    }

    /// Times of one kind of event per job.
    fn times(tr: &Trace, f: impl Fn(&Event) -> Option<JobId>) -> BTreeMap<JobId, Time> {
        tr.records
            .iter()
            .filter_map(|r| f(&r.event).map(|j| (j, r.t)))
            .collect()
    }

    fn started(e: &Event) -> Option<JobId> {
        match e {
            Event::Started { job, .. } => Some(*job),
            _ => None,
        }
    }

    fn finished(e: &Event) -> Option<JobId> {
        match e {
            Event::Finished { job, .. } => Some(*job),
            _ => None,
        }
    }

    /// Last come, first served: a second policy for comparisons.
    struct Lifo;
    impl Planner for Lifo {
        fn plan(&mut self, model: &Model, state: &State) -> Plan {
            let mut plan = Plan::default();
            let mut free = state.free_all(model);
            let mut order: Vec<JobId> = state.queued().collect();
            order.sort_by_key(|j| Reverse((model.job(*j).arrival, *j)));
            for j in order {
                if let Some(a) = find_assignment(model, j, &free, &|_, _| 0) {
                    for (need, r) in model.job(j).needs.iter().zip(&a.0) {
                        free[r.index()] -= need.amount;
                    }
                    plan.starts.push(Start {
                        job: j,
                        assignment: a,
                    });
                }
            }
            plan
        }
    }

    #[test]
    fn fixed_durations_give_exact_times() {
        let (mut b, k) = slots(1);
        let a = b.job(job("a", k, 0, 10, 100, 5));
        let c = b.job(job("b", k, 3, 0, 50, 0));
        let m = b.build().unwrap();
        let run = simulate(&m, &mut FirstFit, &SimConfig::default()).unwrap();
        let s = times(&run.trace, started);
        let f = times(&run.trace, finished);
        assert_eq!((s[&a], f[&a]), (Time(0), Time(115)));
        assert_eq!(
            (s[&c], f[&c]),
            (Time(115), Time(165)),
            "b waits for the slot"
        );
        assert!(run.state.all_done());
        assert!(run.stats.unfinished.is_empty());
        // b's wait reason names the holder.
        assert!(run.trace.records.iter().any(|r| matches!(
            &r.event,
            Event::Waiting { job, reason: WaitReason::Resource { held_by, .. } }
                if *job == c && held_by == &vec![a]
        )));
        // The trace of a job reads arrived, started, run, cleanup, finished.
        let kinds: Vec<&str> = run
            .trace
            .records
            .iter()
            .filter(|r| match &r.event {
                Event::Arrived { job }
                | Event::Started { job, .. }
                | Event::Phase { job, .. }
                | Event::Finished { job, .. } => *job == a,
                _ => false,
            })
            .map(|r| match &r.event {
                Event::Arrived { .. } => "arrived",
                Event::Started { .. } => "started",
                Event::Phase {
                    phase: Phase::Run, ..
                } => "run",
                Event::Phase { .. } => "cleanup",
                _ => "finished",
            })
            .collect();
        assert_eq!(kinds, ["arrived", "started", "run", "cleanup", "finished"]);
    }

    fn random_model(n: usize) -> Model {
        let (mut b, k) = slots(2);
        for i in 0..n {
            let mut j = Job::new(format!("j{i}"))
                .needs(k, 1)
                .arrival(Time(i as i64 * 7_000))
                .run(Estimate {
                    p50: Dur::secs(10),
                    p90: Dur::secs(40),
                });
            j.sim.p_fail = 0.1;
            j.sim.p_flake = 0.2;
            b.job(j);
        }
        b.build().unwrap()
    }

    #[test]
    fn same_seed_same_trace() {
        let m = random_model(200);
        let cfg = SimConfig {
            seed: 11,
            flake_retries: 1,
            ..SimConfig::default()
        };
        let a = simulate(&m, &mut FirstFit, &cfg).unwrap();
        let b = simulate(&m, &mut FirstFit, &cfg).unwrap();
        assert_eq!(a, b);
        let c = simulate(
            &m,
            &mut FirstFit,
            &SimConfig {
                seed: 12,
                ..cfg.clone()
            },
        )
        .unwrap();
        assert_ne!(a.trace.records, c.trace.records);
        assert!(a.stats.failed > 0 && a.stats.flakes > 0 && a.stats.retries > 0);
    }

    /// Two policies on one seed see the same world: each job's run phase lasts as long under both.
    #[test]
    fn policies_share_random_numbers() {
        let m = random_model(300);
        let cfg = SimConfig {
            seed: 5,
            flake_retries: 2,
            ..SimConfig::default()
        };
        let run_lengths = |tr: &Trace| -> BTreeMap<JobId, Dur> {
            let mut since = BTreeMap::new();
            let mut out = BTreeMap::new();
            for r in &tr.records {
                if let Event::Phase { job, phase } = r.event {
                    match phase {
                        Phase::Run => {
                            since.insert(job, r.t);
                        }
                        _ => {
                            out.insert(job, r.t - since[&job]);
                        }
                    }
                }
            }
            out
        };
        let a = simulate(&m, &mut FirstFit, &cfg).unwrap();
        let b = simulate(&m, &mut Lifo, &cfg).unwrap();
        assert_ne!(
            times(&a.trace, started),
            times(&b.trace, started),
            "the policies do differ"
        );
        assert_eq!(run_lengths(&a.trace), run_lengths(&b.trace));
        assert_eq!(a.stats.failed, b.stats.failed);
        assert_eq!(a.stats.flakes, b.stats.flakes);
    }

    #[test]
    fn failures_cancel_dependents() {
        let (mut b, k) = slots(1);
        let mut bad = job("bad", k, 0, 0, 10, 2);
        bad.sim.p_fail = 1.0;
        let bad = b.job(bad);
        let next = b.job(job("next", k, 0, 0, 10, 0).after(bad));
        let last = b.job(job("last", k, 50, 0, 10, 0).after(next));
        let m = b.build().unwrap();
        let run = simulate(&m, &mut FirstFit, &SimConfig::default()).unwrap();
        assert_eq!(run.state.outcome(bad), Some(Outcome::Failed));
        assert_eq!(run.state.outcome(next), Some(Outcome::Cancelled));
        assert_eq!(
            run.state.outcome(last),
            Some(Outcome::Cancelled),
            "arrives later"
        );
        assert_eq!(run.stats.cancelled, 2);
        assert_eq!(times(&run.trace, finished)[&bad], Time(12), "cleanup ran");
        assert!(run.trace.records.iter().any(|r| r.event
            == Event::Failed {
                job: bad,
                phase: Phase::Run
            }));

        let kept = simulate(
            &m,
            &mut FirstFit,
            &SimConfig {
                cancel_blocked: false,
                ..SimConfig::default()
            },
        )
        .unwrap();
        assert_eq!(kept.stats.unfinished, vec![next, last]);
    }

    #[test]
    fn flakes_retry_in_place() {
        let (mut b, k) = slots(1);
        let mut j = job("flaky", k, 0, 0, 10, 0);
        j.sim.p_flake = 1.0;
        let j = b.job(j);
        let m = b.build().unwrap();
        let cfg = SimConfig {
            flake_retries: 2,
            ..SimConfig::default()
        };
        let run = simulate(&m, &mut FirstFit, &cfg).unwrap();
        assert_eq!(run.state.outcome(j), Some(Outcome::Failed));
        assert_eq!(
            (run.stats.flakes, run.stats.retries, run.stats.failed),
            (3, 2, 1)
        );
        assert_eq!(times(&run.trace, finished)[&j], Time(30), "three attempts");

        // Uniform fail points stop attempts early.
        let early = simulate(
            &m,
            &mut FirstFit,
            &SimConfig {
                fail_point: FailPoint::Uniform,
                ..cfg
            },
        )
        .unwrap();
        assert!(times(&early.trace, finished)[&j] < Time(30));
    }

    #[test]
    fn release_times_wake_the_planner() {
        let (mut b, k) = slots(1);
        let mut j = job("later", k, 0, 0, 10, 0);
        j.release = Some(Time(500));
        let j = b.job(j);
        let m = b.build().unwrap();
        let run = simulate(&m, &mut FirstFit, &SimConfig::default()).unwrap();
        assert_eq!(times(&run.trace, started)[&j], Time(500));
    }

    #[test]
    fn until_stops_the_run() {
        let (mut b, k) = slots(1);
        for i in 0..10 {
            b.job(job(&format!("j{i}"), k, 0, 0, 100, 0));
        }
        let m = b.build().unwrap();
        let run = simulate(
            &m,
            &mut FirstFit,
            &SimConfig {
                until: Some(Time(250)),
                ..SimConfig::default()
            },
        )
        .unwrap();
        assert!(run.stats.stopped_at_until);
        assert_eq!(run.state.now(), Time(250));
        assert_eq!(run.stats.unfinished.len(), 8);
    }

    #[test]
    fn zero_length_phases_finish_in_one_instant() {
        let (mut b, k) = slots(1);
        for i in 0..3 {
            b.job(job(&format!("j{i}"), k, 0, 0, 0, 0));
        }
        let m = b.build().unwrap();
        let run = simulate(&m, &mut FirstFit, &SimConfig::default()).unwrap();
        assert!(run.state.all_done());
        assert_eq!(run.state.now(), Time(0));
    }

    /// A planner that starts every queued job on the first resource, fitting or not.
    struct Greedy;
    impl Planner for Greedy {
        fn plan(&mut self, _: &Model, state: &State) -> Plan {
            Plan {
                starts: state
                    .queued()
                    .map(|job| Start {
                        job,
                        assignment: Assignment(vec![crate::model::ResourceId(0)]),
                    })
                    .collect(),
                ..Plan::default()
            }
        }
    }

    #[test]
    fn bad_starts_are_errors() {
        let (mut b, k) = slots(1);
        b.job(job("a", k, 0, 0, 10, 0));
        b.job(job("b", k, 0, 0, 10, 0));
        let m = b.build().unwrap();
        let err = simulate(&m, &mut Greedy, &SimConfig::default()).unwrap_err();
        assert!(matches!(
            err,
            SimError::Refused {
                error: StateError::OverCapacity { .. },
                ..
            }
        ));
        let cfg = SimConfig {
            durations: [(JobId(9), PhaseDists::default())].into(),
            ..SimConfig::default()
        };
        assert!(matches!(
            Sim::new(&m, &mut FirstFit, &cfg),
            Err(SimError::UnknownJob(_))
        ));
        let cfg = SimConfig {
            max_steps: 3,
            ..SimConfig::default()
        };
        assert!(matches!(
            simulate(&m, &mut FirstFit, &cfg),
            Err(SimError::TooManySteps { .. })
        ));
    }

    #[test]
    fn given_distributions_override_estimates() {
        let (mut b, k) = slots(1);
        let j = b.job(job("j", k, 0, 0, 10, 0));
        let m = b.build().unwrap();
        let cfg = SimConfig {
            durations: [(
                j,
                PhaseDists::run(Dist::Fixed {
                    value: Dur::secs(3),
                }),
            )]
            .into(),
            ..SimConfig::default()
        };
        let run = simulate(&m, &mut FirstFit, &cfg).unwrap();
        assert_eq!(times(&run.trace, finished)[&j], Time(3000));
    }
}
