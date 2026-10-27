//! Properties that hold for every job set, every seed and every planner: generated cases, checked from the
//! trace by an independent replay.
//!
//! - **Capacity:** no start takes a resource over its capacity (the declared one, or the measured one after a
//!   `Capacity` event), and nothing starts on a node that is down.
//! - **Cleanup:** every started job reaches its cleanup and then finishes, releasing all it holds; only a job
//!   on a lost node leaves without it (finished failed or requeued at the instant the node went).
//! - **Progress:** while work remains, something happens: every job finishes, none is left waiting forever.
//! - **Little's law:** the area under the number in system equals the sum of the sojourn times, and the same for
//!   the queue, exactly in integer milliseconds.
//! - **Determinism:** the same inputs give the same trace.
//! - **Aging:** on one exclusive slot, a job waits at most what the jobs that arrived before it plus the aging
//!   time to overtake every class run for, plus one job already running.
//!
//! The batch simulator and the live model (runtime submits, capacity measurements, nodes down and lost,
//! compaction) are both checked, each with `FirstFit` and the list scheduler. Every property runs a fixed
//! number of cases from a fixed seed, so a run is the same every time; a failing case is shrunk to a small one
//! (`shrinks_to_a_small_case` shows it on a planner broken on purpose).

use std::collections::{BTreeMap, BTreeSet};

use choreograph::live::Live;
use choreograph::metrics::{MetricsConfig, from_trace};
use choreograph::model::{Estimate, Job, JobId, KindId, Labels, Model, ModelBuilder, NodeId};
use choreograph::model::{Phase, Phases, ResourceId, Unit};
use choreograph::{
    Backfill, Dur, Event, FirstFit, JobStatus, ListScheduler, LostJobs, Plan, Planner, Policy,
    Record, SimConfig, Time, simulate,
};
use proptest::prelude::*;
use proptest::sample::Index;
use proptest::test_runner::{RngSeed, TestCaseError, TestError, TestRunner};

/// A fixed seed and no failure files: the same cases on every run.
fn config(cases: u32) -> ProptestConfig {
    ProptestConfig {
        cases,
        rng_seed: RngSeed::Fixed(0x0c40_5eed),
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

/// Capacities per node and kind (0: the node has none of that kind). The first node has some of every kind,
/// so every need fits somewhere.
#[derive(Clone, Debug)]
struct Fleet {
    caps: Vec<Vec<u64>>,
}

impl Fleet {
    fn kinds(&self) -> usize {
        self.caps[0].len()
    }

    /// The largest capacity of each kind.
    fn max_cap(&self) -> Vec<u64> {
        (0..self.kinds())
            .map(|k| self.caps.iter().map(|c| c[k]).max().unwrap())
            .collect()
    }
}

fn fleet() -> impl Strategy<Value = Fleet> {
    (1..=3usize, 1..=3usize).prop_flat_map(|(nodes, kinds)| {
        (
            prop::collection::vec(1..=4u64, kinds),
            prop::collection::vec(prop::collection::vec(0..=4u64, kinds), nodes - 1),
        )
            .prop_map(|(first, rest)| Fleet {
                caps: std::iter::once(first).chain(rest).collect(),
            })
    })
}

/// One job: needs as (kind, amount), both cut to the fleet when built; durations in ms.
#[derive(Clone, Debug)]
struct JobSpec {
    needs: Vec<(usize, u64)>,
    setup: i64,
    run: i64,
    /// p90 − p50 of the run phase (the simulator samples a lognormal when it is not 0).
    spread: i64,
    cleanup: i64,
    class: i32,
    arrival: i64,
    /// Comes after an earlier job (picked among those before it).
    after: Option<Index>,
    /// The run phase fails.
    fails: bool,
}

fn job_spec() -> impl Strategy<Value = JobSpec> {
    (
        prop::collection::vec((0..3usize, 1..=4u64), 1..=2),
        (0..=1_000i64, 1..=5_000i64, 0..=3_000i64, 0..=1_000i64),
        -1..=2i32,
        0..=20_000i64,
        prop::option::weighted(0.3, any::<Index>()),
        prop::bool::weighted(0.15),
    )
        .prop_map(
            |(needs, (setup, run, spread, cleanup), class, arrival, after, fails)| JobSpec {
                needs,
                setup,
                run,
                spread,
                cleanup,
                class,
                arrival,
                after,
                fails,
            },
        )
}

impl JobSpec {
    /// The job, with each kind needed once and every amount cut to what the largest resource of its kind has.
    fn job(&self, name: String, kinds: &[KindId], max_cap: &[u64], arrival: Time) -> Job {
        let mut j = Job::new(name).class(self.class).arrival(arrival);
        let mut seen = BTreeSet::new();
        for &(k, amount) in &self.needs {
            let k = k % kinds.len();
            if seen.insert(k) {
                j = j.needs(kinds[k], amount.min(max_cap[k]));
            }
        }
        j.phases = Phases {
            setup: Estimate::fixed(Dur(self.setup)),
            run: Estimate {
                p50: Dur(self.run),
                p90: Dur(self.run + self.spread),
            },
            cleanup: Estimate::fixed(Dur(self.cleanup)),
        };
        if self.fails {
            j.sim.p_fail = 1.0;
        }
        j
    }

    fn duration(&self, phase: Phase) -> Dur {
        Dur(match phase {
            Phase::Setup => self.setup,
            Phase::Run => self.run,
            Phase::Cleanup => self.cleanup,
        })
    }
}

/// The fleet's nodes, kinds and resources (one pool per node and kind it has), without jobs.
fn fleet_builder(fleet: &Fleet) -> (ModelBuilder, Vec<KindId>) {
    let mut b = ModelBuilder::new();
    let kinds: Vec<KindId> = (0..fleet.kinds())
        .map(|k| b.kind(format!("k{k}"), Unit::Count))
        .collect();
    for (n, caps) in fleet.caps.iter().enumerate() {
        let node = b.node(format!("n{n}"), Labels::new());
        for (k, &c) in caps.iter().enumerate() {
            if c > 0 {
                b.pool(format!("n{n}/k{k}"), kinds[k], node, c);
            }
        }
    }
    (b, kinds)
}

/// Which planner a case runs.
#[derive(Clone, Copy, Debug)]
enum Which {
    FirstFit,
    List(Backfill),
}

fn which() -> impl Strategy<Value = Which> {
    prop_oneof![
        Just(Which::FirstFit),
        Just(Which::List(Backfill::Easy)),
        Just(Which::List(Backfill::Conservative)),
        Just(Which::List(Backfill::Off)),
    ]
}

impl Which {
    fn planner(self) -> Box<dyn Planner> {
        match self {
            Which::FirstFit => Box::new(FirstFit),
            Which::List(backfill) => Box::new(ListScheduler::new(Policy {
                backfill,
                ..Policy::default()
            })),
        }
    }
}

fn fail(msg: String) -> TestCaseError {
    TestCaseError::fail(msg)
}

/// Replays a trace on its own bookkeeping, independent of `State`, and checks the capacity and cleanup
/// properties at every event. Follows compactions (the remaining jobs renumbered in order) by keeping a serial
/// number per job.
#[derive(Default)]
struct Replay {
    /// The serial number of each current job id.
    serials: Vec<usize>,
    /// The amount of each need, per serial.
    needs: Vec<Vec<u64>>,
    names: Vec<String>,
    measured: BTreeMap<ResourceId, Option<u64>>,
    down: BTreeSet<NodeId>,
    /// The node that went down last and when.
    lost: Option<(Time, NodeId)>,
    used: BTreeMap<ResourceId, u64>,
    /// Running jobs by serial: what they hold and whether they reached cleanup.
    running: BTreeMap<usize, (Vec<(ResourceId, u64)>, bool)>,
    starts: u64,
}

impl Replay {
    /// A replay that knows the model's jobs from the start (a batch run, ids that never change).
    fn of(model: &Model) -> Replay {
        let mut r = Replay::default();
        for j in model.job_ids() {
            r.register(model, j);
        }
        r
    }

    fn register(&mut self, model: &Model, j: JobId) {
        self.serials.push(self.needs.len());
        let job = model.job(j);
        self.needs
            .push(job.needs.iter().map(|n| n.amount).collect());
        self.names.push(job.name.clone());
    }

    fn serial(&self, j: JobId) -> Result<usize, TestCaseError> {
        self.serials
            .get(j.index())
            .copied()
            .ok_or_else(|| fail(format!("{j:?} is not a known job")))
    }

    fn capacity(&self, model: &Model, r: ResourceId) -> u64 {
        self.measured
            .get(&r)
            .copied()
            .flatten()
            .unwrap_or(model.resource(r).capacity)
    }

    /// Releases what a job held; `cleaned` false is only allowed for a job on the node lost at this instant.
    fn release(
        &mut self,
        model: &Model,
        t: Time,
        s: usize,
        how: &str,
    ) -> Result<(), TestCaseError> {
        let (held, cleaned) = self.running.remove(&s).unwrap();
        if !cleaned {
            let on_lost = self.lost.is_some_and(|(at, n)| {
                at == t && held.iter().any(|(r, _)| model.resource(*r).node == n)
            });
            if !on_lost {
                return Err(fail(format!(
                    "at {t:?}: {} {how} without cleanup",
                    self.names[s]
                )));
            }
        }
        for (r, a) in held {
            *self.used.get_mut(&r).unwrap() -= a;
        }
        Ok(())
    }

    fn feed(&mut self, model: &Model, rec: &Record) -> Result<(), TestCaseError> {
        let t = rec.t;
        match &rec.event {
            Event::Arrived { job } if job.index() >= self.serials.len() => {
                prop_assert_eq!(job.index(), self.serials.len(), "new ids are dense");
                self.register(model, *job);
            }
            Event::Compacted { removed } => {
                let removed: BTreeSet<usize> = removed.iter().map(|j| j.index()).collect();
                for &i in &removed {
                    let s = self.serials[i];
                    prop_assert!(
                        !self.running.contains_key(&s),
                        "{} compacted while running",
                        self.names[s]
                    );
                }
                self.serials = std::mem::take(&mut self.serials)
                    .into_iter()
                    .enumerate()
                    .filter(|(i, _)| !removed.contains(i))
                    .map(|(_, s)| s)
                    .collect();
            }
            Event::Capacity { resource, capacity } => {
                self.measured.insert(*resource, *capacity);
            }
            Event::Node { node, up } => {
                if *up {
                    self.down.remove(node);
                } else {
                    self.down.insert(*node);
                    self.lost = Some((t, *node));
                }
            }
            Event::Started { job, assignment } => {
                let s = self.serial(*job)?;
                let name = &self.names[s];
                prop_assert!(!self.running.contains_key(&s), "{name} started twice");
                prop_assert_eq!(
                    assignment.resources.len(),
                    self.needs[s].len(),
                    "{}: one resource per need",
                    name
                );
                let held: Vec<(ResourceId, u64)> = assignment
                    .resources
                    .iter()
                    .copied()
                    .zip(self.needs[s].iter().copied())
                    .collect();
                for &(r, a) in &held {
                    *self.used.entry(r).or_default() += a;
                }
                for &(r, _) in &held {
                    let (used, cap) = (self.used[&r], self.capacity(model, r));
                    prop_assert!(
                        used <= cap,
                        "at {t:?}: {name} takes {} to {used} of {cap}",
                        model.resource(r).name
                    );
                    let node = model.resource(r).node;
                    prop_assert!(
                        !self.down.contains(&node),
                        "at {t:?}: {name} starts on {}, which is down",
                        model.node(node).name
                    );
                }
                self.running.insert(s, (held, false));
                self.starts += 1;
            }
            Event::Phase {
                job,
                phase: Phase::Cleanup,
            } => {
                let s = self.serial(*job)?;
                match self.running.get_mut(&s) {
                    Some(r) => r.1 = true,
                    None => {
                        return Err(fail(format!("{} cleans up without running", self.names[s])));
                    }
                }
            }
            Event::Finished { job, .. } => {
                let s = self.serial(*job)?;
                if self.running.contains_key(&s) {
                    self.release(model, t, s, "finished")?;
                }
            }
            Event::Requeued { job } => {
                let s = self.serial(*job)?;
                prop_assert!(
                    self.running.contains_key(&s),
                    "{} requeued without running",
                    self.names[s]
                );
                self.release(model, t, s, "requeued")?;
            }
            _ => {}
        }
        Ok(())
    }

    /// At the end nothing runs and nothing is held.
    fn end(&self) -> Result<(), TestCaseError> {
        let running: Vec<&String> = self.running.keys().map(|s| &self.names[*s]).collect();
        prop_assert!(running.is_empty(), "still running at the end: {running:?}");
        prop_assert!(
            self.used.values().all(|u| *u == 0),
            "still held: {:?}",
            self.used
        );
        Ok(())
    }
}

/// The model of a batch case: the fleet and the jobs, each `after` pointing to an earlier job.
fn batch_model(fleet: &Fleet, jobs: &[JobSpec]) -> Model {
    let (mut b, kinds) = fleet_builder(fleet);
    let max_cap = fleet.max_cap();
    for (i, s) in jobs.iter().enumerate() {
        let mut j = s.job(format!("j{i}"), &kinds, &max_cap, Time(s.arrival));
        if let Some(a) = s.after
            && i > 0
        {
            j = j.after(JobId(a.index(i) as u32));
        }
        b.job(j);
    }
    b.build().unwrap()
}

fn batch_case() -> impl Strategy<Value = (Fleet, Vec<JobSpec>, Which, u64)> {
    (
        fleet(),
        prop::collection::vec(job_spec(), 1..=12),
        which(),
        any::<u64>(),
    )
}

/// Every job finishes (none is left waiting), from the simulator's own account.
fn check_progress(
    model: &Model,
    planner: &mut dyn Planner,
    seed: u64,
) -> Result<choreograph::SimRun, TestCaseError> {
    let cfg = SimConfig {
        seed,
        ..SimConfig::default()
    };
    let run = simulate(model, planner, &cfg).map_err(|e| fail(e.to_string()))?;
    let left: Vec<&str> = run
        .stats
        .unfinished
        .iter()
        .map(|j| model.job(*j).name.as_str())
        .collect();
    prop_assert!(left.is_empty(), "never finished: {left:?}");
    prop_assert!(run.state.all_done());
    Ok(run)
}

fn check_batch(
    fleet: &Fleet,
    jobs: &[JobSpec],
    which: Which,
    seed: u64,
) -> Result<(), TestCaseError> {
    let model = batch_model(fleet, jobs);
    let run = check_progress(&model, which.planner().as_mut(), seed)?;
    let mut replay = Replay::of(&model);
    for rec in &run.trace.records {
        replay.feed(&model, rec)?;
    }
    replay.end()?;
    let m = from_trace(&model, &run.trace, &MetricsConfig::default());
    prop_assert!(m.little.holds(), "Little's law: {:?}", m.little);
    prop_assert!(m.resources.iter().all(|r| r.overbooked_starts == 0));
    let again = simulate(
        &model,
        which.planner().as_mut(),
        &SimConfig {
            seed,
            ..SimConfig::default()
        },
    )
    .map_err(|e| fail(e.to_string()))?;
    prop_assert!(again.trace == run.trace, "the same seed gave another trace");
    Ok(())
}

/// One step of a live script.
#[derive(Clone, Debug)]
enum Op {
    Submit(JobSpec),
    /// Let time pass, in ms: phases end and the planner runs at each end.
    Wait(i64),
    /// A measured capacity for a resource (picked among the fleet's).
    Measure(Index, u64),
    /// Back to the declared capacity.
    Unmeasure(Index),
    Down(Index),
    Up(Index),
    Lost(Index, bool),
    Compact,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => job_spec().prop_map(Op::Submit),
        4 => (0..=3_000i64).prop_map(Op::Wait),
        1 => (any::<Index>(), 0..=4u64).prop_map(|(r, c)| Op::Measure(r, c)),
        1 => any::<Index>().prop_map(Op::Unmeasure),
        1 => any::<Index>().prop_map(Op::Down),
        1 => any::<Index>().prop_map(Op::Up),
        1 => (any::<Index>(), any::<bool>()).prop_map(|(n, requeue)| Op::Lost(n, requeue)),
        1 => Just(Op::Compact),
    ]
}

/// Drives a `Live` as an executor would: phases end after the job's fixed durations, the planner runs after
/// every change, queued jobs behind a failed dependency are cancelled. Jobs are named and followed by name
/// across compactions.
struct Driver {
    live: Live,
    planner: Box<dyn Planner>,
    specs: BTreeMap<String, JobSpec>,
    /// When the current phase of each running job ends.
    ends: BTreeMap<String, Time>,
    replay: Replay,
    events: Vec<Record>,
    kinds: Vec<KindId>,
    max_cap: Vec<u64>,
    submitted: usize,
}

/// Lets a `Box<dyn Planner>` be passed where `impl Planner` is asked.
struct Boxed<'a>(&'a mut dyn Planner);

impl Planner for Boxed<'_> {
    fn plan(&mut self, model: &Model, state: &choreograph::State) -> Plan {
        self.0.plan(model, state)
    }
}

impl Driver {
    fn new(fleet: &Fleet, which: Which) -> Driver {
        let (b, kinds) = fleet_builder(fleet);
        let mut live = Live::new(b.build().unwrap(), Time::ZERO);
        live.record_events(true);
        live.measure(MetricsConfig::default());
        Driver {
            live,
            planner: which.planner(),
            specs: BTreeMap::new(),
            ends: BTreeMap::new(),
            replay: Replay::default(),
            events: Vec::new(),
            kinds,
            max_cap: fleet.max_cap(),
            submitted: 0,
        }
    }

    fn id(&self, name: &str) -> JobId {
        self.live.model().job_by_name(name).unwrap()
    }

    /// Feeds the events since the last call to the replay, with the model as it is now.
    fn sync(&mut self) -> Result<(), TestCaseError> {
        for rec in self.live.drain_events() {
            self.replay.feed(self.live.model(), &rec)?;
            self.events.push(rec);
        }
        Ok(())
    }

    /// Cancels what a failed dependency blocks, then starts what the planner says.
    fn round(&mut self) -> Result<(), TestCaseError> {
        loop {
            let (model, state) = (self.live.model(), self.live.state());
            let blocked: Vec<JobId> = state
                .queued()
                .filter(|j| {
                    model
                        .job(*j)
                        .after
                        .iter()
                        .any(|d| state.outcome(*d).is_some_and(|o| !o.satisfies_deps()))
                })
                .collect();
            if blocked.is_empty() {
                break;
            }
            for j in blocked {
                self.live.cancel(j).unwrap();
            }
        }
        let plan = self.live.plan(&mut Boxed(self.planner.as_mut()));
        if !plan.starts.is_empty() {
            self.live
                .start(&plan.batch())
                .map_err(|e| fail(format!("at {:?}: start refused: {e}", self.live.now())))?;
            let now = self.live.now();
            for s in &plan.starts {
                let name = self.live.model().job(s.job).name.clone();
                let setup = self.specs[&name].duration(Phase::Setup);
                self.ends.insert(name, now + setup);
            }
        }
        self.sync()
    }

    /// Ends phases up to `target`, a round after each, then moves time to `target`.
    fn run_until(&mut self, target: Time) -> Result<(), TestCaseError> {
        while let Some((t, name)) = self
            .ends
            .iter()
            .map(|(n, t)| (*t, n.clone()))
            .min()
            .filter(|(t, _)| *t <= target)
        {
            self.live.set_now(t).unwrap();
            let j = self.id(&name);
            let JobStatus::Running { phase, .. } = self.live.state().status(j) else {
                return Err(fail(format!("{name} has a phase end but is not running")));
            };
            let spec = &self.specs[&name];
            let next = if *phase == Phase::Run && spec.fails {
                self.live.fail(j).unwrap();
                Some(Phase::Cleanup)
            } else {
                match self.live.advance(j).unwrap() {
                    JobStatus::Running { phase, .. } => Some(*phase),
                    _ => None,
                }
            };
            match next {
                Some(p) => {
                    let d = self.specs[&name].duration(p);
                    self.ends.insert(name, t + d);
                }
                None => {
                    self.ends.remove(&name);
                }
            }
            self.round()?;
        }
        self.live.set_now(target).unwrap();
        self.round()
    }

    fn apply(&mut self, op: &Op) -> Result<(), TestCaseError> {
        let model = self.live.model();
        let resources = model.resources().len();
        let nodes = model.nodes().len();
        let now = self.live.now();
        match op {
            Op::Submit(spec) => {
                let name = format!("j{}", self.submitted);
                let mut job = spec.job(name.clone(), &self.kinds, &self.max_cap, now);
                if let Some(a) = spec.after
                    && self.submitted > 0
                    && let Some(dep) = model.job_by_name(&format!("j{}", a.index(self.submitted)))
                {
                    job = job.after(dep);
                }
                self.submitted += 1;
                self.specs.insert(name, spec.clone());
                self.live.submit(job).map_err(|e| fail(e.to_string()))?;
            }
            Op::Wait(ms) => return self.run_until(now + Dur(*ms)),
            Op::Measure(r, c) => self
                .live
                .set_capacity(ResourceId(r.index(resources) as u32), Some(*c)),
            Op::Unmeasure(r) => self
                .live
                .set_capacity(ResourceId(r.index(resources) as u32), None),
            Op::Down(n) => self.live.set_node_up(NodeId(n.index(nodes) as u32), false),
            Op::Up(n) => self.live.set_node_up(NodeId(n.index(nodes) as u32), true),
            Op::Lost(n, requeue) => {
                let then = if *requeue {
                    LostJobs::Requeue
                } else {
                    LostJobs::Fail
                };
                for j in self.live.node_lost(NodeId(n.index(nodes) as u32), then) {
                    let name = self.live.model().job(j).name.clone();
                    self.ends.remove(&name);
                }
            }
            Op::Compact => {
                self.sync()?;
                self.live.compact(now);
            }
        }
        self.round()
    }

    /// Back to the declared fleet, every node up, and on until every job is done.
    fn drain(&mut self) -> Result<(), TestCaseError> {
        for r in 0..self.live.model().resources().len() {
            self.live.set_capacity(ResourceId(r as u32), None);
        }
        for n in 0..self.live.model().nodes().len() {
            self.live.set_node_up(NodeId(n as u32), true);
        }
        self.round()?;
        while let Some(t) = self.ends.values().min().copied() {
            self.run_until(t)?;
        }
        let state = self.live.state();
        let left: Vec<&str> = self
            .live
            .model()
            .job_ids()
            .filter(|j| !matches!(state.status(*j), JobStatus::Done { .. }))
            .map(|j| self.live.model().job(j).name.as_str())
            .collect();
        prop_assert!(left.is_empty(), "never finished: {left:?}");
        self.replay.end()
    }
}

/// Runs a live script to the end and returns every event it recorded.
fn check_live(fleet: &Fleet, ops: &[Op], which: Which) -> Result<Vec<Record>, TestCaseError> {
    let mut d = Driver::new(fleet, which);
    for op in ops {
        d.apply(op)?;
    }
    d.drain()?;
    let m = d.live.metrics().unwrap();
    prop_assert!(m.little.holds(), "Little's law: {:?}", m.little);
    prop_assert_eq!(m.arrivals as usize, d.submitted);
    prop_assert_eq!(
        m.jobs.len(),
        d.submitted,
        "every job measured, compacted or not"
    );
    Ok(d.events)
}

proptest! {
    #![proptest_config(config(256))]

    /// The batch simulator: capacity, cleanup, progress, Little's law and determinism.
    #[test]
    fn batch_runs_keep_the_invariants((fleet, jobs, which, seed) in batch_case()) {
        check_batch(&fleet, &jobs, which, seed)?;
    }

    /// The live model under a random script, and the same script twice gives the same events.
    #[test]
    fn live_runs_keep_the_invariants(
        fleet in fleet(),
        ops in prop::collection::vec(op(), 1..=40),
        which in which(),
    ) {
        let events = check_live(&fleet, &ops, which)?;
        prop_assert!(events == check_live(&fleet, &ops, which)?, "the same script gave other events");
    }

    /// Aging bounds the wait on one exclusive slot: a job `g` classes below the top overtakes every later
    /// arrival after waiting `g` aging steps, so it waits at most for the job running when it came plus the
    /// jobs that arrived before `a_j + g·step` (and a millisecond and one priority unit of rounding).
    #[test]
    fn aging_bounds_the_wait(
        jobs in prop::collection::vec((0..=3i32, 0..=120_000i64, 1..=10_000i64), 1..=20),
        step_s in 1..=5i64,
    ) {
        let step = Dur::secs(step_s);
        let mut b = ModelBuilder::new();
        let slot = b.kind("slot", Unit::Count);
        let n = b.node("n", Labels::new());
        b.pool("slot", slot, n, 1);
        for (i, (class, arrival, run)) in jobs.iter().enumerate() {
            b.job(
                Job::new(format!("j{i}"))
                    .needs(slot, 1)
                    .class(*class)
                    .arrival(Time(*arrival))
                    .run(Estimate::fixed(Dur(*run))),
            );
        }
        let model = b.build().unwrap();
        let mut planner = ListScheduler::new(Policy {
            aging_per_class: Some(step),
            ..Policy::default()
        });
        let run = check_progress(&model, &mut planner, 0)?;
        let started: BTreeMap<JobId, Time> = run
            .trace
            .records
            .iter()
            .filter_map(|r| match r.event {
                Event::Started { job, .. } => Some((job, r.t)),
                _ => None,
            })
            .collect();
        let top = jobs.iter().map(|j| j.0).max().unwrap();
        let longest = jobs.iter().map(|j| j.2).max().unwrap();
        for (i, (class, arrival, _)) in jobs.iter().enumerate() {
            let gap = i64::from(top - class);
            let overtake = gap * step.as_ms() + step.as_ms() / 1000 + 1;
            let before: i64 = jobs
                .iter()
                .enumerate()
                .filter(|(k, j)| *k != i && j.1 <= arrival + overtake)
                .map(|(_, j)| j.2)
                .sum();
            let wait = (started[&JobId(i as u32)] - Time(*arrival)).as_ms();
            prop_assert!(
                wait <= longest + before,
                "j{i} (class {class}) waited {wait} ms, bound {} ms", longest + before
            );
        }
    }
}

/// A planner broken on purpose: it never starts a job that needs more than one unit of anything.
struct OnlyUnits;

impl Planner for OnlyUnits {
    fn plan(&mut self, model: &Model, state: &choreograph::State) -> Plan {
        let mut plan = FirstFit.plan(model, state);
        plan.starts
            .retain(|s| model.job(s.job).needs.iter().all(|n| n.amount <= 1));
        plan
    }
}

/// The progress check finds the broken planner and shrinks the case to one job with one need of two units.
#[test]
fn shrinks_to_a_small_case() {
    let mut runner = TestRunner::new(config(256));
    let result = runner.run(
        &(fleet(), prop::collection::vec(job_spec(), 1..=12)),
        |(fleet, jobs)| {
            let model = batch_model(&fleet, &jobs);
            check_progress(&model, &mut OnlyUnits, 0).map(|_| ())
        },
    );
    let Err(TestError::Fail(why, (fleet, jobs))) = result else {
        panic!("the broken planner was not found: {result:?}");
    };
    let model = batch_model(&fleet, &jobs);
    let needs: Vec<u64> = model
        .jobs()
        .iter()
        .flat_map(|j| j.needs.iter().map(|n| n.amount))
        .collect();
    assert_eq!(needs, [2], "minimal case: {fleet:?} {jobs:?} ({why})");
}
