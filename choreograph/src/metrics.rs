//! Metrics from a trace: what a run measured, and what the theory predicts for it.
//!
//! A [`Collector`] reads trace records one at a time, in order, and keeps the running sums: time integrals of
//! the number of jobs in the system and in the queue, of each resource's use and capacity, and one record per
//! finished job. [`Collector::finish`] turns them into [`Metrics`]:
//!
//! - utilisation per resource U_r = ∫ used_r(t) dt / ∫ C_r(t) dt, with the capacity as measured and zero while
//!   its node is down;
//! - the queue length L_q(t) as a step series, its mean and maximum, and the mean number in the system L;
//! - waiting time in queue W_q and sojourn time W per job, overall and per class (mean, p50, p90, max);
//! - lateness L_j = C_j − d_j, tardiness T_j = max(0, L_j), the number of late jobs, weighted tardiness;
//! - makespan, throughput, the share of holding time spent in setup, the cache hit rate;
//! - Little's law L = λW checked on the run ([`Little`]): exact, to the millisecond, when the run starts and
//!   ends empty, a free self-test of the bookkeeping.
//!
//! [`Metrics::compare`] puts the measured values next to the formulas of [`theory`](crate::theory) fed with
//! the measured arrival rate and service times; [`Metrics::batch_means`] and [`confidence_interval`] give
//! intervals instead of single numbers (several replications with different seeds, or batches of one long
//! run), and [`MetricsConfig::warmup`] cuts the start of a run that begins empty.
//!
//! The collector reads job details (name, class, due date, the amount of each need) from the model when an
//! event comes in, so it works on a model that changes at runtime: feed it as things happen
//! ([`Live::measure`](crate::live::Live::measure) does), and a compaction ([`Event::Compacted`]) renumbers its
//! open jobs. For a finished trace whose model has every job, [`from_trace`] does it in one call.
//!
//! ```
//! use choreograph::model::{Job, Labels, ModelBuilder, Unit};
//! use choreograph::metrics::{MetricsConfig, from_trace};
//! use choreograph::{Dur, Estimate, FirstFit, SimConfig, Time, simulate};
//!
//! let mut b = ModelBuilder::new();
//! let slot = b.kind("slot", Unit::Count);
//! let n = b.node("n", Labels::new());
//! b.pool("slot", slot, n, 1);
//! for (i, at) in [0, 0, 5].into_iter().enumerate() {
//!     b.job(Job::new(format!("j{i}")).needs(slot, 1).run(Estimate::fixed(Dur::secs(10))).arrival(Time(at * 1000)));
//! }
//! let model = b.build().unwrap();
//! let run = simulate(&model, &mut FirstFit, &SimConfig::default()).unwrap();
//! let m = from_trace(&model, &run.trace, &MetricsConfig::default());
//! assert_eq!(m.wait.max, Some(Dur::secs(15))); // j2 arrives at 5 s and starts at 20 s
//! assert_eq!(m.makespan, Some(Dur::secs(30)));
//! assert!(m.little.holds()); // started and ended empty: L = λW exactly
//! ```

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::model::{JobId, Model, NodeId, Phase, ResourceId};
use crate::state::Outcome;
use crate::theory;
use crate::time::{Dur, Time};
use crate::trace::{Event, Record, Trace};

/// Settings of a measurement.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsConfig {
    /// Time cut from the start before measuring (a warm-up): jobs that arrive earlier are not counted and the
    /// time averages begin after it. A queue that starts empty looks better than it is in steady state.
    pub warmup: Dur,
    /// Leave out the step series of the queue length (`Metrics::queue_series`), for long runs where only the
    /// summary is wanted.
    pub no_queue_series: bool,
}

/// One finished job as measured.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobMetrics {
    pub name: String,
    pub class: i32,
    pub weight: u32,
    /// When it entered the queue.
    pub arrived: Time,
    /// When it first acquired its resources; `None` when it never ran (cancelled in the queue, cached).
    pub started: Option<Time>,
    /// When it left the system.
    pub finished: Time,
    pub outcome: Outcome,
    pub due: Option<Time>,
    /// Time in the queue, every stretch added up (a job requeued after a lost node waits again).
    pub wait: Dur,
    /// Time holding resources in each phase, every attempt added up.
    pub setup: Dur,
    pub run: Dur,
    pub cleanup: Dur,
    /// Times it went back to the queue because its node was lost.
    pub requeues: u32,
}

impl JobMetrics {
    /// Sojourn (flow) time F_j = C_j − a_j.
    pub fn sojourn(&self) -> Dur {
        self.finished - self.arrived
    }

    /// Time holding resources: setup, run and cleanup.
    pub fn service(&self) -> Dur {
        self.setup + self.run + self.cleanup
    }

    /// Lateness L_j = C_j − d_j, negative when early; `None` without a due date.
    pub fn lateness(&self) -> Option<Dur> {
        self.due.map(|d| self.finished - d)
    }

    /// Tardiness T_j = max(0, L_j).
    pub fn tardiness(&self) -> Option<Dur> {
        self.lateness().map(|l| l.max(Dur::ZERO))
    }

    /// U_j: finished after its due date.
    pub fn late(&self) -> bool {
        self.lateness().is_some_and(|l| l > Dur::ZERO)
    }
}

/// A distribution of durations: count, mean, nearest-rank p50 and p90, maximum.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Summary {
    pub n: u64,
    /// The exact sum.
    pub sum: Dur,
    /// Mean in seconds (0 when empty).
    pub mean: f64,
    /// Squared coefficient of variation, variance/mean² (0 when empty or the mean is 0).
    pub cv2: f64,
    pub p50: Option<Dur>,
    pub p90: Option<Dur>,
    pub max: Option<Dur>,
}

impl Summary {
    pub fn of(values: impl IntoIterator<Item = Dur>) -> Summary {
        let mut v: Vec<Dur> = values.into_iter().collect();
        v.sort();
        let n = v.len();
        if n == 0 {
            return Summary::default();
        }
        let sum: i128 = v.iter().map(|d| i128::from(d.0)).sum();
        let mean = sum as f64 / n as f64;
        let var = v.iter().map(|d| (d.0 as f64 - mean).powi(2)).sum::<f64>() / n as f64;
        let rank = |q: f64| v[((q * n as f64).ceil() as usize).clamp(1, n) - 1];
        Summary {
            n: n as u64,
            sum: Dur(sum.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64),
            mean: mean / 1000.0,
            cv2: if mean > 0.0 { var / (mean * mean) } else { 0.0 },
            p50: Some(rank(0.5)),
            p90: Some(rank(0.9)),
            max: v.last().copied(),
        }
    }
}

/// Metrics of one priority class.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ClassMetrics {
    /// Jobs of the class that left the system.
    pub jobs: u64,
    pub wait: Summary,
    pub sojourn: Summary,
    /// Holding times of the jobs that ran.
    pub service: Summary,
    pub late: u64,
}

/// One resource over the measured window.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResourceMetrics {
    pub name: String,
    /// ∫ used(t) dt, in amount·milliseconds.
    pub busy: i128,
    /// ∫ C(t) dt, in amount·milliseconds: the capacity as measured, 0 while the node is down.
    pub available: i128,
    /// busy / available; `None` when nothing was available.
    pub utilisation: Option<f64>,
    /// The mean amount in use.
    pub mean_used: f64,
    /// The most in use at once.
    pub peak: u64,
    /// Starts that left more in use than the capacity at that moment. Always 0 for a run driven through
    /// [`State`](crate::State), which refuses them; a trace from elsewhere may show some.
    pub overbooked_starts: u64,
}

/// Late jobs and tardiness over the jobs with a due date.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Lateness {
    pub with_due: u64,
    /// Σ U_j.
    pub late: u64,
    /// L_max.
    pub max_lateness: Option<Dur>,
    /// Mean lateness in seconds (0 without due dates).
    pub mean_lateness: f64,
    /// Σ T_j.
    pub total_tardiness: Dur,
    /// Σ w_j T_j in weight·milliseconds.
    pub weighted_tardiness: i128,
}

/// How the finished jobs ended.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcomes {
    pub ok: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub cached: u64,
    pub cached_fail: u64,
}

/// Little's law L = λW checked on the run, in exact integers: ∫ N(t) dt (job·ms) against Σ W_j over the jobs
/// that arrived and left in the window, the same for the queue. On a run that starts and ends empty the two
/// are equal; otherwise they differ by the time of the jobs present at the edges.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Little {
    pub area_system: i128,
    pub sum_sojourn: i128,
    pub area_queue: i128,
    pub sum_wait: i128,
    /// No job in the system when measuring began.
    pub start_empty: bool,
    /// No job in the system at the end.
    pub end_empty: bool,
}

impl Little {
    /// Both identities hold exactly.
    pub fn holds(&self) -> bool {
        self.area_system == self.sum_sojourn && self.area_queue == self.sum_wait
    }
}

/// What a run measured. Rates are per second, mean durations in seconds; exact sums and areas in
/// milliseconds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Metrics {
    /// The measured window: from the collector's start plus the warm-up, to the end.
    pub from: Time,
    pub to: Time,
    /// Jobs that arrived in the window.
    pub arrivals: u64,
    /// The jobs that arrived in the window and left it, in the order they left.
    pub jobs: Vec<JobMetrics>,
    /// Jobs still in the system at the end.
    pub open: u64,
    pub outcomes: Outcomes,
    /// Arrival rate λ per second.
    pub lambda: f64,
    /// Squared coefficient of variation of the gaps between arrivals, c_a².
    pub arrival_cv2: f64,
    /// Time in queue of every job that left (served or not).
    pub wait: Summary,
    pub sojourn: Summary,
    /// Holding time (setup + run + cleanup) of the jobs that ran.
    pub service: Summary,
    pub classes: BTreeMap<i32, ClassMetrics>,
    /// L, the mean number of jobs in the system.
    pub mean_in_system: f64,
    /// L_q, the mean queue length.
    pub mean_queue: f64,
    pub max_queue: u64,
    /// L_q(t): the queue length from each time on, one entry per change (empty with `no_queue_series`).
    pub queue_series: Vec<(Time, u64)>,
    pub resources: Vec<ResourceMetrics>,
    pub lateness: Lateness,
    /// C_max: from the window start to the last job leaving.
    pub makespan: Option<Dur>,
    /// Jobs that left, per hour.
    pub throughput_per_hour: f64,
    /// Setup time over holding time, of the jobs that ran.
    pub setup_share: Option<f64>,
    /// Jobs settled from the cache over jobs that ran or were settled.
    pub cache_hit_rate: Option<f64>,
    pub little: Little,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Open {
    name: String,
    class: i32,
    weight: u32,
    due: Option<Time>,
    arrived: Time,
    counted: bool,
    queued_since: Option<Time>,
    wait: Dur,
    started: Option<Time>,
    phase: Option<(Phase, Time)>,
    phases: [Dur; 3],
    held: Vec<(ResourceId, u64)>,
    requeues: u32,
}

impl Open {
    fn close_phase(&mut self, t: Time) {
        if let Some((p, since)) = self.phase.take() {
            self.phases[p as usize] += t - since;
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Res {
    name: String,
    node: NodeId,
    declared: u64,
    measured: Option<u64>,
    used: u64,
    busy: i128,
    available: i128,
    peak: u64,
    overbooked: u64,
}

/// Reads trace records in order and keeps what [`Metrics`] needs. See the [module docs](self).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Collector {
    cfg: MetricsConfig,
    from: Time,
    last: Time,
    jobs: BTreeMap<JobId, Open>,
    done: Vec<JobMetrics>,
    res: Vec<Res>,
    up: Vec<bool>,
    in_system: u64,
    in_queue: u64,
    at_from: Option<u64>,
    area_system: i128,
    area_queue: i128,
    max_queue: u64,
    series: Vec<(Time, u64)>,
    arrivals: u64,
    last_arrival: Option<Time>,
    gaps: u64,
    gap_sum: i128,
    gap_sq: i128,
}

impl Collector {
    /// Starts measuring at `start` (plus the warm-up) with nothing in the system.
    pub fn new(model: &Model, start: Time, cfg: MetricsConfig) -> Collector {
        let mut c = Collector {
            from: start + cfg.warmup,
            cfg,
            last: start,
            jobs: BTreeMap::new(),
            done: Vec::new(),
            res: Vec::new(),
            up: Vec::new(),
            in_system: 0,
            in_queue: 0,
            at_from: None,
            area_system: 0,
            area_queue: 0,
            max_queue: 0,
            series: Vec::new(),
            arrivals: 0,
            last_arrival: None,
            gaps: 0,
            gap_sum: 0,
            gap_sq: 0,
        };
        c.grow(model);
        c
    }

    /// Takes in resources and nodes added to the model since.
    fn grow(&mut self, model: &Model) {
        for r in &model.resources()[self.res.len().min(model.resources().len())..] {
            self.res.push(Res {
                name: r.name.clone(),
                node: r.node,
                declared: r.capacity,
                measured: None,
                used: 0,
                busy: 0,
                available: 0,
                peak: 0,
                overbooked: 0,
            });
        }
        self.up.resize(model.nodes().len().max(self.up.len()), true);
    }

    fn capacity(&self, r: &Res) -> u64 {
        if self.up.get(r.node.index()).copied().unwrap_or(true) {
            r.measured.unwrap_or(r.declared)
        } else {
            0
        }
    }

    /// Integrates everything up to `t` (only the part after the warm-up counts).
    fn advance(&mut self, t: Time) {
        if t < self.last {
            debug_assert!(false, "trace time went back: {} after {}", t, self.last);
            return;
        }
        if self.at_from.is_none() && t >= self.from {
            self.at_from = Some(self.in_system);
            if !self.cfg.no_queue_series {
                self.series.push((self.from, self.in_queue));
            }
        }
        let a = self.last.max(self.from);
        if t > a {
            let dt = i128::from((t - a).0);
            self.area_system += i128::from(self.in_system) * dt;
            self.area_queue += i128::from(self.in_queue) * dt;
            for i in 0..self.res.len() {
                let cap = self.capacity(&self.res[i]);
                let r = &mut self.res[i];
                r.busy += i128::from(r.used) * dt;
                r.available += i128::from(cap) * dt;
            }
        }
        self.last = t;
    }

    fn queue_changed(&mut self, t: Time, by: i64) {
        self.in_queue = self.in_queue.saturating_add_signed(by);
        if t >= self.from {
            self.max_queue = self.max_queue.max(self.in_queue);
            if !self.cfg.no_queue_series {
                match self.series.last_mut() {
                    Some((at, n)) if *at == t => *n = self.in_queue,
                    _ => self.series.push((t, self.in_queue)),
                }
            }
        }
    }

    fn release(&mut self, held: &[(ResourceId, u64)]) {
        for (r, a) in held {
            if let Some(res) = self.res.get_mut(r.index()) {
                res.used = res.used.saturating_sub(*a);
            }
        }
    }

    /// Takes in one record. `model` is the model as it is when the event happens: a job's details are read
    /// from it when the job arrives and starts. Records must come in time order.
    pub fn record(&mut self, model: &Model, rec: &Record) {
        let t = rec.t;
        self.grow(model);
        self.advance(t);
        match &rec.event {
            Event::Arrived { job } => {
                let Some(j) = model.jobs().get(job.index()) else {
                    return;
                };
                let counted = t >= self.from;
                if counted {
                    self.arrivals += 1;
                    if let Some(prev) = self.last_arrival {
                        let g = i128::from((t - prev).0);
                        self.gaps += 1;
                        self.gap_sum += g;
                        self.gap_sq += g * g;
                    }
                    self.last_arrival = Some(t);
                }
                self.jobs.insert(
                    *job,
                    Open {
                        name: j.name.clone(),
                        class: j.class,
                        weight: j.weight,
                        due: j.due,
                        arrived: t,
                        counted,
                        queued_since: Some(t),
                        wait: Dur::ZERO,
                        started: None,
                        phase: None,
                        phases: [Dur::ZERO; 3],
                        held: Vec::new(),
                        requeues: 0,
                    },
                );
                self.in_system += 1;
                self.queue_changed(t, 1);
            }
            Event::Started { job, assignment } => {
                let Some(o) = self.jobs.get_mut(job) else {
                    return;
                };
                if let Some(q) = o.queued_since.take() {
                    o.wait += t - q;
                }
                o.started.get_or_insert(t);
                o.phase = Some((Phase::Setup, t));
                o.held = model
                    .jobs()
                    .get(job.index())
                    .map(|j| {
                        j.needs
                            .iter()
                            .zip(&assignment.0)
                            .map(|(n, r)| (*r, n.amount))
                            .collect()
                    })
                    .unwrap_or_default();
                let held = o.held.clone();
                let mut take: BTreeMap<ResourceId, u64> = BTreeMap::new();
                for (r, a) in held {
                    *take.entry(r).or_default() += a;
                }
                for (r, a) in take {
                    let Some(res) = self.res.get(r.index()) else {
                        continue;
                    };
                    let cap = self.capacity(res);
                    let res = &mut self.res[r.index()];
                    res.used += a;
                    res.peak = res.peak.max(res.used);
                    if res.used > cap {
                        res.overbooked += 1;
                    }
                }
                self.queue_changed(t, -1);
            }
            Event::Phase { job, phase } => {
                if let Some(o) = self.jobs.get_mut(job) {
                    o.close_phase(t);
                    o.phase = Some((*phase, t));
                }
            }
            Event::Cancelled { job } => {
                if self.jobs.get(job).is_some_and(|o| o.phase.is_none()) {
                    self.leave(t, *job, Outcome::Cancelled);
                }
            }
            Event::Finished { job, outcome } => self.leave(t, *job, *outcome),
            Event::Requeued { job } => {
                let Some(o) = self.jobs.get_mut(job) else {
                    return;
                };
                o.close_phase(t);
                o.queued_since = Some(t);
                o.requeues += 1;
                let held = std::mem::take(&mut o.held);
                self.release(&held);
                self.queue_changed(t, 1);
            }
            Event::Capacity { resource, capacity } => {
                if let Some(r) = self.res.get_mut(resource.index()) {
                    r.measured = *capacity;
                }
            }
            Event::Node { node, up } => {
                if let Some(u) = self.up.get_mut(node.index()) {
                    *u = *up;
                }
            }
            Event::Compacted { removed } => {
                let mut removed = removed.clone();
                removed.sort();
                self.jobs = std::mem::take(&mut self.jobs)
                    .into_iter()
                    .filter(|(j, _)| removed.binary_search(j).is_err())
                    .map(|(j, o)| (JobId(j.0 - removed.partition_point(|r| *r < j) as u32), o))
                    .collect();
            }
            // TODO(CORE-6): a paused job counts as holding all it held, and its pause as busy time.
            Event::Failed { .. }
            | Event::Waiting { .. }
            | Event::Paused { .. }
            | Event::Resumed { .. } => {}
        }
    }

    /// The job leaves the system.
    fn leave(&mut self, t: Time, job: JobId, outcome: Outcome) {
        let Some(mut o) = self.jobs.remove(&job) else {
            return;
        };
        if let Some(q) = o.queued_since.take() {
            o.wait += t - q;
            self.queue_changed(t, -1);
        }
        o.close_phase(t);
        self.release(&o.held);
        self.in_system -= 1;
        if o.counted {
            self.done.push(JobMetrics {
                name: o.name,
                class: o.class,
                weight: o.weight,
                arrived: o.arrived,
                started: o.started,
                finished: t,
                outcome,
                due: o.due,
                wait: o.wait,
                setup: o.phases[Phase::Setup as usize],
                run: o.phases[Phase::Run as usize],
                cleanup: o.phases[Phase::Cleanup as usize],
                requeues: o.requeues,
            });
        }
    }

    /// The metrics so far, as if the run ended at `end`.
    pub fn metrics(&self, end: Time) -> Metrics {
        self.clone().finish(end)
    }

    /// Ends the measurement at `end` (not before the last record).
    pub fn finish(mut self, end: Time) -> Metrics {
        let end = end.max(self.last);
        self.advance(end);
        let from = self.from.min(end);
        let span = (end - from).0;
        let secs = span as f64 / 1000.0;
        let per = |x: f64| if span > 0 { x / secs } else { 0.0 };
        let jobs = self.done;
        let ran = || jobs.iter().filter(|j| j.started.is_some());

        let mut outcomes = Outcomes::default();
        for j in &jobs {
            *match j.outcome {
                Outcome::Ok => &mut outcomes.ok,
                Outcome::Failed => &mut outcomes.failed,
                Outcome::Cancelled => &mut outcomes.cancelled,
                Outcome::Cached => &mut outcomes.cached,
                Outcome::CachedFail => &mut outcomes.cached_fail,
            } += 1;
        }

        let mut classes: BTreeMap<i32, Vec<&JobMetrics>> = BTreeMap::new();
        for j in &jobs {
            classes.entry(j.class).or_default().push(j);
        }
        let classes = classes
            .into_iter()
            .map(|(c, js)| {
                let m = ClassMetrics {
                    jobs: js.len() as u64,
                    wait: Summary::of(js.iter().map(|j| j.wait)),
                    sojourn: Summary::of(js.iter().map(|j| j.sojourn())),
                    service: Summary::of(
                        js.iter()
                            .filter(|j| j.started.is_some())
                            .map(|j| j.service()),
                    ),
                    late: js.iter().filter(|j| j.late()).count() as u64,
                };
                (c, m)
            })
            .collect();

        let mut lateness = Lateness::default();
        let mut sum_late: i128 = 0;
        for j in &jobs {
            let (Some(l), Some(tard)) = (j.lateness(), j.tardiness()) else {
                continue;
            };
            lateness.with_due += 1;
            lateness.late += u64::from(j.late());
            lateness.max_lateness = Some(lateness.max_lateness.map_or(l, |m| m.max(l)));
            lateness.total_tardiness += tard;
            lateness.weighted_tardiness += i128::from(j.weight) * i128::from(tard.0);
            sum_late += i128::from(l.0);
        }
        if lateness.with_due > 0 {
            lateness.mean_lateness = sum_late as f64 / lateness.with_due as f64 / 1000.0;
        }

        let service: i128 = ran().map(|j| i128::from(j.service().0)).sum();
        let setup: i128 = ran().map(|j| i128::from(j.setup.0)).sum();
        let settled = outcomes.cached + outcomes.cached_fail;
        let ran_n = ran().count() as u64;

        let arrival_cv2 = if self.gaps > 0 && self.gap_sum > 0 {
            let n = self.gaps as f64;
            let mean = self.gap_sum as f64 / n;
            (self.gap_sq as f64 / n - mean * mean).max(0.0) / (mean * mean)
        } else {
            0.0
        };

        let little = Little {
            area_system: self.area_system,
            sum_sojourn: jobs.iter().map(|j| i128::from(j.sojourn().0)).sum(),
            area_queue: self.area_queue,
            sum_wait: jobs.iter().map(|j| i128::from(j.wait.0)).sum(),
            start_empty: self.at_from.unwrap_or(self.in_system) == 0,
            end_empty: self.in_system == 0,
        };

        let resources = self
            .res
            .iter()
            .map(|r| ResourceMetrics {
                name: r.name.clone(),
                busy: r.busy,
                available: r.available,
                utilisation: (r.available > 0).then(|| r.busy as f64 / r.available as f64),
                mean_used: if span > 0 {
                    r.busy as f64 / span as f64
                } else {
                    0.0
                },
                peak: r.peak,
                overbooked_starts: r.overbooked,
            })
            .collect();

        Metrics {
            from,
            to: end,
            arrivals: self.arrivals,
            open: self.in_system,
            outcomes,
            lambda: per(self.arrivals as f64),
            arrival_cv2,
            wait: Summary::of(jobs.iter().map(|j| j.wait)),
            sojourn: Summary::of(jobs.iter().map(|j| j.sojourn())),
            service: Summary::of(ran().map(|j| j.service())),
            classes,
            mean_in_system: if span > 0 {
                self.area_system as f64 / span as f64
            } else {
                0.0
            },
            mean_queue: if span > 0 {
                self.area_queue as f64 / span as f64
            } else {
                0.0
            },
            max_queue: self.max_queue,
            queue_series: self.series,
            resources,
            lateness,
            makespan: jobs.iter().map(|j| j.finished - from).max(),
            throughput_per_hour: per(jobs.len() as f64) * 3600.0,
            setup_share: (service > 0).then(|| setup as f64 / service as f64),
            cache_hit_rate: (settled + ran_n > 0)
                .then(|| settled as f64 / (settled + ran_n) as f64),
            little,
            jobs,
        }
    }
}

/// The metrics of a whole trace whose model has every job in it: measured from the first record to the last.
pub fn from_trace(model: &Model, trace: &Trace, cfg: &MetricsConfig) -> Metrics {
    let start = trace.records.first().map_or(Time::ZERO, |r| r.t);
    let end = trace.records.last().map_or(start, |r| r.t);
    let mut c = Collector::new(model, start, cfg.clone());
    for r in &trace.records {
        c.record(model, r);
    }
    c.finish(end)
}

/// A mean with a confidence interval: mean ± half_width.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Interval {
    pub mean: f64,
    pub half_width: f64,
    pub n: usize,
}

/// The Student-t confidence interval of the mean of independent samples (replications with different seeds,
/// or batch means) at a confidence level such as 0.95; `None` with fewer than two samples.
pub fn confidence_interval(samples: &[f64], level: f64) -> Option<Interval> {
    let n = samples.len();
    if n < 2 || !(0.0..1.0).contains(&level) {
        return None;
    }
    let mean = samples.iter().sum::<f64>() / n as f64;
    let var = samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
    let t = t_quantile(0.5 + level / 2.0, (n - 1) as f64);
    Some(Interval {
        mean,
        half_width: t * (var / n as f64).sqrt(),
        n,
    })
}

impl Metrics {
    /// Batch means of one long run: the jobs that left, in arrival order, cut into `batches` batches of equal
    /// size; the confidence interval of the mean wait in queue (seconds) over the batch means. Batches long
    /// against the correlation of consecutive waits make the batch means nearly independent.
    pub fn batch_means(&self, batches: usize, level: f64) -> Option<Interval> {
        if batches < 2 || self.jobs.len() < batches {
            return None;
        }
        let mut js: Vec<&JobMetrics> = self.jobs.iter().collect();
        js.sort_by_key(|j| j.arrived);
        let size = js.len() / batches;
        let means: Vec<f64> = js
            .chunks_exact(size)
            .take(batches)
            .map(|b| b.iter().map(|j| j.wait.as_secs_f64()).sum::<f64>() / b.len() as f64)
            .collect();
        confidence_interval(&means, level)
    }

    /// The measured values next to the theory's predictions, with the measured arrival rate, service time and
    /// variabilities as inputs, for a system of `servers` identical slots (each job holding one).
    pub fn compare(&self, servers: u32) -> Comparison {
        let lambda = self.lambda;
        let es = self.service.mean;
        let cs2 = self.service.cv2;
        let ca2 = self.arrival_cv2;
        let c = servers.max(1);
        let secs = (self.to - self.from).as_secs_f64();
        let mut rows = Vec::new();
        let mut row = |quantity: String, formula: &'static str, measured: f64, predicted| {
            rows.push(Row {
                quantity,
                formula: formula.to_string(),
                measured,
                predicted,
            })
        };
        row(
            "utilisation ρ".into(),
            "λ·E[S]/c",
            if secs > 0.0 {
                self.service.sum.as_secs_f64() / (secs * f64::from(c))
            } else {
                0.0
            },
            Some(theory::utilisation(lambda, es, c)),
        );
        let wq = self.wait.mean;
        if c == 1 {
            row("W_q".into(), "M/M/1", wq, theory::mm1_wq(lambda, es));
            row(
                "W_q".into(),
                "Pollaczek–Khinchine M/G/1",
                wq,
                theory::pk_wq(lambda, es, cs2),
            );
            row(
                "W_q".into(),
                "Kingman G/G/1",
                wq,
                theory::kingman_wq(lambda, es, ca2, cs2),
            );
        } else {
            row(
                "W_q".into(),
                "M/M/c Erlang C",
                wq,
                theory::mmc_wq(lambda, es, c),
            );
            row(
                "W_q".into(),
                "Allen–Cunneen G/G/c",
                wq,
                theory::allen_cunneen_wq(lambda, es, c, ca2, cs2),
            );
        }
        row(
            "L".into(),
            "Little λ·W",
            self.mean_in_system,
            Some(theory::little_l(lambda, self.sojourn.mean)),
        );
        row(
            "L_q".into(),
            "Little λ·W_q",
            self.mean_queue,
            Some(theory::little_l(lambda, wq)),
        );
        if c == 1 && self.classes.len() > 1 && secs > 0.0 {
            let ks: Vec<(&i32, &ClassMetrics)> = self.classes.iter().rev().collect();
            let input: Vec<theory::Class> = ks
                .iter()
                .map(|(_, m)| theory::Class {
                    lambda: m.jobs as f64 / secs,
                    mean: m.service.mean,
                    cs2: m.service.cv2,
                })
                .collect();
            for ((k, m), w) in ks.iter().zip(theory::cobham_wq(&input)) {
                row(
                    format!("W_q class {k}"),
                    "Cobham M/G/1 priority",
                    m.wait.mean,
                    w,
                );
            }
        }
        Comparison { rows }
    }
}

/// Measured values next to predictions, from [`Metrics::compare`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Comparison {
    pub rows: Vec<Row>,
}

/// One measured quantity and one formula's prediction of it (`None` where the formula has no finite value,
/// such as a queue at ρ ≥ 1).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub quantity: String,
    pub formula: String,
    pub measured: f64,
    pub predicted: Option<f64>,
}

impl Row {
    /// (measured − predicted)/predicted.
    pub fn relative_error(&self) -> Option<f64> {
        self.predicted
            .filter(|p| *p != 0.0)
            .map(|p| (self.measured - p) / p)
    }
}

impl fmt::Display for Comparison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{:<16} {:<26} {:>12} {:>12} {:>8}",
            "quantity", "formula", "measured", "predicted", "error"
        )?;
        for r in &self.rows {
            let p = r.predicted.map_or("-".into(), |p| format!("{p:.4}"));
            let e = r
                .relative_error()
                .map_or("-".into(), |e| format!("{:+.1}%", e * 100.0));
            writeln!(
                f,
                "{:<16} {:<26} {:>12.4} {:>12} {:>8}",
                r.quantity, r.formula, r.measured, p, e
            )?;
        }
        Ok(())
    }
}

/// The p-quantile of Student's t distribution with `df` degrees of freedom (p in (0, 1)), by bisection on the
/// distribution function.
pub fn t_quantile(p: f64, df: f64) -> f64 {
    if p == 0.5 {
        return 0.0;
    }
    if p < 0.5 {
        return -t_quantile(1.0 - p, df);
    }
    let (mut lo, mut hi) = (0.0, 1.0);
    while t_cdf(hi, df) < p && hi < 1e12 {
        hi *= 2.0;
    }
    for _ in 0..200 {
        let mid = 0.5 * (lo + hi);
        if t_cdf(mid, df) < p {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    0.5 * (lo + hi)
}

/// Student's t distribution function: 1 − I_x(df/2, 1/2)/2 with x = df/(df + t²) for t ≥ 0.
fn t_cdf(t: f64, df: f64) -> f64 {
    let x = df / (df + t * t);
    let tail = 0.5 * beta_inc(df / 2.0, 0.5, x);
    if t >= 0.0 { 1.0 - tail } else { tail }
}

/// The regularised incomplete beta function I_x(a, b), by its continued fraction (modified Lentz).
fn beta_inc(a: f64, b: f64, x: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    let ln_front =
        libm::lgamma(a + b) - libm::lgamma(a) - libm::lgamma(b) + a * x.ln() + b * (1.0 - x).ln();
    if x > (a + 1.0) / (a + b + 2.0) {
        return 1.0 - beta_inc(b, a, 1.0 - x);
    }
    let tiny = 1e-300;
    let mut c = 1.0;
    let mut d = 1.0 - (a + b) * x / (a + 1.0);
    if d.abs() < tiny {
        d = tiny;
    }
    d = 1.0 / d;
    let mut h = d;
    for m in 1..300 {
        let m = f64::from(m);
        for num in [
            m * (b - m) * x / ((a + 2.0 * m - 1.0) * (a + 2.0 * m)),
            -(a + m) * (a + b + m) * x / ((a + 2.0 * m) * (a + 2.0 * m + 1.0)),
        ] {
            d = 1.0 + num * d;
            if d.abs() < tiny {
                d = tiny;
            }
            c = 1.0 + num / c;
            if c.abs() < tiny {
                c = tiny;
            }
            d = 1.0 / d;
            h *= d * c;
        }
        if (d * c - 1.0).abs() < 1e-15 {
            break;
        }
    }
    ln_front.exp() * h / a
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::Live;
    use crate::model::{Estimate, Job, Labels, ModelBuilder, Unit};
    use crate::state::Assignment;
    use crate::{FirstFit, LostJobs};

    fn s(x: i64) -> Time {
        Time(x * 1000)
    }

    fn ev(tr: &mut Trace, t: i64, e: Event) {
        tr.push(s(t), e);
    }

    /// One slot; a (class 0, weight 2, due 10 s) runs 0-12 s with 2 s setup and 2 s cleanup, b (class 1, due
    /// 40 s) arrives at 4 s and runs 12-20 s, c arrives at 5 s and is cancelled in the queue at 8 s.
    fn three() -> (Model, Trace) {
        let mut b = ModelBuilder::new();
        let k = b.kind("slot", Unit::Count);
        let n = b.node("n", Labels::new());
        b.pool("slot", k, n, 1);
        let mut a = Job::new("a").needs(k, 1).due(s(10));
        a.weight = 2;
        b.job(a);
        b.job(Job::new("b").needs(k, 1).class(1).due(s(40)));
        b.job(Job::new("c").needs(k, 1));
        let m = b.build().unwrap();
        let (ja, jb, jc) = (JobId(0), JobId(1), JobId(2));
        let slot = Assignment(vec![ResourceId(0)]);
        let mut tr = Trace::new("hand");
        let phase = |job, phase| Event::Phase { job, phase };
        ev(&mut tr, 0, Event::Arrived { job: ja });
        ev(
            &mut tr,
            0,
            Event::Started {
                job: ja,
                assignment: slot.clone(),
            },
        );
        ev(&mut tr, 2, phase(ja, Phase::Run));
        ev(&mut tr, 4, Event::Arrived { job: jb });
        ev(&mut tr, 5, Event::Arrived { job: jc });
        ev(&mut tr, 8, Event::Cancelled { job: jc });
        ev(&mut tr, 10, phase(ja, Phase::Cleanup));
        ev(
            &mut tr,
            12,
            Event::Finished {
                job: ja,
                outcome: Outcome::Ok,
            },
        );
        ev(
            &mut tr,
            12,
            Event::Started {
                job: jb,
                assignment: slot,
            },
        );
        ev(&mut tr, 12, phase(jb, Phase::Run));
        ev(&mut tr, 20, phase(jb, Phase::Cleanup));
        ev(
            &mut tr,
            20,
            Event::Finished {
                job: jb,
                outcome: Outcome::Ok,
            },
        );
        (m, tr)
    }

    #[test]
    fn hand_built_trace_is_measured_exactly() {
        let (m, tr) = three();
        let x = from_trace(&m, &tr, &MetricsConfig::default());
        assert_eq!((x.from, x.to), (s(0), s(20)));
        assert_eq!(x.arrivals, 3);
        assert_eq!(x.open, 0);
        let names: Vec<&str> = x.jobs.iter().map(|j| j.name.as_str()).collect();
        assert_eq!(names, ["c", "a", "b"], "in the order they left");
        let a = &x.jobs[1];
        assert_eq!(
            (a.setup, a.run, a.cleanup),
            (Dur::secs(2), Dur::secs(8), Dur::secs(2))
        );
        assert_eq!(a.lateness(), Some(Dur::secs(2)));
        assert_eq!(x.jobs[2].wait, Dur::secs(8));
        assert_eq!(x.jobs[0].started, None);
        assert_eq!(
            x.outcomes,
            Outcomes {
                ok: 2,
                cancelled: 1,
                ..Outcomes::default()
            }
        );

        // Waits 0, 3, 8 s; sojourns 12, 3, 16 s.
        assert_eq!(x.wait.sum, Dur::secs(11));
        assert_eq!(x.wait.mean, 11.0 / 3.0);
        assert_eq!(x.wait.p50, Some(Dur::secs(3)));
        assert_eq!(x.wait.p90, Some(Dur::secs(8)));
        assert_eq!(x.wait.max, Some(Dur::secs(8)));
        assert_eq!(x.sojourn.sum, Dur::secs(31));
        assert_eq!(x.service.sum, Dur::secs(20));
        assert_eq!(x.classes[&0].jobs, 2);
        assert_eq!(x.classes[&0].wait.sum, Dur::secs(3));
        assert_eq!(x.classes[&1].sojourn.max, Some(Dur::secs(16)));
        assert_eq!(x.classes[&0].late, 1);

        // N(t) = 1, 2, 3, 2, 1 on [0,4), [4,5), [5,8), [8,12), [12,20): 31 job·s; the queue 11 job·s.
        assert_eq!(x.mean_in_system, 31.0 / 20.0);
        assert_eq!(x.mean_queue, 11.0 / 20.0);
        assert_eq!(x.max_queue, 2);
        assert_eq!(
            x.queue_series,
            vec![(s(0), 0), (s(4), 1), (s(5), 2), (s(8), 1), (s(12), 0)]
        );
        assert_eq!(x.resources[0].utilisation, Some(1.0));
        assert_eq!(x.resources[0].peak, 1);
        assert_eq!(x.resources[0].overbooked_starts, 0);

        assert_eq!(x.lateness.with_due, 2);
        assert_eq!(x.lateness.late, 1);
        assert_eq!(x.lateness.max_lateness, Some(Dur::secs(2)));
        assert_eq!(x.lateness.mean_lateness, -9.0);
        assert_eq!(x.lateness.total_tardiness, Dur::secs(2));
        assert_eq!(x.lateness.weighted_tardiness, 4000);
        assert_eq!(x.makespan, Some(Dur::secs(20)));
        assert_eq!(x.throughput_per_hour, 540.0);
        assert_eq!(x.setup_share, Some(0.1));
        assert_eq!(x.cache_hit_rate, Some(0.0));
        assert_eq!(x.lambda, 0.15);
        // Gaps 4 s and 1 s: mean 2.5, variance 2.25.
        assert!((x.arrival_cv2 - 0.36).abs() < 1e-12);

        assert_eq!(
            x.little,
            Little {
                area_system: 31_000,
                sum_sojourn: 31_000,
                area_queue: 11_000,
                sum_wait: 11_000,
                start_empty: true,
                end_empty: true,
            }
        );
        assert!(x.little.holds());
    }

    #[test]
    fn little_differs_by_the_jobs_left_at_the_end() {
        let (m, mut tr) = three();
        tr.records.truncate(10); // up to b's start at 12 s
        let x = from_trace(&m, &tr, &MetricsConfig::default());
        assert_eq!(x.open, 1);
        assert!(!x.little.end_empty);
        // b has been in the system 8 s and is not among the jobs that left.
        assert_eq!(x.little.area_system - x.little.sum_sojourn, 8_000);
        // ... all of it in the queue.
        assert_eq!(x.little.area_queue - x.little.sum_wait, 8_000);
    }

    #[test]
    fn warmup_cuts_early_jobs_and_time() {
        let (m, tr) = three();
        let cfg = MetricsConfig {
            warmup: Dur::secs(4),
            ..MetricsConfig::default()
        };
        let x = from_trace(&m, &tr, &cfg);
        assert_eq!(x.from, s(4));
        assert_eq!(x.arrivals, 2, "a arrived in the warm-up");
        assert_eq!(x.jobs.len(), 2);
        assert!(!x.little.start_empty);
        // N(t) from 4 s: 2, 3, 2, 1 = 2 + 9 + 8 + 8.
        assert_eq!(x.little.area_system, 27_000);
        assert_eq!(x.queue_series[0], (s(4), 1));
    }

    #[test]
    fn requeue_capacity_and_node_down() {
        let mut b = ModelBuilder::new();
        let ram = b.kind("ram", Unit::Count);
        let n0 = b.node("n0", Labels::new());
        let n1 = b.node("n1", Labels::new());
        let r0 = b.pool("n0/ram", ram, n0, 10);
        let r1 = b.pool("n1/ram", ram, n1, 10);
        let x = b.job(Job::new("x").needs(ram, 4));
        let m = b.build().unwrap();
        let mut tr = Trace::new("hand");
        ev(&mut tr, 0, Event::Arrived { job: x });
        ev(
            &mut tr,
            0,
            Event::Started {
                job: x,
                assignment: Assignment(vec![r0]),
            },
        );
        ev(
            &mut tr,
            0,
            Event::Capacity {
                resource: r1,
                capacity: Some(5),
            },
        );
        ev(
            &mut tr,
            10,
            Event::Node {
                node: n0,
                up: false,
            },
        );
        ev(&mut tr, 10, Event::Requeued { job: x });
        ev(
            &mut tr,
            15,
            Event::Started {
                job: x,
                assignment: Assignment(vec![r1]),
            },
        );
        ev(
            &mut tr,
            15,
            Event::Phase {
                job: x,
                phase: Phase::Run,
            },
        );
        ev(
            &mut tr,
            25,
            Event::Phase {
                job: x,
                phase: Phase::Cleanup,
            },
        );
        ev(
            &mut tr,
            25,
            Event::Finished {
                job: x,
                outcome: Outcome::Ok,
            },
        );
        let x = from_trace(&m, &tr, &MetricsConfig::default());
        let j = &x.jobs[0];
        assert_eq!(
            (j.wait, j.setup, j.run, j.requeues),
            (Dur::secs(5), Dur::secs(10), Dur::secs(10), 1)
        );
        assert_eq!(j.started, Some(s(0)), "the first start");
        let (u0, u1) = (&x.resources[0], &x.resources[1]);
        assert_eq!(
            (u0.busy, u0.available),
            (40_000, 100_000),
            "no capacity while down"
        );
        assert_eq!(u0.utilisation, Some(0.4));
        assert_eq!(
            (u1.busy, u1.available),
            (40_000, 125_000),
            "the measured capacity"
        );
        assert!(x.little.holds());
    }

    #[test]
    fn an_overbooked_start_is_counted() {
        let (m, mut tr) = three();
        // b starts while a still holds the slot.
        let at = tr.records.iter().position(|r| r.t == s(12)).unwrap();
        let mut started = tr.records.remove(at + 1);
        started.t = s(10);
        tr.records.insert(6, started);
        let x = from_trace(&m, &tr, &MetricsConfig::default());
        assert_eq!(x.resources[0].overbooked_starts, 1);
        assert_eq!(x.resources[0].peak, 2);
    }

    #[test]
    fn live_measurement_survives_compaction() {
        let mut b = ModelBuilder::new();
        let k = b.kind("slot", Unit::Count);
        let n = b.node("n", Labels::new());
        b.pool("slot", k, n, 1);
        let mut live = Live::new(b.build().unwrap(), Time::ZERO);
        live.measure(MetricsConfig::default());
        live.record_events(true);
        let job = |name: &str| {
            Job::new(name)
                .needs(k, 1)
                .run(Estimate::fixed(Dur::secs(10)))
        };
        let a = live.submit(job("a")).unwrap();
        live.submit(job("b")).unwrap();
        let plan = live.plan(&mut FirstFit);
        live.start(&plan.batch()).unwrap();
        live.set_now(s(10)).unwrap();
        for _ in 0..3 {
            live.advance(a).unwrap();
        }
        let remap = live.compact(s(10));
        assert_eq!(remap.removed().collect::<Vec<_>>(), vec![a]);
        live.submit(job("c")).unwrap();
        let b_id = live.model().job_by_name("b").unwrap();
        let plan = live.plan(&mut FirstFit);
        assert_eq!(plan.starts[0].job, b_id);
        live.start(&plan.batch()).unwrap();
        live.set_now(s(20)).unwrap();
        live.fail(b_id).unwrap();
        live.advance(b_id).unwrap();
        let c = live.model().job_by_name("c").unwrap();
        live.cancel(c).unwrap();
        let x = live.metrics().unwrap();
        let got: Vec<(&str, Dur, Outcome)> = x
            .jobs
            .iter()
            .map(|j| (j.name.as_str(), j.wait, j.outcome))
            .collect();
        assert_eq!(
            got,
            [
                ("a", Dur::ZERO, Outcome::Ok),
                ("b", Dur::secs(10), Outcome::Failed),
                ("c", Dur::secs(10), Outcome::Cancelled),
            ]
        );
        assert!(x.little.holds());
        let events = live.drain_events();
        assert!(matches!(events[0].event, Event::Arrived { .. }));
        assert!(
            events
                .iter()
                .any(|r| r.event == Event::Compacted { removed: vec![a] })
        );
        assert!(live.drain_events().is_empty());
    }

    #[test]
    fn live_events_measure_like_the_live_collector() {
        let mut b = ModelBuilder::new();
        let ram = b.kind("ram", Unit::Count);
        let n0 = b.node("n0", Labels::new());
        let n1 = b.node("n1", Labels::new());
        let r0 = b.pool("n0/ram", ram, n0, 8);
        b.pool("n1/ram", ram, n1, 8);
        let base = b.build().unwrap();
        let mut live = Live::new(base, Time::ZERO);
        live.record_events(true);
        live.measure(MetricsConfig::default());
        let ids: Vec<JobId> = (0..4)
            .map(|i| {
                live.submit(Job::new(format!("j{i}")).needs(ram, 5))
                    .unwrap()
            })
            .collect();
        live.start(&live.plan(&mut FirstFit).batch()).unwrap();
        live.set_capacity(r0, Some(6));
        live.set_now(s(3)).unwrap();
        live.node_lost(n0, LostJobs::Requeue);
        live.advance(ids[1]).unwrap();
        live.set_now(s(4)).unwrap();
        live.set_node_up(n0, true);
        live.set_capacity(r0, None);
        loop {
            live.start(&live.plan(&mut FirstFit).batch()).unwrap();
            let running: Vec<JobId> = live.state().running().collect();
            if running.is_empty() {
                break;
            }
            let t = live.now() + Dur::secs(1);
            live.set_now(t).unwrap();
            for j in running {
                live.advance(j).unwrap();
            }
        }
        assert!(live.state().all_done());
        let mut tr = Trace::new("live");
        tr.records = live.drain_events();
        let from_events = from_trace(live.model(), &tr, &MetricsConfig::default());
        let mut collected = live.metrics().unwrap();
        collected.from = from_events.from;
        assert_eq!(collected, from_events);
        assert!(from_events.little.holds());
        assert_eq!(from_events.jobs.iter().map(|j| j.requeues).sum::<u32>(), 1);
    }

    #[test]
    fn student_t_quantiles() {
        for (df, want) in [
            (1.0, 12.706),
            (2.0, 4.303),
            (10.0, 2.228),
            (30.0, 2.042),
            (1e6, 1.960),
        ] {
            let t = t_quantile(0.975, df);
            assert!((t - want).abs() < 1e-3, "df {df}: {t}");
        }
        assert!((t_quantile(0.95, 5.0) - 2.015).abs() < 1e-3);
        assert_eq!(t_quantile(0.5, 3.0), 0.0);
        let ci = confidence_interval(&[1.0, 2.0, 3.0, 4.0], 0.95).unwrap();
        assert_eq!(ci.mean, 2.5);
        // s = 1.291, t(0.975, 3) = 3.182: 3.182 · 1.291 / 2.
        assert!((ci.half_width - 2.054).abs() < 1e-3, "{ci:?}");
        assert!(confidence_interval(&[1.0], 0.95).is_none());
    }

    #[test]
    fn comparison_on_one_slot() {
        let (m, tr) = three();
        let x = from_trace(&m, &tr, &MetricsConfig::default());
        let c = x.compare(1);
        let formulas: Vec<&str> = c.rows.iter().map(|r| r.formula.as_str()).collect();
        assert!(formulas.contains(&"Kingman G/G/1"));
        assert!(formulas.contains(&"Cobham M/G/1 priority"));
        let little = c.rows.iter().find(|r| r.quantity == "L").unwrap();
        assert!(
            (little.measured - little.predicted.unwrap()).abs() < 1e-12,
            "Little on an empty-ended run"
        );
        let rho = &c.rows[0];
        assert_eq!(rho.measured, 1.0);
        assert!(c.to_string().contains("Pollaczek"));
        assert!(
            x.compare(2)
                .rows
                .iter()
                .any(|r| r.formula == "Allen–Cunneen G/G/c")
        );
    }
}
