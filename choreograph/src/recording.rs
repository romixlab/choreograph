//! Recordings of real history, and the profile of a group of them.
//!
//! A [`Recording`] is one stretch of real history (a day, usually) turned into a model and a trace: each job
//! arrived, started and finished at the recorded times, with free-form tags (`day`, `kind`, `source`) that group
//! recordings. [`Profile::measure`] reads a group and gives the numbers queueing theory takes: the arrival rate
//! while active, how bursty arrivals are (c_a² of the gaps, the index of dispersion of counts, a two-regime fit),
//! service times and failures, and the same per lane. A lane is the part of a job's name before `#`
//! (`repo-a#12` is in lane `repo-a`): one shared resource that serves its jobs one at a time.
//!
//! A job that only arrived (no start) counts for arrivals and nothing else: history that knows when work came
//! in, not how long it took.
//!
//! [`Profile::predicted_wq`] puts Kingman's formula per lane next to the measured wait, and
//! [`Profile::simulate`] runs the lanes in the simulator under the fitted arrivals.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::metrics::{self, MetricsConfig, Summary};
use crate::model::{JobId, Labels, Model, ModelBuilder, Need, Resource, Selector, Unit};
use crate::plan::FirstFit;
use crate::sim::{
    Arrivals, Dist, PhaseDists, SimConfig, SimError, Template, Workload, WorkloadError,
};
use crate::state::Outcome;
use crate::theory;
use crate::time::{Dur, Time};
use crate::trace::{Event, Trace};

/// One stretch of real history: what ran on which resources, and when.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Recording {
    /// Free-form tags that group recordings (`day = 2026-10-22`, `kind = busy`, `source = journal`).
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    pub model: Model,
    pub trace: Trace,
}

impl Recording {
    /// The job's lane: its name up to the first `#`.
    pub fn lane(&self, job: JobId) -> &str {
        lane_of(&self.model.job(job).name)
    }
}

fn lane_of(name: &str) -> &str {
    name.split_once('#').map_or(name, |(l, _)| l)
}

/// How [`Profile::measure`] reads recordings.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ProfileSettings {
    /// The width of the bins arrivals are counted in, for the index of dispersion and the regime fit.
    pub bin: Dur,
}

impl Default for ProfileSettings {
    fn default() -> Self {
        ProfileSettings {
            bin: Dur(3_600_000),
        }
    }
}

/// The numbers of one lane.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Lane {
    pub arrivals: u64,
    /// Arrivals per active hour of the whole group.
    pub per_hour: f64,
    /// c_a² of the gaps between this lane's arrivals within a recording.
    pub ca2: f64,
    /// Service times, start to finish, of the jobs that started and finished.
    pub service: Summary,
    /// Every service time, for an empirical distribution.
    pub samples: Vec<Dur>,
    /// Finished `Ok` or `Failed`.
    pub ran: u64,
    pub failed: u64,
}

/// A two-regime arrival process fitted to binned counts: bins above the mean count are busy, the others quiet;
/// each regime's rate is the mean count of its bins, its stay the mean length of its runs of bins.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Mmpp2Fit {
    pub busy_per_hour: f64,
    pub busy_stay: Dur,
    pub quiet_per_hour: f64,
    pub quiet_stay: Dur,
}

impl Mmpp2Fit {
    fn rates(&self) -> (f64, f64, f64, f64) {
        let per_hour = |d: Dur| 3_600_000.0 / d.0 as f64;
        (
            self.busy_per_hour,
            self.quiet_per_hour,
            per_hour(self.busy_stay),
            per_hour(self.quiet_stay),
        )
    }

    /// The long-run arrival rate per hour ([`theory::mmpp2_rate`]).
    pub fn per_hour(&self) -> f64 {
        let (a, b, r1, r2) = self.rates();
        theory::mmpp2_rate(a, b, r1, r2)
    }

    /// The asymptotic index of dispersion ([`theory::mmpp2_idc`]).
    pub fn idc(&self) -> f64 {
        let (a, b, r1, r2) = self.rates();
        theory::mmpp2_idc(a, b, r1, r2)
    }

    /// The arrival process for the simulator, starting busy.
    pub fn arrivals(&self, count: usize) -> Arrivals {
        Arrivals::mmpp2(
            self.busy_per_hour,
            self.busy_stay,
            self.quiet_per_hour,
            self.quiet_stay,
            count,
        )
    }
}

/// The measured numbers of a group of recordings. Rates are per hour, [`Summary`] means in seconds.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Profile {
    pub recordings: u64,
    /// Active hours: per recording, the bins from its first to its last arrival.
    pub hours: f64,
    pub arrivals: u64,
    /// Arrivals per active hour.
    pub per_hour: f64,
    /// c_a² of the gaps between arrivals within a recording.
    pub ca2: f64,
    /// Index of dispersion of the binned counts, variance over mean (1 for Poisson).
    pub idc: f64,
    pub service: Summary,
    /// Finished `Ok` or `Failed`.
    pub ran: u64,
    pub failed: u64,
    /// Started and never finished, or cancelled.
    pub aborted: u64,
    /// Arrival to start, of the jobs that started.
    pub wait: Summary,
    pub lanes: BTreeMap<String, Lane>,
    /// `None` when every bin had the same count.
    pub mmpp: Option<Mmpp2Fit>,
}

#[derive(Default)]
struct JobTimes {
    arrived: Option<Time>,
    started: Option<Time>,
    finished: Option<(Time, Outcome)>,
    cancelled: bool,
}

/// Mean and c² (variance over mean squared) of a sample.
fn mean_cv2(v: &[f64]) -> (f64, f64) {
    if v.is_empty() {
        return (0.0, 0.0);
    }
    let n = v.len() as f64;
    let mean = v.iter().sum::<f64>() / n;
    let var = v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
    (mean, if mean > 0.0 { var / (mean * mean) } else { 0.0 })
}

fn gaps(times: &mut [Time]) -> impl Iterator<Item = f64> + '_ {
    times.sort();
    times.windows(2).map(|w| (w[1].0 - w[0].0) as f64)
}

impl Profile {
    /// Measures a group of recordings.
    pub fn measure<'a>(
        recordings: impl IntoIterator<Item = &'a Recording>,
        s: &ProfileSettings,
    ) -> Profile {
        let bin = s.bin.0.max(1);
        let bin_hours = bin as f64 / 3_600_000.0;
        let mut p = Profile::default();
        let mut all_gaps = Vec::new();
        let mut lane_gaps: BTreeMap<String, Vec<f64>> = BTreeMap::new();
        // Per recording, the count of each bin in its active span.
        let mut binned: Vec<Vec<u64>> = Vec::new();
        let mut waits = Vec::new();
        let mut services = Vec::new();
        for rec in recordings {
            p.recordings += 1;
            let mut jobs: BTreeMap<JobId, JobTimes> = BTreeMap::new();
            for r in &rec.trace.records {
                match &r.event {
                    Event::Arrived { job } => jobs.entry(*job).or_default().arrived = Some(r.t),
                    Event::Started { job, .. } => jobs.entry(*job).or_default().started = Some(r.t),
                    Event::Finished { job, outcome } => {
                        jobs.entry(*job).or_default().finished = Some((r.t, *outcome))
                    }
                    Event::Cancelled { job } => jobs.entry(*job).or_default().cancelled = true,
                    _ => {}
                }
            }
            let mut arrivals = Vec::new();
            let mut by_lane: BTreeMap<&str, Vec<Time>> = BTreeMap::new();
            for (id, j) in &jobs {
                let lane = rec.lane(*id);
                let l = p.lanes.entry(lane.to_string()).or_default();
                if let Some(a) = j.arrived {
                    arrivals.push(a);
                    by_lane.entry(lane).or_default().push(a);
                    l.arrivals += 1;
                    if let Some(st) = j.started {
                        waits.push(st - a);
                    }
                }
                match (j.started, j.finished) {
                    (Some(st), Some((fin, outcome))) if !j.cancelled => {
                        let d = fin - st;
                        l.samples.push(d);
                        services.push(d);
                        if matches!(outcome, Outcome::Ok | Outcome::Failed) {
                            l.ran += 1;
                            p.ran += 1;
                        }
                        if outcome == Outcome::Failed {
                            l.failed += 1;
                            p.failed += 1;
                        }
                    }
                    (Some(_), _) => p.aborted += 1,
                    _ => {}
                }
            }
            p.arrivals += arrivals.len() as u64;
            all_gaps.extend(gaps(&mut arrivals));
            for (lane, mut t) in by_lane {
                lane_gaps
                    .entry(lane.to_string())
                    .or_default()
                    .extend(gaps(&mut t));
            }
            if let (Some(first), Some(last)) = (arrivals.first(), arrivals.last()) {
                let from = first.0.div_euclid(bin);
                let mut counts = vec![0u64; (last.0.div_euclid(bin) - from + 1) as usize];
                for a in &arrivals {
                    counts[(a.0.div_euclid(bin) - from) as usize] += 1;
                }
                binned.push(counts);
            }
        }
        let bins: Vec<f64> = binned.iter().flatten().map(|&c| c as f64).collect();
        p.hours = bins.len() as f64 * bin_hours;
        if p.hours > 0.0 {
            p.per_hour = p.arrivals as f64 / p.hours;
        }
        p.ca2 = mean_cv2(&all_gaps).1;
        let (mean_count, cv2) = mean_cv2(&bins);
        p.idc = cv2 * mean_count;
        p.mmpp = fit_mmpp2(&binned, mean_count, bin);
        p.service = Summary::of(services);
        p.wait = Summary::of(waits);
        for (name, l) in &mut p.lanes {
            if p.hours > 0.0 {
                l.per_hour = l.arrivals as f64 / p.hours;
            }
            l.ca2 = lane_gaps.get(name).map_or(0.0, |g| mean_cv2(g).1);
            l.samples.sort();
            l.service = Summary::of(l.samples.iter().copied());
        }
        p
    }

    /// The failure probability of a run, failed over ran (0 with no runs).
    pub fn p_fail(&self) -> f64 {
        if self.ran == 0 {
            0.0
        } else {
            self.failed as f64 / self.ran as f64
        }
    }

    /// The mean wait in queue Kingman's G/G/1 formula predicts, per lane with its own rate, service and c_a²
    /// (or `ca2` for every lane when given), weighted by arrivals; in seconds. Lanes without service times are
    /// left out; `None` when no lane is left or one is overloaded.
    pub fn predicted_wq(&self, ca2: Option<f64>) -> Option<f64> {
        let mut sum = 0.0;
        let mut n = 0u64;
        for l in self.lanes.values().filter(|l| l.service.n > 0) {
            let lambda = l.per_hour / 3600.0;
            let w =
                theory::kingman_wq(lambda, l.service.mean, ca2.unwrap_or(l.ca2), l.service.cv2)?;
            sum += w * l.arrivals as f64;
            n += l.arrivals;
        }
        (n > 0).then(|| sum / n as f64)
    }

    /// The lane with the highest arrival rate among those with service times.
    pub fn hottest(&self) -> Option<(&str, &Lane)> {
        self.lanes
            .iter()
            .filter(|(_, l)| l.service.n > 0)
            .max_by(|a, b| a.1.per_hour.total_cmp(&b.1.per_hour).then(b.0.cmp(a.0)))
            .map(|(n, l)| (n.as_str(), l))
    }

    /// The arrival rate per hour in the busy regime (the mean rate without a fit).
    pub fn busy_per_hour(&self) -> f64 {
        self.mmpp.map_or(self.per_hour, |m| m.busy_per_hour)
    }

    /// Runs the lanes in the simulator: one exclusive resource per lane, and when `slots` is given a pool of
    /// that many gate slots every job needs too; arrivals from the regime fit (Poisson at the measured rate
    /// without one), lanes picked by their share of arrivals, service drawn from each lane's recorded times.
    /// The measured metrics of the run come back; `None` when no lane has service times.
    pub fn simulate(
        &self,
        slots: Option<u32>,
        s: &ReplaySettings,
    ) -> Result<Option<metrics::Metrics>, ProfileSimError> {
        let lanes: Vec<(&String, &Lane)> = self
            .lanes
            .iter()
            .filter(|(_, l)| l.service.n > 0 && l.arrivals > 0)
            .collect();
        if lanes.is_empty() || self.per_hour <= 0.0 {
            return Ok(None);
        }
        let mut b = ModelBuilder::new();
        let node = b.node("fleet", Labels::new());
        let main = b.kind("main", Unit::Count);
        let gate = slots.map(|c| {
            let k = b.kind("gate", Unit::Count);
            b.pool("gate", k, node, c as u64);
            k
        });
        let mut templates = Vec::new();
        for (name, l) in lanes {
            let r = b.resource(Resource {
                name: format!("main:{name}"),
                kind: main,
                node,
                capacity: 1,
                labels: Labels::new(),
                states: None,
            });
            let mut job = crate::model::Job::new(name.as_str());
            job.needs.push(Need {
                kind: main,
                amount: 1,
                select: Selector {
                    resource: Some(r),
                    ..Selector::default()
                },
                state: None,
                same_node: None,
            });
            if let Some(k) = gate {
                job = job.needs(k, 1);
            }
            templates.push(Template {
                job,
                weight: l.arrivals.min(u32::MAX as u64) as u32,
                dists: PhaseDists::run(Dist::Empirical {
                    samples: l.samples.clone(),
                }),
            });
        }
        let base = b.build()?;
        let arrivals = match self.mmpp {
            Some(m) => m.arrivals(s.jobs),
            None => Arrivals::poisson(self.per_hour, s.jobs),
        };
        let generated = Workload {
            templates,
            arrivals,
            start: Time::ZERO,
        }
        .build(&base, s.seed)?;
        let cfg = SimConfig {
            seed: s.seed,
            durations: generated.durations,
            record_waits: false,
            label: "profile".into(),
            ..SimConfig::default()
        };
        let run = crate::sim::simulate(&generated.model, &mut FirstFit, &cfg)?;
        let mcfg = MetricsConfig {
            warmup: s.warmup,
            no_queue_series: true,
        };
        Ok(Some(metrics::from_trace(
            &generated.model,
            &run.trace,
            &mcfg,
        )))
    }
}

/// Settings of [`Profile::simulate`].
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplaySettings {
    /// Arrivals per simulated run.
    pub jobs: usize,
    /// The seed of the run.
    pub seed: u64,
    /// Jobs that arrive before this much simulated time are left out of the metrics.
    pub warmup: Dur,
}

impl Default for ReplaySettings {
    fn default() -> Self {
        ReplaySettings {
            jobs: 5000,
            seed: 1,
            warmup: Dur(8 * 3_600_000),
        }
    }
}

/// Why a profile cannot be simulated.
#[derive(Debug, thiserror::Error)]
pub enum ProfileSimError {
    #[error(transparent)]
    Model(#[from] crate::model::ModelError),
    #[error(transparent)]
    Workload(#[from] WorkloadError),
    #[error(transparent)]
    Sim(#[from] SimError),
}

/// The two-regime fit of binned counts (see [`Mmpp2Fit`]); runs do not cross recordings.
fn fit_mmpp2(binned: &[Vec<u64>], mean: f64, bin: i64) -> Option<Mmpp2Fit> {
    let bin_hours = bin as f64 / 3_600_000.0;
    // Per regime (busy, quiet): bins, arrivals, runs.
    let mut acc = [(0u64, 0u64, 0u64); 2];
    for counts in binned {
        let mut prev = None;
        for &c in counts {
            let r = usize::from(c as f64 <= mean);
            acc[r].0 += 1;
            acc[r].1 += c;
            if prev != Some(r) {
                acc[r].2 += 1;
            }
            prev = Some(r);
        }
    }
    let [busy, quiet] = acc;
    if busy.0 == 0 || quiet.0 == 0 {
        return None;
    }
    let rate = |a: (u64, u64, u64)| a.1 as f64 / a.0 as f64 / bin_hours;
    let stay = |a: (u64, u64, u64)| Dur((a.0 as f64 / a.2 as f64 * bin as f64).round() as i64);
    Some(Mmpp2Fit {
        busy_per_hour: rate(busy),
        busy_stay: stay(busy),
        quiet_per_hour: rate(quiet),
        quiet_stay: stay(quiet),
    })
}

/// What the profiles of every group pick for the gate slots and the batch size: sized for the worst group,
/// the one whose busy regime predicts the longest wait, not for the average.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Pick {
    /// The group the numbers are for.
    pub worst: String,
    /// Its busy-regime arrival rate per hour, and the c_a² used (the regime fit's index of dispersion).
    pub busy_per_hour: f64,
    pub ca2: f64,
    /// Gate slots for the whole fleet ([`theory::pick_slots`]); `None` when even the most allowed do not do.
    pub slots: Option<u32>,
    /// The hottest lane of the worst group, its busy-regime rate per hour, the batch size picked for it
    /// ([`theory::pick_batch`]) and an entry's mean time to its result in seconds.
    pub lane: Option<String>,
    pub lane_busy_per_hour: f64,
    pub batch: Option<(u32, f64)>,
}

/// The pick for the worst of these groups (see [`Pick`]); `None` when no group has service times. Times in
/// [`theory::PickSettings`] are in seconds here.
pub fn pick<'a>(
    groups: impl IntoIterator<Item = (&'a str, &'a Profile)>,
    s: &theory::PickSettings,
) -> Option<Pick> {
    let mut worst: Option<(&str, &Profile, f64)> = None;
    for (name, p) in groups {
        if p.service.n == 0 {
            continue;
        }
        let busy = p.busy_per_hour();
        let ca2 = p.mmpp.map_or(p.ca2, |m| m.idc());
        let w = theory::kingman_wq(busy / 3600.0, p.service.mean, ca2, p.service.cv2)
            .unwrap_or(f64::INFINITY);
        if worst.is_none_or(|(_, _, ww)| w > ww) {
            worst = Some((name, p, w));
        }
    }
    let (name, p, _) = worst?;
    let busy = p.busy_per_hour();
    let ca2 = p.mmpp.map_or(p.ca2, |m| m.idc());
    let slots = theory::pick_slots(busy / 3600.0, p.service.mean, ca2, p.service.cv2, s);
    let hot = p.hottest();
    let lane_busy = hot.map_or(0.0, |(_, l)| {
        if p.per_hour > 0.0 {
            busy * l.per_hour / p.per_hour
        } else {
            0.0
        }
    });
    let batch = hot.and_then(|(_, l)| {
        let pf = if l.ran > 0 {
            l.failed as f64 / l.ran as f64
        } else {
            p.p_fail()
        };
        theory::pick_batch(
            lane_busy / 3600.0,
            l.service.mean,
            ca2,
            l.service.cv2,
            pf,
            s,
        )
    });
    Some(Pick {
        worst: name.to_string(),
        busy_per_hour: busy,
        ca2,
        slots,
        lane: hot.map(|(n, _)| n.to_string()),
        lane_busy_per_hour: lane_busy,
        batch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Assignment;

    /// A recording of one lane's jobs: (lane, arrived, started, finished, ok) in seconds; `started` None for an
    /// arrival only.
    fn rec(jobs: &[(&str, i64, Option<i64>, i64, bool)]) -> Recording {
        let mut b = ModelBuilder::new();
        b.node("n", Labels::new());
        let mut events = Vec::new();
        for (i, (lane, a, st, fin, ok)) in jobs.iter().enumerate() {
            let id = b.job(crate::model::Job::new(format!("{lane}#{i}")));
            events.push((*a * 1000, Event::Arrived { job: id }));
            if let Some(st) = st {
                events.push((
                    *st * 1000,
                    Event::Started {
                        job: id,
                        assignment: Assignment(Vec::new()),
                    },
                ));
                let outcome = if *ok { Outcome::Ok } else { Outcome::Failed };
                events.push((*fin * 1000, Event::Finished { job: id, outcome }));
            }
        }
        events.sort_by_key(|e| e.0);
        let mut trace = Trace::new("test");
        for (t, e) in events {
            trace.push(Time(t), e);
        }
        Recording {
            tags: BTreeMap::new(),
            model: b.build().unwrap(),
            trace,
        }
    }

    #[test]
    fn measures_rates_service_failures_and_lanes() {
        let h = 3600;
        let r = rec(&[
            ("a", 0, Some(60), 360, true),
            ("a", 600, Some(600), 900, false),
            ("b", 1200, Some(1300), 1400, true),
            ("a", 2 * h, None, 0, true),
        ]);
        let p = Profile::measure([&r], &ProfileSettings::default());
        assert_eq!((p.recordings, p.arrivals), (1, 4));
        // Bins 0..=2: three hours, 4/3 per hour; counts 3, 0, 1.
        assert!((p.hours - 3.0).abs() < 1e-12);
        assert!((p.per_hour - 4.0 / 3.0).abs() < 1e-12);
        assert!(
            (p.idc - (14.0 / 9.0) / (4.0 / 3.0)).abs() < 1e-9,
            "{}",
            p.idc
        );
        assert_eq!((p.ran, p.failed, p.aborted), (3, 1, 0));
        assert!((p.p_fail() - 1.0 / 3.0).abs() < 1e-12);
        assert_eq!(p.service.n, 3);
        assert!((p.service.mean - 700.0 / 3.0).abs() < 1e-9);
        assert_eq!(p.wait.n, 3);
        let a = &p.lanes["a"];
        assert_eq!((a.arrivals, a.ran, a.failed, a.service.n), (3, 2, 1, 2));
        assert!((a.per_hour - 1.0).abs() < 1e-12);
        assert_eq!(p.hottest().unwrap().0, "a");
        // Busy: the bin of 3 (above the mean 4/3); quiet: 0 and 1.
        let m = p.mmpp.unwrap();
        assert!((m.busy_per_hour - 3.0).abs() < 1e-12);
        assert!((m.quiet_per_hour - 0.5).abs() < 1e-12);
        assert_eq!(
            (m.busy_stay, m.quiet_stay),
            (Dur(3_600_000), Dur(7_200_000))
        );
    }

    #[test]
    fn prediction_simulation_and_pick() {
        // Two days of one lane: 5-minute jobs, one every 20 minutes in the morning, rare in the afternoon.
        let mut days = Vec::new();
        for _ in 0..2 {
            let mut jobs = Vec::new();
            for i in 0..12 {
                let a = i * 1200;
                jobs.push(("a", a, Some(a), a + 300 + (i % 3) * 60, i % 5 != 0));
            }
            for i in 0..2 {
                let a = 5 * 3600 + i * 7200;
                jobs.push(("a", a, Some(a), a + 300, true));
            }
            days.push(rec(&jobs));
        }
        let p = Profile::measure(&days, &ProfileSettings::default());
        assert!(p.mmpp.is_some());
        let wq = p.predicted_wq(None).unwrap();
        assert!(wq > 0.0 && wq < 300.0, "{wq}");
        let m = p
            .simulate(Some(2), &ReplaySettings::default())
            .unwrap()
            .unwrap();
        assert!(m.wait.n > 1000);
        let s = theory::PickSettings {
            target_wq: 600.0,
            ..theory::PickSettings::default()
        };
        let quiet = Profile::measure(&days[..1], &ProfileSettings::default());
        let pk = pick([("quiet", &quiet), ("busy", &p)], &s).unwrap();
        assert_eq!(pk.lane.as_deref(), Some("a"));
        assert!(pk.slots.is_some());
        assert_eq!(pk.batch.unwrap().0, 1);
    }
}
