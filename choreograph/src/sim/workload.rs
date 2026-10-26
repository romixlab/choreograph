//! Open workloads: jobs made from templates, arriving by a random process.
//!
//! A simulation runs on a [`Model`] whose jobs carry their arrival times. [`Workload::build`] makes that model:
//! it keeps the base model's nodes, kinds and resources and either keeps its jobs as they are
//! ([`Arrivals::List`]) or replaces them by jobs drawn from templates at times drawn from an arrival process.

use std::collections::BTreeMap;

use rand_core::RngCore;
use serde::{Deserialize, Serialize};

use super::dist::{Dist, DistError, unit};
use super::{Purpose, stream};
use crate::model::{Job, JobId, Model, ModelBuilder, ModelError};
use crate::time::{Dur, Time};
use crate::trace::{Event, Trace};

/// Duration distributions for a job's phases; a phase without one uses its estimate
/// ([`Dist::from_estimate`]).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PhaseDists {
    pub setup: Option<Dist>,
    pub run: Option<Dist>,
    pub cleanup: Option<Dist>,
}

impl PhaseDists {
    /// Only the run phase given.
    pub fn run(d: Dist) -> PhaseDists {
        PhaseDists {
            run: Some(d),
            ..PhaseDists::default()
        }
    }

    pub(crate) fn check(&self) -> Result<(), DistError> {
        [&self.setup, &self.run, &self.cleanup]
            .into_iter()
            .flatten()
            .try_for_each(Dist::check)
    }
}

/// A kind of job in an open workload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Template {
    /// The job to copy. Its `release` and `due`, when set, count from the arrival (a due date 30 min after
    /// arrival is `due = Time(1_800_000)`). Its name gets the arrival number appended (`gate-17`). It may not
    /// have dependencies or a group.
    pub job: Job,
    /// Relative share of arrivals.
    pub weight: u32,
    /// Phase durations; phases without a distribution use the job's estimates.
    pub dists: PhaseDists,
}

/// When jobs arrive.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "arrivals", rename_all = "snake_case")]
pub enum Arrivals {
    /// The base model's own jobs at their `arrival` times (a fixed job list); templates are not used.
    List,
    /// A renewal process: `count` arrivals from `start` with independent gaps drawn from `gap`.
    Renewal { gap: Dist, count: usize },
    /// Arrivals at recorded times (trace replay); each picks a template by weight.
    Replay { times: Vec<Time> },
    /// A Markov-modulated Poisson process (regime switching): `count` arrivals from `start`, Poisson at the
    /// current regime's rate; a regime lasts an exponential time with its mean stay, then the process moves to
    /// one of the other regimes, each equally likely. It starts in `regimes[0]`. Busy and quiet stretches of a
    /// day: two regimes are the usual fit.
    Mmpp { regimes: Vec<Regime>, count: usize },
}

/// One regime of [`Arrivals::Mmpp`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Regime {
    /// Arrivals per hour while in it (0: none).
    pub per_hour: f64,
    /// Mean time spent in it per visit.
    pub mean_stay: Dur,
}

impl Arrivals {
    /// A Poisson process with this many arrivals per hour.
    pub fn poisson(per_hour: f64, count: usize) -> Arrivals {
        Arrivals::Renewal {
            gap: Dist::Exponential {
                mean: Dur(libm::round(3_600_000.0 / per_hour) as i64),
            },
            count,
        }
    }

    /// A renewal process with this mean gap and squared coefficient of variation of the gaps (c_a²): gamma
    /// gaps below 1, exponential at 1 (Poisson), hyperexponential above (bursts).
    pub fn renewal(mean_gap: Dur, ca2: f64, count: usize) -> Arrivals {
        Arrivals::Renewal {
            gap: Dist::with_cs2(mean_gap, ca2),
            count,
        }
    }

    /// Two regimes: busy at `busy_per_hour` for `busy_stay` on average, quiet at `quiet_per_hour` for
    /// `quiet_stay`, starting busy.
    pub fn mmpp2(
        busy_per_hour: f64,
        busy_stay: Dur,
        quiet_per_hour: f64,
        quiet_stay: Dur,
        count: usize,
    ) -> Arrivals {
        Arrivals::Mmpp {
            regimes: vec![
                Regime {
                    per_hour: busy_per_hour,
                    mean_stay: busy_stay,
                },
                Regime {
                    per_hour: quiet_per_hour,
                    mean_stay: quiet_stay,
                },
            ],
            count,
        }
    }

    /// The arrival times of a recorded trace, for replay.
    pub fn replay(trace: &Trace) -> Arrivals {
        let mut times: Vec<Time> = trace
            .records
            .iter()
            .filter(|r| matches!(r.event, Event::Arrived { .. }))
            .map(|r| r.t)
            .collect();
        times.sort();
        Arrivals::Replay { times }
    }
}

/// An open workload over a base model's resources.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Workload {
    pub templates: Vec<Template>,
    pub arrivals: Arrivals,
    /// When the first gap of a renewal process starts.
    pub start: Time,
}

/// Why a workload cannot be built.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum WorkloadError {
    #[error("no templates (or all with weight 0) for generated arrivals")]
    NoTemplates,
    #[error(
        "regime-switching arrivals need regimes with rates >= 0, at least one above 0, and stays > 0"
    )]
    Regimes,
    #[error("template {0:?} has dependencies or a group; generated jobs must stand alone")]
    Linked(String),
    #[error(transparent)]
    Dist(#[from] DistError),
    #[error(transparent)]
    Model(#[from] ModelError),
}

/// A built workload: the model to simulate and the duration distributions of its jobs.
#[derive(Clone, Debug, PartialEq)]
pub struct Generated {
    pub model: Model,
    /// Per job, the phase distributions from its template (for `SimConfig::durations`).
    pub durations: BTreeMap<JobId, PhaseDists>,
}

impl Workload {
    /// Builds the model. Arrival gaps come from the [`Purpose::Arrivals`] stream and template picks from the
    /// [`Purpose::Mix`] stream of `seed`, so the same seed gives the same jobs at the same times.
    pub fn build(&self, base: &Model, seed: u64) -> Result<Generated, WorkloadError> {
        let times: Vec<Time> = match &self.arrivals {
            Arrivals::List => {
                return Ok(Generated {
                    model: base.clone(),
                    durations: BTreeMap::new(),
                });
            }
            Arrivals::Renewal { gap, count } => {
                gap.check()?;
                let mut rng = stream(seed, Purpose::Arrivals, 0, 0);
                let mut t = self.start;
                (0..*count)
                    .map(|_| {
                        t += gap.sample(&mut rng);
                        t
                    })
                    .collect()
            }
            Arrivals::Replay { times } => times.clone(),
            Arrivals::Mmpp { regimes, count } => mmpp_times(regimes, *count, self.start, seed)?,
        };
        let total: u64 = self.templates.iter().map(|t| t.weight as u64).sum();
        if total == 0 && !times.is_empty() {
            return Err(WorkloadError::NoTemplates);
        }
        for t in &self.templates {
            if !t.job.after.is_empty() || t.job.group.is_some() {
                return Err(WorkloadError::Linked(t.job.name.clone()));
            }
            t.dists.check()?;
        }
        let mut b = ModelBuilder::new();
        for n in base.nodes() {
            b.node(n.name.clone(), n.labels.clone());
        }
        for k in base.kinds() {
            b.kind(k.name.clone(), k.unit);
        }
        for r in base.resources() {
            b.resource(r.clone());
        }
        let mut mix = stream(seed, Purpose::Mix, 0, 0);
        let mut durations = BTreeMap::new();
        for (i, at) in times.into_iter().enumerate() {
            let tpl = pick(&self.templates, total, &mut mix);
            let mut job = tpl.job.clone();
            job.name = format!("{}-{i}", tpl.job.name);
            job.arrival = at;
            job.release = job.release.map(|r| at + (r - Time::ZERO));
            job.due = job.due.map(|d| at + (d - Time::ZERO));
            let id = b.job(job);
            if tpl.dists != PhaseDists::default() {
                durations.insert(id, tpl.dists.clone());
            }
        }
        Ok(Generated {
            model: b.build()?,
            durations,
        })
    }
}

/// The arrival times of a regime-switching process, from the [`Purpose::Arrivals`] stream: per step one draw
/// for the gap to the next arrival at the current rate and, on entering a regime, one for its stay and one for
/// the next regime. Times are kept in fractional milliseconds and rounded once.
fn mmpp_times(
    regimes: &[Regime],
    count: usize,
    start: Time,
    seed: u64,
) -> Result<Vec<Time>, WorkloadError> {
    let ok = !regimes.is_empty()
        && regimes
            .iter()
            .all(|r| r.per_hour.is_finite() && r.per_hour >= 0.0 && r.mean_stay.0 > 0)
        && regimes.iter().any(|r| r.per_hour > 0.0);
    if !ok {
        return Err(WorkloadError::Regimes);
    }
    let mut rng = stream(seed, Purpose::Arrivals, 0, 0);
    let exp = |rng: &mut _, mean: f64| -libm::log(unit(rng)) * mean;
    let mut t = start.0 as f64;
    let mut at = 0usize;
    let mut leave = t + exp(&mut rng, regimes[0].mean_stay.0 as f64);
    let mut times = Vec::with_capacity(count);
    while times.len() < count {
        let r = &regimes[at];
        let next = if r.per_hour > 0.0 {
            t + exp(&mut rng, 3_600_000.0 / r.per_hour)
        } else {
            f64::INFINITY
        };
        if next < leave {
            t = next;
            times.push(Time(libm::round(t) as i64));
        } else {
            t = leave;
            if regimes.len() > 1 {
                let k = (unit(&mut rng) * (regimes.len() - 1) as f64) as usize;
                at = if k >= at { k + 1 } else { k }.min(regimes.len() - 1);
            }
            leave = t + exp(&mut rng, regimes[at].mean_stay.0 as f64);
        }
    }
    Ok(times)
}

/// A template by weight (one uniform draw per arrival).
fn pick<'a>(templates: &'a [Template], total: u64, rng: &mut impl RngCore) -> &'a Template {
    let mut x = (unit(rng) * total as f64) as u64;
    for t in templates {
        if x < t.weight as u64 {
            return t;
        }
        x -= t.weight as u64;
    }
    templates.iter().rfind(|t| t.weight > 0).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Labels, Unit};

    fn base() -> (Model, crate::model::KindId) {
        let mut b = ModelBuilder::new();
        let slot = b.kind("slot", Unit::Count);
        let n = b.node("n", Labels::new());
        b.pool("slot", slot, n, 1);
        (b.build().unwrap(), slot)
    }

    #[test]
    fn renewal_gaps_have_the_chosen_rate_and_variability() {
        let (m, slot) = base();
        for ca2 in [0.5, 1.0, 2.05] {
            let w = Workload {
                templates: vec![Template {
                    job: Job::new("j").needs(slot, 1),
                    weight: 1,
                    dists: PhaseDists::default(),
                }],
                arrivals: Arrivals::renewal(Dur::secs(60), ca2, 100_000),
                start: Time::ZERO,
            };
            let g = w.build(&m, 3).unwrap();
            let t: Vec<f64> = g.model.jobs().iter().map(|j| j.arrival.0 as f64).collect();
            let gaps: Vec<f64> = std::iter::once(t[0])
                .chain(t.windows(2).map(|w| w[1] - w[0]))
                .collect();
            let n = gaps.len() as f64;
            let mean = gaps.iter().sum::<f64>() / n;
            let var = gaps.iter().map(|g| (g - mean).powi(2)).sum::<f64>() / n;
            assert!((mean / 60_000.0 - 1.0).abs() < 0.01, "mean {mean}");
            assert!(
                (var / (mean * mean) / ca2 - 1.0).abs() < 0.05,
                "c² {} for {ca2}",
                var / (mean * mean)
            );
        }
    }

    #[test]
    fn templates_mix_by_weight_and_offsets_count_from_arrival() {
        let (m, slot) = base();
        let mut hi = Job::new("hi").needs(slot, 1).class(1);
        hi.due = Some(Time(1000));
        let w = Workload {
            templates: vec![
                Template {
                    job: hi,
                    weight: 1,
                    dists: PhaseDists::run(Dist::Exponential { mean: Dur::secs(1) }),
                },
                Template {
                    job: Job::new("lo").needs(slot, 1),
                    weight: 3,
                    dists: PhaseDists::default(),
                },
            ],
            arrivals: Arrivals::poisson(60.0, 4000),
            start: Time::ZERO,
        };
        let g = w.build(&m, 9).unwrap();
        let hi: Vec<&Job> = g.model.jobs().iter().filter(|j| j.class == 1).collect();
        let share = hi.len() as f64 / 4000.0;
        assert!((share - 0.25).abs() < 0.02, "share {share}");
        assert!(hi.iter().all(|j| j.due == Some(j.arrival + Dur(1000))));
        assert_eq!(g.durations.len(), hi.len(), "only templates with dists");
        assert_eq!(g.model.jobs()[0].name.split('-').nth(1), Some("0"));
        // Same seed, same jobs; another seed, other times.
        assert_eq!(w.build(&m, 9).unwrap(), g);
        assert_ne!(w.build(&m, 10).unwrap().model, g.model);
    }

    #[test]
    fn replay_and_list() {
        let (m, slot) = base();
        let mut tr = Trace::new("test");
        tr.push(Time(5), Event::Arrived { job: JobId(0) });
        tr.push(Time(9), Event::Arrived { job: JobId(1) });
        let w = Workload {
            templates: vec![Template {
                job: Job::new("j").needs(slot, 1),
                weight: 1,
                dists: PhaseDists::default(),
            }],
            arrivals: Arrivals::replay(&tr),
            start: Time::ZERO,
        };
        let g = w.build(&m, 0).unwrap();
        let at: Vec<Time> = g.model.jobs().iter().map(|j| j.arrival).collect();
        assert_eq!(at, vec![Time(5), Time(9)]);
        let list = Workload {
            arrivals: Arrivals::List,
            ..w
        };
        assert_eq!(list.build(&m, 0).unwrap().model, m);
    }

    #[test]
    fn linked_templates_are_refused() {
        let (m, slot) = base();
        let w = Workload {
            templates: vec![Template {
                job: Job::new("j").needs(slot, 1).after(JobId(0)),
                weight: 1,
                dists: PhaseDists::default(),
            }],
            arrivals: Arrivals::poisson(1.0, 2),
            start: Time::ZERO,
        };
        assert!(matches!(w.build(&m, 0), Err(WorkloadError::Linked(_))));
    }

    #[test]
    fn mmpp_matches_its_rate_and_dispersion() {
        let (m, slot) = base();
        // Busy 10/h for 2 h, quiet 1/h for 6 h: λ̄ = 3.25/h, index of dispersion 15.0.
        let w = Workload {
            templates: vec![Template {
                job: Job::new("j").needs(slot, 1),
                weight: 1,
                dists: PhaseDists::default(),
            }],
            arrivals: Arrivals::mmpp2(10.0, Dur::secs(7200), 1.0, Dur::secs(6 * 3600), 200_000),
            start: Time::ZERO,
        };
        let g = w.build(&m, 5).unwrap();
        let t: Vec<i64> = g.model.jobs().iter().map(|j| j.arrival.0).collect();
        assert!(t.windows(2).all(|w| w[0] <= w[1]));
        let hours = *t.last().unwrap() as f64 / 3_600_000.0;
        let rate = t.len() as f64 / hours;
        assert!((rate / 3.25 - 1.0).abs() < 0.03, "rate {rate}");
        // Counts in 48 h windows: their variance over their mean nears the asymptotic index.
        let win = 48 * 3_600_000;
        let mut counts = vec![0f64; (t.last().unwrap() / win) as usize];
        for x in &t {
            if let Some(c) = counts.get_mut((x / win) as usize) {
                *c += 1.0;
            }
        }
        let n = counts.len() as f64;
        let mean = counts.iter().sum::<f64>() / n;
        let var = counts.iter().map(|c| (c - mean).powi(2)).sum::<f64>() / n;
        let idc = crate::theory::mmpp2_idc(10.0, 1.0, 0.5, 1.0 / 6.0);
        assert!(
            (var / mean / idc - 1.0).abs() < 0.2,
            "idc {} vs {idc}",
            var / mean
        );
        assert_eq!(w.build(&m, 5).unwrap(), g, "same seed, same times");
        let bad = Workload {
            arrivals: Arrivals::mmpp2(0.0, Dur::secs(1), 0.0, Dur::secs(1), 3),
            ..w
        };
        assert!(matches!(bad.build(&m, 0), Err(WorkloadError::Regimes)));
    }
}
