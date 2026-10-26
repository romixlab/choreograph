//! Long simulated runs against the queueing formulas of `theory`.
//!
//! Each check runs an open workload on one pool of `c` identical slots with the `FirstFit` planner (first come,
//! first served) and compares the mean waiting time in queue W_q, measured from the trace, with the formula.
//! The first tenth of the jobs is dropped as warm-up (the system starts empty). Every run has 200 000 jobs and
//! is seeded, so each check gives the same numbers every time. Each tolerance is three standard deviations of
//! the relative error, measured over twelve seeds at this run length (waits of consecutive jobs are strongly
//! correlated near full load, so the error shrinks slower than for independent samples).

use std::collections::BTreeMap;

use choreograph::metrics::{MetricsConfig, from_trace};
use choreograph::model::Model;
use choreograph::model::{Job, Labels, ModelBuilder, Unit};
use choreograph::sim::SimRun;
use choreograph::sim::{Arrivals, Dist, PhaseDists, SimConfig, Template, Workload, simulate};
use choreograph::theory;
use choreograph::{Dur, FirstFit, Time, Trace, trace::Event};

const JOBS: usize = 200_000;

/// Mean wait in queue (start − arrival) in seconds and the share of jobs that waited at all, over the jobs
/// after the warm-up.
fn waits(tr: &Trace, jobs: usize) -> (f64, f64) {
    let mut arrived = BTreeMap::new();
    let mut waits = Vec::with_capacity(jobs);
    for r in &tr.records {
        match &r.event {
            Event::Arrived { job } => {
                arrived.insert(*job, r.t);
            }
            Event::Started { job, .. } if job.index() >= jobs / 10 => {
                waits.push((r.t - arrived[job]).as_secs_f64());
            }
            _ => {}
        }
    }
    assert_eq!(waits.len(), jobs - jobs / 10, "every job ran");
    let n = waits.len() as f64;
    let waited = waits.iter().filter(|w| **w > 0.0).count() as f64;
    (waits.iter().sum::<f64>() / n, waited / n)
}

/// Simulates Poisson arrivals at utilisation `rho` on `c` slots with service times from `service`; returns
/// the measured W_q in seconds, the share of jobs that waited, and the arrival rate per second.
fn run(c: u64, rho: f64, service: Dist) -> (f64, f64, f64) {
    let (_, r, lambda) = sim(c, rho, service);
    let (wq, waited) = waits(&r.trace, JOBS);
    (wq, waited, lambda)
}

/// The run behind [`run`]: the model, the finished run and the arrival rate per second.
fn sim(c: u64, rho: f64, service: Dist) -> (Model, SimRun, f64) {
    let jobs = JOBS;
    let seed = 1;
    let mean_s = service.mean_ms() / 1000.0;
    let lambda = rho * c as f64 / mean_s;
    let mut b = ModelBuilder::new();
    let slot = b.kind("slot", Unit::Count);
    let n = b.node("n", Labels::new());
    b.pool("slots", slot, n, c);
    let base = b.build().unwrap();
    let w = Workload {
        templates: vec![Template {
            job: Job::new("job").needs(slot, 1),
            weight: 1,
            dists: PhaseDists::run(service),
        }],
        arrivals: Arrivals::poisson(lambda * 3600.0, jobs),
        start: Time::ZERO,
    };
    let g = w.build(&base, seed).unwrap();
    let cfg = SimConfig {
        seed,
        durations: g.durations,
        record_waits: false,
        ..SimConfig::default()
    };
    let r = simulate(&g.model, &mut FirstFit, &cfg).unwrap();
    assert!(r.state.all_done());
    (g.model, r, lambda)
}

fn within(measured: f64, predicted: f64, rel: f64) {
    let err = (measured - predicted).abs() / predicted;
    assert!(
        err <= rel,
        "measured {measured:.2} s, theory {predicted:.2} s, off by {:.1} % (tolerance {:.0} %)",
        err * 100.0,
        rel * 100.0
    );
}

const EXP_60S: Dist = Dist::Exponential {
    mean: Dur::secs(60),
};

#[test]
fn mm1_half_load() {
    let (wq, _, lambda) = run(1, 0.5, EXP_60S);
    within(wq, theory::mm1_wq(lambda, 60.0).unwrap(), 0.05);
}

#[test]
fn mm1_heavy_load() {
    let (wq, _, lambda) = run(1, 0.8, EXP_60S);
    within(wq, theory::mm1_wq(lambda, 60.0).unwrap(), 0.08);
}

/// M/M/3 at ρ = 0.8: the mean wait and Erlang C, the share of arrivals that wait.
#[test]
fn mmc_erlang_c() {
    let (wq, waited, lambda) = run(3, 0.8, EXP_60S);
    within(wq, theory::mmc_wq(lambda, 60.0, 3).unwrap(), 0.08);
    let pc = theory::erlang_c(3, lambda * 60.0).unwrap();
    assert!(
        (waited - pc).abs() < 0.02,
        "waited {waited:.3}, Erlang C {pc:.3}"
    );
}

/// M/D/1 at ρ = 0.7: Pollaczek–Khinchine with c_s² = 0, half the M/M/1 wait.
#[test]
fn md1_pollaczek_khinchine() {
    let (wq, _, lambda) = run(
        1,
        0.7,
        Dist::Fixed {
            value: Dur::secs(60),
        },
    );
    within(wq, theory::pk_wq(lambda, 60.0, 0.0).unwrap(), 0.05);
}

/// M/G/1 at ρ = 0.7 with lognormal service from p50 138 s and p90 471 s (c_s² ≈ 1.5).
#[test]
fn mg1_lognormal_pollaczek_khinchine() {
    let service = Dist::LogNormal {
        p50: Dur::secs(138),
        p90: Dur::secs(471),
    };
    let fit = theory::lognormal_from_p50_p90(138.0, 471.0);
    let (wq, _, lambda) = run(1, 0.7, service);
    within(wq, theory::pk_wq(lambda, fit.mean, fit.cs2).unwrap(), 0.10);
}

/// The metrics of the M/M/3 run: Little's law exact over the whole run (it starts and ends empty), and after a
/// warm-up of a tenth of the run, the comparison's Erlang C row within the tolerance of `mmc_erlang_c` (its
/// inputs, λ and E[S], are measured from the run).
#[test]
fn metrics_compare_mmc() {
    let (model, r, lambda) = sim(3, 0.8, EXP_60S);
    let all = from_trace(&model, &r.trace, &MetricsConfig::default());
    assert!(all.little.holds(), "{:?}", all.little);
    assert_eq!(all.jobs.len(), JOBS);
    let warm = MetricsConfig {
        warmup: Dur((JOBS as f64 / 10.0 / lambda * 1000.0) as i64),
        ..MetricsConfig::default()
    };
    let m = from_trace(&model, &r.trace, &warm);
    let cmp = m.compare(3);
    let row = cmp
        .rows
        .iter()
        .find(|r| r.formula == "M/M/c Erlang C")
        .unwrap();
    within(row.measured, row.predicted.unwrap(), 0.08);
    let rho = &cmp.rows[0];
    within(rho.measured, 0.8, 0.02);
    within(m.resources[0].utilisation.unwrap(), 0.8, 0.02);
    // Batch means give an interval that covers the theory's value.
    let ci = m.batch_means(20, 0.95).unwrap();
    let wq = theory::mmc_wq(lambda, 60.0, 3).unwrap();
    assert!(
        (ci.mean - wq).abs() <= ci.half_width.max(0.08 * wq),
        "{ci:?} vs {wq}"
    );
    println!("{cmp}");
}
