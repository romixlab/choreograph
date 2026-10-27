//! Learning phase durations from history.
//!
//! A real plan is only as good as its estimates (3.11): durations are right-skewed, so the mean sits above the
//! median and a plan must never reserve on it alone. This module learns an EWMA mean and streaming p50/p90/p99
//! per [`EstimateKey`] (what kind of work, which phase, on which resource and node), blended with a prior per
//! job kind and phase while samples are scarce (a cold start). The planner is meant to read estimates through
//! the [`Estimates`] trait rather than reaching into a learner's internals, so a fixed set of priors (no history
//! yet) or a test double can stand in for it.
//!
//! The model has no separate "job kind" of its own yet (only a job's name), so [`EstimateKey::job_kind`] is the
//! caller's own tag (a test name, a build step): whatever identifies one recurring kind of work across many job
//! instances over time.

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};

use crate::model::{NodeId, Phase, ResourceId};
use crate::time::Dur;

/// What a learned duration is kept for: a kind of work, its phase, on this resource, on this node. The same key
/// tracks one recurring kind of work (a test name, a build step) across many job instances over time.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EstimateKey {
    /// The caller's tag for a recurring kind of work (a test name, a build step).
    pub job_kind: String,
    pub phase: Phase,
    pub resource: ResourceId,
    pub node: NodeId,
}

/// An EWMA mean and streaming quantiles for a phase's duration. Also the shape of a prior: a prior is just a
/// `Quantiles` value supplied up front, blended with observed samples the same way.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quantiles {
    pub mean: Dur,
    pub p50: Dur,
    pub p90: Dur,
    pub p99: Dur,
}

/// Tunables for learning durations. Every one has a default and this doc line (no bare constants).
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct EstimateSettings {
    /// EWMA weight on the newest sample (`beta` in `Ŝ ← βS + (1 − β)Ŝ`), in `(0, 1]`; higher adapts faster but
    /// noisier. Default 0.3.
    pub beta: f64,
    /// How many of the most recent samples the streaming quantiles (p50, p90, p99) are computed over; at least
    /// 1. Default 200.
    pub n: usize,
    /// How many virtual samples a job kind's prior counts for: with `prior_weight` real samples seen, the prior
    /// and the observed data are weighted equally; it fades out, but never fully, as more samples arrive.
    /// Default 20.
    pub prior_weight: f64,
}

impl Default for EstimateSettings {
    fn default() -> EstimateSettings {
        EstimateSettings {
            beta: 0.3,
            n: 200,
            prior_weight: 20.0,
        }
    }
}

/// Duration estimates, as the planner is meant to read them: never the learner's internals directly, so a fixed
/// prior table or a test double can stand in for history.
pub trait Estimates {
    /// The current estimate for this key, blending any learned samples with the job kind and phase's prior.
    /// `None` when there is neither a prior nor a sample yet; the caller falls back to the job's own static
    /// estimate.
    fn estimate(&self, key: &EstimateKey) -> Option<Quantiles>;
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct KeyState {
    count: u64,
    ewma_ms: f64,
    window: VecDeque<i64>,
}

impl KeyState {
    fn observe(&mut self, ms: i64, settings: &EstimateSettings) {
        self.ewma_ms = if self.count == 0 {
            ms as f64
        } else {
            settings.beta * ms as f64 + (1.0 - settings.beta) * self.ewma_ms
        };
        self.count += 1;
        if self.window.len() >= settings.n.max(1) {
            self.window.pop_front();
        }
        self.window.push_back(ms);
    }

    fn quantiles(&self) -> Quantiles {
        let mut sorted: Vec<i64> = self.window.iter().copied().collect();
        sorted.sort_unstable();
        Quantiles {
            mean: Dur::ms(self.ewma_ms.round() as i64),
            p50: Dur::ms(interpolated_quantile(&sorted, 0.50)),
            p90: Dur::ms(interpolated_quantile(&sorted, 0.90)),
            p99: Dur::ms(interpolated_quantile(&sorted, 0.99)),
        }
    }
}

/// The value at quantile `q` of a sorted sample, linearly interpolated between the closest ranks (the method
/// used by most statistics packages). `sorted` must be non-empty and sorted ascending.
fn interpolated_quantile(sorted: &[i64], q: f64) -> i64 {
    if sorted.len() == 1 {
        return sorted[0];
    }
    let idx = q * (sorted.len() - 1) as f64;
    let lo = idx.floor() as usize;
    let hi = idx.ceil() as usize;
    if lo == hi {
        return sorted[lo];
    }
    let frac = idx - lo as f64;
    let (a, b) = (sorted[lo] as f64, sorted[hi] as f64);
    (a + frac * (b - a)).round() as i64
}

/// How much weight the observed data gets against the prior, once `count` real samples (clamped to the
/// quantile window `n`, since that is as much data as the streaming quantiles ever see) have arrived.
fn data_weight(count: u64, settings: &EstimateSettings) -> f64 {
    let count = count.min(settings.n as u64) as f64;
    if settings.prior_weight <= 0.0 {
        return 1.0;
    }
    count / (settings.prior_weight + count)
}

fn blend(observed: Quantiles, prior: Quantiles, w: f64) -> Quantiles {
    let mix = |o: Dur, p: Dur| {
        Dur::ms((w * o.as_ms() as f64 + (1.0 - w) * p.as_ms() as f64).round() as i64)
    };
    Quantiles {
        mean: mix(observed.mean, prior.mean),
        p50: mix(observed.p50, prior.p50),
        p90: mix(observed.p90, prior.p90),
        p99: mix(observed.p99, prior.p99),
    }
}

/// Learns durations from observed samples per [`EstimateKey`], blended with priors per job kind and phase for a
/// cold start.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Learner {
    settings: EstimateSettings,
    #[serde(with = "pairs")]
    priors: BTreeMap<(String, Phase), Quantiles>,
    #[serde(with = "pairs")]
    state: BTreeMap<EstimateKey, KeyState>,
    /// Per job kind and phase wherever it ran: for a job not yet placed (an estimate at submit).
    #[serde(default, with = "pairs")]
    anywhere: BTreeMap<(String, Phase), KeyState>,
}

impl Default for Learner {
    fn default() -> Learner {
        Learner::new(EstimateSettings::default())
    }
}

/// Maps with non-string keys as lists of pairs, which every serde format can write.
mod pairs {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<K: Serialize, V: Serialize, S: Serializer>(
        m: &BTreeMap<K, V>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        s.collect_seq(m.iter())
    }

    pub fn deserialize<
        'de,
        K: Deserialize<'de> + Ord,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    >(
        d: D,
    ) -> Result<BTreeMap<K, V>, D::Error> {
        Ok(Vec::<(K, V)>::deserialize(d)?.into_iter().collect())
    }
}

impl Learner {
    pub fn new(settings: EstimateSettings) -> Learner {
        Learner {
            settings,
            priors: BTreeMap::new(),
            state: BTreeMap::new(),
            anywhere: BTreeMap::new(),
        }
    }

    pub fn settings(&self) -> &EstimateSettings {
        &self.settings
    }

    /// Sets the cold-start prior for every key with this job kind and phase, whatever the resource and node.
    pub fn with_prior(
        mut self,
        job_kind: impl Into<String>,
        phase: Phase,
        prior: Quantiles,
    ) -> Learner {
        self.priors.insert((job_kind.into(), phase), prior);
        self
    }

    /// Records one observed duration for this key, updating its EWMA mean and streaming quantiles.
    pub fn observe(&mut self, key: &EstimateKey, d: Dur) {
        self.state
            .entry(key.clone())
            .or_default()
            .observe(d.as_ms(), &self.settings);
    }

    /// Records one observed duration of a job kind's phase wherever it ran (see
    /// [`estimate_kind`](Learner::estimate_kind)).
    pub fn observe_kind(&mut self, job_kind: &str, phase: Phase, d: Dur) {
        self.anywhere
            .entry((job_kind.to_string(), phase))
            .or_default()
            .observe(d.as_ms(), &self.settings);
    }

    /// The estimate for a job kind's phase wherever it runs, blended with its prior like
    /// [`Estimates::estimate`]: what a job of that kind is planned with before it is placed.
    pub fn estimate_kind(&self, job_kind: &str, phase: Phase) -> Option<Quantiles> {
        let k = (job_kind.to_string(), phase);
        self.combine(self.anywhere.get(&k), self.priors.get(&k).copied())
    }

    fn combine(&self, observed: Option<&KeyState>, prior: Option<Quantiles>) -> Option<Quantiles> {
        match (observed, prior) {
            (None, None) => None,
            (None, Some(p)) => Some(p),
            (Some(s), None) => Some(s.quantiles()),
            (Some(s), Some(p)) => Some(blend(
                s.quantiles(),
                p,
                data_weight(s.count, &self.settings),
            )),
        }
    }
}

impl Estimates for Learner {
    fn estimate(&self, key: &EstimateKey) -> Option<Quantiles> {
        let prior = self.priors.get(&(key.job_kind.clone(), key.phase)).copied();
        self.combine(self.state.get(key), prior)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{NodeId, ResourceId};

    fn key(job_kind: &str, phase: Phase) -> EstimateKey {
        EstimateKey {
            job_kind: job_kind.into(),
            phase,
            resource: ResourceId(0),
            node: NodeId(0),
        }
    }

    /// splitmix64: a minimal seeded generator for synthetic test data, so these tests stay deterministic and
    /// dependency-free (no `rand` crate for one module's tests).
    struct Rng(u64);

    impl Rng {
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z ^ (z >> 31)
        }
        /// Uniform in `[0, 1)`.
        fn next_f64(&mut self) -> f64 {
            (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
        }
        fn uniform_ms(&mut self, max_ms: f64) -> i64 {
            (self.next_f64() * max_ms) as i64
        }
        /// Inverse-CDF sampling: `-mean * ln(1 - u)`.
        fn exponential_ms(&mut self, mean_ms: f64) -> i64 {
            (-mean_ms * (1.0 - self.next_f64()).ln()) as i64
        }
    }

    #[test]
    fn no_prior_no_data_is_none() {
        let l = Learner::new(EstimateSettings::default());
        assert_eq!(l.estimate(&key("flash", Phase::Setup)), None);
    }

    #[test]
    fn cold_start_uses_prior_alone() {
        let prior = Quantiles {
            mean: Dur::secs(20),
            p50: Dur::secs(18),
            p90: Dur::secs(30),
            p99: Dur::secs(40),
        };
        let l = Learner::new(EstimateSettings::default()).with_prior("flash", Phase::Setup, prior);
        assert_eq!(l.estimate(&key("flash", Phase::Setup)), Some(prior));
        // A different phase or job kind never sees this prior.
        assert_eq!(l.estimate(&key("flash", Phase::Run)), None);
        assert_eq!(l.estimate(&key("other", Phase::Setup)), None);
    }

    #[test]
    fn ewma_matches_the_formula() {
        let settings = EstimateSettings {
            beta: 0.5,
            ..EstimateSettings::default()
        };
        let mut l = Learner::new(settings);
        let k = key("gate", Phase::Run);
        let mut expected = 0.0;
        for (i, ms) in [10_000i64, 20_000, 5_000, 30_000].into_iter().enumerate() {
            l.observe(&k, Dur::ms(ms));
            expected = if i == 0 {
                ms as f64
            } else {
                0.5 * ms as f64 + 0.5 * expected
            };
            let got = l.estimate(&k).unwrap().mean.as_ms();
            assert_eq!(got, expected.round() as i64);
        }
    }

    #[test]
    fn window_evicts_the_oldest_sample() {
        let settings = EstimateSettings {
            n: 3,
            ..EstimateSettings::default()
        };
        let mut l = Learner::new(settings);
        let k = key("gate", Phase::Run);
        l.observe(&k, Dur::hours(100)); // an outlier that must fall out of the window
        for _ in 0..3 {
            l.observe(&k, Dur::secs(5));
        }
        let q = l.estimate(&k).unwrap();
        assert_eq!(q.p50, Dur::secs(5));
        assert_eq!(q.p90, Dur::secs(5));
        assert_eq!(q.p99, Dur::secs(5));
    }

    #[test]
    fn prior_fades_but_never_fully() {
        let settings = EstimateSettings {
            n: 50,
            prior_weight: 10.0,
            ..EstimateSettings::default()
        };
        let prior = Quantiles {
            mean: Dur::secs(100),
            p50: Dur::secs(100),
            p90: Dur::secs(100),
            p99: Dur::secs(100),
        };
        let mut l = Learner::new(settings).with_prior("gate", Phase::Run, prior);
        let k = key("gate", Phase::Run);
        // A constant observed value makes every quantile exactly that value, so the blend is checkable exactly.
        for _ in 0..200 {
            l.observe(&k, Dur::secs(10));
        }
        let w: f64 = 50.0 / (10.0 + 50.0); // count clamped at n = 50
        let expected = (w * 10_000.0 + (1.0 - w) * 100_000.0).round() as i64;
        assert_eq!(l.estimate(&k).unwrap().mean.as_ms(), expected);
        assert_eq!(l.estimate(&k).unwrap().p50.as_ms(), expected);
        // The prior never disappears entirely: it still pulls the blend away from the raw 10s.
        assert!(l.estimate(&k).unwrap().mean > Dur::secs(10));
    }

    #[test]
    fn independent_keys_do_not_mix() {
        let mut l = Learner::new(EstimateSettings::default());
        let a = key("a", Phase::Run);
        let b = key("b", Phase::Run);
        l.observe(&a, Dur::secs(10));
        l.observe(&b, Dur::secs(1000));
        assert_eq!(l.estimate(&a).unwrap().mean, Dur::secs(10));
        assert_eq!(l.estimate(&b).unwrap().mean, Dur::secs(1000));
    }

    #[test]
    fn quantiles_converge_on_a_known_uniform_distribution() {
        let settings = EstimateSettings {
            n: 2000,
            prior_weight: 0.0,
            ..EstimateSettings::default()
        };
        let mut l = Learner::new(settings);
        let k = key("gate", Phase::Run);
        let mut rng = Rng(0x5eed_1234);
        for _ in 0..5000 {
            l.observe(&k, Dur::ms(rng.uniform_ms(1000.0)));
        }
        let q = l.estimate(&k).unwrap();
        // Uniform[0, 1000): p-th quantile is exactly p * 1000.
        assert!((q.p50.as_ms() - 500).abs() < 40, "p50 = {}", q.p50.as_ms());
        assert!((q.p90.as_ms() - 900).abs() < 40, "p90 = {}", q.p90.as_ms());
        assert!((q.p99.as_ms() - 990).abs() < 40, "p99 = {}", q.p99.as_ms());
    }

    #[test]
    fn quantiles_converge_on_a_known_exponential_distribution() {
        let settings = EstimateSettings {
            n: 2000,
            prior_weight: 0.0,
            ..EstimateSettings::default()
        };
        let mut l = Learner::new(settings);
        let k = key("gate", Phase::Run);
        let mean_ms = 300_000.0; // 5 minutes, about tpm_cli's landing lane (plan.md 3.1)
        let mut rng = Rng(0xc0ffee);
        for _ in 0..5000 {
            l.observe(&k, Dur::ms(rng.exponential_ms(mean_ms)));
        }
        let q = l.estimate(&k).unwrap();
        // Exponential(mean): quantile(p) = -mean * ln(1 - p).
        let expected_p50 = -mean_ms * (1.0 - 0.50_f64).ln();
        let expected_p90 = -mean_ms * (1.0 - 0.90_f64).ln();
        let expected_p99 = -mean_ms * (1.0 - 0.99_f64).ln();
        let tol = 0.08; // 8%: a heavy right tail needs more slack than the uniform case
        assert!(
            (q.p50.as_ms() as f64 - expected_p50).abs() < expected_p50 * tol,
            "p50 = {}, expected ~{expected_p50}",
            q.p50.as_ms()
        );
        assert!(
            (q.p90.as_ms() as f64 - expected_p90).abs() < expected_p90 * tol,
            "p90 = {}, expected ~{expected_p90}",
            q.p90.as_ms()
        );
        assert!(
            (q.p99.as_ms() as f64 - expected_p99).abs() < expected_p99 * tol,
            "p99 = {}, expected ~{expected_p99}",
            q.p99.as_ms()
        );
    }

    #[test]
    fn default_settings_are_sane() {
        let s = EstimateSettings::default();
        assert!(s.beta > 0.0 && s.beta <= 1.0);
        assert!(s.n >= 1);
        assert!(s.prior_weight >= 0.0);
    }
}
