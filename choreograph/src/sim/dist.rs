//! Random durations: phase lengths and gaps between arrivals.
//!
//! Every sampler here is written out in the library (no distribution crate) and uses [`libm`] for `ln`, `exp`
//! and `cos`, so a seed gives the same numbers on every machine and with every dependency version. Samples are
//! rounded to whole milliseconds and never negative.

use rand_core::RngCore;
use serde::{Deserialize, Serialize};

use crate::model::Estimate;
use crate::theory::Z90;
use crate::time::Dur;

/// A distribution of durations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "dist", rename_all = "snake_case")]
pub enum Dist {
    /// Always the same value (c² = 0).
    Fixed { value: Dur },
    /// Exponential with this mean (c² = 1): service times of M/M/c, gaps of a Poisson process.
    Exponential { mean: Dur },
    /// Lognormal fitted to a median and a 90th percentile, the usual shape of build and test times.
    LogNormal { p50: Dur, p90: Dur },
    /// Gamma with this mean and squared coefficient of variation (shape 1/c², any c² > 0).
    Gamma { mean: Dur, cs2: f64 },
    /// Two-phase hyperexponential with balanced means, for c² > 1 (bursty arrivals).
    HyperExp { mean: Dur, cs2: f64 },
    /// One of these recorded values, each equally likely (bootstrap from history).
    Empirical { samples: Vec<Dur> },
}

/// Why a distribution cannot be sampled.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum DistError {
    #[error("{0:?}: negative mean or value")]
    Negative(Dist),
    #[error("{0:?}: lognormal needs 0 < p50 <= p90")]
    LogNormal(Dist),
    #[error("{0:?}: gamma needs c² > 0")]
    Gamma(Dist),
    #[error("{0:?}: hyperexponential needs c² >= 1")]
    HyperExp(Dist),
    #[error("empirical distribution without samples")]
    Empty,
}

/// A uniform number in the open interval (0, 1) from 53 random bits.
pub fn unit(rng: &mut impl RngCore) -> f64 {
    ((rng.next_u64() >> 11) as f64 + 0.5) * (1.0 / (1u64 << 53) as f64)
}

/// A standard normal number (Box–Muller; always two uniforms, so the stream position is predictable).
pub fn std_normal(rng: &mut impl RngCore) -> f64 {
    let u1 = unit(rng);
    let u2 = unit(rng);
    libm::sqrt(-2.0 * libm::log(u1)) * libm::cos(2.0 * std::f64::consts::PI * u2)
}

/// An exponential number with mean 1.
pub fn std_exp(rng: &mut impl RngCore) -> f64 {
    -libm::log(unit(rng))
}

/// A gamma number with this shape and scale 1 (Marsaglia–Tsang; shape < 1 boosted by u^(1/shape)).
pub fn std_gamma(rng: &mut impl RngCore, shape: f64) -> f64 {
    if shape < 1.0 {
        let boost = libm::pow(unit(rng), 1.0 / shape);
        return std_gamma(rng, shape + 1.0) * boost;
    }
    let d = shape - 1.0 / 3.0;
    let c = 1.0 / libm::sqrt(9.0 * d);
    loop {
        let x = std_normal(rng);
        let v = 1.0 + c * x;
        if v <= 0.0 {
            continue;
        }
        let v = v * v * v;
        let u = unit(rng);
        if libm::log(u) < 0.5 * x * x + d - d * v + d * libm::log(v) {
            return d * v;
        }
    }
}

fn ms(x: f64) -> Dur {
    Dur(libm::round(x.max(0.0)) as i64)
}

impl Dist {
    /// The distribution an estimate implies: fixed when p50 = p90 (or p50 is 0), otherwise lognormal.
    pub fn from_estimate(e: Estimate) -> Dist {
        if e.p50 == e.p90 || e.p50 <= Dur::ZERO {
            Dist::Fixed { value: e.p50 }
        } else {
            Dist::LogNormal {
                p50: e.p50,
                p90: e.p90,
            }
        }
    }

    /// A distribution with this mean and squared coefficient of variation: fixed for 0, gamma below 1,
    /// exponential at 1, hyperexponential above (the gaps of a renewal arrival process with a chosen c_a²).
    pub fn with_cs2(mean: Dur, cs2: f64) -> Dist {
        if cs2 <= 0.0 {
            Dist::Fixed { value: mean }
        } else if cs2 < 1.0 {
            Dist::Gamma { mean, cs2 }
        } else if cs2 == 1.0 {
            Dist::Exponential { mean }
        } else {
            Dist::HyperExp { mean, cs2 }
        }
    }

    /// Checks the parameters.
    pub fn check(&self) -> Result<(), DistError> {
        let neg = |d: Dur| d < Dur::ZERO;
        match self {
            Dist::Fixed { value: m } | Dist::Exponential { mean: m } if neg(*m) => {
                Err(DistError::Negative(self.clone()))
            }
            Dist::LogNormal { p50, p90 } if *p50 <= Dur::ZERO || p90 < p50 => {
                Err(DistError::LogNormal(self.clone()))
            }
            Dist::Gamma { mean, cs2 } if neg(*mean) || cs2.is_nan() || *cs2 <= 0.0 => {
                Err(DistError::Gamma(self.clone()))
            }
            Dist::HyperExp { mean, cs2 } if neg(*mean) || cs2.is_nan() || *cs2 < 1.0 => {
                Err(DistError::HyperExp(self.clone()))
            }
            Dist::Empirical { samples } if samples.is_empty() => Err(DistError::Empty),
            Dist::Empirical { samples } if samples.iter().any(|d| neg(*d)) => {
                Err(DistError::Negative(self.clone()))
            }
            _ => Ok(()),
        }
    }

    /// Draws one duration. The parameters must pass [`Dist::check`].
    pub fn sample(&self, rng: &mut impl RngCore) -> Dur {
        match self {
            Dist::Fixed { value } => *value,
            Dist::Exponential { mean } => ms(mean.0 as f64 * std_exp(rng)),
            Dist::LogNormal { p50, p90 } => {
                let sigma = libm::log(p90.0 as f64 / p50.0 as f64) / Z90;
                ms(p50.0 as f64 * libm::exp(sigma * std_normal(rng)))
            }
            Dist::Gamma { mean, cs2 } => ms(mean.0 as f64 * cs2 * std_gamma(rng, 1.0 / cs2)),
            Dist::HyperExp { mean, cs2 } => {
                let (p1, m1, m2) = hyperexp_branches(mean.0 as f64, *cs2);
                let m = if unit(rng) < p1 { m1 } else { m2 };
                ms(m * std_exp(rng))
            }
            Dist::Empirical { samples } => {
                let i = (unit(rng) * samples.len() as f64) as usize;
                samples[i.min(samples.len() - 1)]
            }
        }
    }

    /// The mean in milliseconds (before rounding to whole milliseconds).
    pub fn mean_ms(&self) -> f64 {
        match self {
            Dist::Fixed { value } => value.0 as f64,
            Dist::Exponential { mean } | Dist::Gamma { mean, .. } | Dist::HyperExp { mean, .. } => {
                mean.0 as f64
            }
            Dist::LogNormal { p50, p90 } => {
                let sigma = libm::log(p90.0 as f64 / p50.0 as f64) / Z90;
                p50.0 as f64 * libm::exp(sigma * sigma / 2.0)
            }
            Dist::Empirical { samples } => {
                samples.iter().map(|d| d.0 as f64).sum::<f64>() / samples.len() as f64
            }
        }
    }

    /// The squared coefficient of variation Var/mean², the c² the queueing formulas take.
    pub fn cs2(&self) -> f64 {
        match self {
            Dist::Fixed { .. } => 0.0,
            Dist::Exponential { .. } => 1.0,
            Dist::Gamma { cs2, .. } | Dist::HyperExp { cs2, .. } => *cs2,
            Dist::LogNormal { p50, p90 } => {
                let sigma = libm::log(p90.0 as f64 / p50.0 as f64) / Z90;
                libm::exp(sigma * sigma) - 1.0
            }
            Dist::Empirical { samples } => {
                let m = self.mean_ms();
                if m == 0.0 {
                    return 0.0;
                }
                let var = samples
                    .iter()
                    .map(|d| (d.0 as f64 - m).powi(2))
                    .sum::<f64>()
                    / samples.len() as f64;
                var / (m * m)
            }
        }
    }
}

/// Balanced-means H2: branch probability p1 and the branch means, for mean `m` and c² ≥ 1.
fn hyperexp_branches(m: f64, cs2: f64) -> (f64, f64, f64) {
    let p1 = 0.5 * (1.0 + libm::sqrt((cs2 - 1.0) / (cs2 + 1.0)));
    let p2 = 1.0 - p1;
    // Each branch carries half the mean: p_i·m_i = m/2.
    (p1, m / (2.0 * p1), m / (2.0 * p2))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand_chacha::ChaCha8Rng;
    use rand_core::SeedableRng;

    /// Sample mean and c² over n draws.
    fn moments(d: &Dist, n: usize) -> (f64, f64) {
        let mut rng = ChaCha8Rng::seed_from_u64(7);
        let xs: Vec<f64> = (0..n).map(|_| d.sample(&mut rng).0 as f64).collect();
        let m = xs.iter().sum::<f64>() / n as f64;
        let var = xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / n as f64;
        (m, var / (m * m))
    }

    fn close(a: f64, b: f64, rel: f64) {
        assert!((a - b).abs() <= rel * b.abs().max(1e-9), "{a} vs {b}");
    }

    #[test]
    fn sample_moments_match_the_parameters() {
        let s = Dur::secs(100);
        for d in [
            Dist::Exponential { mean: s },
            Dist::Gamma { mean: s, cs2: 0.25 },
            Dist::Gamma { mean: s, cs2: 2.0 },
            Dist::HyperExp { mean: s, cs2: 3.0 },
            Dist::LogNormal {
                p50: Dur::secs(138),
                p90: Dur::secs(471),
            },
            Dist::Empirical {
                samples: vec![Dur::secs(1), Dur::secs(2), Dur::secs(6)],
            },
        ] {
            d.check().unwrap();
            let (m, c2) = moments(&d, 400_000);
            close(m, d.mean_ms(), 0.01);
            // Heavy tails converge slowly in the second moment.
            close(c2, d.cs2(), 0.05);
        }
        assert_eq!(moments(&Dist::Fixed { value: s }, 10).0, 100_000.0);
    }

    #[test]
    fn lognormal_matches_the_theory_fit() {
        let d = Dist::LogNormal {
            p50: Dur::secs(138),
            p90: Dur::secs(471),
        };
        let fit = crate::theory::lognormal_from_p50_p90(138.0, 471.0);
        close(d.mean_ms() / 1000.0, fit.mean, 1e-9);
        close(d.cs2(), fit.cs2, 1e-9);
        // The median of many draws sits at p50.
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        let mut xs: Vec<Dur> = (0..100_001).map(|_| d.sample(&mut rng)).collect();
        xs.sort();
        close(xs[50_000].0 as f64, 138_000.0, 0.01);
        close(xs[90_000].0 as f64, 471_000.0, 0.01);
    }

    #[test]
    fn with_cs2_picks_the_family() {
        let m = Dur::secs(1);
        assert_eq!(Dist::with_cs2(m, 0.0), Dist::Fixed { value: m });
        assert!(matches!(Dist::with_cs2(m, 0.5), Dist::Gamma { .. }));
        assert_eq!(Dist::with_cs2(m, 1.0), Dist::Exponential { mean: m });
        assert!(matches!(Dist::with_cs2(m, 2.05), Dist::HyperExp { .. }));
    }

    #[test]
    fn estimates_and_checks() {
        let e = Estimate {
            p50: Dur::secs(10),
            p90: Dur::secs(30),
        };
        assert!(matches!(Dist::from_estimate(e), Dist::LogNormal { .. }));
        assert_eq!(
            Dist::from_estimate(Estimate::fixed(Dur::secs(3))),
            Dist::Fixed {
                value: Dur::secs(3)
            }
        );
        assert!(
            Dist::HyperExp {
                mean: Dur::secs(1),
                cs2: 0.5
            }
            .check()
            .is_err()
        );
        assert_eq!(
            Dist::Empirical { samples: vec![] }.check(),
            Err(DistError::Empty)
        );
    }

    /// The samplers are pinned: a change to them changes every seeded trace, so it must be deliberate.
    #[test]
    fn samplers_are_pinned() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let got: Vec<i64> = [
            Dist::Exponential {
                mean: Dur::secs(10),
            },
            Dist::LogNormal {
                p50: Dur::secs(10),
                p90: Dur::secs(30),
            },
            Dist::Gamma {
                mean: Dur::secs(10),
                cs2: 0.5,
            },
            Dist::HyperExp {
                mean: Dur::secs(10),
                cs2: 2.0,
            },
        ]
        .iter()
        .map(|d| d.sample(&mut rng).0)
        .collect();
        assert_eq!(got, vec![3829, 7820, 6924, 1384]);
    }
}
