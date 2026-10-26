//! Queueing and scheduling formulas, to put predictions next to measurements.
//!
//! Units are the caller's, as long as they are consistent: rates per minute with times in minutes, or per hour
//! with hours. Functions that only hold for a stable system (utilisation ρ < 1) return `None` otherwise.
//!
//! Notation: arrival rate λ, mean service time E\[S\] (`mean`), servers c, utilisation ρ = λ·E\[S\]/c, squared
//! coefficients of variation of inter-arrival times `ca2` (c_a²) and of service times `cs2` (c_s²). Poisson
//! arrivals and exponential service have c² = 1.

/// Utilisation ρ = λ·E\[S\]/c.
pub fn utilisation(lambda: f64, mean: f64, c: u32) -> f64 {
    lambda * mean / c as f64
}

/// Little's law: mean number in the system L = λ·W.
pub fn little_l(lambda: f64, w: f64) -> f64 {
    lambda * w
}

/// M/M/1 mean waiting time in queue: W_q = ρ/(1 − ρ)·E\[S\].
pub fn mm1_wq(lambda: f64, mean: f64) -> Option<f64> {
    let rho = lambda * mean;
    (rho < 1.0).then(|| rho / (1.0 - rho) * mean)
}

/// Pollaczek–Khinchine, M/G/1 mean waiting time (exact): W_q = λ·E\[S²\]/(2(1 − ρ)) with
/// E\[S²\] = E\[S\]²(1 + c_s²).
pub fn pk_wq(lambda: f64, mean: f64, cs2: f64) -> Option<f64> {
    let rho = lambda * mean;
    (rho < 1.0).then(|| lambda * mean * mean * (1.0 + cs2) / (2.0 * (1.0 - rho)))
}

/// Kingman's G/G/1 approximation: W_q ≈ ρ/(1 − ρ)·(c_a² + c_s²)/2·E\[S\].
pub fn kingman_wq(lambda: f64, mean: f64, ca2: f64, cs2: f64) -> Option<f64> {
    let rho = lambda * mean;
    (rho < 1.0).then(|| rho / (1.0 - rho) * (ca2 + cs2) / 2.0 * mean)
}

/// Erlang B blocking probability for offered load `a` = λ·E\[S\] Erlangs on `c` servers (stable recursion).
pub fn erlang_b(c: u32, a: f64) -> f64 {
    let mut b = 1.0;
    for k in 1..=c {
        b = a * b / (k as f64 + a * b);
    }
    b
}

/// Erlang C: the probability that an arrival has to wait in M/M/c, offered load `a` Erlangs, `c` servers.
pub fn erlang_c(c: u32, a: f64) -> Option<f64> {
    let cf = c as f64;
    if a >= cf {
        return None;
    }
    let b = erlang_b(c, a);
    Some(cf * b / (cf - a * (1.0 - b)))
}

/// M/M/c mean waiting time in queue: W_q = C(c, a)/(cμ − λ).
pub fn mmc_wq(lambda: f64, mean: f64, c: u32) -> Option<f64> {
    let a = lambda * mean;
    let pc = erlang_c(c, a)?;
    Some(pc / (c as f64 / mean - lambda))
}

/// Allen–Cunneen G/G/c approximation: W_q ≈ W_q(M/M/c)·(c_a² + c_s²)/2.
pub fn allen_cunneen_wq(lambda: f64, mean: f64, c: u32, ca2: f64, cs2: f64) -> Option<f64> {
    Some(mmc_wq(lambda, mean, c)? * (ca2 + cs2) / 2.0)
}

/// One priority class for [`cobham_wq`]: arrival rate, mean service time, c_s².
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Class {
    pub lambda: f64,
    pub mean: f64,
    pub cs2: f64,
}

/// Cobham's formula, non-preemptive priority M/G/1: the mean waiting time of each class, highest class first.
/// W_q,k = W₀/((1 − σ_{k−1})(1 − σ_k)) with W₀ = Σ λ_i E\[S_i²\]/2 and σ_k the load of classes 1..k.
/// `None` for a class whose cumulative load σ_k reaches 1 (it waits without bound).
pub fn cobham_wq(classes: &[Class]) -> Vec<Option<f64>> {
    let w0: f64 = classes
        .iter()
        .map(|k| k.lambda * k.mean * k.mean * (1.0 + k.cs2) / 2.0)
        .sum();
    let mut sigma_prev = 0.0;
    classes
        .iter()
        .map(|k| {
            let sigma = sigma_prev + k.lambda * k.mean;
            let w = (sigma < 1.0).then(|| w0 / ((1.0 - sigma_prev) * (1.0 - sigma)));
            sigma_prev = sigma;
            w
        })
        .collect()
}

/// A lognormal distribution fitted to a median and a 90th percentile.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LogNormal {
    /// σ of the underlying normal: ln(p90/p50)/z₀.₉.
    pub sigma: f64,
    /// Mean p50·e^{σ²/2}.
    pub mean: f64,
    /// Squared coefficient of variation e^{σ²} − 1.
    pub cs2: f64,
}

/// The standard normal's 0.9 quantile.
pub const Z90: f64 = 1.281_551_565_545;

/// Fits a lognormal to p50 and p90 (p90 ≥ p50 > 0).
pub fn lognormal_from_p50_p90(p50: f64, p90: f64) -> LogNormal {
    let sigma = (p90 / p50).ln() / Z90;
    LogNormal {
        sigma,
        mean: p50 * (sigma * sigma / 2.0).exp(),
        cs2: (sigma * sigma).exp() - 1.0,
    }
}

/// Speculative checks (merge trains) at depth `k` with an independent failure probability `p` per entry: the
/// share of gate runs whose result is used, (1 − (1 − p)^k)/(k·p).
pub fn speculation_useful_fraction(p: f64, k: u32) -> f64 {
    if p <= 0.0 {
        return 1.0;
    }
    (1.0 - (1.0 - p).powi(k as i32)) / (k as f64 * p)
}

/// Aging: a job whose base priority is `gap` below the highest class, gaining `alpha` priority per unit time,
/// outranks every fresh arrival after `gap/alpha`.
pub fn aging_overtake(gap: f64, alpha: f64) -> f64 {
    gap / alpha
}

/// A two-state Markov-modulated Poisson process (MMPP): arrivals at `rate1` while in state 1 and `rate2` in
/// state 2, leaving state 1 at rate `r1` and state 2 at rate `r2` (one over the mean stay). The long-run
/// arrival rate λ̄ = (λ₁r₂ + λ₂r₁)/(r₁ + r₂).
pub fn mmpp2_rate(rate1: f64, rate2: f64, r1: f64, r2: f64) -> f64 {
    (rate1 * r2 + rate2 * r1) / (r1 + r2)
}

/// The asymptotic index of dispersion of counts of a two-state MMPP, Var N(t)/E N(t) as t → ∞:
/// I = 1 + 2(λ₁ − λ₂)²r₁r₂/((r₁ + r₂)³·λ̄). A Poisson process has I = 1; regimes that switch slowly between
/// busy and quiet give I ≫ 1. In heavy traffic I takes the place of c_a² in Kingman and Allen–Cunneen.
pub fn mmpp2_idc(rate1: f64, rate2: f64, r1: f64, r2: f64) -> f64 {
    let mean = mmpp2_rate(rate1, rate2, r1, r2);
    if mean <= 0.0 {
        return 1.0;
    }
    1.0 + 2.0 * (rate1 - rate2).powi(2) * r1 * r2 / ((r1 + r2).powi(3) * mean)
}

/// Batched gates with bisection: entries tested together in batches of `b`, each failing independently with
/// probability `p`. A green batch costs one run; a red one is split in half and each half tested, recursing,
/// about 2·log₂ b more runs to find one culprit. Gate runs per entry ≈ (1 + (1 − (1 − p)^b)·2·log₂ b)/b; 1 for
/// b = 1.
pub fn batch_runs_per_entry(p: f64, b: u32) -> f64 {
    if b <= 1 {
        return 1.0;
    }
    let bf = b as f64;
    (1.0 + (1.0 - (1.0 - p).powi(b as i32)) * 2.0 * bf.log2()) / bf
}

/// What [`pick_slots`] and [`pick_batch`] aim for.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct PickSettings {
    /// The highest utilisation ρ a pick of gate slots may run at: headroom for estimates that are off.
    pub max_util: f64,
    /// The mean wait in queue a pick of gate slots must stay under, in the caller's time unit.
    pub target_wq: f64,
    /// Never pick more gate slots than this.
    pub max_slots: u32,
    /// The largest batch [`pick_batch`] considers.
    pub max_batch: u32,
}

impl Default for PickSettings {
    fn default() -> Self {
        PickSettings {
            max_util: 0.7,
            target_wq: 10.0,
            max_slots: 16,
            max_batch: 8,
        }
    }
}

/// The smallest number of gate slots c with ρ ≤ `max_util` and the Allen–Cunneen W_q ≤ `target_wq`; `None` when
/// even `max_slots` do not get there. Pass the arrival process's index of dispersion as `ca2` for bursty
/// (regime-switching) arrivals: it is the c_a² heavy traffic sees.
pub fn pick_slots(lambda: f64, mean: f64, ca2: f64, cs2: f64, s: &PickSettings) -> Option<u32> {
    (1..=s.max_slots).find(|&c| {
        utilisation(lambda, mean, c) <= s.max_util
            && allen_cunneen_wq(lambda, mean, c, ca2, cs2).is_some_and(|w| w <= s.target_wq)
    })
}

/// An entry's mean time from arrival to its result through one lane (a repository's main branch) that gates
/// batches of `b`: waiting for the batch to fill, (b − 1)/(2λ); Kingman's W_q for gate runs arriving at
/// λ·[`batch_runs_per_entry`] with the entries' c_a² (an upper bound: batching smooths arrivals); the run itself;
/// and, when the batch is red (1 − (1 − p)^b), log₂ b more rounds of bisection. `None` when the lane is
/// overloaded.
pub fn batch_latency(lambda: f64, mean: f64, ca2: f64, cs2: f64, p: f64, b: u32) -> Option<f64> {
    let b = b.max(1);
    let bf = b as f64;
    let runs = lambda * batch_runs_per_entry(p, b);
    let form = (bf - 1.0) / (2.0 * lambda);
    let red = 1.0 - (1.0 - p).powi(b as i32);
    Some(form + kingman_wq(runs, mean, ca2, cs2)? + mean + red * bf.log2() * mean)
}

/// The batch size in 1..=`max_batch` with the lowest [`batch_latency`], and that latency; ties go to the smaller
/// batch. `None` when every size overloads the lane.
pub fn pick_batch(
    lambda: f64,
    mean: f64,
    ca2: f64,
    cs2: f64,
    p: f64,
    s: &PickSettings,
) -> Option<(u32, f64)> {
    (1..=s.max_batch.max(1))
        .filter_map(|b| batch_latency(lambda, mean, ca2, cs2, p, b).map(|l| (b, l)))
        .fold(None, |best: Option<(u32, f64)>, (b, l)| match best {
            Some((_, bl)) if bl <= l => best,
            _ => Some((b, l)),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: f64, b: f64, tol: f64) {
        assert!((a - b).abs() <= tol, "{a} vs {b}");
    }

    #[test]
    fn mm1_and_pk_agree_for_exponential_service() {
        let (l, s) = (0.5, 1.0);
        close(mm1_wq(l, s).unwrap(), 1.0, 1e-12);
        close(pk_wq(l, s, 1.0).unwrap(), 1.0, 1e-12);
        close(kingman_wq(l, s, 1.0, 1.0).unwrap(), 1.0, 1e-12);
        assert_eq!(mm1_wq(1.0, 1.0), None);
    }

    #[test]
    fn erlang_c_textbook_values() {
        // c = 1 reduces to ρ.
        close(erlang_c(1, 0.7).unwrap(), 0.7, 1e-12);
        // a = 2.4, c = 3: 0.647 (closed form a^c/c!·c/(c−a) over the sum).
        close(erlang_c(3, 2.4).unwrap(), 0.6472, 1e-4);
        assert_eq!(erlang_c(2, 2.4), None);
    }

    /// The landing lane of the busiest repository: E[S] = 5 min, c_s² = 1.5, c_a² = 2.05.
    #[test]
    fn landing_lane_worked_numbers() {
        let (s, ca2, cs2) = (5.0, 2.05, 1.5);
        for (per_hour, kingman, pk) in [
            (1.57, 1.3, 0.9),
            (3.9, 4.3, 3.0),
            (9.6, 35.5, 25.0),
            (11.0, 97.6, 68.7),
        ] {
            let l = per_hour / 60.0;
            close(kingman_wq(l, s, ca2, cs2).unwrap(), kingman, 0.05);
            close(pk_wq(l, s, cs2).unwrap(), pk, 0.05);
        }
    }

    /// A pool of gate slots: E[S] = 4 min, peak 36/h, c_a² = 3.19, c_s² = 1.5.
    #[test]
    fn gate_pool_worked_numbers() {
        let l = 36.0 / 60.0;
        assert_eq!(
            mmc_wq(l, 4.0, 2),
            None,
            "two slots are overloaded at the peak"
        );
        close(mmc_wq(l, 4.0, 3).unwrap(), 4.3, 0.05);
        close(allen_cunneen_wq(l, 4.0, 3, 3.19, 1.5).unwrap(), 10.1, 0.05);
        close(allen_cunneen_wq(l, 4.0, 4, 3.19, 1.5).unwrap(), 1.7, 0.05);
    }

    #[test]
    fn cobham_and_conservation() {
        // λ = 9.6/h, E[S] = 5 min, c_s² = 1.5, 30 % of the load in the high class.
        let l = 9.6 / 60.0;
        let hi = Class {
            lambda: 0.3 * l,
            mean: 5.0,
            cs2: 1.5,
        };
        let lo = Class {
            lambda: 0.7 * l,
            mean: 5.0,
            cs2: 1.5,
        };
        let w = cobham_wq(&[hi, lo]);
        close(w[0].unwrap(), 6.58, 0.01);
        close(w[1].unwrap(), 32.9, 0.05);
        // Kleinrock's conservation law: Σ ρ_k W_k equals FIFO's ρ·W.
        let fifo = pk_wq(l, 5.0, 1.5).unwrap();
        let rho = l * 5.0;
        close(
            0.24 * w[0].unwrap() + 0.56 * w[1].unwrap(),
            rho * fifo,
            1e-9,
        );
    }

    #[test]
    fn lognormal_fit_of_a_gate() {
        // A test suite with p50 138 s and p90 471 s.
        let f = lognormal_from_p50_p90(138.0, 471.0);
        close(f.sigma, 0.958, 0.001);
        close(f.mean, 218.0, 1.0);
        close(f.cs2, 1.50, 0.01);
    }

    #[test]
    fn speculation_yield() {
        let p = 0.216;
        close(speculation_useful_fraction(p, 1), 1.0, 1e-12);
        close(speculation_useful_fraction(p, 2), 0.892, 0.001);
        close(speculation_useful_fraction(p, 4), 0.720, 0.001);
        close(speculation_useful_fraction(0.0, 4), 1.0, 1e-12);
    }

    #[test]
    fn little_and_aging() {
        close(little_l(3.9 / 60.0, 9.3), 0.6045, 1e-4);
        close(aging_overtake(2.0, 1.0), 2.0, 1e-12);
        close(utilisation(36.0, 4.0 / 60.0, 3), 0.8, 1e-12);
    }

    #[test]
    fn mmpp2_reduces_to_poisson_and_grows_with_contrast() {
        // Equal rates: a Poisson process.
        close(mmpp2_rate(3.0, 3.0, 0.5, 0.5), 3.0, 1e-12);
        close(mmpp2_idc(3.0, 3.0, 0.5, 0.5), 1.0, 1e-12);
        // Busy 10/h for 2 h, quiet 1/h for 6 h: λ̄ = (10·1/6 + 1·1/2)/(2/3) = 3.25/h.
        let (r1, r2) = (0.5, 1.0 / 6.0);
        close(mmpp2_rate(10.0, 1.0, r1, r2), 3.25, 1e-12);
        // I = 1 + 2·81·(1/12)/((2/3)³·3.25) = 15.02.
        close(mmpp2_idc(10.0, 1.0, r1, r2), 15.02, 0.01);
    }

    #[test]
    fn batched_gate_runs_per_entry() {
        close(batch_runs_per_entry(0.22, 1), 1.0, 1e-12);
        // b = 2: (1 + 0.3916·2)/2 = 0.892; b = 4: (1 + 0.6298·4)/4 = 0.880.
        close(batch_runs_per_entry(0.22, 2), 0.892, 0.001);
        close(batch_runs_per_entry(0.22, 4), 0.880, 0.001);
        // b = 8: fewer runs still, but a red batch takes 2·log₂ 8 = 6 more rounds; at p = 0.05 far fewer.
        close(batch_runs_per_entry(0.22, 8), 0.772, 0.001);
        close(batch_runs_per_entry(0.05, 8), 0.377, 0.001);
    }

    #[test]
    fn slots_for_a_bursty_gate_pool() {
        let s = PickSettings::default();
        // 36/h at 4 min, c_s² 1.5: three slots run at ρ = 0.8, over the 0.7 cap; four give W_q 1.7 with c_a² 3.19.
        assert_eq!(pick_slots(0.6, 4.0, 3.19, 1.5, &s), Some(4));
        // Burstier arrivals need more.
        let w4 = allen_cunneen_wq(0.6, 4.0, 4, 30.0, 1.5).unwrap();
        assert!(w4 > 10.0, "{w4}");
        assert_eq!(pick_slots(0.6, 4.0, 30.0, 1.5, &s), Some(5));
        // Beyond max_slots: none.
        let tight = PickSettings { max_slots: 2, ..s };
        assert_eq!(pick_slots(0.6, 4.0, 3.19, 1.5, &tight), None);
    }

    #[test]
    fn batches_pay_off_only_under_load() {
        let s = PickSettings::default();
        // A quiet lane, 2/h at 5 min: batching only adds the wait to fill the batch.
        let (b, l) = pick_batch(2.0 / 60.0, 5.0, 2.0, 1.5, 0.2, &s).unwrap();
        assert_eq!(b, 1);
        close(
            l,
            5.0 + kingman_wq(2.0 / 60.0, 5.0, 2.0, 1.5).unwrap(),
            1e-9,
        );
        // A lane past its capacity, 14/h at 5 min (ρ = 1.17): one at a time overloads, batches of a few carry it.
        assert_eq!(batch_latency(14.0 / 60.0, 5.0, 2.0, 1.5, 0.05, 1), None);
        let (b, _) = pick_batch(14.0 / 60.0, 5.0, 2.0, 1.5, 0.05, &s).unwrap();
        assert!(b >= 2, "{b}");
    }
}
