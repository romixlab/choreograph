//! The settings of the [`ListScheduler`](crate::ListScheduler): how priorities age, how deadlines raise them,
//! how jobs are placed and how backfilling works.
//!
//! Every field has a default and a doc line; a policy loads from TOML (feature `toml`), where every key is
//! optional and durations are written like `1h30m`:
//!
//! ```toml
//! aging_per_class = "1h"      # waiting this long raises a job by one class; "off" turns aging off
//! deadline_horizon = "30m"    # urgency starts when the slack is this small
//! deadline_class = 2          # the class a job reaches at zero slack (default: the model's highest class)
//! backfill = "easy"           # easy, conservative or off
//! reserve_with = "p90"        # p50 or p90: the estimate reservations and backfilling plan with
//! placement = "best_fit"      # best_fit or least_share
//! locality = true             # prefer assignments on fewer nodes
//! open_floor = "5m"           # an open-ended job is always planned to run at least this much longer
//! preempt_gap = 1             # classes above a Preempt::Ask job that may ask it to give way; "off": never
//! ```

use serde::{Deserialize, Serialize};

use crate::model::Estimate;
use crate::time::Dur;

/// How lower-priority jobs may start ahead of a blocked higher one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Backfill {
    /// EASY backfilling: the highest blocked job gets a reservation at its earliest feasible start; a later job
    /// starts only if it does not delay that reservation (it ends before it, or leaves it room).
    #[default]
    Easy,
    /// Conservative backfilling: every blocked job gets a reservation in priority order, and a later job may
    /// delay none of them. Fairer and better ETAs, packs less.
    Conservative,
    /// No reservations: any job that fits starts (a large job can starve behind a stream of small ones).
    Off,
}

/// Which estimate of a duration a plan uses.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Quantile {
    P50,
    #[default]
    P90,
}

impl Quantile {
    pub fn of(self, e: Estimate) -> Dur {
        match self {
            Quantile::P50 => e.p50,
            Quantile::P90 => e.p90,
        }
    }
}

/// How the planner picks among the assignments that fit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Placement {
    /// Best fit on the dominant resource: the assignment that leaves the least free share of the resource the
    /// job takes the largest share of, so large holes stay free for large jobs.
    #[default]
    BestFit,
    /// The assignment where the job's dominant share of capacity is smallest: heavy jobs go to large nodes,
    /// load spreads.
    LeastShare,
}

/// The list scheduler's settings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    /// Waiting time that raises a job's priority by one class (1/α, the aging rate α in classes per unit of
    /// time). A job `g` classes lower overtakes a fresh arrival after `g` times this. `None`: no aging.
    #[serde(with = "opt_dur")]
    pub aging_per_class: Option<Dur>,
    /// The deadline horizon H: a job's urgency rises from nothing at slack H to `deadline_class` at slack 0
    /// (slack = due date − now − remaining p90).
    #[serde(with = "dur")]
    pub deadline_horizon: Dur,
    /// The class a job with a due date reaches at zero slack. `None`: the highest class of any job in the model.
    pub deadline_class: Option<i32>,
    /// The backfilling mode.
    pub backfill: Backfill,
    /// The estimate reservations and backfilling plan with (p90: a backfilled job overruns it one time in ten).
    pub reserve_with: Quantile,
    /// How the planner picks among assignments that fit.
    pub placement: Placement,
    /// Prefer assignments that span fewer nodes before comparing the fit.
    pub locality: bool,
    /// An open-ended job (`Job::open`) is planned to end at its estimate while it is young, and from its age
    /// once it runs longer (the quantile of a lognormal fitted to its p50 and p90, given that it has lasted
    /// so long); never sooner than this from now, so a job past every guess still holds its resources in
    /// the plan instead of seeming to end at once.
    #[serde(with = "dur")]
    pub open_floor: Dur,
    /// How many classes higher a job that cannot start must be to ask a running `Preempt::Ask` job to give
    /// way. `None` (`"off"`): never ask.
    #[serde(with = "opt_gap")]
    pub preempt_gap: Option<i32>,
}

impl Default for Policy {
    fn default() -> Policy {
        Policy {
            aging_per_class: Some(Dur::hours(1)),
            deadline_horizon: Dur::mins(30),
            deadline_class: None,
            backfill: Backfill::Easy,
            reserve_with: Quantile::P90,
            placement: Placement::BestFit,
            locality: true,
            open_floor: Dur::mins(5),
            preempt_gap: Some(1),
        }
    }
}

/// Why a policy could not be loaded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("policy: {0}")]
pub struct PolicyError(pub String);

impl Policy {
    /// Loads a policy from TOML; missing keys keep their defaults, unknown keys are refused.
    #[cfg(feature = "toml")]
    pub fn from_toml(text: &str) -> Result<Policy, PolicyError> {
        let p: Policy = toml::from_str(text).map_err(|e| PolicyError(e.to_string()))?;
        p.check()?;
        Ok(p)
    }

    /// The policy as TOML, every key written out.
    #[cfg(feature = "toml")]
    pub fn to_toml(&self) -> String {
        toml::to_string(self).expect("a policy always serialises")
    }

    /// Refuses settings that make no sense (a zero or negative aging step or horizon).
    pub fn check(&self) -> Result<(), PolicyError> {
        if self.aging_per_class.is_some_and(|d| d <= Dur::ZERO) {
            return Err(PolicyError(
                "aging_per_class must be positive or \"off\"".into(),
            ));
        }
        if self.deadline_horizon <= Dur::ZERO {
            return Err(PolicyError("deadline_horizon must be positive".into()));
        }
        if self.open_floor < Dur::ZERO {
            return Err(PolicyError("open_floor must not be negative".into()));
        }
        if self.preempt_gap.is_some_and(|g| g < 1) {
            return Err(PolicyError(
                "preempt_gap must be at least 1 or \"off\"".into(),
            ));
        }
        Ok(())
    }
}

/// A duration as text (`1h30m`) or as an integer of milliseconds.
#[derive(Deserialize)]
#[serde(untagged)]
enum DurText {
    Ms(i64),
    Text(String),
}

impl DurText {
    fn parse<E: serde::de::Error>(self) -> Result<Option<Dur>, E> {
        match self {
            DurText::Ms(ms) => Ok(Some(Dur(ms))),
            DurText::Text(s) if s == "off" => Ok(None),
            DurText::Text(s) => s.parse().map(Some).map_err(E::custom),
        }
    }
}

pub(crate) mod dur {
    use super::*;

    pub fn serialize<S: serde::Serializer>(d: &Dur, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&d.to_string())
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Dur, D::Error> {
        DurText::deserialize(d)?
            .parse()?
            .ok_or_else(|| serde::de::Error::custom("a duration is needed here, not \"off\""))
    }
}

pub(crate) mod opt_dur {
    use super::*;

    pub fn serialize<S: serde::Serializer>(d: &Option<Dur>, s: S) -> Result<S::Ok, S::Error> {
        match d {
            Some(d) => s.serialize_str(&d.to_string()),
            None => s.serialize_str("off"),
        }
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Dur>, D::Error> {
        DurText::deserialize(d)?.parse()
    }
}

/// A class gap as an integer, or `"off"`.
pub(crate) mod opt_gap {
    use super::*;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Gap {
        N(i32),
        Text(String),
    }

    pub fn serialize<S: serde::Serializer>(g: &Option<i32>, s: S) -> Result<S::Ok, S::Error> {
        match g {
            Some(g) => s.serialize_i32(*g),
            None => s.serialize_str("off"),
        }
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<i32>, D::Error> {
        match Gap::deserialize(d)? {
            Gap::N(n) => Ok(Some(n)),
            Gap::Text(t) if t == "off" => Ok(None),
            Gap::Text(t) => Err(serde::de::Error::custom(format!(
                "{t:?}: a number of classes or \"off\""
            ))),
        }
    }
}

#[cfg(all(test, feature = "toml"))]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_partial_toml() {
        assert_eq!(Policy::from_toml("").unwrap(), Policy::default());
        let p = Policy::from_toml(
            r#"
            aging_per_class = "2h30m"
            backfill = "conservative"
            reserve_with = "p50"
            "#,
        )
        .unwrap();
        assert_eq!(p.aging_per_class, Some(Dur::mins(150)));
        assert_eq!(p.backfill, Backfill::Conservative);
        assert_eq!(p.reserve_with, Quantile::P50);
        assert_eq!(p.deadline_horizon, Dur::mins(30), "default kept");
    }

    #[test]
    fn round_trip_and_errors() {
        let p = Policy {
            aging_per_class: None,
            deadline_class: Some(3),
            placement: Placement::LeastShare,
            ..Policy::default()
        };
        assert_eq!(Policy::from_toml(&p.to_toml()).unwrap(), p);
        assert!(Policy::from_toml("agin = \"1h\"").is_err(), "unknown key");
        assert!(Policy::from_toml("aging_per_class = \"1x\"").is_err());
        assert!(Policy::from_toml("aging_per_class = \"0s\"").is_err());
        assert!(Policy::from_toml("deadline_horizon = \"off\"").is_err());
        assert_eq!(
            Policy::from_toml("preempt_gap = \"off\"")
                .unwrap()
                .preempt_gap,
            None
        );
        assert!(Policy::from_toml("preempt_gap = 0").is_err());
        let p = Policy::from_toml("open_floor = \"15m\"\npreempt_gap = 2").unwrap();
        assert_eq!((p.open_floor, p.preempt_gap), (Dur::mins(15), Some(2)));
        assert_eq!(Policy::from_toml(&p.to_toml()).unwrap(), p);
    }
}
