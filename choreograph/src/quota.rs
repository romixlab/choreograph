//! Quotas that refill: a resource whose use is limited not by how many hold it at once but by what is left of
//! an allowance that comes back at a known time (a weekly cap with its reset, a short rolling window, an API
//! key's rate limit, a bench's daily duty cycle).
//!
//! A client reports a resource's [`Quota`] as it reads it from outside: one [`Window`] per allowance, each with
//! what is used, its limit, an optional soft level and when it resets. While any window is at or past its stop
//! level (the soft level, else the limit), nothing new starts on the resource: its capacity counts as 0, as
//! for a node that is down, and the jobs that hold it keep running. At a window's reset its use counts as 0
//! (until the next report), so the planner books the refill ahead: a waiting job gets a reservation and an ETA
//! from the reset, and its wait reason names the window, its use and the reset
//! ([`WaitReason::Quota`](crate::WaitReason::Quota)). The holders of a resource hear at every heartbeat which
//! windows are at or past a level ([`QuotaCrossing`]): the soft level to plan their end, the limit to end now.

use serde::{Deserialize, Serialize};

use crate::budget::Level;
use crate::model::ResourceId;
use crate::time::{Dur, Time};

/// What is left of a resource's refilling allowances, as last reported.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Quota {
    /// Every allowance; new starts wait while any one of them is spent.
    pub windows: Vec<Window>,
}

/// One allowance that refills.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Window {
    /// Its name (`week`, `5h`, `daily`), unique within the quota.
    pub name: String,
    /// How much of it is used, in any unit (percent, tokens, requests, minutes).
    pub used: u64,
    /// The cap, in the unit of `used`: at it the holders are told to end now.
    pub limit: u64,
    /// At it nothing new starts and the holders are told to plan their end. `None`: the limit stops new
    /// starts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soft: Option<u64>,
    /// When its use goes back to 0. `None`: not known, it is spent until a new report says otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_at: Option<Time>,
    /// It refills again every so long after `resets_at` (a weekly cap, a daily duty cycle). `None`: once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every: Option<Dur>,
}

impl Window {
    /// The level at or past which nothing new starts: the soft level, else the limit.
    pub fn stop_level(&self) -> u64 {
        self.soft.unwrap_or(self.limit)
    }

    /// What is used at `t`: as reported before the reset, 0 from it on.
    pub fn used_at(&self, t: Time) -> u64 {
        match self.resets_at {
            Some(r) if r <= t => 0,
            _ => self.used,
        }
    }

    /// The next reset after `t`, if known: `resets_at`, then every `every` after it.
    pub fn next_reset(&self, t: Time) -> Option<Time> {
        let r = self.resets_at?;
        if r > t {
            return Some(r);
        }
        let p = self.every?.as_ms();
        let k = (t - r).as_ms() / p + 1;
        Some(r + Dur(k * p))
    }

    /// Whether it stops new starts at `t`.
    pub fn spent_at(&self, t: Time) -> bool {
        self.used_at(t) >= self.stop_level()
    }
}

/// Why a quota stops new starts: the window that holds them longest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Spent<'q> {
    pub window: &'q Window,
    /// When every spent window has refilled; `None` when one of them has no known reset.
    pub reopens: Option<Time>,
}

impl Quota {
    /// Whether new starts wait at `t`, and until when: the spent window with the latest reset (one with no
    /// known reset counts as latest), ties to the first in order.
    pub fn spent_at(&self, t: Time) -> Option<Spent<'_>> {
        let mut out: Option<Spent<'_>> = None;
        for w in self.windows.iter().filter(|w| w.spent_at(t)) {
            let reopens = w.resets_at.filter(|r| *r > t);
            let later = match &out {
                None => true,
                Some(s) => match (s.reopens, reopens) {
                    (None, _) => false,
                    (Some(_), None) => true,
                    (Some(a), Some(b)) => b > a,
                },
            };
            if later {
                out = Some(Spent { window: w, reopens });
            }
        }
        out
    }

    /// Every window at or past a level at `t`, the highest level each, in the windows' order.
    pub fn crossings(&self, resource: ResourceId, t: Time) -> Vec<QuotaCrossing> {
        self.windows
            .iter()
            .filter_map(|w| {
                let used = w.used_at(t);
                let level = if used >= w.limit {
                    Level::Hard
                } else if w.soft.is_some_and(|s| used >= s) {
                    Level::Soft
                } else {
                    return None;
                };
                Some(QuotaCrossing {
                    resource,
                    window: w.name.clone(),
                    level,
                    used,
                    limit: w.limit,
                    resets_at: w.next_reset(t),
                })
            })
            .collect()
    }

    /// Checks a report: named windows, unique names, a limit above 0, the soft level not above it, a positive
    /// refill period that follows a reset.
    pub fn check(&self) -> Result<(), String> {
        for (i, w) in self.windows.iter().enumerate() {
            if w.name.is_empty() {
                return Err("a quota window needs a name".into());
            }
            if self.windows[..i].iter().any(|o| o.name == w.name) {
                return Err(format!("quota window {:?} is given twice", w.name));
            }
            if w.limit == 0 {
                return Err(format!("quota window {:?} has limit 0", w.name));
            }
            if let Some(s) = w.soft
                && s > w.limit
            {
                return Err(format!(
                    "quota window {:?}: soft level {s} above limit {}",
                    w.name, w.limit
                ));
            }
            match (w.every, w.resets_at) {
                (Some(p), _) if p <= Dur::ZERO => {
                    return Err(format!("quota window {:?}: every must be positive", w.name));
                }
                (Some(_), None) => {
                    return Err(format!("quota window {:?}: every needs resets_at", w.name));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// A quota window of a held resource at or past one of its levels: told to the holders.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaCrossing {
    pub resource: ResourceId,
    pub window: String,
    pub level: Level,
    /// What is used now.
    pub used: u64,
    pub limit: u64,
    /// Its next reset, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_at: Option<Time>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(name: &str, used: u64, soft: Option<u64>, resets_at: Option<i64>) -> Window {
        Window {
            name: name.into(),
            used,
            limit: 100,
            soft,
            resets_at: resets_at.map(Time),
            every: None,
        }
    }

    #[test]
    fn a_spent_window_reopens_at_its_reset() {
        let q = Quota {
            windows: vec![window("week", 84, Some(80), Some(1_000))],
        };
        let s = q.spent_at(Time(0)).unwrap();
        assert_eq!(
            (s.window.name.as_str(), s.reopens),
            ("week", Some(Time(1_000)))
        );
        assert!(q.spent_at(Time(999)).is_some());
        assert_eq!(q.spent_at(Time(1_000)), None);
    }

    #[test]
    fn the_latest_reset_of_the_spent_windows_holds_the_starts() {
        let q = Quota {
            windows: vec![
                window("5h", 100, None, Some(500)),
                window("week", 90, Some(80), Some(2_000)),
                window("day", 10, Some(80), Some(100)),
            ],
        };
        let s = q.spent_at(Time(0)).unwrap();
        assert_eq!(
            (s.window.name.as_str(), s.reopens),
            ("week", Some(Time(2_000)))
        );
        let unknown = Quota {
            windows: vec![
                window("week", 90, Some(80), Some(2_000)),
                window("x", 100, None, None),
            ],
        };
        let s = unknown.spent_at(Time(0)).unwrap();
        assert_eq!((s.window.name.as_str(), s.reopens), ("x", None));
    }

    #[test]
    fn crossings_name_the_highest_level_and_the_next_reset() {
        let mut w = window("week", 84, Some(80), Some(1_000));
        w.every = Some(Dur(7_000));
        let q = Quota {
            windows: vec![
                w,
                window("5h", 100, None, None),
                window("day", 10, Some(80), None),
            ],
        };
        let c = q.crossings(ResourceId(3), Time(0));
        let got: Vec<(&str, Level, Option<Time>)> = c
            .iter()
            .map(|c| (c.window.as_str(), c.level, c.resets_at))
            .collect();
        assert_eq!(
            got,
            [
                ("week", Level::Soft, Some(Time(1_000))),
                ("5h", Level::Hard, None)
            ]
        );
        assert!(q.crossings(ResourceId(3), Time(1_000))[0].window == "5h");
        assert_eq!(q.windows[0].next_reset(Time(1_000)), Some(Time(8_000)));
        assert_eq!(q.windows[0].next_reset(Time(8_001)), Some(Time(15_000)));
    }

    #[test]
    fn bad_reports_are_refused() {
        let ok = window("w", 1, Some(50), Some(10));
        assert!(
            Quota {
                windows: vec![ok.clone()]
            }
            .check()
            .is_ok()
        );
        assert!(
            Quota {
                windows: vec![ok.clone(), ok.clone()]
            }
            .check()
            .is_err()
        );
        let mut w = ok.clone();
        w.soft = Some(101);
        assert!(
            Quota { windows: vec![w] }
                .check()
                .unwrap_err()
                .contains("above")
        );
        let mut w = ok.clone();
        w.limit = 0;
        assert!(Quota { windows: vec![w] }.check().is_err());
        let mut w = ok;
        (w.every, w.resets_at) = (Some(Dur(5)), None);
        assert!(Quota { windows: vec![w] }.check().is_err());
    }
}
