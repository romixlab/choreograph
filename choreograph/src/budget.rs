//! Budgets on a lease: what a job may use while it holds its resources (tokens, context, energy, wall time,
//! any unit its holder counts), each with a soft and a hard level. Inside its budgets the holder decides
//! alone; crossing a level is an event on the lease that its holder hears at the next heartbeat or report and
//! acts on (finish up, hand over, stop), not a guard that holds it. Only a hard level whose budget says
//! [`OnHard::Stop`] ends the job, as a cancel would.
//!
//! A lease runs in steps: a [`restart`](crate::Authority::restart) in place starts a fresh process inside
//! the same lease, with the same place and resources (a checkpoint and restart, a context handoff). A budget
//! counts over the whole lease or over the current step ([`Scope`]).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::time::{Dur, Time};

/// A limit on what a job uses under its lease.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    /// What it limits: the name the holder reports its use under (`tokens`, `context`), or any name for a
    /// [`Meter::Wall`] budget.
    pub name: String,
    /// Who measures it: the holder's reports (default) or the authority's clock.
    #[serde(default)]
    pub meter: Meter,
    /// Over the whole lease (default) or the current step.
    #[serde(default)]
    pub scope: Scope,
    /// Crossing it tells the holder to plan its end (finish up, hand over). `None`: no soft level.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soft: Option<u64>,
    /// Crossing it tells the holder to end now, or ends the job (see [`OnHard`]). `None`: no hard level.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard: Option<u64>,
    /// What crossing the hard level does: tell the holder (default) or stop the job.
    #[serde(default)]
    pub on_hard: OnHard,
}

/// Who measures a budget's use.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Meter {
    /// The holder reports it ([`Authority::report_use`](crate::Authority::report_use)).
    #[default]
    Reported,
    /// Wall time in milliseconds since the lease was granted or the step began, pauses included, measured by
    /// the authority.
    Wall,
}

/// What a budget counts over.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// The whole lease: every step's last report added up.
    #[default]
    Lease,
    /// The current step only: a restart in place begins it again (a context window).
    Step,
}

/// What crossing a hard level does.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnHard {
    /// The holder is told and decides.
    #[default]
    Tell,
    /// The job is cancelled: its holder hears `stop`, cleans up and releases.
    Stop,
}

/// How far over a budget a lease is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    Soft,
    Hard,
}

/// A budget at or past one of its levels.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Crossing {
    pub budget: String,
    pub level: Level,
    /// What is used now, over the budget's scope.
    pub used: u64,
    /// The level crossed.
    pub limit: u64,
}

/// What a lease has used, step by step.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    /// Restarts in place so far: the current step is `restarts + 1`.
    #[serde(default)]
    pub restarts: u32,
    /// When the current step began.
    #[serde(default)]
    pub step_since: Time,
    /// Per name, the sum of the last reports of the steps before this one.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub closed: BTreeMap<String, u64>,
    /// Per name, the current step's last report.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub current: BTreeMap<String, u64>,
    /// Per budget, the highest level crossed so far (over its scope): a crossing is acted on once.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub crossed: BTreeMap<String, Level>,
}

impl Usage {
    /// A first step beginning now.
    pub fn new(now: Time) -> Usage {
        Usage {
            step_since: now,
            ..Usage::default()
        }
    }

    /// What a budget has used by `now` on a lease granted at `granted`.
    pub fn used(&self, b: &Budget, granted: Time, now: Time) -> u64 {
        let ms = |d: Dur| d.as_ms().max(0) as u64;
        let cur = self.current.get(&b.name).copied().unwrap_or(0);
        match (b.meter, b.scope) {
            (Meter::Wall, Scope::Lease) => ms(now - granted),
            (Meter::Wall, Scope::Step) => ms(now - self.step_since),
            (Meter::Reported, Scope::Lease) => self
                .closed
                .get(&b.name)
                .copied()
                .unwrap_or(0)
                .saturating_add(cur),
            (Meter::Reported, Scope::Step) => cur,
        }
    }

    /// Every budget at or past a level now, the highest level each, in the budgets' order.
    pub fn over(&self, budgets: &[Budget], granted: Time, now: Time) -> Vec<Crossing> {
        budgets
            .iter()
            .filter_map(|b| {
                let used = self.used(b, granted, now);
                let at = |l: Option<u64>| l.filter(|l| used >= *l);
                let (level, limit) = match (at(b.hard), at(b.soft)) {
                    (Some(h), _) => (Level::Hard, h),
                    (None, Some(s)) => (Level::Soft, s),
                    (None, None) => return None,
                };
                Some(Crossing {
                    budget: b.name.clone(),
                    level,
                    used,
                    limit,
                })
            })
            .collect()
    }

    /// Takes the holder's report of the current step: each name it gives is set, the others keep their
    /// value.
    pub fn report(&mut self, values: &BTreeMap<String, u64>) {
        for (k, v) in values {
            self.current.insert(k.clone(), *v);
        }
    }

    /// Begins the next step now: the current step's reports are added to the closed ones, and what step
    /// budgets had crossed is forgotten.
    pub fn restart(&mut self, budgets: &[Budget], now: Time) {
        for (k, v) in std::mem::take(&mut self.current) {
            let c = self.closed.entry(k).or_insert(0);
            *c = c.saturating_add(v);
        }
        self.crossed.retain(|name, _| {
            budgets
                .iter()
                .any(|b| b.name == *name && b.scope == Scope::Lease)
        });
        self.restarts += 1;
        self.step_since = now;
    }
}

/// Checks a job's budgets: unique names, at least one level, soft below hard.
pub fn check(budgets: &[Budget]) -> Result<(), String> {
    for (i, b) in budgets.iter().enumerate() {
        if b.name.is_empty() {
            return Err("a budget needs a name".into());
        }
        if budgets[..i].iter().any(|o| o.name == b.name) {
            return Err(format!("budget {:?} is given twice", b.name));
        }
        match (b.soft, b.hard) {
            (None, None) => return Err(format!("budget {:?} has no soft or hard level", b.name)),
            (Some(s), Some(h)) if s > h => {
                return Err(format!(
                    "budget {:?}: soft level {s} above hard level {h}",
                    b.name
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(name: &str, meter: Meter, scope: Scope, soft: u64, hard: u64) -> Budget {
        Budget {
            name: name.into(),
            meter,
            scope,
            soft: Some(soft),
            hard: Some(hard),
            on_hard: OnHard::Tell,
        }
    }

    #[test]
    fn lease_budgets_add_up_steps_and_step_budgets_begin_again() {
        let bs = [
            budget("tokens", Meter::Reported, Scope::Lease, 100, 200),
            budget("context", Meter::Reported, Scope::Step, 50, 80),
            budget("wall", Meter::Wall, Scope::Lease, 60_000, 120_000),
        ];
        let t0 = Time(1_000);
        let mut u = Usage::new(t0);
        u.report(&[("tokens".into(), 90), ("context".into(), 60)].into());
        let over = u.over(&bs, t0, t0 + Dur::secs(30));
        assert_eq!(
            over,
            [Crossing {
                budget: "context".into(),
                level: Level::Soft,
                used: 60,
                limit: 50
            }]
        );
        u.restart(&bs, t0 + Dur::secs(30));
        assert_eq!(u.restarts, 1);
        u.report(&[("tokens".into(), 30), ("context".into(), 10)].into());
        let over = u.over(&bs, t0, t0 + Dur::secs(130));
        let levels: Vec<(&str, Level, u64)> = over
            .iter()
            .map(|c| (c.budget.as_str(), c.level, c.used))
            .collect();
        assert_eq!(
            levels,
            [("tokens", Level::Soft, 120), ("wall", Level::Hard, 130_000)],
            "the step's context began again; tokens add up over the steps"
        );
    }

    #[test]
    fn bad_budgets_are_refused() {
        let ok = budget("t", Meter::Reported, Scope::Lease, 1, 2);
        assert!(check(std::slice::from_ref(&ok)).is_ok());
        assert!(check(&[ok.clone(), ok.clone()]).is_err());
        let mut b = ok.clone();
        b.soft = Some(3);
        assert!(check(&[b]).unwrap_err().contains("above"));
        let mut b = ok;
        (b.soft, b.hard) = (None, None);
        assert!(check(&[b]).is_err());
    }
}
