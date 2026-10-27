//! Standing instructions: what a node may do while it can't reach the scheduler, handed out ahead of time.
//!
//! A service that schedules for a fleet gives every node a [`Standing`]: per job kind a [`Rule`] with the
//! options in order ([`Choice::Local`]: run here within limits; [`Choice::Go`]: send it to another node), then
//! wait. The node keeps the newest one it got (the highest [`Standing::serial`]) and, while the service does not
//! answer, asks [`decide`] what to do with a job: the same pure function on every node, so no node makes up a
//! rule of its own. A standing runs out: [`Standing::valid_ms`] and [`Rule::valid_ms`] count from the moment the
//! node received it, on the node's own clock, so a skewed clock can neither shorten nor stretch it. Past that,
//! every job waits.
//!
//! A node with no scheduler at all (a fleet of one) uses [`Standing::lone`]: whatever fits its room runs
//! there, with no end.
//!
//! ```
//! use std::collections::BTreeMap;
//! use choreograph::standing::{decide, LocalView, Standing, Verdict};
//! use choreograph::Time;
//!
//! let lone = Standing::lone("bench-1");
//! let view = LocalView { room: BTreeMap::from([("channels".into(), 4)]), ..LocalView::default() };
//! let needs = BTreeMap::from([("channels".into(), 2)]);
//! let v = decide(&lone, Time(0), Time(1_000_000_000), "soak", &needs, &view, &[]);
//! assert_eq!(v, Verdict::Run { choice: 0 });
//! ```

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::time::{Dur, Time};

/// The share key that stands for every resource kind not named on its own.
pub const ANY_KIND: &str = "*";

/// What a node may do while it can't reach the service, handed out ahead of time.
///
/// Strict on reading (unknown keys are refused): a key a reader skipped could widen a rule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Standing {
    /// The node it is for.
    pub node: String,
    /// Grows with every change for this node; a node keeps the highest it got.
    pub serial: u64,
    /// The service's API version it was made for; a client of another API ignores it.
    pub api: String,
    /// When the service made it (the service's clock), for people.
    pub issued: Time,
    /// How long it holds from the moment the node received it (the node's own clock). `None`: no end (a
    /// lone node's).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_ms: Option<u64>,
    /// Per job kind; a kind not listed takes `other`.
    #[serde(default)]
    pub kinds: BTreeMap<String, Rule>,
    pub other: Rule,
}

/// What to do with a job of one kind: the options in order, then wait.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    #[serde(default)]
    pub options: Vec<Choice>,
    /// Its own validity when shorter than the whole standing's (a short one for a `go` list that goes stale).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_ms: Option<u64>,
    /// One line for people when it comes to waiting: "gates wait for the scheduler on this node".
    #[serde(default)]
    pub why: String,
}

/// One option of a rule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Choice {
    /// Run here without asking, within limits.
    Local(Local),
    /// Send it to one of these nodes, in order; the node there decides by its own standing.
    Go(Go),
}

/// Run here without asking, within limits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Local {
    /// At most this many such jobs at once (all kinds sharing `pool`).
    pub max: u32,
    /// Jobs of kinds whose local option names the same pool count together for `max` and `share`. None: the
    /// kind's own pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    /// Per resource kind, the part of the node's room the pool's jobs may hold together, per mille.
    /// [`ANY_KIND`] (`"*"`) stands for every kind not named; a kind with no share is not limited here.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub share: BTreeMap<String, u32>,
    /// Hold back while the node is under pressure.
    #[serde(default)]
    pub pressure: bool,
    /// Each such job runs at most this long.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_run: Option<Dur>,
    /// A lock the client provides and takes before it runs the job (one at a time across machines). Opaque
    /// here: the client knows what it names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclusive: Option<String>,
}

/// Send the job to another node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Go {
    /// In order; the service filled it from where such jobs fit lately.
    pub nodes: Vec<String>,
}

/// A node's own view, supplied by the client for [`decide`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalView {
    /// The node's room per resource kind, as it measures it (the same numbers it reports).
    #[serde(default)]
    pub room: BTreeMap<String, u64>,
    /// Under pressure now.
    #[serde(default)]
    pub pressed: bool,
    /// The jobs it runs under its standing now.
    #[serde(default)]
    pub running: Vec<LocalJob>,
}

/// A job a node runs under its standing, without a lease.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalJob {
    pub kind: String,
    /// What it holds, per resource kind.
    #[serde(default)]
    pub needs: BTreeMap<String, u64>,
    /// Since when it runs (the node's clock).
    pub since: Time,
}

/// What to do with a job, by the standing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "do")]
pub enum Verdict {
    /// Run it here under the rule's option `choice` (a [`Choice::Local`]: its `max_run` and `exclusive`
    /// apply).
    Run { choice: usize },
    /// Send it to `node`; if that node can't take it, try `rest` in order, then ask again with them tried.
    Go { node: String, rest: Vec<String> },
    /// No option fits now: wait and ask again. `until_ms`: how long the rule still holds (none: no end).
    Wait { why: String, until_ms: Option<u64> },
    /// The standing ran out at `at` (the node's clock): every job waits for the service.
    Expired { at: Time },
}

impl Standing {
    /// The built-in standing of a node with no scheduler: every kind runs here as long as it fits the room
    /// (all kinds share it), not under pressure, with no end.
    pub fn lone(node: &str) -> Standing {
        Standing {
            node: node.into(),
            serial: 0,
            api: String::new(),
            issued: Time::ZERO,
            valid_ms: None,
            kinds: BTreeMap::new(),
            other: Rule {
                options: vec![Choice::Local(Local {
                    max: u32::MAX,
                    pool: Some("lone".into()),
                    share: BTreeMap::from([(ANY_KIND.into(), 1000)]),
                    pressure: true,
                    max_run: None,
                    exclusive: None,
                })],
                valid_ms: None,
                why: "no scheduler: what fits this node's room runs here".into(),
            },
        }
    }

    /// The rule for a job kind.
    pub fn rule(&self, kind: &str) -> &Rule {
        self.kinds.get(kind).unwrap_or(&self.other)
    }

    /// How long the rule for `kind` holds from receipt: the shorter of the rule's and the whole's. None: no
    /// end.
    pub fn valid_for(&self, kind: &str) -> Option<u64> {
        min_some(self.rule(kind).valid_ms, self.valid_ms)
    }

    /// When the rule for `kind` runs out, for a standing received at `got_at`. None: never.
    pub fn ends(&self, kind: &str, got_at: Time) -> Option<Time> {
        self.valid_for(kind).map(|ms| after(got_at, ms))
    }

    /// When the whole standing runs out, for one received at `got_at`. None: never.
    pub fn ends_all(&self, got_at: Time) -> Option<Time> {
        self.valid_ms.map(|ms| after(got_at, ms))
    }

    /// The same instructions, whatever the serial and issue time: a service gives a new serial only when this
    /// changes.
    pub fn same_rules(&self, o: &Standing) -> bool {
        (
            &self.node,
            &self.api,
            self.valid_ms,
            &self.kinds,
            &self.other,
        ) == (&o.node, &o.api, o.valid_ms, &o.kinds, &o.other)
    }

    /// The pools a job of `kind` counts in: each local option's pool, the kind's own name when it names none.
    fn pools_of<'a>(&'a self, kind: &'a str) -> impl Iterator<Item = &'a str> {
        self.rule(kind).options.iter().filter_map(move |c| match c {
            Choice::Local(l) => Some(l.pool.as_deref().unwrap_or(kind)),
            Choice::Go(_) => None,
        })
    }
}

/// What to do with a job of `kind` needing `needs` (per resource kind) on a node that received `s` at `got_at`
/// and sees `view` at `now`, with the nodes in `tried` already tried. Pure: the same answer for the same
/// arguments on every node.
///
/// Past the rule's end it is [`Verdict::Expired`]. Otherwise the rule's options are tried in order: a
/// [`Choice::Local`] fits when the pool has fewer than `max` jobs running, the node is not under pressure (if
/// the option says so) and, for every need with a share, what the pool's running jobs hold plus this need is
/// at most that share of the room; a [`Choice::Go`] fits while one of its nodes is untried (and is not this
/// node). The first that fits decides; none fitting is [`Verdict::Wait`].
pub fn decide(
    s: &Standing,
    got_at: Time,
    now: Time,
    kind: &str,
    needs: &BTreeMap<String, u64>,
    view: &LocalView,
    tried: &[String],
) -> Verdict {
    let ends = s.ends(kind, got_at);
    if let Some(at) = ends
        && now > at
    {
        return Verdict::Expired { at };
    }
    let rule = s.rule(kind);
    let mut why = Vec::new();
    if !rule.why.is_empty() {
        why.push(rule.why.clone());
    }
    for (i, c) in rule.options.iter().enumerate() {
        match c {
            Choice::Local(l) => match fits(s, l, kind, needs, view) {
                Ok(()) => return Verdict::Run { choice: i },
                Err(e) => why.push(e),
            },
            Choice::Go(g) => {
                let mut left = g
                    .nodes
                    .iter()
                    .filter(|n| **n != s.node && !tried.contains(n));
                if let Some(node) = left.next() {
                    return Verdict::Go {
                        node: node.clone(),
                        rest: left.cloned().collect(),
                    };
                }
                if !g.nodes.is_empty() {
                    why.push(format!("tried {}", g.nodes.join(", ")));
                }
            }
        }
    }
    if why.is_empty() {
        why.push(format!("no option for {kind} on {}", s.node));
    }
    Verdict::Wait {
        why: why.join("; "),
        until_ms: ends.map(|at| u64::try_from((at - now).as_ms()).unwrap_or(0)),
    }
}

/// Whether one more job of `kind` fits a local option now; the reason when not.
fn fits(
    s: &Standing,
    l: &Local,
    kind: &str,
    needs: &BTreeMap<String, u64>,
    view: &LocalView,
) -> Result<(), String> {
    let pool = l.pool.as_deref().unwrap_or(kind);
    let mates: Vec<&LocalJob> = view
        .running
        .iter()
        .filter(|j| j.kind == kind || s.pools_of(&j.kind).any(|p| p == pool))
        .collect();
    let what = match &l.pool {
        Some(p) => format!("pool {p}"),
        None => kind.to_string(),
    };
    if mates.len() >= l.max as usize {
        return Err(format!(
            "{what}: at most {} at once here, {} running",
            l.max,
            mates.len()
        ));
    }
    if l.pressure && view.pressed {
        return Err(format!("{what}: the node is under pressure"));
    }
    for (k, &amount) in needs {
        let Some(pm) = share_of(l, k) else {
            continue;
        };
        let room = view.room.get(k).copied().unwrap_or(0);
        let limit = (u128::from(room) * u128::from(pm) / 1000) as u64;
        let held: u64 = mates
            .iter()
            .map(|j| j.needs.get(k).copied().unwrap_or(0))
            .fold(0, u64::saturating_add);
        if held.saturating_add(amount) > limit {
            return Err(format!(
                "{what}: needs {amount} {k}, {} of {limit} ({pm}‰ of the room) left",
                limit.saturating_sub(held)
            ));
        }
    }
    Ok(())
}

/// The share of the room a local option gives a resource kind, per mille; None: not limited.
pub fn share_of(l: &Local, kind: &str) -> Option<u32> {
    l.share.get(kind).or_else(|| l.share.get(ANY_KIND)).copied()
}

fn min_some(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

fn after(t: Time, ms: u64) -> Time {
    Time(t.0.saturating_add(i64::try_from(ms).unwrap_or(i64::MAX)))
}

impl fmt::Display for Choice {
    /// One line for people: "here, at most 2 at once, ram 800‰, not under pressure".
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Choice::Local(l) => {
                write!(f, "here")?;
                if l.max != u32::MAX {
                    write!(f, ", at most {} at once", l.max)?;
                }
                if let Some(p) = &l.pool {
                    write!(f, " (pool {p})")?;
                }
                for (k, pm) in &l.share {
                    let k = if k == ANY_KIND { "any room" } else { k };
                    write!(f, ", {k} {pm}‰")?;
                }
                if l.pressure {
                    write!(f, ", not under pressure")?;
                }
                if let Some(d) = l.max_run {
                    write!(f, ", each at most {d}")?;
                }
                if let Some(x) = &l.exclusive {
                    write!(f, ", one at a time ({x})")?;
                }
                Ok(())
            }
            Choice::Go(g) if g.nodes.is_empty() => write!(f, "to another node (none fits now)"),
            Choice::Go(g) => write!(f, "to {}", g.nodes.join(", then ")),
        }
    }
}

impl fmt::Display for Rule {
    /// The options in order, then "wait".
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for c in &self.options {
            write!(f, "{c}; then ")?;
        }
        write!(f, "wait")?;
        if !self.why.is_empty() {
            write!(f, " ({})", self.why)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn local(max: u32, share: &[(&str, u32)]) -> Choice {
        Choice::Local(Local {
            max,
            pool: None,
            share: share.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
            pressure: true,
            max_run: None,
            exclusive: None,
        })
    }

    fn standing(kinds: &[(&str, Rule)], valid: Option<u64>) -> Standing {
        Standing {
            node: "n1".into(),
            serial: 1,
            api: "v1".into(),
            issued: Time(0),
            valid_ms: valid,
            kinds: kinds
                .iter()
                .map(|(k, r)| (k.to_string(), r.clone()))
                .collect(),
            other: Rule {
                why: "other kinds wait".into(),
                ..Rule::default()
            },
        }
    }

    fn map(kv: &[(&str, u64)]) -> BTreeMap<String, u64> {
        kv.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    fn job(kind: &str, needs: &[(&str, u64)]) -> LocalJob {
        LocalJob {
            kind: kind.into(),
            needs: map(needs),
            since: Time(0),
        }
    }

    #[test]
    fn local_then_go_then_wait() {
        let build = Rule {
            options: vec![
                local(2, &[("ram", 800)]),
                Choice::Go(Go {
                    nodes: vec!["n2".into(), "n1".into(), "n3".into()],
                }),
            ],
            valid_ms: Some(3_600_000),
            why: "builds wait for the scheduler".into(),
        };
        let s = standing(&[("build", build)], Some(12 * 3_600_000));
        let mut view = LocalView {
            room: map(&[("ram", 100)]),
            ..LocalView::default()
        };
        let need = map(&[("ram", 50)]);
        let d = |view: &LocalView, tried: &[String], now| {
            decide(&s, Time(1000), Time(now), "build", &need, view, tried)
        };
        assert_eq!(d(&view, &[], 2000), Verdict::Run { choice: 0 });
        view.running.push(job("build", &[("ram", 50)]));
        // 50 held + 50 > 80: goes, never to itself.
        assert_eq!(
            d(&view, &[], 2000),
            Verdict::Go {
                node: "n2".into(),
                rest: vec!["n3".into()]
            }
        );
        assert_eq!(
            d(&view, &["n2".into()], 2000),
            Verdict::Go {
                node: "n3".into(),
                rest: vec![]
            }
        );
        let Verdict::Wait { why, until_ms } = d(&view, &["n2".into(), "n3".into()], 2000) else {
            panic!()
        };
        assert_eq!(until_ms, Some(3_600_000 - 1000));
        assert!(why.starts_with("builds wait for the scheduler; build: needs 50 ram, 30 of 80"));
        // The rule's own hour, from receipt.
        assert_eq!(
            d(&view, &[], 1000 + 3_600_001),
            Verdict::Expired {
                at: Time(1000 + 3_600_000)
            }
        );
        // Other kinds take `other`.
        assert!(matches!(
            decide(&s, Time(0), Time(0), "dist", &need, &view, &[]),
            Verdict::Wait { why, .. } if why == "other kinds wait"
        ));
        // Under pressure the local option holds back.
        view.running.clear();
        view.pressed = true;
        assert!(matches!(d(&view, &[], 2000), Verdict::Go { .. }));
    }

    #[test]
    fn a_pool_counts_every_kind_in_it() {
        let pooled = |max| Rule {
            options: vec![Choice::Local(Local {
                max,
                pool: Some("heavy".into()),
                share: BTreeMap::new(),
                pressure: false,
                max_run: None,
                exclusive: None,
            })],
            ..Rule::default()
        };
        let s = standing(&[("build", pooled(2)), ("dist", pooled(2))], None);
        let view = LocalView {
            running: vec![job("build", &[]), job("dist", &[])],
            ..LocalView::default()
        };
        assert!(matches!(
            decide(&s, Time(0), Time(5), "build", &BTreeMap::new(), &view, &[]),
            Verdict::Wait { why, until_ms: None } if why.contains("pool heavy: at most 2 at once here, 2 running")
        ));
    }

    #[test]
    fn standing_is_strict_json() {
        let s = Standing::lone("n");
        let text = serde_json::to_string(&s).unwrap();
        assert_eq!(serde_json::from_str::<Standing>(&text).unwrap(), s);
        let widened = text.replacen("\"max\"", "\"unlimited\":true,\"max\"", 1);
        assert!(serde_json::from_str::<Standing>(&widened).is_err());
        assert_eq!(
            s.other.to_string(),
            "here (pool lone), any room 1000‰, not under pressure; then wait (no scheduler: what fits this node's room runs here)"
        );
    }

    fn arb_local() -> impl Strategy<Value = Local> {
        (
            0u32..4,
            prop::option::of(Just("p".to_string())),
            prop::collection::btree_map(
                prop::sample::select(vec!["ram", "cores", ANY_KIND]),
                0u32..1200,
                0..3,
            ),
            any::<bool>(),
        )
            .prop_map(|(max, pool, share, pressure)| Local {
                max,
                pool,
                share: share.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
                pressure,
                max_run: None,
                exclusive: None,
            })
    }

    fn arb_needs() -> impl Strategy<Value = BTreeMap<String, u64>> {
        prop::collection::btree_map(
            prop::sample::select(vec!["ram", "cores", "gpu"]).prop_map(String::from),
            0u64..200,
            0..3,
        )
    }

    fn arb_view() -> impl Strategy<Value = LocalView> {
        (
            arb_needs(),
            any::<bool>(),
            prop::collection::vec(
                (
                    prop::sample::select(vec!["build", "dist", "gate"]),
                    arb_needs(),
                ),
                0..5,
            ),
        )
            .prop_map(|(room, pressed, running)| LocalView {
                room,
                pressed,
                running: running
                    .into_iter()
                    .map(|(k, needs)| LocalJob {
                        kind: k.into(),
                        needs,
                        since: Time(0),
                    })
                    .collect(),
            })
    }

    fn arb_standing() -> impl Strategy<Value = Standing> {
        let rule = (
            prop::collection::vec(
                prop_oneof![
                    arb_local().prop_map(Choice::Local),
                    prop::collection::vec(prop::sample::select(vec!["n1", "n2", "n3"]), 0..3)
                        .prop_map(|n| Choice::Go(Go {
                            nodes: n.into_iter().map(String::from).collect()
                        })),
                ],
                0..3,
            ),
            prop::option::of(0u64..10_000),
        )
            .prop_map(|(options, valid_ms)| Rule {
                options,
                valid_ms,
                why: String::new(),
            });
        (
            prop::collection::btree_map(
                prop::sample::select(vec!["build", "dist", "gate"]).prop_map(String::from),
                rule.clone(),
                0..3,
            ),
            rule,
            prop::option::of(0u64..10_000),
        )
            .prop_map(|(kinds, other, valid_ms)| Standing {
                node: "n1".into(),
                serial: 1,
                api: "v1".into(),
                issued: Time(0),
                valid_ms,
                kinds,
                other,
            })
    }

    proptest! {
        /// A run never takes the pool past its `max` or a resource kind past its share of the room.
        #[test]
        fn decide_never_over_max_or_share(
            s in arb_standing(),
            kind in prop::sample::select(vec!["build", "dist", "gate", "other"]),
            needs in arb_needs(),
            view in arb_view(),
            now in 0i64..20_000,
        ) {
            if let Verdict::Run { choice } = decide(&s, Time(0), Time(now), kind, &needs, &view, &[]) {
                let Choice::Local(l) = &s.rule(kind).options[choice] else {
                    panic!("a run is under a local option")
                };
                let pool = l.pool.as_deref().unwrap_or(kind);
                let mates: Vec<&LocalJob> = view.running.iter()
                    .filter(|j| j.kind == kind || s.pools_of(&j.kind).any(|p| p == pool))
                    .collect();
                prop_assert!(mates.len() < l.max as usize);
                prop_assert!(!(l.pressure && view.pressed));
                for (k, &amount) in &needs {
                    if let Some(pm) = share_of(l, k) {
                        let room = u128::from(view.room.get(k).copied().unwrap_or(0));
                        let held: u128 = mates.iter().map(|j| u128::from(j.needs.get(k).copied().unwrap_or(0))).sum();
                        prop_assert!((held + u128::from(amount)) * 1000 <= room * u128::from(pm));
                    }
                }
            }
        }

        /// Past its end a standing only ever says wait.
        #[test]
        fn expired_always_waits(
            s in arb_standing(),
            kind in prop::sample::select(vec!["build", "dist", "gate", "other"]),
            needs in arb_needs(),
            view in arb_view(),
            got in -1_000i64..1_000,
            late in 1i64..1_000_000,
        ) {
            if let Some(ms) = s.valid_for(kind) {
                let now = Time(got + ms as i64 + late);
                prop_assert_eq!(
                    decide(&s, Time(got), now, kind, &needs, &view, &[]),
                    Verdict::Expired { at: Time(got + ms as i64) }
                );
            }
        }

        /// A lone node's standing never runs out, and runs what fits its room.
        #[test]
        fn lone_never_expires(
            kind in "[a-z]{1,8}",
            needs in arb_needs(),
            view in arb_view(),
            got in any::<i64>(),
            now in any::<i64>(),
        ) {
            let s = Standing::lone("n1");
            let v = decide(&s, Time(got), Time(now), &kind, &needs, &view, &[]);
            let fits = !view.pressed && needs.iter().all(|(k, a)| {
                let held: u64 = view.running.iter().map(|j| j.needs.get(k).copied().unwrap_or(0)).sum();
                held + a <= view.room.get(k).copied().unwrap_or(0)
            });
            let expired = matches!(v, Verdict::Expired { .. });
            prop_assert!(!expired);
            prop_assert_eq!(v == Verdict::Run { choice: 0 }, fits);
        }
    }
}
