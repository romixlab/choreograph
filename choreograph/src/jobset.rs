//! The TOML job-set format (feature `toml`).
//!
//! ```toml
//! [[node]]
//! name = "pc-a"
//! labels = { arch = "x86_64" }
//!
//! [[node]]
//! name = "pc-b"
//! policy = { builds = "false" }   # only needs that name builds = "false" (node_labels or tolerate) land here
//!
//! [[kind]]
//! name = "ram"
//! unit = "bytes"          # count (default), bytes or millicores
//!
//! [[resource]]
//! name = "pc-a/ram"
//! kind = "ram"
//! node = "pc-a"
//! capacity = "32GiB"      # default 1: an exclusive resource
//!
//! [[resource]]
//! name = "supply"
//! kind = "supply"
//! node = "pc-a"
//! states = ["off", "3V3", "12V"]
//! initial = "off"         # default: the first state
//! safe = "off"            # default: the initial state
//! transitions = [{ to = "off", dur = "100ms" }, { from = "off", to = "12V", dur = "1s" }]
//!
//! [[job]]
//! name = "test-1"
//! needs = [
//!   { kind = "ram", amount = "2GiB", same_node = "t" },
//!   { kind = "supply", state = "12V", same_node = "t" },
//! ]
//! setup = "5s"                         # a fixed duration, or { p50 = "...", p90 = "..." }
//! run = { p50 = "2m", p90 = "8m" }
//! after = ["test-0"]
//! class = 1                            # higher is more important
//! arrival = "10m"                      # times are durations since the start
//! due = "2h"
//! group = "change-42"                  # jobs of a group start together
//! p_fail = 0.2                         # for the simulator
//! ```
//!
//! Amounts follow the kind's unit: `count` takes integers, `bytes` takes `10.7GiB`, `500MB`, `1024`, and
//! `millicores` takes cores (`6.8`) or millicores (`500m`).

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::model::{
    Estimate, GroupId, Job, JobId, Labels, Model, ModelBuilder, ModelError, Need, Phases, Preempt,
    Resource, Selector, SimHints, StateSpace, Transition, Unit,
};
use crate::time::{Dur, ParseDurError, Time};

/// Why a job set could not be loaded.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum JobSetError {
    #[error("TOML: {0}")]
    Toml(String),
    #[error("{context}: unknown {what} {name:?}")]
    Unknown {
        context: String,
        what: &'static str,
        name: String,
    },
    #[error("{context}: {value:?} is not an amount for unit {unit:?}")]
    BadAmount {
        context: String,
        value: String,
        unit: Unit,
    },
    #[error("{context}: {source}")]
    BadDur {
        context: String,
        source: ParseDurError,
    },
    #[error("{0}")]
    Model(#[from] ModelError),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    #[serde(default)]
    node: Vec<NodeT>,
    #[serde(default)]
    kind: Vec<KindT>,
    #[serde(default)]
    resource: Vec<ResourceT>,
    #[serde(default)]
    job: Vec<JobT>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeT {
    name: String,
    #[serde(default)]
    labels: Labels,
    #[serde(default)]
    policy: Labels,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KindT {
    name: String,
    #[serde(default)]
    unit: Unit,
}

#[derive(Deserialize, Clone)]
#[serde(untagged)]
enum Amount {
    Int(u64),
    Float(f64),
    Str(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceT {
    name: String,
    kind: String,
    node: String,
    capacity: Option<Amount>,
    #[serde(default)]
    labels: Labels,
    states: Option<Vec<String>>,
    initial: Option<String>,
    safe: Option<String>,
    #[serde(default)]
    transitions: Vec<TransitionT>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TransitionT {
    from: Option<String>,
    to: String,
    dur: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NeedT {
    kind: String,
    amount: Option<Amount>,
    resource: Option<String>,
    node: Option<String>,
    #[serde(default)]
    labels: Labels,
    #[serde(default)]
    node_labels: Labels,
    #[serde(default)]
    tolerate: Labels,
    state: Option<String>,
    same_node: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum EstimateT {
    Fixed(String),
    Range { p50: String, p90: String },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JobT {
    name: String,
    #[serde(default)]
    needs: Vec<NeedT>,
    setup: Option<EstimateT>,
    run: Option<EstimateT>,
    cleanup: Option<EstimateT>,
    #[serde(default)]
    after: Vec<String>,
    group: Option<String>,
    #[serde(default)]
    class: i32,
    weight: Option<u32>,
    arrival: Option<String>,
    release: Option<String>,
    due: Option<String>,
    #[serde(default)]
    preempt: Preempt,
    #[serde(default)]
    inputs: BTreeMap<String, String>,
    #[serde(default)]
    p_fail: f64,
    #[serde(default)]
    p_flake: f64,
}

/// Parses a job set from TOML text into a validated [`Model`].
pub fn from_toml(text: &str) -> Result<Model, JobSetError> {
    let f: File = toml::from_str(text).map_err(|e| JobSetError::Toml(e.to_string()))?;
    let mut b = ModelBuilder::new();
    let mut nodes = BTreeMap::new();
    for n in f.node {
        let id = b.node(n.name.clone(), n.labels);
        b.policy(id, n.policy);
        nodes.insert(n.name, id);
    }
    let mut kinds = BTreeMap::new();
    for k in f.kind {
        let id = b.kind(k.name.clone(), k.unit);
        kinds.insert(k.name, (id, k.unit));
    }
    let unknown = |context: &str, what, name: &str| JobSetError::Unknown {
        context: context.to_string(),
        what,
        name: name.to_string(),
    };
    let mut resources = BTreeMap::new();
    for r in f.resource {
        let ctx = format!("resource {:?}", r.name);
        let (kind, unit) = *kinds
            .get(&r.kind)
            .ok_or_else(|| unknown(&ctx, "kind", &r.kind))?;
        let node = *nodes
            .get(&r.node)
            .ok_or_else(|| unknown(&ctx, "node", &r.node))?;
        let capacity = match &r.capacity {
            Some(a) => amount(a, unit, &ctx)?,
            None => 1,
        };
        let states = match r.states {
            None => None,
            Some(states) => {
                let idx = |s: &str| {
                    states
                        .iter()
                        .position(|x| x == s)
                        .ok_or_else(|| unknown(&ctx, "state", s))
                };
                let initial = match &r.initial {
                    Some(s) => idx(s)?,
                    None => 0,
                };
                let safe = match &r.safe {
                    Some(s) => idx(s)?,
                    None => initial,
                };
                let mut transitions = Vec::new();
                for t in &r.transitions {
                    transitions.push(Transition {
                        from: t.from.as_deref().map(idx).transpose()?,
                        to: idx(&t.to)?,
                        dur: dur(&t.dur, &ctx)?,
                    });
                }
                Some(StateSpace {
                    states: states.clone(),
                    initial,
                    safe,
                    transitions,
                })
            }
        };
        let id = b.resource(Resource {
            name: r.name.clone(),
            kind,
            node,
            capacity,
            labels: r.labels,
            states,
        });
        resources.insert(r.name, id);
    }
    let job_ids: BTreeMap<String, JobId> = f
        .job
        .iter()
        .enumerate()
        .map(|(i, j)| (j.name.clone(), JobId(i as u32)))
        .collect();
    let mut groups: BTreeMap<String, Vec<JobId>> = BTreeMap::new();
    for (i, j) in f.job.into_iter().enumerate() {
        let ctx = format!("job {:?}", j.name);
        let mut needs = Vec::new();
        for n in j.needs {
            let (kind, unit) = *kinds
                .get(&n.kind)
                .ok_or_else(|| unknown(&ctx, "kind", &n.kind))?;
            let resource = n
                .resource
                .as_deref()
                .map(|r| {
                    resources
                        .get(r)
                        .copied()
                        .ok_or_else(|| unknown(&ctx, "resource", r))
                })
                .transpose()?;
            let node = n
                .node
                .as_deref()
                .map(|x| {
                    nodes
                        .get(x)
                        .copied()
                        .ok_or_else(|| unknown(&ctx, "node", x))
                })
                .transpose()?;
            needs.push(Need {
                kind,
                amount: match &n.amount {
                    Some(a) => amount(a, unit, &ctx)?,
                    None => 1,
                },
                select: Selector {
                    resource,
                    node,
                    labels: n.labels,
                    node_labels: n.node_labels,
                    tolerate: n.tolerate,
                },
                state: n.state,
                same_node: n.same_node,
            });
        }
        let est = |e: &Option<EstimateT>| -> Result<Estimate, JobSetError> {
            Ok(match e {
                None => Estimate::default(),
                Some(EstimateT::Fixed(s)) => Estimate::fixed(dur(s, &ctx)?),
                Some(EstimateT::Range { p50, p90 }) => Estimate {
                    p50: dur(p50, &ctx)?,
                    p90: dur(p90, &ctx)?,
                },
            })
        };
        let time = |s: &Option<String>| -> Result<Option<Time>, JobSetError> {
            s.as_deref()
                .map(|s| dur(s, &ctx).map(|d| Time::ZERO + d))
                .transpose()
        };
        let mut after = Vec::new();
        for a in &j.after {
            after.push(
                *job_ids
                    .get(a)
                    .ok_or_else(|| unknown(&ctx, "job (after)", a))?,
            );
        }
        if let Some(g) = &j.group {
            groups.entry(g.clone()).or_default().push(JobId(i as u32));
        }
        b.job(Job {
            name: j.name.clone(),
            needs,
            phases: Phases {
                setup: est(&j.setup)?,
                run: est(&j.run)?,
                cleanup: est(&j.cleanup)?,
            },
            after,
            group: None,
            class: j.class,
            weight: j.weight.unwrap_or(1),
            arrival: time(&j.arrival)?.unwrap_or(Time::ZERO),
            release: time(&j.release)?,
            due: time(&j.due)?,
            preempt: j.preempt,
            inputs: j.inputs,
            sim: SimHints {
                p_fail: j.p_fail,
                p_flake: j.p_flake,
            },
        });
    }
    for (name, members) in groups {
        let _: GroupId = b.group(name, members);
    }
    Ok(b.build()?)
}

fn dur(s: &str, ctx: &str) -> Result<Dur, JobSetError> {
    s.parse().map_err(|e| JobSetError::BadDur {
        context: ctx.to_string(),
        source: e,
    })
}

/// Parses an amount in a kind's unit.
pub fn parse_amount(text: &str, unit: Unit) -> Option<u64> {
    let t = text.trim();
    match unit {
        Unit::Count => t.parse().ok(),
        Unit::Millicores => match t.strip_suffix('m') {
            Some(milli) => milli.trim().parse().ok(),
            None => t
                .parse::<f64>()
                .ok()
                .filter(|v| *v >= 0.0)
                .map(|v| (v * 1000.0).round() as u64),
        },
        Unit::Bytes => {
            let split = t
                .find(|c: char| !(c.is_ascii_digit() || c == '.'))
                .unwrap_or(t.len());
            let (num, suffix) = t.split_at(split);
            let num: f64 = num.parse().ok()?;
            let scale: f64 = match suffix.trim() {
                "" | "B" => 1.0,
                "K" | "KB" | "k" | "kB" => 1e3,
                "M" | "MB" => 1e6,
                "G" | "GB" => 1e9,
                "T" | "TB" => 1e12,
                "Ki" | "KiB" => 1024.0,
                "Mi" | "MiB" => 1024.0 * 1024.0,
                "Gi" | "GiB" => 1024.0 * 1024.0 * 1024.0,
                "Ti" | "TiB" => 1024.0f64.powi(4),
                _ => return None,
            };
            (num >= 0.0).then(|| (num * scale).round() as u64)
        }
    }
}

fn amount(a: &Amount, unit: Unit, ctx: &str) -> Result<u64, JobSetError> {
    let text = match a {
        Amount::Int(i) => i.to_string(),
        Amount::Float(f) => f.to_string(),
        Amount::Str(s) => s.clone(),
    };
    parse_amount(&text, unit).ok_or_else(|| JobSetError::BadAmount {
        context: ctx.to_string(),
        value: text,
        unit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SET: &str = r#"
[[node]]
name = "pc-a"

[[kind]]
name = "ram"
unit = "bytes"

[[kind]]
name = "cores"
unit = "millicores"

[[kind]]
name = "supply"

[[resource]]
name = "pc-a/ram"
kind = "ram"
node = "pc-a"
capacity = "32GiB"

[[resource]]
name = "pc-a/cores"
kind = "cores"
node = "pc-a"
capacity = 24

[[resource]]
name = "supply"
kind = "supply"
node = "pc-a"
states = ["off", "3V3", "12V"]
transitions = [{ to = "off", dur = "100ms" }, { from = "off", to = "12V", dur = "1s" }]

[[job]]
name = "build"
needs = [{ kind = "ram", amount = "10.7GiB", same_node = "b" }, { kind = "cores", amount = 6.8, same_node = "b" }]
run = { p50 = "138s", p90 = "471s" }
p_fail = 0.2

[[job]]
name = "test"
needs = [{ kind = "supply", state = "12V" }]
setup = "1s"
run = "10s"
after = ["build"]
class = 1
due = "1h"
group = "g"
"#;

    #[test]
    fn loads_a_job_set() {
        let m = from_toml(SET).unwrap();
        assert_eq!(m.jobs().len(), 2);
        let build = m.job(m.job_by_name("build").unwrap());
        assert_eq!(build.needs[0].amount, 11_489_037_517);
        assert_eq!(build.needs[1].amount, 6800);
        assert_eq!(build.phases.run.p90, Dur::secs(471));
        assert_eq!(build.sim.p_fail, 0.2);
        let r = m.resource(m.resource_by_name("pc-a/cores").unwrap());
        assert_eq!(r.capacity, 24_000);
        let test = m.job(m.job_by_name("test").unwrap());
        assert_eq!(test.after, vec![m.job_by_name("build").unwrap()]);
        assert_eq!(test.due, Some(Time(3_600_000)));
        assert!(test.group.is_some());
        let sp = m
            .resource(m.resource_by_name("supply").unwrap())
            .states
            .as_ref()
            .unwrap();
        assert_eq!(sp.setup_time(0, 2), Some(Dur::secs(1)));
    }

    #[test]
    fn node_policy_keeps_jobs_away_unless_asked() {
        let set = r#"
[[node]]
name = "light"
policy = { builds = "false" }
[[kind]]
name = "slot"
[[resource]]
name = "light/slot"
kind = "slot"
node = "light"
[[job]]
name = "lint"
needs = [{ kind = "slot", node_labels = { builds = "false" } }]
"#;
        let m = from_toml(set).unwrap();
        let light = m.node_by_name("light").unwrap();
        assert_eq!(m.node(light).policy["builds"], "false");
        let bad = set.replace(", node_labels = { builds = \"false\" }", "");
        assert!(matches!(
            from_toml(&bad),
            Err(JobSetError::Model(ModelError::NoMatch { .. }))
        ));
        let tolerant = set.replace("node_labels = {", "tolerate = {");
        let m = from_toml(&tolerant).unwrap();
        assert_eq!(m.job(JobId(0)).needs[0].select.tolerate["builds"], "false");
    }

    #[test]
    fn errors_name_the_place() {
        let e = from_toml("[[job]]\nname = \"x\"\nneeds = [{ kind = \"nope\" }]\n").unwrap_err();
        assert_eq!(e.to_string(), "job \"x\": unknown kind \"nope\"");
        let e = from_toml("[[job]]\nname = \"x\"\nrun = \"5 minutes\"\n").unwrap_err();
        assert!(matches!(e, JobSetError::BadDur { .. }));
        assert!(matches!(from_toml("[[jobs]]\n"), Err(JobSetError::Toml(_))));
    }

    #[test]
    fn amounts() {
        assert_eq!(parse_amount("500m", Unit::Millicores), Some(500));
        assert_eq!(parse_amount("1.5", Unit::Millicores), Some(1500));
        assert_eq!(parse_amount("2GB", Unit::Bytes), Some(2_000_000_000));
        assert_eq!(parse_amount("1KiB", Unit::Bytes), Some(1024));
        assert_eq!(parse_amount("3", Unit::Count), Some(3));
        assert_eq!(parse_amount("1.5", Unit::Count), None);
        assert_eq!(parse_amount("2 parsecs", Unit::Bytes), None);
    }
}
