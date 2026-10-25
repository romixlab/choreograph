//! The static model: nodes, resource kinds, resources, jobs and groups.
//!
//! In the terms of resource-constrained project scheduling (RCPSP), every resource here is *renewable*: it has a
//! capacity per unit of time and gets its amount back when the job that holds it ends. An exclusive resource (a
//! debug probe, an oscilloscope, a relay channel, a repository's main branch) is one with capacity 1. A resource
//! may also carry a *state* (`supply: off | 3V3 | 12V`) with setup transitions between states and one safe state
//! to return to.
//!
//! A [`Model`] is built with [`ModelBuilder`] (or loaded from a TOML job set) and validated once; afterwards ids
//! are dense indices and every reference in it is known to be valid.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::time::{Dur, Time};

macro_rules! id_type {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub u32);

        impl $name {
            /// The index into the model's list.
            pub const fn index(self) -> usize {
                self.0 as usize
            }
        }
    };
}

id_type!(
    /// A node: a place resources live on (a PC, a bench).
    NodeId
);
id_type!(
    /// A resource kind (`ram`, `cores`, `probe`, `scope`).
    KindId
);
id_type!(
    /// A resource.
    ResourceId
);
id_type!(
    /// A job.
    JobId
);
id_type!(
    /// A group of jobs that start all together or not at all.
    GroupId
);

/// Labels for matching needs to resources (`arch = "arm64"`, `probe = "jlink"`).
pub type Labels = BTreeMap<String, String>;

/// How amounts of a kind are counted and printed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Unit {
    /// Whole items (slots, probes, checkouts).
    #[default]
    Count,
    /// Bytes (RAM, disk); job sets may write `10.7GiB`, `500MB`.
    Bytes,
    /// CPU in thousandths of a core; job sets write cores (`6.8`).
    Millicores,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
    pub labels: Labels,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Kind {
    pub name: String,
    pub unit: Unit,
}

/// The states a resource can be in, how long it takes to move between them, and which one is safe.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StateSpace {
    /// State names; indices into this list identify states.
    pub states: Vec<String>,
    /// The state at the start.
    pub initial: usize,
    /// Where cleanup and recovery take the resource (supply off, relays open).
    pub safe: usize,
    /// Setup transitions with their duration. A move without a declared transition is not possible.
    pub transitions: Vec<Transition>,
}

/// A setup step that changes a resource's state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Transition {
    /// From this state, or from any state when `None`.
    pub from: Option<usize>,
    pub to: usize,
    pub dur: Dur,
}

impl StateSpace {
    /// Index of a state by name.
    pub fn state(&self, name: &str) -> Option<usize> {
        self.states.iter().position(|s| s == name)
    }

    /// The setup time from one state to another: zero when already there, `None` when no transition is declared.
    pub fn setup_time(&self, from: usize, to: usize) -> Option<Dur> {
        if from == to {
            return Some(Dur::ZERO);
        }
        self.transitions
            .iter()
            .filter(|t| t.to == to && t.from.is_none_or(|f| f == from))
            .map(|t| t.dur)
            .min()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Resource {
    pub name: String,
    pub kind: KindId,
    pub node: NodeId,
    /// Amount available at once; 1 for an exclusive resource.
    pub capacity: u64,
    pub labels: Labels,
    /// States with setup transitions and a safe state, for stateful resources.
    pub states: Option<StateSpace>,
}

impl Resource {
    /// An exclusive resource: one holder at a time.
    pub fn is_exclusive(&self) -> bool {
        self.capacity == 1
    }
}

/// Which resources can satisfy a need.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Selector {
    /// Only this resource.
    pub resource: Option<ResourceId>,
    /// Only resources on this node.
    pub node: Option<NodeId>,
    /// Every label here must be on the resource with the same value.
    pub labels: Labels,
    /// Every label here must be on the resource's node with the same value.
    pub node_labels: Labels,
}

/// One thing a job needs: an amount of one resource of a kind, picked by a selector.
///
/// A need is satisfied by exactly one resource (RAM on one PC, not split across two).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Need {
    pub kind: KindId,
    pub amount: u64,
    pub select: Selector,
    /// The state the resource must be in while the job runs (`12V`); reaching it costs the setup transition.
    pub state: Option<String>,
    /// Needs of one job with the same tag must be satisfied on the same node (a build's RAM, cores and checkout).
    pub same_node: Option<String>,
}

/// A duration estimate: median and 90th percentile.
///
/// Durations are right-skewed; plan reservations with `p90`, show `p50` as the likely value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Estimate {
    pub p50: Dur,
    pub p90: Dur,
}

impl Estimate {
    /// An exact duration (p50 = p90).
    pub const fn fixed(d: Dur) -> Estimate {
        Estimate { p50: d, p90: d }
    }
}

/// The phases of a job, in order. Cleanup runs after a failure or a cancel too.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    Setup,
    Run,
    Cleanup,
}

impl Phase {
    pub const ALL: [Phase; 3] = [Phase::Setup, Phase::Run, Phase::Cleanup];
}

/// Duration estimates per phase.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Phases {
    pub setup: Estimate,
    pub run: Estimate,
    pub cleanup: Estimate,
}

impl Phases {
    pub fn get(&self, p: Phase) -> Estimate {
        match p {
            Phase::Setup => self.setup,
            Phase::Run => self.run,
            Phase::Cleanup => self.cleanup,
        }
    }

    /// The whole job's median, the sum of the phases' medians (a rough central value).
    pub fn total_p50(&self) -> Dur {
        Phase::ALL.iter().map(|p| self.get(*p).p50).sum()
    }

    /// The sum of the phases' p90s: an upper estimate for reservations (the p90 of a sum is at most this).
    pub fn total_p90(&self) -> Dur {
        Phase::ALL.iter().map(|p| self.get(*p).p90).sum()
    }
}

/// Whether a running job may be interrupted for another.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Preempt {
    /// Never: a flash, a calibration, a build that would lose its work.
    #[default]
    Never,
    /// Only between phases.
    AtPhaseBoundary,
}

/// Hints for the simulator: how often the job fails or flakes. Ignored by the planner.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SimHints {
    /// Probability that the run phase fails (a red gate, a failed test).
    pub p_fail: f64,
    /// Probability of a flaky failure that would pass on a retry.
    pub p_flake: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Job {
    pub name: String,
    pub needs: Vec<Need>,
    pub phases: Phases,
    /// Jobs that must be done first.
    pub after: Vec<JobId>,
    pub group: Option<GroupId>,
    /// Priority class; higher is more important.
    pub class: i32,
    /// Weight for weighted objectives (Σ w_j C_j, Σ w_j T_j) and fair shares.
    pub weight: u32,
    /// When the job enters the queue.
    pub arrival: Time,
    /// Earliest start (r_j); `None` means on arrival.
    pub release: Option<Time>,
    /// Due date (d_j): a soft deadline that raises the priority as it nears.
    pub due: Option<Time>,
    pub preempt: Preempt,
    /// Named content hashes of everything that decides the result (firmware, test version, board), the cache key.
    pub inputs: BTreeMap<String, String>,
    pub sim: SimHints,
}

impl Job {
    /// The earliest time the job may start: the later of its arrival and release time.
    pub fn earliest_start(&self) -> Time {
        self.release.map_or(self.arrival, |r| r.max(self.arrival))
    }
}

/// Jobs that start together or not at all (a change that spans several repositories).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Group {
    pub name: String,
    pub members: Vec<JobId>,
}

/// A validated model. Build it with [`ModelBuilder`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Model {
    nodes: Vec<Node>,
    kinds: Vec<Kind>,
    resources: Vec<Resource>,
    jobs: Vec<Job>,
    groups: Vec<Group>,
}

impl Model {
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }
    pub fn kinds(&self) -> &[Kind] {
        &self.kinds
    }
    pub fn resources(&self) -> &[Resource] {
        &self.resources
    }
    pub fn jobs(&self) -> &[Job] {
        &self.jobs
    }
    pub fn groups(&self) -> &[Group] {
        &self.groups
    }
    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id.index()]
    }
    pub fn kind(&self, id: KindId) -> &Kind {
        &self.kinds[id.index()]
    }
    pub fn resource(&self, id: ResourceId) -> &Resource {
        &self.resources[id.index()]
    }
    pub fn job(&self, id: JobId) -> &Job {
        &self.jobs[id.index()]
    }
    pub fn group(&self, id: GroupId) -> &Group {
        &self.groups[id.index()]
    }
    pub fn job_ids(&self) -> impl Iterator<Item = JobId> + '_ {
        (0..self.jobs.len() as u32).map(JobId)
    }
    pub fn resource_ids(&self) -> impl Iterator<Item = ResourceId> + '_ {
        (0..self.resources.len() as u32).map(ResourceId)
    }
    pub fn job_by_name(&self, name: &str) -> Option<JobId> {
        self.jobs
            .iter()
            .position(|j| j.name == name)
            .map(|i| JobId(i as u32))
    }
    pub fn resource_by_name(&self, name: &str) -> Option<ResourceId> {
        self.resources
            .iter()
            .position(|r| r.name == name)
            .map(|i| ResourceId(i as u32))
    }
    pub fn node_by_name(&self, name: &str) -> Option<NodeId> {
        self.nodes
            .iter()
            .position(|n| n.name == name)
            .map(|i| NodeId(i as u32))
    }
    pub fn kind_by_name(&self, name: &str) -> Option<KindId> {
        self.kinds
            .iter()
            .position(|k| k.name == name)
            .map(|i| KindId(i as u32))
    }

    /// Whether resource `r` matches need `need` by kind, selector and capacity (ignoring what is held now).
    pub fn matches(&self, need: &Need, r: ResourceId) -> bool {
        let res = self.resource(r);
        let sel = &need.select;
        res.kind == need.kind
            && res.capacity >= need.amount
            && sel.resource.is_none_or(|x| x == r)
            && sel.node.is_none_or(|n| n == res.node)
            && sel.labels.iter().all(|(k, v)| res.labels.get(k) == Some(v))
            && sel
                .node_labels
                .iter()
                .all(|(k, v)| self.node(res.node).labels.get(k) == Some(v))
            && need
                .state
                .as_ref()
                .is_none_or(|s| res.states.as_ref().is_some_and(|sp| sp.state(s).is_some()))
    }

    /// The resources that could ever satisfy a need (empty: the need can never be met, a configuration error).
    pub fn candidates(&self, need: &Need) -> Vec<ResourceId> {
        self.resource_ids()
            .filter(|r| self.matches(need, *r))
            .collect()
    }
}

/// Why a model is not valid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ModelError {
    #[error("two {what}s are named {name:?}")]
    Duplicate { what: &'static str, name: String },
    #[error(
        "job {job:?}: need #{need} can never be met: no resource matches its kind, selector, state and amount"
    )]
    NoMatch { job: String, need: usize },
    #[error("job {job:?}: needs tagged same_node={tag:?} cannot all be met on one node")]
    NoCommonNode { job: String, tag: String },
    #[error("the dependencies form a cycle through job {job:?}")]
    Cycle { job: String },
    #[error("job {job:?}: estimate of phase {phase:?} has p90 below p50 or a negative value")]
    BadEstimate { job: String, phase: Phase },
    #[error("resource {resource:?}: {problem}")]
    BadStates { resource: String, problem: String },
    #[error("resource {resource:?}: capacity 0")]
    ZeroCapacity { resource: String },
    #[error(
        "job {job:?} is in group {group:?} but not listed as its member, or the other way round"
    )]
    GroupMismatch { job: String, group: String },
    #[error("{what} id {id} is out of range")]
    BadId { what: &'static str, id: u32 },
}

/// Builds a [`Model`] step by step; [`ModelBuilder::build`] validates it.
#[derive(Clone, Debug, Default)]
pub struct ModelBuilder {
    nodes: Vec<Node>,
    kinds: Vec<Kind>,
    resources: Vec<Resource>,
    jobs: Vec<Job>,
    groups: Vec<Group>,
}

impl ModelBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn node(&mut self, name: impl Into<String>, labels: Labels) -> NodeId {
        self.nodes.push(Node {
            name: name.into(),
            labels,
        });
        NodeId(self.nodes.len() as u32 - 1)
    }

    pub fn kind(&mut self, name: impl Into<String>, unit: Unit) -> KindId {
        self.kinds.push(Kind {
            name: name.into(),
            unit,
        });
        KindId(self.kinds.len() as u32 - 1)
    }

    pub fn resource(&mut self, r: Resource) -> ResourceId {
        self.resources.push(r);
        ResourceId(self.resources.len() as u32 - 1)
    }

    /// A pooled resource without labels or states.
    pub fn pool(
        &mut self,
        name: impl Into<String>,
        kind: KindId,
        node: NodeId,
        capacity: u64,
    ) -> ResourceId {
        self.resource(Resource {
            name: name.into(),
            kind,
            node,
            capacity,
            labels: Labels::new(),
            states: None,
        })
    }

    pub fn job(&mut self, j: Job) -> JobId {
        self.jobs.push(j);
        JobId(self.jobs.len() as u32 - 1)
    }

    /// A group of existing jobs; sets their `group`.
    pub fn group(&mut self, name: impl Into<String>, members: Vec<JobId>) -> GroupId {
        let id = GroupId(self.groups.len() as u32);
        for m in &members {
            if let Some(j) = self.jobs.get_mut(m.index()) {
                j.group = Some(id);
            }
        }
        self.groups.push(Group {
            name: name.into(),
            members,
        });
        id
    }

    /// Mutable access to a job added earlier (to add `after` links to jobs added later).
    pub fn job_mut(&mut self, id: JobId) -> &mut Job {
        &mut self.jobs[id.index()]
    }

    /// Checks every reference and constraint and returns the model.
    pub fn build(self) -> Result<Model, ModelError> {
        let m = Model {
            nodes: self.nodes,
            kinds: self.kinds,
            resources: self.resources,
            jobs: self.jobs,
            groups: self.groups,
        };
        validate(&m)?;
        Ok(m)
    }
}

fn dup<'a>(what: &'static str, names: impl Iterator<Item = &'a String>) -> Result<(), ModelError> {
    let mut seen = std::collections::BTreeSet::new();
    for n in names {
        if !seen.insert(n) {
            return Err(ModelError::Duplicate {
                what,
                name: n.clone(),
            });
        }
    }
    Ok(())
}

fn validate(m: &Model) -> Result<(), ModelError> {
    dup("node", m.nodes.iter().map(|n| &n.name))?;
    dup("kind", m.kinds.iter().map(|n| &n.name))?;
    dup("resource", m.resources.iter().map(|n| &n.name))?;
    dup("job", m.jobs.iter().map(|n| &n.name))?;
    dup("group", m.groups.iter().map(|n| &n.name))?;

    let bad = |what, id: u32, len: usize| {
        if id as usize >= len {
            Err(ModelError::BadId { what, id })
        } else {
            Ok(())
        }
    };
    for r in &m.resources {
        bad("kind", r.kind.0, m.kinds.len())?;
        bad("node", r.node.0, m.nodes.len())?;
        if r.capacity == 0 {
            return Err(ModelError::ZeroCapacity {
                resource: r.name.clone(),
            });
        }
        if let Some(sp) = &r.states {
            let n = sp.states.len();
            let problem = if n == 0 {
                Some("no states".to_string())
            } else if sp.initial >= n || sp.safe >= n {
                Some("initial or safe state out of range".to_string())
            } else if sp
                .transitions
                .iter()
                .any(|t| t.to >= n || t.from.is_some_and(|f| f >= n) || t.dur < Dur::ZERO)
            {
                Some("a transition refers to an unknown state or has a negative duration".into())
            } else {
                None
            };
            if let Some(problem) = problem {
                return Err(ModelError::BadStates {
                    resource: r.name.clone(),
                    problem,
                });
            }
        }
    }
    for g in &m.groups {
        for j in &g.members {
            bad("job", j.0, m.jobs.len())?;
        }
    }
    for (ji, j) in m.jobs.iter().enumerate() {
        for p in Phase::ALL {
            let e = j.phases.get(p);
            if e.p50 < Dur::ZERO || e.p90 < e.p50 {
                return Err(ModelError::BadEstimate {
                    job: j.name.clone(),
                    phase: p,
                });
            }
        }
        for a in &j.after {
            bad("job", a.0, m.jobs.len())?;
        }
        if let Some(g) = j.group {
            bad("group", g.0, m.groups.len())?;
            if !m.groups[g.index()].members.contains(&JobId(ji as u32)) {
                return Err(ModelError::GroupMismatch {
                    job: j.name.clone(),
                    group: m.groups[g.index()].name.clone(),
                });
            }
        }
        for (ni, need) in j.needs.iter().enumerate() {
            bad("kind", need.kind.0, m.kinds.len())?;
            if let Some(r) = need.select.resource {
                bad("resource", r.0, m.resources.len())?;
            }
            if let Some(n) = need.select.node {
                bad("node", n.0, m.nodes.len())?;
            }
            if m.candidates(need).is_empty() {
                return Err(ModelError::NoMatch {
                    job: j.name.clone(),
                    need: ni,
                });
            }
        }
        // Needs that share a same_node tag need a node with a candidate for each of them.
        let mut tags: BTreeMap<&str, Vec<&Need>> = BTreeMap::new();
        for need in &j.needs {
            if let Some(t) = &need.same_node {
                tags.entry(t).or_default().push(need);
            }
        }
        for (tag, needs) in tags {
            let ok = (0..m.nodes.len() as u32).map(NodeId).any(|n| {
                needs
                    .iter()
                    .all(|need| m.candidates(need).iter().any(|r| m.resource(*r).node == n))
            });
            if !ok {
                return Err(ModelError::NoCommonNode {
                    job: j.name.clone(),
                    tag: tag.to_string(),
                });
            }
        }
    }
    for g in &m.groups {
        for j in &g.members {
            if m.jobs[j.index()]
                .group
                .and_then(|x| m.groups.get(x.index()))
                != Some(g)
            {
                return Err(ModelError::GroupMismatch {
                    job: m.jobs[j.index()].name.clone(),
                    group: g.name.clone(),
                });
            }
        }
    }
    // Cycle check over `after` (iterative DFS with colours).
    let n = m.jobs.len();
    let mut colour = vec![0u8; n]; // 0 new, 1 on stack, 2 done
    for start in 0..n {
        if colour[start] != 0 {
            continue;
        }
        let mut stack = vec![(start, 0usize)];
        colour[start] = 1;
        while let Some((v, i)) = stack.pop() {
            let after = &m.jobs[v].after;
            if i < after.len() {
                stack.push((v, i + 1));
                let w = after[i].index();
                match colour[w] {
                    0 => {
                        colour[w] = 1;
                        stack.push((w, 0));
                    }
                    1 => {
                        return Err(ModelError::Cycle {
                            job: m.jobs[w].name.clone(),
                        });
                    }
                    _ => {}
                }
            } else {
                colour[v] = 2;
            }
        }
    }
    Ok(())
}

/// A job with defaults, for builders and tests: no needs, zero durations, class 0, weight 1, arrival at 0.
impl Job {
    pub fn new(name: impl Into<String>) -> Job {
        Job {
            name: name.into(),
            needs: Vec::new(),
            phases: Phases::default(),
            after: Vec::new(),
            group: None,
            class: 0,
            weight: 1,
            arrival: Time::ZERO,
            release: None,
            due: None,
            preempt: Preempt::Never,
            inputs: BTreeMap::new(),
            sim: SimHints::default(),
        }
    }

    /// Adds a need of `amount` of `kind`, any matching resource.
    pub fn needs(mut self, kind: KindId, amount: u64) -> Job {
        self.needs.push(Need {
            kind,
            amount,
            select: Selector::default(),
            state: None,
            same_node: None,
        });
        self
    }

    /// Sets the run phase's estimate.
    pub fn run(mut self, e: Estimate) -> Job {
        self.phases.run = e;
        self
    }

    pub fn after(mut self, j: JobId) -> Job {
        self.after.push(j);
        self
    }

    pub fn class(mut self, c: i32) -> Job {
        self.class = c;
        self
    }

    pub fn arrival(mut self, t: Time) -> Job {
        self.arrival = t;
        self
    }

    pub fn due(mut self, t: Time) -> Job {
        self.due = Some(t);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> (ModelBuilder, KindId, KindId, NodeId, NodeId) {
        let mut b = ModelBuilder::new();
        let ram = b.kind("ram", Unit::Bytes);
        let probe = b.kind("probe", Unit::Count);
        let a = b.node("a", Labels::new());
        let c = b.node("b", Labels::new());
        b.pool("a-ram", ram, a, 16);
        b.pool("b-ram", ram, c, 32);
        b.pool("b-probe", probe, c, 1);
        (b, ram, probe, a, c)
    }

    #[test]
    fn valid_model() {
        let (mut b, ram, probe, ..) = base();
        let j1 = b.job(Job::new("j1").needs(ram, 8));
        b.job(Job::new("j2").needs(probe, 1).after(j1));
        let m = b.build().unwrap();
        assert_eq!(m.jobs().len(), 2);
        assert_eq!(m.candidates(&m.job(j1).needs[0]).len(), 2);
        assert!(
            m.resource(m.resource_by_name("b-probe").unwrap())
                .is_exclusive()
        );
    }

    #[test]
    fn no_match_is_an_error() {
        let (mut b, ram, ..) = base();
        b.job(Job::new("big").needs(ram, 64));
        assert!(matches!(
            b.build(),
            Err(ModelError::NoMatch { need: 0, .. })
        ));
    }

    #[test]
    fn same_node_needs_a_common_node() {
        let (mut b, ram, probe, ..) = base();
        let mut j = Job::new("j").needs(ram, 20).needs(probe, 1);
        j.needs[0].same_node = Some("x".into());
        j.needs[1].same_node = Some("x".into());
        b.job(j.clone());
        assert!(b.clone().build().is_ok(), "b has 32 RAM and the probe");
        let (mut b2, ram2, probe2, a, _) = base();
        let mut j2 = Job::new("j").needs(ram2, 8).needs(probe2, 1);
        j2.needs[0].same_node = Some("x".into());
        j2.needs[0].select.node = Some(a);
        j2.needs[1].same_node = Some("x".into());
        b2.job(j2);
        assert!(matches!(b2.build(), Err(ModelError::NoCommonNode { .. })));
    }

    #[test]
    fn cycles_are_found() {
        let (mut b, ..) = base();
        let x = b.job(Job::new("x"));
        let y = b.job(Job::new("y").after(x));
        b.job_mut(x).after.push(y);
        assert!(matches!(b.build(), Err(ModelError::Cycle { .. })));
    }

    #[test]
    fn duplicates_and_estimates() {
        let (mut b, ..) = base();
        b.job(Job::new("x"));
        b.job(Job::new("x"));
        assert!(matches!(b.build(), Err(ModelError::Duplicate { .. })));
        let (mut b, ..) = base();
        b.job(Job::new("x").run(Estimate {
            p50: Dur::secs(10),
            p90: Dur::secs(5),
        }));
        assert!(matches!(b.build(), Err(ModelError::BadEstimate { .. })));
    }

    #[test]
    fn setup_times() {
        let sp = StateSpace {
            states: vec!["off".into(), "3V3".into(), "12V".into()],
            initial: 0,
            safe: 0,
            transitions: vec![
                Transition {
                    from: None,
                    to: 0,
                    dur: Dur::ms(100),
                },
                Transition {
                    from: Some(0),
                    to: 2,
                    dur: Dur::secs(1),
                },
            ],
        };
        assert_eq!(sp.setup_time(2, 0), Some(Dur::ms(100)));
        assert_eq!(sp.setup_time(0, 2), Some(Dur::secs(1)));
        assert_eq!(sp.setup_time(1, 2), None);
        assert_eq!(sp.setup_time(1, 1), Some(Dur::ZERO));
    }

    #[test]
    fn groups_link_both_ways() {
        let (mut b, ..) = base();
        let x = b.job(Job::new("x"));
        let y = b.job(Job::new("y"));
        let g = b.group("xy", vec![x, y]);
        let m = b.build().unwrap();
        assert_eq!(m.job(y).group, Some(g));
        assert_eq!(m.group(g).members, vec![x, y]);
    }
}
