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
    /// What the node is and has (`arch = "x86_64"`, a toolchain, a tool version); needs select on them.
    pub labels: Labels,
    /// Policy labels that keep work away (`builds = "false"` on a node that hosts services): a need lands on
    /// this node's resources only when its selector names every one of them with the same value, in
    /// `node_labels` (it asks for such a node) or in `tolerate` (it accepts one), so a job that says nothing
    /// never gets it.
    #[serde(default, skip_serializing_if = "Labels::is_empty")]
    pub policy: Labels,
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
    /// Every label here must be on the resource's node (as a label or a policy label) with the same value.
    pub node_labels: Labels,
    /// Policy labels this need accepts without asking for them (see [`Node::policy`]): the need may land on a
    /// node that has them, or on any other.
    #[serde(default, skip_serializing_if = "Labels::is_empty")]
    pub tolerate: Labels,
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
    /// The planner may ask its holder to give way (pause or release) for a job at least
    /// [`Policy::preempt_gap`](crate::Policy::preempt_gap) classes higher that cannot start otherwise: an
    /// open-ended session that can checkpoint, a soak test that can stop and go on later. The holder decides
    /// how; nothing is taken from it by force.
    Ask,
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
    /// The caller's tag for a recurring kind of work (`session`, `soak`, `gate`): durations are learned per
    /// kind ([`Authority`](crate::Authority) learns from every release), and policy is set per kind by the caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// An open-ended job has no time limit: it holds its resources until its holder releases it (an agent
    /// session, a person debugging on a bench, a long soak test). Its phase estimates are a learned guess, not
    /// a bound: plans assume it ends at that guess while it is young and predict again from its age once it
    /// runs longer (see [`Policy::open_floor`](crate::Policy::open_floor)).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub open: bool,
    /// The kinds a pause keeps held (a checkout, a bench's state); every other need is given back while the
    /// job is paused and taken again on resume. Empty: a pause gives back everything.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pause_keeps: Vec<KindId>,
    /// What it may use under its lease (tokens, context, wall time), with soft and hard levels: the holder
    /// hears when it crosses one (see [`budget`](crate::budget)). Set per job or by the caller per kind.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub budgets: Vec<crate::budget::Budget>,
    /// Smaller alternatives to `needs` for a moldable job (a soak test on 4 channels or on 2, a test run with
    /// fewer threads), from larger to smaller: shape 0 is `needs`, shape `k` is `shapes[k - 1]`. The planner
    /// grants the first shape that fits now, and the [`Assignment`](crate::Assignment) says which one it
    /// got. Empty: the job has the one shape `needs`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shapes: Vec<Vec<Need>>,
    /// How long the job may wait in the queue before it starts anyway, on the resources it is pinned to,
    /// over their capacity (a run that must not starve, a person's "start it now"). Counted from when it
    /// could first start (its earliest start, and the end of the jobs it comes after). Only for a job outside
    /// a group whose every need, in its smallest shape, names a node or a resource (see [`Job::pinned`]).
    /// `None`: it waits for room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_anyway_after: Option<Dur>,
}

impl Job {
    /// The earliest time the job may start: the later of its arrival and release time.
    pub fn earliest_start(&self) -> Time {
        self.release.map_or(self.arrival, |r| r.max(self.arrival))
    }

    /// How many shapes the job has: 1 plus its alternatives.
    pub fn shape_count(&self) -> usize {
        1 + self.shapes.len()
    }

    /// The needs of shape `k` (0 is `needs`), `None` past the last.
    pub fn shape(&self, k: usize) -> Option<&[Need]> {
        match k {
            0 => Some(&self.needs),
            k => self.shapes.get(k - 1).map(Vec::as_slice),
        }
    }

    /// The needs an assignment of this job meets: those of its shape (empty for a shape the job lacks).
    pub fn needs_for(&self, a: &crate::state::Assignment) -> &[Need] {
        self.shape(a.shape).unwrap_or_default()
    }

    /// The index of the smallest shape, the last one.
    pub fn smallest(&self) -> usize {
        self.shapes.len()
    }

    /// Whether every need of shape `k` names a node or a resource, so it has one place to go.
    pub fn pinned(&self, k: usize) -> bool {
        self.shape(k).is_some_and(|needs| {
            needs
                .iter()
                .all(|n| n.select.node.is_some() || n.select.resource.is_some())
        })
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

    /// Whether resource `r` matches need `need` by kind, selector, its node's policy labels and declared
    /// capacity (ignoring what is held now, a measured capacity and whether the node is up: see [`State`]).
    ///
    /// [`State`]: crate::state::State
    pub fn matches(&self, need: &Need, r: ResourceId) -> bool {
        let res = self.resource(r);
        let sel = &need.select;
        let node = self.node(res.node);
        res.kind == need.kind
            && res.capacity >= need.amount
            && sel.resource.is_none_or(|x| x == r)
            && sel.node.is_none_or(|n| n == res.node)
            && sel.labels.iter().all(|(k, v)| res.labels.get(k) == Some(v))
            && sel
                .node_labels
                .iter()
                .all(|(k, v)| node.labels.get(k) == Some(v) || node.policy.get(k) == Some(v))
            && node
                .policy
                .iter()
                .all(|(k, v)| sel.node_labels.get(k) == Some(v) || sel.tolerate.get(k) == Some(v))
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

/// How [`Model::compact`] renumbered jobs and groups: the new id of each old one that was kept.
///
/// Ids stay dense and keep their order, so ties broken by id are broken the same way after a compaction.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Remap {
    jobs: Vec<Option<JobId>>,
    groups: Vec<Option<GroupId>>,
}

impl Remap {
    /// The new id of an old job, `None` when it was removed.
    pub fn job(&self, old: JobId) -> Option<JobId> {
        self.jobs.get(old.index()).copied().flatten()
    }

    /// The new id of an old group, `None` when it was removed.
    pub fn group(&self, old: GroupId) -> Option<GroupId> {
        self.groups.get(old.index()).copied().flatten()
    }

    /// The jobs that were removed, by their old ids.
    pub fn removed(&self) -> impl Iterator<Item = JobId> + '_ {
        self.jobs
            .iter()
            .enumerate()
            .filter(|(_, n)| n.is_none())
            .map(|(i, _)| JobId(i as u32))
    }

    /// Whether nothing was removed.
    pub fn is_identity(&self) -> bool {
        self.jobs.iter().all(Option::is_some) && self.groups.iter().all(Option::is_some)
    }
}

/// Changes at runtime, each checked like [`ModelBuilder::build`]. They go through [`Live`], which keeps a
/// [`State`] in step with the model.
///
/// [`Live`]: crate::live::Live
/// [`State`]: crate::state::State
impl Model {
    pub(crate) fn add_node(&mut self, node: Node) -> Result<NodeId, ModelError> {
        if self.nodes.iter().any(|n| n.name == node.name) {
            return Err(ModelError::Duplicate {
                what: "node",
                name: node.name,
            });
        }
        self.nodes.push(node);
        Ok(NodeId(self.nodes.len() as u32 - 1))
    }

    pub(crate) fn add_kind(&mut self, kind: Kind) -> Result<KindId, ModelError> {
        if self.kinds.iter().any(|k| k.name == kind.name) {
            return Err(ModelError::Duplicate {
                what: "kind",
                name: kind.name,
            });
        }
        self.kinds.push(kind);
        Ok(KindId(self.kinds.len() as u32 - 1))
    }

    pub(crate) fn add_resource(&mut self, r: Resource) -> Result<ResourceId, ModelError> {
        if self.resources.iter().any(|x| x.name == r.name) {
            return Err(ModelError::Duplicate {
                what: "resource",
                name: r.name,
            });
        }
        check_resource(self, &r)?;
        self.resources.push(r);
        Ok(ResourceId(self.resources.len() as u32 - 1))
    }

    pub(crate) fn set_node_labels(&mut self, n: NodeId, labels: Labels) {
        self.nodes[n.index()].labels = labels;
    }

    pub(crate) fn set_node_policy(&mut self, n: NodeId, policy: Labels) {
        self.nodes[n.index()].policy = policy;
    }

    /// Adds jobs, as one group named `group` when given (their `group` fields are set to it). The jobs may come
    /// after each other and after jobs already in the model. All or nothing: on an error the model is unchanged.
    pub(crate) fn add_jobs(
        &mut self,
        jobs: Vec<Job>,
        group: Option<String>,
    ) -> Result<Vec<JobId>, ModelError> {
        let first = self.jobs.len();
        let groups = self.groups.len();
        let ids: Vec<JobId> = (first..first + jobs.len())
            .map(|i| JobId(i as u32))
            .collect();
        self.jobs.extend(jobs);
        if let Some(name) = group {
            let g = GroupId(groups as u32);
            for j in &mut self.jobs[first..] {
                j.group = Some(g);
            }
            self.groups.push(Group {
                name,
                members: ids.clone(),
            });
        }
        let checked = self.check_added(first, groups);
        if checked.is_err() {
            self.jobs.truncate(first);
            self.groups.truncate(groups);
        }
        checked.map(|()| ids)
    }

    fn check_added(&self, first: usize, groups: usize) -> Result<(), ModelError> {
        let (old, new) = self.jobs.split_at(first);
        for (i, j) in new.iter().enumerate() {
            if old.iter().chain(&new[..i]).any(|o| o.name == j.name) {
                return Err(ModelError::Duplicate {
                    what: "job",
                    name: j.name.clone(),
                });
            }
        }
        let (old, new) = self.groups.split_at(groups);
        if let Some(g) = new.first()
            && old.iter().any(|o| o.name == g.name)
        {
            return Err(ModelError::Duplicate {
                what: "group",
                name: g.name.clone(),
            });
        }
        check_jobs(self, first)
    }

    /// Removes the jobs whose `keep` is false and the groups left without members, and renumbers the rest in
    /// order. The caller makes sure no kept job comes after a removed one.
    pub(crate) fn compact(&mut self, keep: &[bool]) -> Remap {
        let mut next = 0u32;
        let jobs: Vec<Option<JobId>> = keep
            .iter()
            .map(|k| {
                k.then(|| {
                    next += 1;
                    JobId(next - 1)
                })
            })
            .collect();
        let mut next = 0u32;
        let groups: Vec<Option<GroupId>> = self
            .groups
            .iter()
            .map(|g| {
                g.members.iter().any(|m| keep[m.index()]).then(|| {
                    next += 1;
                    GroupId(next - 1)
                })
            })
            .collect();
        let remap = Remap { jobs, groups };
        let mut i = 0;
        self.jobs.retain(|_| {
            i += 1;
            keep[i - 1]
        });
        for j in &mut self.jobs {
            j.after.retain_mut(|a| match remap.job(*a) {
                Some(n) => {
                    *a = n;
                    true
                }
                None => false,
            });
            j.group = j.group.and_then(|g| remap.group(g));
        }
        let mut i = 0;
        self.groups.retain(|_| {
            i += 1;
            remap.groups[i - 1].is_some()
        });
        for g in &mut self.groups {
            g.members = g.members.iter().filter_map(|m| remap.job(*m)).collect();
        }
        remap
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
    #[error("job {job:?}: {problem}")]
    BadBudget { job: String, problem: String },
    #[error(
        "job {job:?}: need #{need} of shape {shape} can never be met: no resource matches its kind, selector, state and amount"
    )]
    NoMatchInShape {
        job: String,
        shape: usize,
        need: usize,
    },
    #[error("job {job:?}: shape {shape} has no needs")]
    EmptyShape { job: String, shape: usize },
    #[error("job {job:?} cannot start anyway: {problem}")]
    StartAnyway { job: String, problem: &'static str },
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
            policy: Labels::new(),
        });
        NodeId(self.nodes.len() as u32 - 1)
    }

    /// Sets a node's policy labels (see [`Node::policy`]).
    pub fn policy(&mut self, node: NodeId, policy: Labels) {
        self.nodes[node.index()].policy = policy;
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
    for r in &m.resources {
        check_resource(m, r)?;
    }
    check_jobs(m, 0)
}

fn bad(what: &'static str, id: u32, len: usize) -> Result<(), ModelError> {
    if id as usize >= len {
        Err(ModelError::BadId { what, id })
    } else {
        Ok(())
    }
}

fn check_resource(m: &Model, r: &Resource) -> Result<(), ModelError> {
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
    Ok(())
}

/// Checks the jobs from index `first` on, and the groups that have a member among them. Jobs before `first` are
/// known to be valid and can only depend on each other, so a cycle must pass through a new job.
fn check_jobs(m: &Model, first: usize) -> Result<(), ModelError> {
    for g in &m.groups {
        for j in &g.members {
            bad("job", j.0, m.jobs.len())?;
        }
    }
    for (ji, j) in m.jobs.iter().enumerate().skip(first) {
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
        for k in &j.pause_keeps {
            bad("kind", k.0, m.kinds.len())?;
        }
        crate::budget::check(&j.budgets).map_err(|problem| ModelError::BadBudget {
            job: j.name.clone(),
            problem,
        })?;
        if let Some(g) = j.group {
            bad("group", g.0, m.groups.len())?;
            if !m.groups[g.index()].members.contains(&JobId(ji as u32)) {
                return Err(ModelError::GroupMismatch {
                    job: j.name.clone(),
                    group: m.groups[g.index()].name.clone(),
                });
            }
        }
        if j.start_anyway_after.is_some_and(|d| d < Dur::ZERO) {
            return Err(ModelError::StartAnyway {
                job: j.name.clone(),
                problem: "a negative wait",
            });
        }
        if j.start_anyway_after.is_some() && j.group.is_some() {
            return Err(ModelError::StartAnyway {
                job: j.name.clone(),
                problem: "a member of a group starts with its group",
            });
        }
        for k in 0..j.shape_count() {
            let shape = j.shape(k).unwrap_or_default();
            if k > 0 && shape.is_empty() {
                return Err(ModelError::EmptyShape {
                    job: j.name.clone(),
                    shape: k,
                });
            }
            check_needs(m, &j.name, k, shape)?;
        }
        if j.start_anyway_after.is_some() && !j.pinned(j.smallest()) {
            return Err(ModelError::StartAnyway {
                job: j.name.clone(),
                problem: "not every need names a node or a resource",
            });
        }
    }
    for g in &m.groups {
        if first > 0 && g.members.iter().all(|j| j.index() < first) {
            continue;
        }
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
    // Cycle check over `after` (iterative DFS with colours); jobs before `first` count as done.
    let n = m.jobs.len();
    let mut colour = vec![0u8; n]; // 0 new, 1 on stack, 2 done
    colour[..first].fill(2);
    for start in first..n {
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

/// Checks the needs of shape `shape` of job `job`: ids in range, a candidate for each, a common node per
/// same_node tag.
fn check_needs(m: &Model, job: &str, shape: usize, needs: &[Need]) -> Result<(), ModelError> {
    for (ni, need) in needs.iter().enumerate() {
        bad("kind", need.kind.0, m.kinds.len())?;
        if let Some(r) = need.select.resource {
            bad("resource", r.0, m.resources.len())?;
        }
        if let Some(n) = need.select.node {
            bad("node", n.0, m.nodes.len())?;
        }
        if m.candidates(need).is_empty() {
            return Err(if shape == 0 {
                ModelError::NoMatch {
                    job: job.to_string(),
                    need: ni,
                }
            } else {
                ModelError::NoMatchInShape {
                    job: job.to_string(),
                    shape,
                    need: ni,
                }
            });
        }
    }
    // Needs that share a same_node tag need a node with a candidate for each of them.
    let mut tags: BTreeMap<&str, Vec<&Need>> = BTreeMap::new();
    for need in needs {
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
                job: job.to_string(),
                tag: tag.to_string(),
            });
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
            kind: None,
            open: false,
            pause_keeps: Vec::new(),
            budgets: Vec::new(),
            shapes: Vec::new(),
            start_anyway_after: None,
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

    /// Makes it open-ended (see [`Job::open`]).
    pub fn open(mut self) -> Job {
        self.open = true;
        self
    }

    /// Tags it with a kind of work (see [`Job::kind`]).
    pub fn kind(mut self, kind: impl Into<String>) -> Job {
        self.kind = Some(kind.into());
        self
    }

    pub fn preempt(mut self, p: Preempt) -> Job {
        self.preempt = p;
        self
    }

    /// Adds a budget (see [`Job::budgets`]).
    pub fn budget(mut self, b: crate::budget::Budget) -> Job {
        self.budgets.push(b);
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
