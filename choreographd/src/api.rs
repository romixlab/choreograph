//! The JSON API: what clients send and what the service answers. Times are milliseconds since the Unix epoch,
//! durations in answers are milliseconds; durations in requests may also be text (`"5m"`, `"1h30m"`).
//!
//! | Call | Path | Body | Answer |
//! |---|---|---|---|
//! | submit a job | `POST /v1/jobs` | [`SubmitJob`] | [`JobView`] |
//! | submit jobs that start together | `POST /v1/groups` | [`SubmitGroup`] | `[JobView]` |
//! | look at a job | `GET /v1/jobs/{id}` | | [`JobView`] |
//! | wait until it runs or ends | `POST /v1/jobs/{id}/wait?timeout=30s` (`0`: answer at once) | | [`JobView`] |
//! | cancel it | `POST /v1/jobs/{id}/cancel` | | [`JobView`] |
//! | keep its lease | `POST /v1/leases/{token}/heartbeat` | | [`BeatView`] |
//! | end its lease | `POST /v1/leases/{token}/release` | [`Release`] | [`JobView`] |
//! | pause its job | `POST /v1/leases/{token}/pause` | | [`JobView`] |
//! | go on after a pause | `POST /v1/leases/{token}/resume` | | [`JobView`] |
//! | report what it used | `POST /v1/leases/{token}/usage` | [`UseReport`] | [`UsageView`] |
//! | restart in place | `POST /v1/leases/{token}/restart` | | [`JobView`] |
//! | tell a job's holder | `POST /v1/jobs/{id}/tell` | [`TellJob`] | [`JobView`] |
//! | report a node | `POST /v1/nodes/{name}` | [`NodeReport`] | [`NodeView`] (with its [`Standing`]) |
//! | a node's standing instruction | `GET /v1/nodes/{name}/standing` | | [`Standing`] |
//! | the queue | `GET /v1/queue` | | [`QueueView`] |
//! | is it up | `GET /v1/health` (no token) | | [`Health`] |
//!
//! With `tokens = true` every call but `health` carries `Authorization: Bearer <token>`; without (the default,
//! on a Unix socket) a call names its client in the `Choreograph-Client` header (`local` when it does not).
//! Every call is one request and one answer, none longer than the service's `max_wait`, so the API fits a
//! proxy that carries single requests between machines. A refusal is an HTTP error status with an
//! [`ApiError`] body.

use std::collections::BTreeMap;

use choreograph::budget::{Budget, Meter, OnHard, Scope};
use choreograph::model::{Labels, Preempt};
use choreograph::{Crossing, Dur, Outcome, Time};
use serde::{Deserialize, Deserializer, Serialize};

pub use choreograph::standing::{Choice, LocalJob, Rule, Standing};

/// The API's version: the paths' prefix, and what a [`Standing`] says it was made for.
pub const API_VERSION: &str = "v1";

/// The service's id of a job: never reused, stable for the job's whole life (the library's own ids are
/// renumbered when finished jobs are compacted).
pub type JobRef = u64;

/// A job to queue.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitJob {
    /// A name for people (`gate tpm_cli`); it need not be unique.
    pub name: String,
    /// What it holds while it runs, all at once.
    #[serde(default)]
    pub needs: Vec<NeedSpec>,
    /// How long its phases take; plans and ETAs use them. Missing: zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup: Option<EstimateSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<EstimateSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup: Option<EstimateSpec>,
    /// Priority class; higher goes first.
    #[serde(default)]
    pub class: i32,
    /// Weight for fair shares; 1 when missing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<u32>,
    /// Not before this time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release: Option<Time>,
    /// A soft deadline: its priority rises as it nears.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub due: Option<Time>,
    /// Jobs that must end well first (their ids; they must still be known to the service).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<JobRef>,
    /// Its kind of work (`session`, `soak`, `gate`): what it leaves out of `open`, `preempt`, `pause_keeps`
    /// and `run` comes from the configured `[[job_kind]]`, and a missing `run` from what releases of the kind
    /// have taught.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Open-ended: no time limit, it holds its resources until released (default: its kind's, else bounded).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open: Option<bool>,
    /// Whether it may be asked to give way: `never`, `at_phase_boundary`, `ask` (default: its kind's, else
    /// `never`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preempt: Option<Preempt>,
    /// The resource kinds a pause keeps held (default: its kind's, else none).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause_keeps: Option<Vec<String>>,
    /// What it may use under its lease (default: its kind's, else none).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budgets: Option<Vec<BudgetSpec>>,
    /// Smaller alternatives to `needs` for a moldable job, from larger to smaller (a soak on 4 channels or
    /// on 2, a run with fewer threads): the service grants the largest that fits when it starts, and
    /// `JobView::shape` says which. With `needs` empty, the first of them stands for `needs`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shapes: Vec<Vec<NeedSpec>>,
    /// How long it may wait before it starts anyway, over capacity, on the node or resource each of its
    /// needs names (`"0s"`: at once). Only for a kind whose `allow_start_anyway` is true; default: its
    /// kind's `start_anyway_after` when it is pinned so.
    #[serde(
        default,
        deserialize_with = "opt_dur",
        skip_serializing_if = "Option::is_none"
    )]
    pub start_anyway_after: Option<Dur>,
}

/// A budget on a job's lease (see [`choreograph::budget`]): the holder hears when it crosses a level.
/// Levels are numbers in the unit the holder reports, or for a `wall` meter a duration (`"4h"` or
/// milliseconds).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetSpec {
    /// The name its use is reported under (`tokens`, `context`), any name for a wall budget.
    pub name: String,
    /// `reported` (default: the holder reports it) or `wall` (time since the grant or the step began).
    #[serde(default)]
    pub meter: Meter,
    /// `lease` (default: every step added up) or `step` (begins again at a restart in place).
    #[serde(default)]
    pub scope: Scope,
    #[serde(
        default,
        deserialize_with = "opt_amount",
        skip_serializing_if = "Option::is_none"
    )]
    pub soft: Option<u64>,
    #[serde(
        default,
        deserialize_with = "opt_amount",
        skip_serializing_if = "Option::is_none"
    )]
    pub hard: Option<u64>,
    /// `tell` (default: the holder decides) or `stop` (the job is cancelled at the hard level).
    #[serde(default)]
    pub on_hard: OnHard,
}

impl From<BudgetSpec> for Budget {
    fn from(b: BudgetSpec) -> Budget {
        Budget {
            name: b.name,
            meter: b.meter,
            scope: b.scope,
            soft: b.soft,
            hard: b.hard,
            on_hard: b.on_hard,
        }
    }
}

/// One need of a job.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NeedSpec {
    /// The resource kind, as configured.
    pub kind: String,
    /// How much; 1 when missing.
    #[serde(default = "one")]
    pub amount: u64,
    /// Exactly this resource (`node-a/ram`, or a shared one such as `repo/main`, made on first use).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    /// Only on this node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Labels the resource must have.
    #[serde(default, skip_serializing_if = "Labels::is_empty")]
    pub labels: Labels,
    /// Labels its node must have (labels or policy labels).
    #[serde(default, skip_serializing_if = "Labels::is_empty")]
    pub node_labels: Labels,
    /// Policy labels it accepts on a node without asking for them (`builds = "false"`).
    #[serde(default, skip_serializing_if = "Labels::is_empty")]
    pub tolerate: Labels,
    /// Needs with the same tag land on one node (a build's RAM and cores).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub same_node: Option<String>,
}

fn one() -> u64 {
    1
}

/// A duration estimate: the median and the 90th percentile (the median when missing).
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EstimateSpec {
    #[serde(deserialize_with = "dur")]
    pub p50: Dur,
    #[serde(
        default,
        deserialize_with = "opt_dur",
        skip_serializing_if = "Option::is_none"
    )]
    pub p90: Option<Dur>,
}

/// Jobs that start together or not at all.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitGroup {
    pub name: String,
    pub jobs: Vec<SubmitJob>,
}

/// Where a job is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    /// Waiting to start; `wait` says why.
    Queued,
    /// Holding its resources under a lease.
    Running,
    /// Paused by its holder: it keeps its lease and what its kind keeps on pause, gave back the rest, and no
    /// plan counts on it ending. After a resume it waits (`wait` says why) for what it gave back.
    Paused,
    /// Ended; `outcome` says how.
    Done,
}

/// A job as the service sees it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobView {
    pub id: JobRef,
    pub name: String,
    /// The client that submitted it.
    pub client: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Open-ended: held until released, its ETA a learned guess.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub open: bool,
    pub state: JobState,
    /// Why it waits, while queued or resuming after a pause.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait: Option<WaitView>,
    /// When it is expected to start and finish, while queued or running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eta: Option<EtaView>,
    /// Its lease, while running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<LeaseView>,
    /// The shape of its needs it was granted, while running (0: `needs`, `k`: `shapes[k - 1]`), for a job
    /// with several.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shape: Option<usize>,
    /// How many shapes it has, when more than one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shapes: Option<usize>,
    /// While queued: when it starts anyway, over capacity, if it still waits then.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_anyway_at: Option<Time>,
    /// It started anyway, past its wait, counted over the capacity of what it holds.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub over: bool,
    /// How it ended, once done.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<Outcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub done_at: Option<Time>,
}

/// Why a queued job waits, in words and as data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitView {
    /// The reason's tag: `release`, `dep`, `dep_failed`, `group`, `resource`, `capacity`, `nodes_down`,
    /// `same_node`, `no_match`, `reserved_for`, `interlock`, `approval`, or `planning` before the first plan.
    pub why: String,
    /// One line for people: "needs 8 GiB of ram, at most 3 GiB free on one node".
    pub text: String,
    /// The job it waits for or behind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<JobRef>,
    /// The resource it waits for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    /// The kind it lacks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// When what it waits for is expected (a release time, a resource freeing up).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<Time>,
}

/// Projected start and finish, if durations take their median (`p50`) or their 90th percentile (`p90`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EtaView {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_p50: Option<Time>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_p50: Option<Time>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_p90: Option<Time>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_p90: Option<Time>,
}

/// A held lease.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseView {
    /// The fencing token: heartbeats and the release name it; work that must happen once checks it.
    pub token: u64,
    pub granted: Time,
    /// Lost unless a heartbeat comes first.
    pub expires: Time,
    /// What it holds, one per need in the order submitted.
    pub held: Vec<HeldView>,
    /// Since when its job is paused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused: Option<Time>,
    /// A more important job waits for what it holds: pause or release when you can.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub give_way: bool,
    /// The step of the job under this lease: 1, and one more at each restart in place.
    pub step: u32,
    /// Every budget at or past a level now.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub over: Vec<Crossing>,
}

/// One resource a lease holds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldView {
    pub resource: String,
    pub node: String,
    pub kind: String,
    pub amount: u64,
    /// Given back for a pause: taken again on resume.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub given_back: bool,
}

/// The answer to a heartbeat.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeatView {
    pub expires: Time,
    /// The job was cancelled: stop it, clean up and release.
    pub stop: bool,
    /// A more important job waits for what it holds: pause or release when you can (only for a job whose
    /// `preempt` is `ask`). Nothing is taken from it.
    #[serde(default)]
    pub give_way: bool,
    /// Every budget at or past a level now: plan the end at a soft level, end at a hard one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub over: Vec<Crossing>,
    /// Tells for the holder since its last heartbeat, oldest first, each handed over once: the wakes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wakes: Vec<WakeView>,
}

/// A tell handed to a holder.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WakeView {
    /// The job whose holder sent it; none from outside a lease.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<JobRef>,
    pub at: Time,
    pub text: String,
}

/// What the current step of a lease has used, per name (`tokens`, `context`): each name given is set, the
/// others keep their last value. Report the step's running total, not the difference since the last report.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UseReport {
    pub used: BTreeMap<String, u64>,
}

/// The answer to a use report.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageView {
    pub step: u32,
    /// Every budget at or past a level now.
    #[serde(default)]
    pub over: Vec<Crossing>,
    /// The job was cancelled (a stopping budget's hard level, or a cancel): stop it, clean up and release.
    pub stop: bool,
}

/// A message for a job's holder, handed over with its next heartbeat.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TellJob {
    pub text: String,
    /// The sender's lease token, when the sender holds one (it must be the caller's): tells between two
    /// leases count for the pair guard. None from outside a lease.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<u64>,
}

/// The end of a lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Release {
    /// `ok`, `failed` or `cancelled`.
    pub outcome: Outcome,
}

/// What a node reports about itself, every few seconds.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeReport {
    /// What it is and has (`arch`, a toolchain, a tool version).
    #[serde(default)]
    pub labels: Labels,
    /// Policy labels that keep work away unless a job asks for or tolerates them (`builds = "false"`).
    #[serde(default)]
    pub policy: Labels,
    /// Whether it takes new work; `false` drains it (running jobs go on).
    #[serde(default = "yes")]
    pub up: bool,
    /// The room per kind as measured now (`ram` in bytes, `cores` in millicores, `gate` slots). A kind it
    /// reported before and leaves out now has no room.
    #[serde(default)]
    pub room: BTreeMap<String, u64>,
    /// The jobs it runs under its standing instruction, without a lease: counted as used on this node (taken
    /// off its room) until a report leaves them out.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub local: Vec<LocalJob>,
}

fn yes() -> bool {
    true
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// A node with its resources.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeView {
    pub name: String,
    pub up: bool,
    pub labels: Labels,
    #[serde(default, skip_serializing_if = "Labels::is_empty")]
    pub policy: Labels,
    /// When it last reported (since this service started); `None` for the shared node and before a report.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported: Option<Time>,
    pub resources: Vec<ResourceView>,
    /// The jobs it last reported running under its standing instruction.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub local: Vec<LocalJob>,
    /// Its current standing instruction: what it may do while it can't reach the service. Keep the one with
    /// the highest serial, and count its validity from when it arrived.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standing: Option<Standing>,
}

/// A resource and how much of it is used.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceView {
    pub name: String,
    pub kind: String,
    /// The capacity planned with (the measured one when there is one).
    pub capacity: u64,
    pub used: u64,
    /// Held by jobs its node runs under its standing instruction, already taken off `capacity`.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub local: u64,
    /// The jobs holding it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub holders: Vec<JobRef>,
}

/// The whole picture: nodes, running jobs, the queue in priority order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueueView {
    pub now: Time,
    pub nodes: Vec<NodeView>,
    pub running: Vec<JobView>,
    /// In the order the scheduler would start them.
    pub queued: Vec<JobView>,
    /// Ended and not yet compacted away, newest first.
    pub done: Vec<JobView>,
}

/// The service is up.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Health {
    pub version: String,
    pub now: Time,
}

/// A refusal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    /// A stable code: `unauthorized`, `forbidden`, `not_found`, `invalid`, `stale_lease`, `expired_lease`,
    /// `conflict`, `pair_loop` (two holders told each other back and forth too often), `internal`.
    pub error: String,
    pub message: String,
}

fn dur<'de, D: Deserializer<'de>>(d: D) -> Result<Dur, D::Error> {
    match DurText::deserialize(d)? {
        DurText::Ms(ms) => Ok(Dur(ms)),
        DurText::Text(t) => t.parse().map_err(serde::de::Error::custom),
    }
}

pub(crate) fn opt_dur<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Dur>, D::Error> {
    dur(d).map(Some)
}

/// An amount: a number, or a duration as text, in milliseconds.
fn opt_amount<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    let ms = match DurText::deserialize(d)? {
        DurText::Ms(n) => n,
        DurText::Text(t) => t.parse::<Dur>().map_err(serde::de::Error::custom)?.as_ms(),
    };
    u64::try_from(ms)
        .map(Some)
        .map_err(|_| serde::de::Error::custom("a budget level must not be negative"))
}

#[derive(Deserialize)]
#[serde(untagged)]
enum DurText {
    Ms(i64),
    Text(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_are_text_or_milliseconds() {
        let j: SubmitJob = serde_json::from_str(
            r#"{"name": "g", "run": {"p50": "5m", "p90": 600000}, "needs": [{"kind": "gate"}]}"#,
        )
        .unwrap();
        let run = j.run.unwrap();
        assert_eq!((run.p50, run.p90), (Dur::mins(5), Some(Dur::mins(10))));
        assert_eq!(j.needs[0].amount, 1);
        assert!(serde_json::from_str::<SubmitJob>(r#"{"name": "g", "nedds": []}"#).is_err());
    }
}
