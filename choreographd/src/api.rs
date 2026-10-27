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
//! | report a node | `POST /v1/nodes/{name}` | [`NodeReport`] | [`NodeView`] |
//! | the queue | `GET /v1/queue` | | [`QueueView`] |
//! | is it up | `GET /v1/health` (no token) | | [`Health`] |
//!
//! With `tokens = true` every call but `health` carries `Authorization: Bearer <token>`; without (the default,
//! on a Unix socket) a call names its client in the `Choreograph-Client` header (`local` when it does not).
//! Every call is one request and one answer, none longer than the service's `max_wait`, so the API fits a
//! proxy that carries single requests between machines. A refusal is an HTTP error status with an
//! [`ApiError`] body.

use std::collections::BTreeMap;

use choreograph::model::{Labels, Preempt};
use choreograph::{Dur, Outcome, Time};
use serde::{Deserialize, Deserializer, Serialize};

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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeatView {
    pub expires: Time,
    /// The job was cancelled: stop it, clean up and release.
    pub stop: bool,
    /// A more important job waits for what it holds: pause or release when you can (only for a job whose
    /// `preempt` is `ask`). Nothing is taken from it.
    #[serde(default)]
    pub give_way: bool,
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
}

fn yes() -> bool {
    true
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
}

/// A resource and how much of it is used.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceView {
    pub name: String,
    pub kind: String,
    /// The capacity planned with (the measured one when there is one).
    pub capacity: u64,
    pub used: u64,
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
    /// `conflict`, `internal`.
    pub error: String,
    pub message: String,
}

fn dur<'de, D: Deserializer<'de>>(d: D) -> Result<Dur, D::Error> {
    match DurText::deserialize(d)? {
        DurText::Ms(ms) => Ok(Dur(ms)),
        DurText::Text(t) => t.parse().map_err(serde::de::Error::custom),
    }
}

fn opt_dur<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Dur>, D::Error> {
    dur(d).map(Some)
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
