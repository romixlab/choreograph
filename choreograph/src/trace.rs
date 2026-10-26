//! The event trace: what happened, when.
//!
//! One format for a simulated run, a real executor's journal and the GUI's playback. A trace starts with a
//! [`Header`] that names the format version; every [`Record`] after it is one event at one time, in time order
//! (events at the same time in the order they were applied).
//!
//! Version 2 adds the events of a model that changes at runtime ([`Live`](crate::live::Live)): a job requeued
//! when its node is lost, a capacity measured, a node going down or up, finished jobs compacted away.

use serde::{Deserialize, Serialize};

use crate::model::{JobId, NodeId, Phase, ResourceId};
use crate::plan::WaitReason;
use crate::state::{Assignment, Outcome};
use crate::time::Time;

/// The trace format version. Readers refuse a newer major version.
pub const TRACE_VERSION: u32 = 2;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    pub version: u32,
    /// What produced the trace (`choreograph-sim 0.1.0`, seed, policy), free text.
    pub source: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// The job entered the queue.
    Arrived { job: JobId },
    /// The job acquired its resources and began setup.
    Started { job: JobId, assignment: Assignment },
    /// The job moved into a phase.
    Phase { job: JobId, phase: Phase },
    /// The job failed in a phase (cleanup follows).
    Failed { job: JobId, phase: Phase },
    /// The job was cancelled.
    Cancelled { job: JobId },
    /// The job finished and released its resources.
    Finished { job: JobId, outcome: Outcome },
    /// The reason a job waits changed (recorded only on change, to keep traces small).
    Waiting { job: JobId, reason: WaitReason },
    /// The node of a running job was lost: the job released everything without cleanup and is queued again.
    Requeued { job: JobId },
    /// A resource's capacity was measured (`None`: back to the declared one).
    Capacity {
        resource: ResourceId,
        capacity: Option<u64>,
    },
    /// A node went down (its resources count with no capacity) or came back up.
    Node { node: NodeId, up: bool },
    /// These finished jobs were removed; every later job id is renumbered down past them, in order (see
    /// [`Remap`](crate::model::Remap)).
    Compacted { removed: Vec<JobId> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub t: Time,
    #[serde(flatten)]
    pub event: Event,
}

/// A whole trace in memory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trace {
    pub header: Header,
    pub records: Vec<Record>,
}

impl Trace {
    pub fn new(source: impl Into<String>) -> Trace {
        Trace {
            header: Header {
                version: TRACE_VERSION,
                source: source.into(),
            },
            records: Vec::new(),
        }
    }

    /// Appends an event; times must not go back.
    pub fn push(&mut self, t: Time, event: Event) {
        debug_assert!(self.records.last().is_none_or(|r| r.t <= t));
        self.records.push(Record { t, event });
    }
}
