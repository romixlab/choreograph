//! choreograph plans and runs jobs over shared resources.
//!
//! Each job needs some resources (instruments, connections, RAM, CPU, a node), goes through setup, run and
//! cleanup, and carries a priority class, a weight, a release time and a due date. The library keeps it safe
//! (a start takes every need at once or nothing, nothing goes over capacity, cleanup always runs) and in order
//! (dependencies, groups that start together), and it explains why each waiting job waits.
//!
//! The library does no IO and reads no clock (sans-IO): the caller passes the time in and applies the
//! decisions, so tests, the simulator and a real executor drive the same code and get the same answers.
//!
//! - [`model`]: nodes, resource kinds, resources, jobs, groups; [`ModelBuilder`] validates a model.
//! - [`state`]: the runtime bookkeeping; [`State::start`] applies a batch of starts all or nothing.
//! - [`plan`]: the [`Planner`] trait, [`Plan`], [`WaitReason`] and the [`FirstFit`] baseline.
//! - [`list`]: [`ListScheduler`], the main planner: priorities with aging and deadline urgency, placement,
//!   backfilling with reservations, and a forward plan with ETAs.
//! - [`policy`]: the list scheduler's settings, [`Policy`], loadable from TOML.
//! - [`trace`]: the event trace shared by simulation, execution and playback.
//! - [`theory`]: queueing formulas (Erlang C, Kingman, Pollaczek–Khinchine, Cobham, ...) for comparison.
//! - [`jobset`] (feature `toml`): the TOML job-set format.
//!
//! ```
//! use choreograph::{FirstFit, Planner, State, Time};
//!
//! let model = choreograph::jobset::from_toml(r#"
//!     [[node]]
//!     name = "bench"
//!     [[kind]]
//!     name = "scope"
//!     [[resource]]
//!     name = "scope-1"
//!     kind = "scope"
//!     node = "bench"
//!     [[job]]
//!     name = "a"
//!     needs = [{ kind = "scope" }]
//!     run = "10s"
//!     [[job]]
//!     name = "b"
//!     needs = [{ kind = "scope" }]
//!     run = "10s"
//! "#).unwrap();
//! let mut state = State::new(&model, Time::ZERO);
//! for j in model.job_ids() {
//!     state.arrive(j).unwrap();
//! }
//! let plan = FirstFit.plan(&model, &state);
//! assert_eq!(plan.starts.len(), 1); // one scope: b waits for a
//! state.start(&model, &plan.batch()).unwrap();
//! ```

#[cfg(feature = "toml")]
pub mod jobset;
pub mod list;
pub mod model;
pub mod plan;
pub mod policy;
pub mod state;
pub mod theory;
pub mod time;
pub mod trace;

pub use list::{Eta, Forecast, ListScheduler, Span};
pub use model::{
    Estimate, Job, JobId, Model, ModelBuilder, ModelError, Need, NodeId, Phase, ResourceId,
};
pub use plan::{FirstFit, Plan, Planner, Reservation, Start, WaitReason};
pub use policy::{Backfill, Placement, Policy, Quantile};
pub use state::{Assignment, JobStatus, Outcome, State, StateError};
pub use time::{Dur, Time};
pub use trace::{Event, Record, Trace};
