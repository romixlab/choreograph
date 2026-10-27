//! choreographd: the choreograph scheduler as a service.
//!
//! One service owns the fleet's resources: nodes report their room ([`api::NodeReport`]), clients submit jobs
//! with their needs, wait with a live wait reason and ETA, and run under a lease they keep with heartbeats and
//! end with a release. Everything accepted is journaled before it is answered, so a restart picks up where
//! it stopped, leases and fencing tokens included. The service knows nodes, resource kinds, labels and jobs,
//! nothing about what the jobs do.
//!
//! - [`config`]: the TOML configuration (kinds, policy, lease and service settings, clients and tokens).
//! - [`api`]: the JSON requests and answers.
//! - [`service`]: the state and every call, with the time passed in.
//! - [`daemon`]: the service under tokio's clock: the ticker and the long-poll waits.
//! - [`store`]: the journal on disk.
//! - [`http`]: the routes, authorised by bearer tokens.
//! - [`front`]: the routes for a proxy in the same process that vouches for its callers.
//! - [`tls`]: TLS on a pure-Rust crypto provider, the accept loop.
//! - [`client`]: a typed client, over HTTPS or straight into a router in the same process.

pub mod api;
pub mod client;
pub mod config;
pub mod daemon;
pub mod front;
pub mod http;
pub mod listen;
pub mod service;
pub mod store;
pub mod tls;

/// The version, the git SHA and the commit time, as `choreographd --version` and the health call show them.
pub const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("GIT_SHA"),
    ", commit ",
    env!("BUILD_TIME"),
    ")"
);
