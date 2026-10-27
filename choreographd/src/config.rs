//! The configuration file: where to listen, where the state lives, the resource kinds, the scheduler's
//! policy, the lease settings, the service's own settings, and, for a service on the network, TLS and the
//! clients with their tokens.
//!
//! By default the service listens on a Unix socket and trusts whoever may open it (its file mode is the access
//! control): each call names its client in the `Choreograph-Client` header. That is the setup behind a
//! proxy that carries the calls between machines (a mesh) and knows who calls. Without one, the service
//! listens on TCP with TLS and a token per client:
//!
//! ```toml
//! listen = "0.0.0.0:7470"              # default "/run/choreographd/api.sock"
//! state_dir = "/var/lib/choreographd"
//! tokens = true                        # every call carries a client's token
//!
//! [tls]
//! cert = "/etc/choreographd/cert.pem"   # the certificate chain, PEM
//! key = "/etc/choreographd/key.pem"     # its private key, PEM
//!
//! [[kind]]
//! name = "ram"
//! unit = "bytes"
//!
//! [[kind]]
//! name = "main"        # one holder at a time per repository, made when a job first names one
//! shared = true
//!
//! [[job_kind]]         # what jobs of a kind are, unless a job says otherwise; every key but name optional
//! name = "session"
//! open = true          # no time limit: held until released
//! preempt = "ask"      # its holder may be asked to give way for a more important job
//! pause_keeps = ["main"]
//! run = { p50 = "2h", p90 = "6h" }  # the guess until releases of the kind have taught one
//! budgets = [                        # what a job may use under its lease, see choreograph::budget
//!   { name = "context", scope = "step", soft = 150000, hard = 190000 },
//!   { name = "wall", meter = "wall", soft = "8h", hard = "12h", on_hard = "stop" },
//! ]
//!
//! [[standing]]         # what a node may do while it can't reach this service, see StandingConfig
//! node_labels = { builds = "true" }  # or node = "node-a"; neither: every node
//! kind = "build"                     # a job kind, "*" for any
//! options = [
//!   { local = { max = 2, share = { ram = 800 }, pressure = true } },
//!   { go = { best = 2, rank_by = "ram" } },
//! ]
//! valid = "24h"
//! why = "builds wait for the scheduler on this node"
//!
//! [policy]             # the list scheduler (choreograph::Policy); every key optional
//! aging_per_class = "1h"
//!
//! [lease]              # choreograph::LeaseSettings
//! ttl = "1m"
//! pair_bounces = 6     # tells back and forth between two holders before the next is refused; "off"
//! pair_window = "10m"  # a pair quiet this long counts again
//!
//! [settings]           # this service's own settings, see Settings
//! node_stale = "2m"
//!
//! [[client]]
//! name = "node-a"
//! token_sha256 = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
//! report = ["node-a"]  # the nodes it may report; "*" for any
//! ```
//!
//! Durations are written like `1h30m` or as milliseconds. Unknown keys are refused, so a typo is an error,
//! not a silent default.

use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use std::collections::BTreeMap;

use choreograph::model::{Labels, Preempt, Unit};
use choreograph::{Dur, LeaseSettings, Policy};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::api::{BudgetSpec, EstimateSpec};

/// The whole configuration file.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Where to listen: a Unix socket (an absolute path, the default `/run/choreographd/api.sock`) or a TCP
    /// address and port. TCP on anything but loopback needs `[tls]` and `tokens = true`.
    #[serde(default)]
    pub listen: Listen,
    /// The file mode of the Unix socket, octal: who may open it may call the service.
    #[serde(default = "socket_mode", with = "octal")]
    pub socket_mode: u32,
    /// Where the journal lives (`journal.jsonl`); the service needs to write there.
    pub state_dir: PathBuf,
    /// Serve over TLS with these files (TCP only); without it, plain HTTP.
    #[serde(default)]
    pub tls: Option<TlsFiles>,
    /// Every call carries a bearer token of one of the `[[client]]`s. Without tokens, whoever may connect is
    /// trusted to name its client (`Choreograph-Client` header, `local` when missing) and may report any node.
    #[serde(default)]
    pub tokens: bool,
    /// The resource kinds. A kind is added when it first appears here and kept after; its unit cannot change.
    #[serde(default, rename = "kind")]
    pub kinds: Vec<KindConfig>,
    /// The job kinds: what a job that names its kind is unless it says otherwise. A kind without an entry
    /// takes the defaults (bounded, never preempted, its run learned from earlier releases).
    #[serde(default, rename = "job_kind")]
    pub job_kinds: Vec<JobKindConfig>,
    /// Standing instructions: what each node may do while it can't reach the service, handed out ahead of
    /// time with every answer to its report.
    #[serde(default, rename = "standing")]
    pub standing: Vec<StandingConfig>,
    #[serde(default)]
    pub policy: Policy,
    #[serde(default)]
    pub lease: LeaseSettings,
    #[serde(default)]
    pub settings: Settings,
    /// Who may talk to the service, one token each.
    #[serde(default, rename = "client")]
    pub clients: Vec<ClientConfig>,
}

/// The certificate and key the service presents.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsFiles {
    /// The certificate chain, PEM, leaf first.
    pub cert: PathBuf,
    /// The private key, PEM (PKCS#8, SEC1 or PKCS#1).
    pub key: PathBuf,
}

/// A resource kind.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KindConfig {
    pub name: String,
    /// How amounts are counted: `count` (default), `bytes`, `millicores`.
    #[serde(default)]
    pub unit: Unit,
    /// Resources of a shared kind belong to no machine (a repository's `main`): one is made the first time a
    /// job names it (`resource = "repo/main"` in a need), with `capacity`, on the node named by
    /// [`Settings::shared_node`]. Resources of other kinds come from node reports.
    #[serde(default)]
    pub shared: bool,
    /// The capacity of each resource of a shared kind; 1 (one holder at a time) by default.
    #[serde(default = "one")]
    pub capacity: u64,
}

fn one() -> u64 {
    1
}

/// A kind of job (`session`, `soak`, `gate`): a job names its kind, and what it leaves out comes from here.
/// Durations are learned per kind from every release that ended well.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobKindConfig {
    pub name: String,
    /// Open-ended: no time limit, it holds its resources until its holder releases it (see
    /// [`choreograph::Job::open`]). Default: bounded.
    #[serde(default)]
    pub open: bool,
    /// Whether a running job may be asked to give way for a more important one: `never` (default),
    /// `at_phase_boundary` or `ask`.
    #[serde(default)]
    pub preempt: Preempt,
    /// The resource kinds a pause keeps held (they must be configured `[[kind]]`s); everything else is given
    /// back while paused. Default: none, a pause gives back everything.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pause_keeps: Vec<String>,
    /// How long its run takes before releases of the kind have taught an estimate; a job's own `run` wins
    /// over both. Default: none (zero).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<EstimateSpec>,
    /// What a job of the kind may use under its lease, with soft and hard levels (a job's own `budgets`
    /// win). Default: none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub budgets: Vec<BudgetSpec>,
}

/// One standing instruction: for the nodes and job kind it selects, what a node may do while it can't reach
/// the service. For each node and kind the most specific entry wins: one naming the node over one selecting
/// by labels (more labels over fewer) over one for every node; at the same, one naming the kind over `"*"`;
/// then the first in the file. A kind no entry names takes the most specific `"*"` entry, and without one it
/// waits.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StandingConfig {
    /// Only this node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Only nodes with these labels (labels or policy labels).
    #[serde(default, skip_serializing_if = "Labels::is_empty")]
    pub node_labels: Labels,
    /// The job kind, `"*"` for any.
    pub kind: String,
    /// What the node may do, in order; when none fits, the job waits.
    #[serde(default)]
    pub options: Vec<OptionConfig>,
    /// How long it holds from the moment the node receives it. Default: `settings.standing_valid`.
    #[serde(default, skip_serializing_if = "Option::is_none", with = "opt_dur")]
    pub valid: Option<Dur>,
    /// One line for people when a job waits under it.
    #[serde(default)]
    pub why: String,
}

/// One option of a standing instruction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OptionConfig {
    /// Run on the node itself, within limits (see [`choreograph::standing::Local`]).
    Local(LocalConfig),
    /// Send the job to other nodes.
    Go(GoConfig),
}

/// Run on the node itself, within limits.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalConfig {
    /// At most this many such jobs at once (all kinds sharing `pool`). Default: no count, only the room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<u32>,
    /// Kinds whose local options name the same pool count together. Default: the kind's own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    /// Per resource kind (`"*"` for every other), the part of the node's room the pool may hold, per mille.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub share: BTreeMap<String, u32>,
    /// Hold back while the node is under pressure.
    #[serde(default)]
    pub pressure: bool,
    /// Each such job runs at most this long.
    #[serde(default, skip_serializing_if = "Option::is_none", with = "opt_dur")]
    pub max_run: Option<Dur>,
    /// A lock the client takes before it runs the job (opaque to the service).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclusive: Option<String>,
}

/// Send the job to other nodes: the named ones in order, then up to `best` more that the service picks when
/// it hands the instruction out (up, not this node, with `node_labels`, any policy label tolerated, most free
/// `rank_by` first). The list goes stale as the room changes: give such an entry a short `valid`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<String>,
    #[serde(default)]
    pub best: u32,
    /// Labels a picked node must have (labels or policy labels).
    #[serde(default, skip_serializing_if = "Labels::is_empty")]
    pub node_labels: Labels,
    /// Policy labels accepted on a picked node without asking for them.
    #[serde(default, skip_serializing_if = "Labels::is_empty")]
    pub tolerate: Labels,
    /// The resource kind whose free room ranks the picks. Default: by name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank_by: Option<String>,
}

impl StandingConfig {
    /// Whether it selects a node with these labels and policy labels.
    pub fn selects(&self, node: &str, labels: &Labels, policy: &Labels) -> bool {
        self.node.as_ref().is_none_or(|n| n == node)
            && self
                .node_labels
                .iter()
                .all(|(k, v)| labels.get(k) == Some(v) || policy.get(k) == Some(v))
    }

    /// How specific its node selector is: a node over labels (more over fewer) over every node.
    pub fn specificity(&self) -> (bool, usize) {
        (self.node.is_some(), self.node_labels.len())
    }
}

fn socket_mode() -> u32 {
    0o660
}

/// Where the service listens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Listen {
    /// A Unix socket at this path.
    Unix(PathBuf),
    /// A TCP address and port.
    Tcp(SocketAddr),
}

impl Default for Listen {
    fn default() -> Listen {
        Listen::Unix("/run/choreographd/api.sock".into())
    }
}

impl FromStr for Listen {
    type Err = String;

    fn from_str(s: &str) -> Result<Listen, String> {
        if let Ok(a) = s.parse() {
            return Ok(Listen::Tcp(a));
        }
        let p = Path::new(s);
        if p.is_absolute() {
            return Ok(Listen::Unix(p.into()));
        }
        Err(format!(
            "listen = {s:?}: an absolute socket path or an address with its port (127.0.0.1:7470)"
        ))
    }
}

impl fmt::Display for Listen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Listen::Unix(p) => write!(f, "{}", p.display()),
            Listen::Tcp(a) => write!(f, "{a}"),
        }
    }
}

impl Serialize for Listen {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Listen {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Listen, D::Error> {
        String::deserialize(d)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// A client and what it may do. Every client may submit jobs, wait for them, keep and release their leases,
/// cancel them and look at the queue; only its own jobs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    pub name: String,
    /// The SHA-256 of its token, hex (`choreographd token` makes a token and prints this line). The token
    /// itself is never stored here.
    pub token_sha256: String,
    /// The nodes it may report (`"*"`: any).
    #[serde(default)]
    pub report: Vec<String>,
}

impl ClientConfig {
    pub fn may_report(&self, node: &str) -> bool {
        self.report.iter().any(|n| n == "*" || n == node)
    }
}

/// The service's own settings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    /// A node that has not reported for this long is taken out of placement (drained: its running jobs keep
    /// their leases) until it reports again.
    #[serde(with = "dur")]
    pub node_stale: Dur,
    /// A queued job whose client has not asked about it (a wait) for this long is cancelled: nobody would take
    /// its lease. Longer than `max_wait`.
    #[serde(with = "dur")]
    pub queued_stale: Dur,
    /// The longest a wait call is held open before it answers with the current wait reason.
    #[serde(with = "dur")]
    pub max_wait: Dur,
    /// How often the scheduler looks again with nothing else happening (aging, stale nodes and jobs).
    #[serde(with = "dur")]
    pub tick: Dur,
    /// Finished jobs stay visible this long, then are compacted away.
    #[serde(with = "dur")]
    pub keep_done: Dur,
    /// After this many journal lines a fresh snapshot replaces the journal.
    pub snapshot_every: u64,
    /// The node shared resources (see [`KindConfig::shared`]) are placed on; no machine may report under it.
    pub shared_node: String,
    /// The largest request body accepted, in bytes.
    pub max_body: usize,
    /// A TLS handshake that takes longer is given up.
    #[serde(with = "dur")]
    pub handshake: Dur,
    /// How long a standing instruction holds from the moment a node receives it, when its entry gives no
    /// `valid`. Default: a day.
    #[serde(with = "dur")]
    pub standing_valid: Dur,
}

impl Default for Settings {
    fn default() -> Settings {
        Settings {
            node_stale: Dur::mins(2),
            queued_stale: Dur::mins(3),
            max_wait: Dur::secs(60),
            tick: Dur::secs(10),
            keep_done: Dur::mins(10),
            snapshot_every: 10_000,
            shared_node: "shared".into(),
            max_body: 1 << 20,
            handshake: Dur::secs(10),
            standing_valid: Dur::hours(24),
        }
    }
}

/// Why the configuration cannot be used. The service exits with code 78 on it: only a person can fix it.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{path}: {err}")]
    Read { path: PathBuf, err: std::io::Error },
    #[error("{path}: {err}")]
    Parse { path: PathBuf, err: String },
    #[error("{0}")]
    Invalid(String),
}

impl Config {
    /// Reads and checks a configuration file.
    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|err| ConfigError::Read {
            path: path.into(),
            err,
        })?;
        Config::parse(&text).map_err(|e| match e {
            ConfigError::Parse { err, .. } => ConfigError::Parse {
                path: path.into(),
                err,
            },
            e => e,
        })
    }

    /// Parses and checks configuration text.
    pub fn parse(text: &str) -> Result<Config, ConfigError> {
        let c: Config = toml::from_str(text).map_err(|e| ConfigError::Parse {
            path: PathBuf::new(),
            err: e.to_string(),
        })?;
        c.check()?;
        Ok(c)
    }

    /// Refuses what cannot work: duplicate names, a bad token hash, settings out of range.
    pub fn check(&self) -> Result<(), ConfigError> {
        let bad = |m: String| Err(ConfigError::Invalid(m));
        match &self.listen {
            Listen::Unix(_) if self.tls.is_some() => {
                return bad("[tls] is for a TCP listener, not a Unix socket".into());
            }
            Listen::Tcp(a) if !a.ip().is_loopback() && (self.tls.is_none() || !self.tokens) => {
                return bad(format!(
                    "listen = \"{a}\" reaches beyond this machine: it needs [tls] and tokens = true"
                ));
            }
            _ => {}
        }
        if self.socket_mode > 0o777 {
            return bad(format!(
                "socket_mode {:o} is not a file mode",
                self.socket_mode
            ));
        }
        if self.tokens && self.clients.is_empty() {
            return bad("tokens = true but no [[client]] has a token".into());
        }
        if !self.tokens && !self.clients.is_empty() {
            return bad("[[client]] tokens are used only with tokens = true".into());
        }
        self.policy
            .check()
            .map_err(|e| ConfigError::Invalid(e.to_string()))?;
        self.lease
            .check()
            .map_err(|e| ConfigError::Invalid(e.to_string()))?;
        for (i, k) in self.kinds.iter().enumerate() {
            if self.kinds[..i].iter().any(|o| o.name == k.name) {
                return bad(format!("kind {:?} is declared twice", k.name));
            }
            if k.name.is_empty() || k.name.contains('/') {
                return bad(format!("kind {:?}: a name without '/'", k.name));
            }
            if k.capacity == 0 {
                return bad(format!("kind {:?}: capacity 0", k.name));
            }
        }
        for (i, k) in self.job_kinds.iter().enumerate() {
            if self.job_kinds[..i].iter().any(|o| o.name == k.name) {
                return bad(format!("job kind {:?} is declared twice", k.name));
            }
            if k.name.is_empty() {
                return bad("a job kind needs a name".into());
            }
            let budgets: Vec<choreograph::Budget> =
                k.budgets.iter().cloned().map(Into::into).collect();
            if let Err(e) = choreograph::budget::check(&budgets) {
                return bad(format!("job kind {:?}: {e}", k.name));
            }
            if let Some(r) = k.pause_keeps.iter().find(|r| self.kind(r).is_none()) {
                return bad(format!(
                    "job kind {:?}: pause_keeps names {r:?}, which is not a [[kind]]",
                    k.name
                ));
            }
        }
        self.check_standing()?;
        for (i, c) in self.clients.iter().enumerate() {
            if self.clients[..i].iter().any(|o| o.name == c.name) {
                return bad(format!("client {:?} is declared twice", c.name));
            }
            if parse_hash(&c.token_sha256).is_none() {
                return bad(format!(
                    "client {:?}: token_sha256 is not 64 hex digits",
                    c.name
                ));
            }
            if self
                .clients
                .iter()
                .take(i)
                .any(|o| o.token_sha256.eq_ignore_ascii_case(&c.token_sha256))
            {
                return bad(format!(
                    "client {:?} has the same token as another client",
                    c.name
                ));
            }
            if c.report.iter().any(|n| n == &self.settings.shared_node) {
                return bad(format!(
                    "client {:?} may report {:?}, the shared node",
                    c.name, self.settings.shared_node
                ));
            }
        }
        let s = &self.settings;
        for (name, d) in [
            ("node_stale", s.node_stale),
            ("queued_stale", s.queued_stale),
            ("max_wait", s.max_wait),
            ("tick", s.tick),
            ("keep_done", s.keep_done),
            ("handshake", s.handshake),
            ("standing_valid", s.standing_valid),
        ] {
            if d <= Dur::ZERO {
                return bad(format!("settings.{name} must be positive"));
            }
        }
        if s.queued_stale <= s.max_wait {
            return bad("settings.queued_stale must be longer than settings.max_wait".into());
        }
        if s.snapshot_every == 0 {
            return bad("settings.snapshot_every must be at least 1".into());
        }
        if s.shared_node.is_empty() {
            return bad("settings.shared_node must not be empty".into());
        }
        Ok(())
    }

    fn check_standing(&self) -> Result<(), ConfigError> {
        let bad = |m: String| Err(ConfigError::Invalid(m));
        for (i, e) in self.standing.iter().enumerate() {
            let at = format!("[[standing]] {} (kind {:?})", i + 1, e.kind);
            if e.kind.is_empty() {
                return bad(format!("{at}: kind is empty; \"*\" for any"));
            }
            if self.standing[..i]
                .iter()
                .any(|o| (&o.node, &o.node_labels, &o.kind) == (&e.node, &e.node_labels, &e.kind))
            {
                return bad(format!(
                    "{at}: another entry selects the same nodes and kind"
                ));
            }
            if e.node.as_ref().is_some_and(|n| n.is_empty()) {
                return bad(format!("{at}: node is empty"));
            }
            if e.valid.is_some_and(|d| d <= Dur::ZERO) {
                return bad(format!("{at}: valid must be positive"));
            }
            for o in &e.options {
                match o {
                    OptionConfig::Local(l) => {
                        if let Some((k, pm)) = l.share.iter().find(|(_, pm)| **pm > 1000) {
                            return bad(format!(
                                "{at}: share of {k} is {pm}‰, at most 1000 (the whole room)"
                            ));
                        }
                        if let Some(k) = l
                            .share
                            .keys()
                            .find(|k| *k != "*" && self.kind(k).is_none_or(|c| c.shared))
                        {
                            return bad(format!("{at}: share names {k:?}, not a node's [[kind]]"));
                        }
                        if l.max_run.is_some_and(|d| d <= Dur::ZERO) {
                            return bad(format!("{at}: max_run must be positive"));
                        }
                    }
                    OptionConfig::Go(g) => {
                        if g.nodes.is_empty() && g.best == 0 {
                            return bad(format!("{at}: go names no nodes and picks none (best)"));
                        }
                        if let Some(k) = &g.rank_by
                            && self.kind(k).is_none_or(|c| c.shared)
                        {
                            return bad(format!("{at}: rank_by {k:?} is not a node's [[kind]]"));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn kind(&self, name: &str) -> Option<&KindConfig> {
        self.kinds.iter().find(|k| k.name == name)
    }

    pub fn job_kind(&self, name: &str) -> Option<&JobKindConfig> {
        self.job_kinds.iter().find(|k| k.name == name)
    }

    /// Who a call comes from: with tokens, the client its bearer token belongs to; without, the client it
    /// names itself (`local` when it does not), which may report any node. `None`: refused.
    pub fn caller(&self, bearer: Option<&str>, named: Option<&str>) -> Option<ClientConfig> {
        if self.tokens {
            return self.client_by_token(bearer?).cloned();
        }
        let name = named.unwrap_or("local");
        valid_client_name(name).then(|| ClientConfig {
            name: name.into(),
            token_sha256: String::new(),
            report: vec!["*".into()],
        })
    }

    /// The client a bearer token belongs to. Compares the token's hash with every client's, without stopping
    /// at the first difference.
    pub fn client_by_token(&self, token: &str) -> Option<&ClientConfig> {
        let h: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let mut found = None;
        for c in &self.clients {
            let Some(want) = parse_hash(&c.token_sha256) else {
                continue;
            };
            let diff = want
                .iter()
                .zip(h.iter())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b));
            if diff == 0 {
                found = Some(c);
            }
        }
        found
    }
}

/// A client name a caller may give itself: 1 to 128 visible ASCII characters.
pub fn valid_client_name(name: &str) -> bool {
    (1..=128).contains(&name.len()) && name.bytes().all(|b| b.is_ascii_graphic())
}

/// The hex SHA-256 of a token, as `token_sha256` takes it.
pub fn token_hash(token: &str) -> String {
    hex(&Sha256::digest(token.as_bytes()))
}

/// A new random token: 32 bytes from the operating system, hex.
pub fn new_token() -> Result<String, getrandom::Error> {
    let mut b = [0u8; 32];
    getrandom::fill(&mut b)?;
    Ok(hex(&b))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn parse_hash(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

/// An optional duration as text (`1h30m`) or milliseconds.
pub(crate) mod opt_dur {
    use choreograph::Dur;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Option<Dur>, s: S) -> Result<S::Ok, S::Error> {
        match d {
            Some(d) => super::dur::serialize(d, s),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Dur>, D::Error> {
        super::dur::deserialize(d).map(Some)
    }
}

/// A file mode as octal text (`"0660"`).
mod octal {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(m: &u32, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{m:04o}"))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u32, D::Error> {
        let t = String::deserialize(d)?;
        u32::from_str_radix(&t, 8).map_err(|_| {
            serde::de::Error::custom(format!("{t:?} is not an octal file mode (\"0660\")"))
        })
    }
}

/// A duration as text (`1h30m`) or milliseconds.
pub(crate) mod dur {
    use choreograph::Dur;
    use serde::{Deserialize, Deserializer, Serializer};

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Text {
        Ms(i64),
        Text(String),
    }

    pub fn serialize<S: Serializer>(d: &Dur, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&d.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Dur, D::Error> {
        match Text::deserialize(d)? {
            Text::Ms(ms) => Ok(Dur(ms)),
            Text::Text(t) => t.parse().map_err(serde::de::Error::custom),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"
        listen = "127.0.0.1:7470"
        state_dir = "/var/lib/choreographd"
        tokens = true
        [tls]
        cert = "c.pem"
        key = "k.pem"
    "#;

    fn with_clients(base: &str) -> String {
        format!(
            "{base}\n[[client]]\nname = \"a\"\ntoken_sha256 = \"{}\"\nreport = [\"n1\"]\n[[client]]\nname = \"b\"\ntoken_sha256 = \"{}\"\n",
            token_hash("ta"),
            token_hash("tb").to_uppercase()
        )
    }

    #[test]
    fn a_minimal_config_takes_the_defaults() {
        let c = Config::parse("state_dir = \"/var/lib/choreographd\"").unwrap();
        assert_eq!(c.listen, Listen::Unix("/run/choreographd/api.sock".into()));
        assert_eq!(
            (c.socket_mode, c.tls.as_ref(), c.tokens),
            (0o660, None, false)
        );
        assert_eq!(c.settings, Settings::default());
        assert_eq!(
            c.settings.standing_valid,
            Dur::hours(24),
            "a day unless raised"
        );
        assert_eq!(c.policy, Policy::default());
        assert!(c.kinds.is_empty() && c.clients.is_empty());
        // Without tokens a caller names itself, `local` by default, and may report any node.
        assert_eq!(c.caller(None, None).unwrap().name, "local");
        let n = c.caller(Some("ignored"), Some("node-b/lander")).unwrap();
        assert_eq!(n.name, "node-b/lander");
        assert!(n.may_report("anything"));
        assert!(c.caller(None, Some("two words")).is_none());
        assert!(c.caller(None, Some("")).is_none());
    }

    #[test]
    fn tokens_are_matched_by_their_hash() {
        let c = Config::parse(&with_clients(BASE)).unwrap();
        assert_eq!(
            c.caller(Some("ta"), Some("b")).unwrap().name,
            "a",
            "with tokens the name header is ignored"
        );
        assert!(c.caller(None, Some("a")).is_none());
        assert_eq!(c.client_by_token("ta").unwrap().name, "a");
        assert_eq!(c.client_by_token("tb").unwrap().name, "b");
        assert!(c.client_by_token("tc").is_none());
        assert!(c.clients[0].may_report("n1") && !c.clients[0].may_report("n2"));
        assert_eq!(new_token().unwrap().len(), 64);
    }

    #[test]
    fn the_example_config_is_valid() {
        let c = Config::parse(include_str!("../deploy/config.example.toml")).unwrap();
        assert_eq!(c.kinds.len(), 4);
        assert!(c.kind("main").unwrap().shared);
        let session = c.job_kind("session").unwrap();
        assert!(session.open && session.preempt == Preempt::Ask);
        let wall = session.budgets.iter().find(|b| b.name == "wall").unwrap();
        assert_eq!(
            wall.hard,
            Some(Dur::hours(12).as_ms() as u64),
            "durations as text"
        );
        assert!(!c.tokens && c.tls.is_none());
        let c = Config::parse(include_str!("../deploy/config.tls.example.toml")).unwrap();
        assert!(c.tokens && c.tls.is_some() && c.clients.len() == 2);
        let c = Config::parse(include_str!("../deploy/config.build-fleet.example.toml")).unwrap();
        assert_eq!(c.standing.len(), 6);
        assert_eq!(c.standing[5].valid, Some(Dur::hours(7 * 24)));
        let c = Config::parse(include_str!("../deploy/config.hil-bench.example.toml")).unwrap();
        let OptionConfig::Local(soak) = &c.standing[0].options[0] else {
            panic!("{:?}", c.standing[0])
        };
        assert_eq!(
            (soak.share["channels"], soak.max_run),
            (500, Some(Dur::hours(4)))
        );
    }

    #[test]
    fn mistakes_are_refused() {
        for (extra, want) in [
            ("[settings]\nmax_wait = \"5m\"", "queued_stale"),
            (
                "[[kind]]\nname = \"ram\"\n[[kind]]\nname = \"ram\"",
                "twice",
            ),
            ("[[client]]\nname = \"a\"\ntoken_sha256 = \"abc\"", "64 hex"),
            ("bogus = 1", "unknown field"),
            (
                "[[job_kind]]\nname = \"s\"\npause_keeps = [\"bench\"]",
                "not a [[kind]]",
            ),
            (
                "[[job_kind]]\nname = \"s\"\n[[job_kind]]\nname = \"s\"",
                "twice",
            ),
            (
                "[[job_kind]]\nname = \"s\"\npreempt = \"maybe\"",
                "unknown variant",
            ),
            (
                "[[job_kind]]\nname = \"s\"\nbudgets = [{ name = \"t\", soft = 5, hard = 4 }]",
                "above",
            ),
            ("[lease]\npair_bounces = \"lots\"", "a number or"),
            ("[settings]\ntick = \"soon\"", "not a duration"),
            ("[[standing]]\nkind = \"\"", "kind is empty"),
            (
                "[[standing]]\nkind = \"b\"\n[[standing]]\nkind = \"b\"",
                "same nodes and kind",
            ),
            (
                "[[standing]]\nkind = \"b\"\noptions = [{ local = { share = { ram = 1001 } } }]",
                "at most 1000",
            ),
            (
                "[[standing]]\nkind = \"b\"\noptions = [{ local = { share = { disk = 10 } } }]",
                "not a node's [[kind]]",
            ),
            (
                "[[standing]]\nkind = \"b\"\noptions = [{ go = {} }]",
                "picks none",
            ),
            (
                "[[standing]]\nkind = \"b\"\noptions = [{ go = { best = 1, rank_by = \"main\" } }]",
                "rank_by",
            ),
            (
                "[[standing]]\nkind = \"b\"\noptions = [{ local = { mxa = 1 } }]",
                "unknown field",
            ),
            ("[[standing]]\nkind = \"b\"\nvalid = \"0s\"", "positive"),
        ] {
            let e = Config::parse(&with_clients(&format!("{BASE}\n{extra}")))
                .unwrap_err()
                .to_string();
            assert!(e.contains(want), "{extra}: {e}");
        }
        let plain = "state_dir = \"/s\"";
        for (text, want) in [
            (BASE.to_string(), "no [[client]]"),
            (with_clients(plain), "only with tokens"),
            (
                format!("{plain}\nlisten = \"relative.sock\""),
                "absolute socket path",
            ),
            (format!("{plain}\nsocket_mode = \"rw\""), "octal"),
            (
                format!("{plain}\n[tls]\ncert = \"c\"\nkey = \"k\""),
                "not a Unix socket",
            ),
            (
                format!("{plain}\nlisten = \"0.0.0.0:7470\""),
                "needs [tls] and tokens",
            ),
            (
                with_clients(&format!("{plain}\nlisten = \"[::]:7470\"\ntokens = true")),
                "needs [tls] and tokens",
            ),
        ] {
            let e = Config::parse(&text).unwrap_err().to_string();
            assert!(e.contains(want), "{text}: {e}");
        }
        // Loopback without TLS or tokens is fine: only this machine reaches it.
        Config::parse(&format!("{plain}\nlisten = \"127.0.0.1:7470\"")).unwrap();
    }
}
