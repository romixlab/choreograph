//! Leases: every start is a lease its holder keeps alive, and every accepted change is journaled.
//!
//! [`Authority`] owns a [`Live`] model and is the only way to change it once it is in place. A start granted
//! from a plan becomes a [`Lease`]: a fencing [`Token`] (never reused, rising with every grant), the job, and
//! an expiry one TTL ahead ([`LeaseSettings::ttl`]). The holder (the process that runs the job) then
//!
//! - sends a [`heartbeat`](Authority::heartbeat) well within the TTL, which moves the expiry forward and tells
//!   it whether the job was cancelled in the meantime;
//! - may report phases ([`advance`](Authority::advance)) and a failure ([`fail`](Authority::fail));
//! - ends with a [`release`](Authority::release) carrying the outcome, which frees what the job held.
//!
//! A holder that stops sending heartbeats loses its lease at the expiry: from then on every call with its
//! token is refused, and [`expire`](Authority::expire) gives the job's resources back and requeues or fails it
//! ([`LeaseSettings::on_expiry`]). A job granted again gets a new, higher token, so a late call from the old
//! holder (a paused process, a slow network) is refused as stale instead of acting on someone else's run.
//! Work outside the authority that must not be done twice (a release that pushes to `main`) checks its token
//! right before it acts ([`check_holds`](Authority::check_holds)), or compares it with the newest token the
//! resource was granted with ([`fence`](Authority::fence)) where the work lands.
//!
//! Every change that is accepted appends an [`Entry`] to a journal, which the caller takes with
//! [`take_journal`](Authority::take_journal) and writes to stable storage, append-only, before it answers the
//! client. A refused change changes nothing and journals nothing. After a restart [`Authority::replay`] rebuilds
//! exactly the same authority from the journal: the first entry is a [`snapshot`](Authority::snapshot) (the
//! journal of a new authority starts with one), and a later snapshot written to a fresh journal lets the old
//! one go, so the journal stays as long as the work since the last snapshot. The library does no IO: the
//! journal is data, and the format of the file (JSON lines, say) is the caller's.
//!
//! ```
//! use choreograph::lease::{Authority, LeaseError, LeaseSettings};
//! use choreograph::live::Live;
//! use choreograph::model::{Job, Labels, ModelBuilder, Unit};
//! use choreograph::{Dur, FirstFit, Outcome, Time};
//!
//! let mut b = ModelBuilder::new();
//! let slot = b.kind("gate", Unit::Count);
//! let n = b.node("n", Labels::new());
//! b.pool("n/gate", slot, n, 1);
//! let settings = LeaseSettings { ttl: Dur::secs(30), ..LeaseSettings::default() };
//! let mut auth = Authority::new(Live::new(b.build().unwrap(), Time::ZERO), settings);
//!
//! auth.submit(Job::new("gate").needs(slot, 1)).unwrap();
//! let plan = auth.plan(&mut FirstFit);
//! let old = auth.grant(&plan.batch()).unwrap()[0].token;
//!
//! // The holder goes quiet: its lease expires and the job is queued again.
//! auth.set_now(Time::ZERO + Dur::secs(30)).unwrap();
//! assert_eq!(auth.expire().len(), 1);
//! let plan = auth.plan(&mut FirstFit);
//! let new = auth.grant(&plan.batch()).unwrap()[0].token;
//! assert!(new > old);
//!
//! // The old holder wakes up: refused. The new one finishes.
//! assert!(matches!(auth.release(old, Outcome::Ok), Err(LeaseError::Stale(_))));
//! auth.release(new, Outcome::Ok).unwrap();
//!
//! // A restart rebuilds the same authority from what was journaled.
//! let journal = auth.take_journal();
//! assert_eq!(Authority::replay(journal).unwrap(), auth);
//! ```

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::budget::{Crossing, Level, OnHard, Usage};
use crate::estimates::Learner;
use crate::live::Live;
use crate::model::{
    Estimate, GroupId, Job, JobId, KindId, Labels, ModelError, NodeId, Phase, Remap, Resource,
    ResourceId, Unit,
};
use crate::plan::{Plan, Planner};
use crate::policy::dur;
use crate::state::{Assignment, JobStatus, LostJobs, Outcome, StateError};
use crate::time::{Dur, Time};

/// The journal format version. [`Authority::replay`] refuses a newer one.
pub const JOURNAL_VERSION: u32 = 1;

/// A fencing token: names one lease, never reused, and every grant gets a higher one than any before it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Token(pub u64);

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// A job's hold on its resources while its holder keeps it alive.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub token: Token,
    pub job: JobId,
    /// When it was granted (the job started).
    pub granted: Time,
    /// When it is lost unless a heartbeat comes first: from this time on, its token is refused.
    pub expires: Time,
    /// The planner asks the holder to give way (pause or release) for a more important job; its heartbeats
    /// say so ([`Beat::give_way`]) until it pauses, resumes or ends.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub give_way: bool,
    /// What it has used, step by step (see [`budget`](crate::budget)).
    #[serde(default)]
    pub usage: Usage,
    /// Tells not yet delivered: the next heartbeat hands them over.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mail: Vec<Tell>,
}

/// A message to a lease's holder, delivered (a wake) with its next heartbeat, at most once.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tell {
    /// The job whose holder sent it; `None` from outside any lease (or from a job since compacted away).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<JobId>,
    pub at: Time,
    pub text: String,
}

/// The tells between two jobs, for the pair guard (see [`LeaseSettings::pair_bounces`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Pair {
    /// The two jobs, lower id first.
    a: JobId,
    b: JobId,
    /// Who sent the last tell between them, and when.
    last_from: JobId,
    last: Time,
    /// Tells that went back the other way than the one before, since the pair was last quiet.
    bounces: u32,
}

/// The settings of the leases.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LeaseSettings {
    /// How long a lease lives after its grant or its last heartbeat. A holder sends heartbeats at a third of it
    /// or more often, so one lost or late heartbeat does not cost the lease.
    #[serde(with = "dur")]
    pub ttl: Dur,
    /// What happens to the job of an expired lease: `requeue` (it runs again, for builds and tests) or `fail`
    /// (for work that must not run twice without a look).
    pub on_expiry: LostJobs,
    /// The loop guard between two holders: a tell that goes back the other way than the pair's last one
    /// (A tells B, B tells A) is a bounce, and the tell that would make more than this many is refused.
    /// `None` (`"off"`): no guard. Tells from outside a lease are never counted.
    #[serde(with = "crate::policy::or_off")]
    pub pair_bounces: Option<u32>,
    /// A pair quiet for this long starts counting its bounces again.
    #[serde(with = "dur")]
    pub pair_window: Dur,
}

impl Default for LeaseSettings {
    fn default() -> LeaseSettings {
        LeaseSettings {
            ttl: Dur::secs(60),
            on_expiry: LostJobs::Requeue,
            pair_bounces: Some(6),
            pair_window: Dur::mins(10),
        }
    }
}

impl LeaseSettings {
    /// Loads the settings from TOML (`ttl = "1m"`, `on_expiry = "requeue"`); missing keys keep their defaults,
    /// unknown keys are refused.
    #[cfg(feature = "toml")]
    pub fn from_toml(text: &str) -> Result<LeaseSettings, LeaseError> {
        let s: LeaseSettings =
            toml::from_str(text).map_err(|e| LeaseError::Settings(e.to_string()))?;
        s.check()?;
        Ok(s)
    }

    /// Refuses a TTL that is zero or negative.
    pub fn check(&self) -> Result<(), LeaseError> {
        if self.ttl <= Dur::ZERO {
            return Err(LeaseError::Settings("ttl must be positive".into()));
        }
        if self.pair_window <= Dur::ZERO {
            return Err(LeaseError::Settings("pair_window must be positive".into()));
        }
        Ok(())
    }
}

/// What a heartbeat tells the holder.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Beat {
    /// The new expiry.
    pub expires: Time,
    /// The job was cancelled or marked failed: the holder stops it, cleans up and releases.
    pub stop: bool,
    /// A more important job waits for what this one holds: the holder pauses or releases when it can (see
    /// [`Preempt::Ask`](crate::model::Preempt::Ask)). Nothing is taken from it.
    #[serde(default)]
    pub give_way: bool,
    /// Every budget at or past a level now (see [`budget`](crate::budget)): the holder plans its end at a
    /// soft level and ends at a hard one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub over: Vec<Crossing>,
    /// Tells for the holder since its last heartbeat, oldest first: each is handed over once.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wakes: Vec<Tell>,
}

/// Why a call was refused. A refused call changes nothing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LeaseError {
    #[error("lease {0} is not held: released, expired or never granted")]
    Stale(Token),
    #[error("lease {token} expired at {at}")]
    Expired { token: Token, at: Time },
    #[error("lease {token} does not hold resource {resource:?}")]
    NotHeld { token: Token, resource: ResourceId },
    #[error("lease settings: {0}")]
    Settings(String),
    #[error(
        "jobs {from:?} and {to:?} have told each other back and forth {bounces} times: refused as a loop"
    )]
    PairLoop {
        from: JobId,
        to: JobId,
        bounces: u32,
    },
    #[error(transparent)]
    State(#[from] StateError),
    #[error(transparent)]
    Model(#[from] ModelError),
}

/// Why a journal could not be replayed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReplayError {
    #[error("the journal is empty")]
    Empty,
    #[error("the journal does not start with a snapshot")]
    NoSnapshot,
    #[error("entry {index}: journal version {version} is newer than {JOURNAL_VERSION}")]
    Version { index: usize, version: u32 },
    #[error("entry {index}: refused on replay: {error}")]
    Refused { index: usize, error: LeaseError },
    #[error("entry {index}: replay gave other tokens than the journal records")]
    Diverged { index: usize },
}

/// One accepted change, at the time it was made.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub t: Time,
    pub op: Op,
}

/// The changes the journal records: the inputs of each accepted call, enough to make it again. Written
/// externally tagged (`{"t": 10000, "op": {"heartbeat": {"token": 3}}}`), which every serde format reads
/// back as it was written.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    /// The whole authority; replay starts from here.
    Snapshot {
        version: u32,
        authority: Box<Authority>,
    },
    Submit {
        job: Job,
    },
    SubmitGroup {
        name: String,
        jobs: Vec<Job>,
    },
    AddKind {
        name: String,
        unit: Unit,
    },
    SetSettings {
        settings: LeaseSettings,
    },
    AddNode {
        name: String,
        labels: Labels,
        policy: Labels,
    },
    AddResource {
        resource: Resource,
    },
    SetNodeLabels {
        node: NodeId,
        labels: Labels,
    },
    SetNodePolicy {
        node: NodeId,
        policy: Labels,
    },
    SetCapacity {
        resource: ResourceId,
        capacity: Option<u64>,
    },
    SetNodeUp {
        node: NodeId,
        up: bool,
    },
    NodeLost {
        node: NodeId,
        then: LostJobs,
    },
    Cancel {
        job: JobId,
    },
    Compact {
        done_by: Time,
    },
    /// Starts, with the tokens they were granted (checked on replay).
    Grant {
        batch: Vec<(JobId, Assignment)>,
        tokens: Vec<Token>,
    },
    Heartbeat {
        token: Token,
    },
    Advance {
        token: Token,
    },
    Fail {
        token: Token,
    },
    Release {
        token: Token,
        outcome: Outcome,
    },
    /// The leases that expired (checked on replay).
    Expire {
        tokens: Vec<Token>,
    },
    Pause {
        token: Token,
    },
    Resume {
        token: Token,
    },
    /// Holders asked to give way.
    GiveWay {
        jobs: Vec<JobId>,
    },
    UseReport {
        token: Token,
        values: BTreeMap<String, u64>,
    },
    Restart {
        token: Token,
    },
    Tell {
        from: Option<Token>,
        to: JobId,
        text: String,
    },
}

/// The one authority over a live model: grants leases, keeps them, and journals every change.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Authority {
    live: Live,
    settings: LeaseSettings,
    /// The leases held, in token order.
    leases: Vec<Lease>,
    /// The token the next grant gets.
    next: Token,
    /// The newest token each resource was granted with, indexed by resource id.
    fence: Vec<Option<Token>>,
    /// Durations learned per job kind from every release (see [`Authority::learned`]).
    #[serde(default)]
    learned: Learner,
    /// The tells between pairs of jobs, for the pair guard.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pairs: Vec<Pair>,
    /// Entries not yet taken by the caller.
    #[serde(skip)]
    journal: Vec<Entry>,
}

/// Two authorities are equal when their state is: the entries not yet taken do not count.
impl PartialEq for Authority {
    fn eq(&self, o: &Authority) -> bool {
        self.live == o.live
            && self.settings == o.settings
            && self.leases == o.leases
            && self.next == o.next
            && self.fence == o.fence
            && self.learned == o.learned
            && self.pairs == o.pairs
    }
}

impl Authority {
    /// Takes over a live model: nothing in it may be running yet (every start is a lease). The journal starts
    /// with a snapshot.
    ///
    /// # Panics
    ///
    /// If a job of `live` is running.
    pub fn new(live: Live, settings: LeaseSettings) -> Authority {
        assert!(
            live.state().running().next().is_none(),
            "an authority starts with nothing running"
        );
        let fence = vec![None; live.model().resources().len()];
        let mut a = Authority {
            live,
            settings,
            leases: Vec::new(),
            next: Token(1),
            fence,
            learned: Learner::default(),
            pairs: Vec::new(),
            journal: Vec::new(),
        };
        let s = a.snapshot();
        a.journal.push(s);
        a
    }

    pub fn live(&self) -> &Live {
        &self.live
    }

    pub fn settings(&self) -> &LeaseSettings {
        &self.settings
    }

    pub fn now(&self) -> Time {
        self.live.now()
    }

    /// Moves time forward (never back). Not journaled by itself: each entry carries its time.
    pub fn set_now(&mut self, t: Time) -> Result<(), StateError> {
        self.live.set_now(t)
    }

    /// The leases held, in token order.
    pub fn leases(&self) -> &[Lease] {
        &self.leases
    }

    /// The lease a token names, while it is held (also past its expiry until [`expire`](Authority::expire)).
    pub fn lease(&self, token: Token) -> Option<&Lease> {
        self.find(token).ok().map(|i| &self.leases[i])
    }

    /// The lease a job runs under.
    pub fn lease_of(&self, job: JobId) -> Option<&Lease> {
        self.leases.iter().find(|l| l.job == job)
    }

    /// The newest token a resource was granted with, `None` if it never was. Where work under a lease lands
    /// outside the authority, a write with an older token than the newest one seen there is refused.
    pub fn fence(&self, r: ResourceId) -> Option<Token> {
        self.fence.get(r.index()).copied().flatten()
    }

    /// The earliest expiry of the leases held: when [`expire`](Authority::expire) has something to do next.
    pub fn next_expiry(&self) -> Option<Time> {
        self.leases.iter().map(|l| l.expires).min()
    }

    /// The lease a token names, if it is held and not past its expiry.
    pub fn check(&self, token: Token) -> Result<&Lease, LeaseError> {
        let l = &self.leases[self.find(token)?];
        if self.now() >= l.expires {
            return Err(LeaseError::Expired {
                token,
                at: l.expires,
            });
        }
        Ok(l)
    }

    /// Like [`check`](Authority::check), and the lease's job holds `r`: the check before an action on `r` that
    /// must happen once (a release pushing to `main`).
    pub fn check_holds(&self, token: Token, r: ResourceId) -> Result<&Lease, LeaseError> {
        let l = self.check(token)?;
        let holds = match self.live.state().status(l.job) {
            JobStatus::Running {
                assignment, paused, ..
            } => assignment
                .resources
                .iter()
                .enumerate()
                .any(|(i, x)| *x == r && paused.as_ref().is_none_or(|p| p.held[i])),
            _ => false,
        };
        if !holds {
            return Err(LeaseError::NotHeld { token, resource: r });
        }
        Ok(l)
    }

    /// Asks a planner what starts now.
    pub fn plan(&self, planner: &mut impl Planner) -> Plan {
        self.live.plan(planner)
    }

    /// Starts a batch (all or none, see [`State::start`](crate::State::start)) and grants each start a lease,
    /// in batch order; a resume of a paused job goes on under the lease it has. Returns the leases.
    pub fn grant(&mut self, batch: &[(JobId, Assignment)]) -> Result<Vec<Lease>, LeaseError> {
        let resumes: Vec<Option<usize>> = batch
            .iter()
            .map(|(j, _)| {
                self.live
                    .state()
                    .is_paused(*j)
                    .then(|| self.leases.iter().position(|l| l.job == *j))
                    .flatten()
            })
            .collect();
        self.live.start(batch)?;
        let now = self.now();
        self.fence.resize(self.live.model().resources().len(), None);
        let mut granted = Vec::with_capacity(batch.len());
        for ((job, a), resume) in batch.iter().zip(resumes) {
            if let Some(i) = resume {
                // A resume goes on under its lease: same token, a fresh expiry. The fence of what it takes back
                // stays at the newest token (kinds whose writes are fenced belong in `pause_keeps`).
                let l = &mut self.leases[i];
                l.expires = now + self.settings.ttl;
                l.give_way = false;
                granted.push(l.clone());
                continue;
            }
            let token = self.next;
            self.next = Token(token.0 + 1);
            for r in &a.resources {
                self.fence[r.index()] = Some(token);
            }
            let l = Lease {
                token,
                job: *job,
                granted: now,
                expires: now + self.settings.ttl,
                give_way: false,
                usage: Usage::new(now),
                mail: Vec::new(),
            };
            self.leases.push(l.clone());
            granted.push(l);
        }
        self.log(Op::Grant {
            batch: batch.to_vec(),
            tokens: granted.iter().map(|l| l.token).collect(),
        });
        Ok(granted)
    }

    /// Keeps a lease alive: its expiry moves to one TTL from now, and the holder hears what is new: a
    /// cancel, a request to give way, the budgets it is over (a hard level of an [`OnHard::Stop`] budget
    /// cancels the job here), the tells waiting for it. Refused once it has expired.
    pub fn heartbeat(&mut self, token: Token) -> Result<Beat, LeaseError> {
        self.check(token)?;
        let i = self.find(token)?;
        let expires = self.now() + self.settings.ttl;
        self.leases[i].expires = expires;
        let over = self.cross(i);
        let stop = matches!(
            self.live.state().status(self.leases[i].job),
            JobStatus::Running { ending, .. } if *ending != Outcome::Ok
        );
        let give_way = self.leases[i].give_way;
        let wakes = std::mem::take(&mut self.leases[i].mail);
        self.log(Op::Heartbeat { token });
        Ok(Beat {
            expires,
            stop,
            give_way,
            over,
            wakes,
        })
    }

    /// The holder reports what its current step has used (`tokens`, `context`, any name its budgets count):
    /// each name given is set, the others keep their last value. Returns every budget at or past a level now,
    /// as a heartbeat would; a hard level of an [`OnHard::Stop`] budget cancels the job.
    pub fn report_use(
        &mut self,
        token: Token,
        values: BTreeMap<String, u64>,
    ) -> Result<Vec<Crossing>, LeaseError> {
        self.check(token)?;
        let i = self.find(token)?;
        self.leases[i].usage.report(&values);
        let over = self.cross(i);
        self.log(Op::UseReport { token, values });
        Ok(over)
    }

    /// Restarts the job in place: a fresh process of its holder inside the same lease, with the same token,
    /// place and resources (a checkpoint and restart, a context handoff). The step count rises, step budgets
    /// begin again, lease budgets keep adding up, and the expiry moves to one TTL from now. Not a new start
    /// and not a wake. Returns the restarts so far.
    pub fn restart(&mut self, token: Token) -> Result<u32, LeaseError> {
        self.check(token)?;
        let i = self.find(token)?;
        let now = self.now();
        let budgets = self.live.model().job(self.leases[i].job).budgets.clone();
        let l = &mut self.leases[i];
        l.usage.restart(&budgets, now);
        l.expires = now + self.settings.ttl;
        let n = l.usage.restarts;
        self.log(Op::Restart { token });
        Ok(n)
    }

    /// Leaves a message for the holder of job `to`, handed over with its next heartbeat. `from` is the
    /// sender's lease, or `None` from outside any lease. Between two leases the pair guard applies (see
    /// [`LeaseSettings::pair_bounces`]): a back-and-forth past its limit is refused as a loop.
    pub fn tell(
        &mut self,
        from: Option<Token>,
        to: JobId,
        text: impl Into<String>,
    ) -> Result<(), LeaseError> {
        let text = text.into();
        let sender = match from {
            Some(t) => Some(self.check(t)?.job),
            None => None,
        };
        self.job_id(to)?;
        let i = self
            .leases
            .iter()
            .position(|l| l.job == to)
            .ok_or(StateError::NotRunning(to))?;
        let now = self.now();
        if let Some(s) = sender.filter(|s| *s != to) {
            self.guard_pair(s, to, now)?;
        }
        self.leases[i].mail.push(Tell {
            from: sender,
            at: now,
            text: text.clone(),
        });
        self.log(Op::Tell { from, to, text });
        Ok(())
    }

    /// Counts a tell from `s` to `t` for their pair, or refuses it as a loop.
    fn guard_pair(&mut self, s: JobId, t: JobId, now: Time) -> Result<(), LeaseError> {
        let (a, b) = if s < t { (s, t) } else { (t, s) };
        let window = self.settings.pair_window;
        let limit = self.settings.pair_bounces;
        let i = match self.pairs.iter().position(|p| p.a == a && p.b == b) {
            Some(i) => i,
            None => {
                self.pairs.push(Pair {
                    a,
                    b,
                    last_from: s,
                    last: now,
                    bounces: 0,
                });
                return Ok(());
            }
        };
        let p = &mut self.pairs[i];
        if now - p.last >= window {
            p.bounces = 0;
        }
        let bounces = p.bounces + u32::from(p.last_from != s);
        if limit.is_some_and(|l| bounces > l) {
            return Err(LeaseError::PairLoop {
                from: s,
                to: t,
                bounces: p.bounces,
            });
        }
        p.bounces = bounces;
        p.last_from = s;
        p.last = now;
        Ok(())
    }

    /// Every budget of lease `i` at or past a level now. A level crossed for the first time is noted; a
    /// hard one of an [`OnHard::Stop`] budget cancels the job.
    fn cross(&mut self, i: usize) -> Vec<Crossing> {
        let now = self.now();
        let job = self.leases[i].job;
        let budgets = &self.live.model().job(job).budgets;
        if budgets.is_empty() {
            return Vec::new();
        }
        let l = &self.leases[i];
        let over = l.usage.over(budgets, l.granted, now);
        let mut stop = false;
        for c in &over {
            let was = self.leases[i].usage.crossed.get(&c.budget).copied();
            if was.is_some_and(|w| w >= c.level) {
                continue;
            }
            self.leases[i]
                .usage
                .crossed
                .insert(c.budget.clone(), c.level);
            stop |= c.level == Level::Hard
                && budgets
                    .iter()
                    .any(|b| b.name == c.budget && b.on_hard == OnHard::Stop);
        }
        let ending_ok = matches!(
            self.live.state().status(job),
            JobStatus::Running { ending, .. } if *ending == Outcome::Ok
        );
        if stop && ending_ok {
            self.live
                .cancel(job)
                .expect("a leased job that ends well can be cancelled");
        }
        over
    }

    /// The holder finished the job's current phase (see [`State::advance`](crate::State::advance)); after
    /// cleanup the job is done and the lease ends. Returns the job's new status.
    pub fn advance(&mut self, token: Token) -> Result<JobStatus, LeaseError> {
        let job = self.check(token)?.job;
        let st = self.live.advance(job)?.clone();
        if matches!(st, JobStatus::Done { .. }) {
            self.drop_lease(token);
        }
        self.log(Op::Advance { token });
        Ok(st)
    }

    /// The holder reports the job failed: it goes to cleanup and will end `Failed` (see
    /// [`State::fail`](crate::State::fail)).
    pub fn fail(&mut self, token: Token) -> Result<(), LeaseError> {
        let job = self.check(token)?.job;
        self.live.fail(job)?;
        self.log(Op::Fail { token });
        Ok(())
    }

    /// The holder is done: the job ends with `outcome` (ok, failed or cancelled; a job already ending failed or
    /// cancelled keeps that), its resources are free and the lease ends. Returns the ended lease.
    pub fn release(&mut self, token: Token, outcome: Outcome) -> Result<Lease, LeaseError> {
        let job = self.check(token)?.job;
        self.live.finish(job, outcome)?;
        let l = self.drop_lease(token);
        if let (Some(kind), Some(Outcome::Ok)) = (
            &self.live.model().job(job).kind,
            self.live.state().outcome(job),
        ) {
            let held = self.now() - l.granted;
            self.learned.observe_kind(kind, Phase::Run, held);
        }
        self.log(Op::Release { token, outcome });
        Ok(l)
    }

    /// The holder pauses its job (see [`State::pause`](crate::State::pause)): it keeps its lease and place,
    /// gives back what it does not keep on pause, and no plan counts on it ending. It still sends heartbeats.
    pub fn pause(&mut self, token: Token) -> Result<(), LeaseError> {
        let job = self.check(token)?.job;
        self.live.pause(job)?;
        let i = self.find(token)?;
        self.leases[i].give_way = false;
        self.log(Op::Pause { token });
        Ok(())
    }

    /// The holder of a paused job wants to go on: the job waits for what it gave back on the same resources,
    /// and a later [`grant`](Authority::grant) of it continues the same lease.
    pub fn resume(&mut self, token: Token) -> Result<(), LeaseError> {
        let job = self.check(token)?.job;
        self.live.resume(job)?;
        self.log(Op::Resume { token });
        Ok(())
    }

    /// Asks the holders of these jobs to give way (a plan's [`give_way`](Plan::give_way)): their heartbeats
    /// say so. Returns the jobs newly asked; asking again changes nothing.
    pub fn ask_give_way(&mut self, jobs: impl IntoIterator<Item = JobId>) -> Vec<JobId> {
        let mut asked = Vec::new();
        for j in jobs {
            if let Some(l) = self.leases.iter_mut().find(|l| l.job == j)
                && !l.give_way
            {
                l.give_way = true;
                asked.push(j);
            }
        }
        if !asked.is_empty() {
            self.log(Op::GiveWay {
                jobs: asked.clone(),
            });
        }
        asked
    }

    /// What a job of this kind is expected to hold its lease for, learned from the releases of earlier jobs of
    /// the kind that ended well (granted to released, pauses included): the estimate to submit an open-ended
    /// job with. `None` before the first such release.
    pub fn learned(&self, kind: &str) -> Option<Estimate> {
        self.learned
            .estimate_kind(kind, Phase::Run)
            .map(|q| Estimate {
                p50: q.p50,
                p90: q.p90.max(q.p50),
            })
    }

    /// Ends every lease whose expiry has come: its job gives back what it holds and is requeued or failed as
    /// [`LeaseSettings::on_expiry`] says. Returns the expired leases in token order.
    pub fn expire(&mut self) -> Vec<Lease> {
        let now = self.now();
        let (gone, kept): (Vec<Lease>, Vec<Lease>) = std::mem::take(&mut self.leases)
            .into_iter()
            .partition(|l| now >= l.expires);
        self.leases = kept;
        if gone.is_empty() {
            return gone;
        }
        for l in &gone {
            self.live
                .abandon(l.job, self.settings.on_expiry)
                .expect("a leased job is running");
        }
        self.log(Op::Expire {
            tokens: gone.iter().map(|l| l.token).collect(),
        });
        gone
    }

    /// Cancels a job: a queued one is done at once; a running one goes to cleanup, and its holder learns it
    /// from the next heartbeat ([`Beat::stop`]) and releases.
    pub fn cancel(&mut self, job: JobId) -> Result<(), LeaseError> {
        self.job_id(job)?;
        self.live.cancel(job)?;
        self.log(Op::Cancel { job });
        Ok(())
    }

    /// See [`Live::submit`].
    pub fn submit(&mut self, job: Job) -> Result<JobId, LeaseError> {
        let id = self.live.submit(job.clone())?;
        self.log(Op::Submit { job });
        Ok(id)
    }

    /// See [`Live::submit_group`].
    pub fn submit_group(
        &mut self,
        name: impl Into<String>,
        jobs: Vec<Job>,
    ) -> Result<(GroupId, Vec<JobId>), LeaseError> {
        let name = name.into();
        let ids = self.live.submit_group(name.clone(), jobs.clone())?;
        self.log(Op::SubmitGroup { name, jobs });
        Ok(ids)
    }

    /// Replaces the lease settings (a new TTL from the configuration after a restart). Leases held keep their
    /// expiry until their next heartbeat, which uses the new TTL.
    pub fn set_settings(&mut self, settings: LeaseSettings) -> Result<(), LeaseError> {
        settings.check()?;
        self.settings = settings.clone();
        self.log(Op::SetSettings { settings });
        Ok(())
    }

    /// See [`Live::add_kind`].
    pub fn add_kind(&mut self, name: impl Into<String>, unit: Unit) -> Result<KindId, LeaseError> {
        let name = name.into();
        let k = self.live.add_kind(name.clone(), unit)?;
        self.log(Op::AddKind { name, unit });
        Ok(k)
    }

    /// See [`Live::add_node`].
    pub fn add_node(
        &mut self,
        name: impl Into<String>,
        labels: Labels,
        policy: Labels,
    ) -> Result<NodeId, LeaseError> {
        let name = name.into();
        let n = self
            .live
            .add_node(name.clone(), labels.clone(), policy.clone())?;
        self.log(Op::AddNode {
            name,
            labels,
            policy,
        });
        Ok(n)
    }

    /// See [`Live::add_resource`].
    pub fn add_resource(&mut self, resource: Resource) -> Result<ResourceId, LeaseError> {
        let r = self.live.add_resource(resource.clone())?;
        self.fence.resize(self.live.model().resources().len(), None);
        self.log(Op::AddResource { resource });
        Ok(r)
    }

    /// See [`Live::set_node_labels`].
    pub fn set_node_labels(&mut self, node: NodeId, labels: Labels) -> Result<(), LeaseError> {
        self.node_id(node)?;
        self.live.set_node_labels(node, labels.clone());
        self.log(Op::SetNodeLabels { node, labels });
        Ok(())
    }

    /// See [`Live::set_node_policy`].
    pub fn set_node_policy(&mut self, node: NodeId, policy: Labels) -> Result<(), LeaseError> {
        self.node_id(node)?;
        self.live.set_node_policy(node, policy.clone());
        self.log(Op::SetNodePolicy { node, policy });
        Ok(())
    }

    /// See [`Live::set_capacity`].
    pub fn set_capacity(
        &mut self,
        resource: ResourceId,
        capacity: Option<u64>,
    ) -> Result<(), LeaseError> {
        if resource.index() >= self.live.model().resources().len() {
            return Err(ModelError::BadId {
                what: "resource",
                id: resource.0,
            }
            .into());
        }
        self.live.set_capacity(resource, capacity);
        self.log(Op::SetCapacity { resource, capacity });
        Ok(())
    }

    /// See [`Live::set_node_up`].
    pub fn set_node_up(&mut self, node: NodeId, up: bool) -> Result<(), LeaseError> {
        self.node_id(node)?;
        self.live.set_node_up(node, up);
        self.log(Op::SetNodeUp { node, up });
        Ok(())
    }

    /// See [`Live::node_lost`]: the leases of the jobs that ran there end with them. Returns those jobs.
    pub fn node_lost(&mut self, node: NodeId, then: LostJobs) -> Result<Vec<JobId>, LeaseError> {
        self.node_id(node)?;
        let hit = self.live.node_lost(node, then);
        self.leases.retain(|l| !hit.contains(&l.job));
        self.log(Op::NodeLost { node, then });
        Ok(hit)
    }

    /// See [`Live::compact`]; leases follow their jobs to the new ids.
    pub fn compact(&mut self, done_by: Time) -> Remap {
        let remap = self.live.compact(done_by);
        for l in &mut self.leases {
            l.job = remap.job(l.job).expect("a leased job is running and kept");
            for m in &mut l.mail {
                m.from = m.from.and_then(|j| remap.job(j));
            }
        }
        self.pairs
            .retain_mut(|p| match (remap.job(p.a), remap.job(p.b)) {
                (Some(a), Some(b)) => {
                    p.last_from = if p.last_from == p.a { a } else { b };
                    (p.a, p.b) = (a, b);
                    true
                }
                _ => false,
            });
        self.log(Op::Compact { done_by });
        remap
    }

    /// The whole authority as a journal entry. A fresh journal that starts with it replaces the old one.
    pub fn snapshot(&self) -> Entry {
        let mut a = self.clone();
        a.journal.clear();
        Entry {
            t: self.now(),
            op: Op::Snapshot {
                version: JOURNAL_VERSION,
                authority: Box::new(a),
            },
        }
    }

    /// The entries journaled since the last call, in order, for the caller to append to stable storage.
    pub fn take_journal(&mut self) -> Vec<Entry> {
        std::mem::take(&mut self.journal)
    }

    /// Rebuilds an authority from its journal: the first entry is a snapshot, each later entry is applied
    /// again at its time (a later snapshot replaces all before it). The result equals the authority that wrote
    /// the journal, with no entries left to take.
    pub fn replay(entries: impl IntoIterator<Item = Entry>) -> Result<Authority, ReplayError> {
        let mut it = entries.into_iter().enumerate();
        let Some((_, first)) = it.next() else {
            return Err(ReplayError::Empty);
        };
        let mut a = match first.op {
            Op::Snapshot { version, authority } => Self::restore(0, version, authority)?,
            _ => return Err(ReplayError::NoSnapshot),
        };
        for (index, e) in it {
            if let Op::Snapshot { version, authority } = e.op {
                a = Self::restore(index, version, authority)?;
                continue;
            }
            let refused = |error| ReplayError::Refused { index, error };
            a.set_now(e.t).map_err(|e| refused(e.into()))?;
            a.apply(e.op)
                .map_err(refused)?
                .then_some(())
                .ok_or(ReplayError::Diverged { index })?;
        }
        a.journal.clear();
        Ok(a)
    }

    fn restore(
        index: usize,
        version: u32,
        authority: Box<Authority>,
    ) -> Result<Authority, ReplayError> {
        if version > JOURNAL_VERSION {
            return Err(ReplayError::Version { index, version });
        }
        Ok(*authority)
    }

    /// Makes a journaled change again; `false` when it gave other tokens than recorded.
    fn apply(&mut self, op: Op) -> Result<bool, LeaseError> {
        match op {
            Op::Snapshot { .. } => unreachable!("handled by replay"),
            Op::Submit { job } => self.submit(job).map(drop)?,
            Op::SubmitGroup { name, jobs } => self.submit_group(name, jobs).map(drop)?,
            Op::AddKind { name, unit } => self.add_kind(name, unit).map(drop)?,
            Op::SetSettings { settings } => self.set_settings(settings)?,
            Op::AddNode {
                name,
                labels,
                policy,
            } => self.add_node(name, labels, policy).map(drop)?,
            Op::AddResource { resource } => self.add_resource(resource).map(drop)?,
            Op::SetNodeLabels { node, labels } => self.set_node_labels(node, labels)?,
            Op::SetNodePolicy { node, policy } => self.set_node_policy(node, policy)?,
            Op::SetCapacity { resource, capacity } => self.set_capacity(resource, capacity)?,
            Op::SetNodeUp { node, up } => self.set_node_up(node, up)?,
            Op::NodeLost { node, then } => self.node_lost(node, then).map(drop)?,
            Op::Cancel { job } => self.cancel(job)?,
            Op::Compact { done_by } => drop(self.compact(done_by)),
            Op::Grant { batch, tokens } => {
                let got = self.grant(&batch)?;
                return Ok(got.iter().map(|l| l.token).eq(tokens));
            }
            Op::Heartbeat { token } => self.heartbeat(token).map(drop)?,
            Op::Advance { token } => self.advance(token).map(drop)?,
            Op::Fail { token } => self.fail(token)?,
            Op::Release { token, outcome } => self.release(token, outcome).map(drop)?,
            Op::Expire { tokens } => {
                let got = self.expire();
                return Ok(got.iter().map(|l| l.token).eq(tokens));
            }
            Op::Pause { token } => self.pause(token)?,
            Op::Resume { token } => self.resume(token)?,
            Op::GiveWay { jobs } => {
                let want = jobs.clone();
                return Ok(self.ask_give_way(jobs) == want);
            }
            Op::UseReport { token, values } => self.report_use(token, values).map(drop)?,
            Op::Restart { token } => self.restart(token).map(drop)?,
            Op::Tell { from, to, text } => self.tell(from, to, text)?,
        }
        Ok(true)
    }

    fn log(&mut self, op: Op) {
        self.journal.push(Entry { t: self.now(), op });
    }

    fn find(&self, token: Token) -> Result<usize, LeaseError> {
        self.leases
            .binary_search_by_key(&token, |l| l.token)
            .map_err(|_| LeaseError::Stale(token))
    }

    fn drop_lease(&mut self, token: Token) -> Lease {
        let i = self.find(token).expect("the lease was just checked");
        self.leases.remove(i)
    }

    fn node_id(&self, n: NodeId) -> Result<(), LeaseError> {
        if n.index() >= self.live.model().nodes().len() {
            return Err(ModelError::BadId {
                what: "node",
                id: n.0,
            }
            .into());
        }
        Ok(())
    }

    fn job_id(&self, j: JobId) -> Result<(), LeaseError> {
        if j.index() >= self.live.model().jobs().len() {
            return Err(ModelError::BadId {
                what: "job",
                id: j.0,
            }
            .into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rand_chacha::ChaCha8Rng;
    use rand_core::{RngCore, SeedableRng};

    use super::*;
    use crate::model::{KindId, ModelBuilder, Unit};
    use crate::plan::FirstFit;

    /// Two nodes `a` and `b` with 16 RAM each, and a repo's `main` (one holder at a time) on `a`.
    struct Fleet {
        auth: Authority,
        ram: KindId,
        main: KindId,
        main_r: ResourceId,
    }

    fn fleet(settings: LeaseSettings) -> Fleet {
        let mut b = ModelBuilder::new();
        let ram = b.kind("ram", Unit::Count);
        let main = b.kind("main", Unit::Count);
        let na = b.node("a", Labels::new());
        let nb = b.node("b", Labels::new());
        b.pool("a/ram", ram, na, 16);
        b.pool("b/ram", ram, nb, 16);
        let main_r = b.pool("repo/main", main, na, 1);
        Fleet {
            auth: Authority::new(Live::new(b.build().unwrap(), Time::ZERO), settings),
            ram,
            main,
            main_r,
        }
    }

    fn ttl30() -> LeaseSettings {
        LeaseSettings {
            ttl: Dur::secs(30),
            ..LeaseSettings::default()
        }
    }

    fn at(s: i64) -> Time {
        Time::ZERO + Dur::secs(s)
    }

    /// Plans with first fit and grants what starts.
    fn grant(auth: &mut Authority) -> Vec<Lease> {
        let plan = auth.plan(&mut FirstFit);
        auth.grant(&plan.batch()).unwrap()
    }

    fn job(auth: &Authority, name: &str) -> JobId {
        auth.live().model().job_by_name(name).unwrap()
    }

    #[test]
    fn kinds_and_settings_added_later_are_journaled() {
        let Fleet { mut auth, .. } = fleet(ttl30());
        let gate = auth.add_kind("gate", Unit::Count).unwrap();
        assert!(matches!(
            auth.add_kind("gate", Unit::Count),
            Err(LeaseError::Model(ModelError::Duplicate { .. }))
        ));
        let na = auth.live().model().node_by_name("a").unwrap();
        auth.add_resource(Resource {
            name: "a/gate".into(),
            kind: gate,
            node: na,
            capacity: 2,
            labels: Labels::new(),
            states: None,
        })
        .unwrap();
        let ttl = LeaseSettings {
            ttl: Dur::secs(90),
            ..LeaseSettings::default()
        };
        assert!(
            auth.set_settings(LeaseSettings {
                ttl: Dur::ZERO,
                ..ttl.clone()
            })
            .is_err()
        );
        auth.set_settings(ttl.clone()).unwrap();
        auth.submit(Job::new("g").needs(gate, 2)).unwrap();
        let l = grant(&mut auth).remove(0);
        assert_eq!(l.expires, at(90));
        let again = Authority::replay(auth.take_journal()).unwrap();
        assert_eq!(again, auth);
        assert_eq!(again.settings(), &ttl);
    }

    #[test]
    fn a_heartbeat_keeps_a_lease_and_silence_expires_it() {
        let Fleet { mut auth, ram, .. } = fleet(ttl30());
        let a = auth.submit(Job::new("a").needs(ram, 10)).unwrap();
        let l = grant(&mut auth).remove(0);
        assert_eq!((l.token, l.job, l.expires), (Token(1), a, at(30)));
        assert_eq!(auth.lease_of(a), Some(&l));
        assert_eq!(auth.next_expiry(), Some(at(30)));

        auth.set_now(at(20)).unwrap();
        let beat = auth.heartbeat(l.token).unwrap();
        assert_eq!(
            beat,
            Beat {
                expires: at(50),
                stop: false,
                give_way: false,
                over: vec![],
                wakes: vec![],
            }
        );
        auth.set_now(at(49)).unwrap();
        assert!(auth.expire().is_empty(), "not yet");
        assert!(auth.check(l.token).is_ok());

        // Silent from 20 s on: the lease is lost at 50 s, a late heartbeat changes nothing.
        auth.set_now(at(50)).unwrap();
        auth.take_journal();
        let before = auth.clone();
        assert_eq!(
            auth.heartbeat(l.token),
            Err(LeaseError::Expired {
                token: l.token,
                at: at(50)
            })
        );
        assert_eq!(
            auth.release(l.token, Outcome::Ok),
            Err(LeaseError::Expired {
                token: l.token,
                at: at(50)
            })
        );
        assert_eq!(auth, before);
        assert!(auth.take_journal().is_empty(), "a refusal journals nothing");

        let gone = auth.expire();
        assert_eq!(gone.len(), 1);
        assert_eq!(gone[0].token, l.token);
        assert!(auth.leases().is_empty());
        assert_eq!(
            *auth.live().state().status(a),
            JobStatus::Queued,
            "requeued"
        );
        let rs = auth.live().model().resource_ids().collect::<Vec<_>>();
        assert!(
            rs.iter().all(|r| auth.live().state().used(*r) == 0),
            "all given back"
        );
        assert_eq!(auth.next_expiry(), None);
    }

    #[test]
    fn a_stale_token_is_refused_after_the_job_is_granted_again() {
        let Fleet { mut auth, ram, .. } = fleet(ttl30());
        auth.submit(Job::new("a").needs(ram, 10)).unwrap();
        let old = grant(&mut auth)[0].token;
        auth.set_now(at(30)).unwrap();
        auth.expire();
        let new = grant(&mut auth)[0].token;
        assert!(new > old);

        // The old holder comes back: every call with its token is refused and changes nothing.
        auth.take_journal();
        let before = auth.clone();
        assert_eq!(auth.heartbeat(old), Err(LeaseError::Stale(old)));
        assert_eq!(auth.advance(old), Err(LeaseError::Stale(old)));
        assert_eq!(auth.fail(old), Err(LeaseError::Stale(old)));
        assert_eq!(
            auth.release(old, Outcome::Failed),
            Err(LeaseError::Stale(old))
        );
        assert_eq!(auth.check(old), Err(LeaseError::Stale(old)));
        assert_eq!(auth.heartbeat(Token(99)), Err(LeaseError::Stale(Token(99))));
        assert_eq!(auth, before);
        assert!(auth.take_journal().is_empty());

        // The new holder is not disturbed.
        let a = job(&auth, "a");
        auth.release(new, Outcome::Ok).unwrap();
        assert_eq!(auth.live().state().outcome(a), Some(Outcome::Ok));
        assert_eq!(
            auth.release(new, Outcome::Ok),
            Err(LeaseError::Stale(new)),
            "once"
        );
    }

    #[test]
    fn the_fence_on_main_lets_only_the_newest_holder_push() {
        let Fleet {
            mut auth,
            main,
            main_r,
            ..
        } = fleet(ttl30());
        auth.submit(Job::new("release 1.2.0").needs(main, 1))
            .unwrap();
        let first = grant(&mut auth)[0].token;
        assert_eq!(auth.fence(main_r), Some(first));
        assert!(auth.check_holds(first, main_r).is_ok());
        let other = auth
            .live()
            .model()
            .resource_ids()
            .find(|r| *r != main_r)
            .unwrap();
        assert_eq!(
            auth.check_holds(first, other),
            Err(LeaseError::NotHeld {
                token: first,
                resource: other
            })
        );

        // The first holder stalls past its TTL; the release is granted to another holder.
        auth.set_now(at(31)).unwrap();
        auth.expire();
        let second = grant(&mut auth)[0].token;
        assert_eq!(auth.fence(main_r), Some(second));
        assert_eq!(
            auth.check_holds(first, main_r),
            Err(LeaseError::Stale(first))
        );
        assert!(auth.check_holds(second, main_r).is_ok());
        // Where the push lands, an older token than the fence is refused.
        assert!(first < auth.fence(main_r).unwrap());
    }

    #[test]
    fn release_ends_the_job_with_its_outcome() {
        let Fleet { mut auth, ram, .. } = fleet(ttl30());
        let a = auth.submit(Job::new("a").needs(ram, 10)).unwrap();
        let b = auth.submit(Job::new("b").needs(ram, 10).after(a)).unwrap();
        let c = auth.submit(Job::new("c").needs(ram, 10)).unwrap();
        let ls = grant(&mut auth);
        assert_eq!(ls.iter().map(|l| l.job).collect::<Vec<_>>(), vec![a, c]);
        assert_eq!(
            ls.iter().map(|l| l.token).collect::<Vec<_>>(),
            vec![Token(1), Token(2)]
        );

        // A refused grant (b waits for a) changes nothing.
        let r0 = auth.live().model().resource_ids().next().unwrap();
        assert!(matches!(
            auth.grant(&[(b, Assignment::new(vec![r0]))]),
            Err(LeaseError::State(StateError::DepNotDone { .. }))
        ));
        assert_eq!(auth.leases().len(), 2);

        // Not a way a run ends.
        assert!(matches!(
            auth.release(ls[0].token, Outcome::Cached),
            Err(LeaseError::State(StateError::NotARunOutcome(
                Outcome::Cached
            )))
        ));
        auth.set_now(at(5)).unwrap();
        assert!(matches!(
            auth.advance(ls[0].token).unwrap(),
            JobStatus::Running {
                phase: crate::model::Phase::Run,
                ..
            }
        ));
        let l = auth.release(ls[0].token, Outcome::Ok).unwrap();
        assert_eq!(l.job, a);
        assert_eq!(
            *auth.live().state().status(a),
            JobStatus::Done {
                at: at(5),
                outcome: Outcome::Ok
            }
        );
        auth.fail(ls[1].token).unwrap();
        auth.release(ls[1].token, Outcome::Ok).unwrap();
        assert_eq!(
            auth.live().state().outcome(c),
            Some(Outcome::Failed),
            "a failure sticks"
        );
        assert_eq!(grant(&mut auth)[0].job, b, "a passed: b goes on");
    }

    #[test]
    fn phases_reported_to_the_end_end_the_lease() {
        let Fleet { mut auth, ram, .. } = fleet(ttl30());
        let a = auth.submit(Job::new("a").needs(ram, 10)).unwrap();
        let t = grant(&mut auth)[0].token;
        auth.advance(t).unwrap();
        auth.advance(t).unwrap();
        let st = auth.advance(t).unwrap();
        assert!(matches!(
            st,
            JobStatus::Done {
                outcome: Outcome::Ok,
                ..
            }
        ));
        assert!(auth.lease_of(a).is_none());
        assert_eq!(auth.heartbeat(t), Err(LeaseError::Stale(t)));
    }

    #[test]
    fn a_cancel_reaches_the_holder_through_its_heartbeat() {
        let Fleet { mut auth, ram, .. } = fleet(ttl30());
        let a = auth.submit(Job::new("a").needs(ram, 10)).unwrap();
        let t = grant(&mut auth)[0].token;
        auth.cancel(a).unwrap();
        assert!(auth.heartbeat(t).unwrap().stop);
        auth.release(t, Outcome::Ok).unwrap();
        assert_eq!(auth.live().state().outcome(a), Some(Outcome::Cancelled));
        assert!(matches!(
            auth.cancel(JobId(9)),
            Err(LeaseError::Model(ModelError::BadId { what: "job", id: 9 }))
        ));
    }

    #[test]
    fn an_expiry_can_fail_the_job_instead() {
        let settings = LeaseSettings {
            ttl: Dur::secs(10),
            on_expiry: LostJobs::Fail,
            ..LeaseSettings::default()
        };
        let Fleet { mut auth, ram, .. } = fleet(settings);
        let a = auth.submit(Job::new("a").needs(ram, 10)).unwrap();
        let b = auth.submit(Job::new("b").needs(ram, 10)).unwrap();
        let ls = grant(&mut auth);
        auth.set_now(at(5)).unwrap();
        auth.heartbeat(ls[1].token).unwrap();
        auth.set_now(at(10)).unwrap();
        let gone = auth.expire();
        assert_eq!(gone.iter().map(|l| l.job).collect::<Vec<_>>(), vec![a]);
        assert_eq!(auth.live().state().outcome(a), Some(Outcome::Failed));
        assert!(auth.lease_of(b).is_some());
    }

    #[test]
    fn a_lost_node_ends_its_leases() {
        let Fleet { mut auth, ram, .. } = fleet(ttl30());
        auth.submit(Job::new("x").needs(ram, 10)).unwrap();
        auth.submit(Job::new("y").needs(ram, 10)).unwrap();
        let ls = grant(&mut auth);
        let hit = auth.node_lost(NodeId(1), LostJobs::Requeue).unwrap();
        assert_eq!(hit.len(), 1);
        let gone = ls.iter().find(|l| l.job == hit[0]).unwrap().token;
        assert_eq!(auth.heartbeat(gone), Err(LeaseError::Stale(gone)));
        assert_eq!(auth.leases().len(), 1);
        assert!(auth.node_lost(NodeId(5), LostJobs::Requeue).is_err());
    }

    #[test]
    fn leases_follow_their_jobs_through_a_compaction() {
        let Fleet { mut auth, ram, .. } = fleet(ttl30());
        auth.submit(Job::new("a").needs(ram, 10)).unwrap();
        auth.submit(Job::new("b").needs(ram, 10)).unwrap();
        let ls = grant(&mut auth);
        auth.release(ls[0].token, Outcome::Ok).unwrap();
        let remap = auth.compact(Time::ZERO);
        assert_eq!(remap.removed().count(), 1);
        let b = job(&auth, "b");
        assert_eq!(b, JobId(0));
        assert_eq!(auth.lease_of(b).unwrap().token, ls[1].token);
        auth.release(ls[1].token, Outcome::Ok).unwrap();
        assert_eq!(auth.live().state().outcome(b), Some(Outcome::Ok));
    }

    #[test]
    fn settings_load_from_toml() {
        assert_eq!(
            LeaseSettings::from_toml("").unwrap(),
            LeaseSettings::default()
        );
        let s = LeaseSettings::from_toml("ttl = \"2m\"\non_expiry = \"fail\"").unwrap();
        assert_eq!((s.ttl, s.on_expiry), (Dur::mins(2), LostJobs::Fail));
        assert!(LeaseSettings::from_toml("ttl = \"0s\"").is_err());
        assert!(LeaseSettings::from_toml("tt = \"1m\"").is_err());
    }

    /// The journal written out as JSON lines and read back, as a caller stores it.
    fn through_text(entries: &[Entry]) -> Vec<Entry> {
        let text: String = entries
            .iter()
            .map(|e| serde_json::to_string(e).unwrap() + "\n")
            .collect();
        text.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    /// A run that uses every kind of change.
    fn eventful() -> (Authority, Vec<Entry>) {
        let Fleet {
            mut auth,
            ram,
            main,
            main_r,
        } = fleet(ttl30());
        let mut journal = auth.take_journal();
        let a = auth.submit(Job::new("a").needs(ram, 10)).unwrap();
        auth.submit_group(
            "change",
            vec![
                Job::new("g1").needs(ram, 4),
                Job::new("g2").needs(ram, 4).after(a),
            ],
        )
        .unwrap();
        auth.submit(Job::new("rel").needs(main, 1)).unwrap();
        let first = grant(&mut auth);
        auth.set_now(at(10)).unwrap();
        auth.heartbeat(first[0].token).unwrap();
        auth.advance(first[0].token).unwrap();
        let svc = auth
            .add_node(
                "svc",
                Labels::new(),
                [("builds".into(), "false".into())].into(),
            )
            .unwrap();
        auth.add_resource(Resource {
            name: "svc/ram".into(),
            kind: ram,
            node: svc,
            capacity: 8,
            labels: Labels::new(),
            states: None,
        })
        .unwrap();
        auth.set_node_labels(NodeId(0), [("toolchain".into(), "stable".into())].into())
            .unwrap();
        auth.set_node_policy(svc, Labels::new()).unwrap();
        auth.set_capacity(main_r, Some(1)).unwrap();
        auth.set_node_up(NodeId(1), false).unwrap();
        auth.set_node_up(NodeId(1), true).unwrap();
        auth.set_now(at(45)).unwrap();
        auth.expire();
        auth.release(first[0].token, Outcome::Ok).unwrap_err();
        let second = grant(&mut auth);
        auth.fail(second[0].token).unwrap();
        auth.release(second[0].token, Outcome::Failed).unwrap();
        auth.cancel(job(&auth, "g1")).unwrap();
        auth.submit(Job::new("late").needs(ram, 2)).unwrap();
        grant(&mut auth);
        auth.node_lost(NodeId(1), LostJobs::Requeue).unwrap();
        auth.set_now(at(50)).unwrap();
        auth.compact(at(50));
        journal.extend(auth.take_journal());
        (auth, journal)
    }

    #[test]
    fn a_restart_rebuilds_the_same_state_from_the_journal() {
        let (auth, journal) = eventful();
        assert!(matches!(journal[0].op, Op::Snapshot { .. }));
        let ops: std::collections::BTreeSet<_> = journal
            .iter()
            .map(|e| {
                let v = serde_json::to_value(e).unwrap();
                v["op"].as_object().unwrap().keys().next().unwrap().clone()
            })
            .collect();
        assert_eq!(ops.len(), 18, "every kind of change is in the run: {ops:?}");
        let rebuilt = Authority::replay(through_text(&journal)).unwrap();
        assert_eq!(rebuilt, auth);
        assert!(rebuilt.clone().take_journal().is_empty());
        // The rebuilt authority goes on like the one that wrote the journal.
        let (mut x, mut y) = (auth, rebuilt);
        for a in [&mut x, &mut y] {
            a.set_now(at(100)).unwrap();
            a.expire();
            grant(a);
        }
        assert_eq!(x, y);
    }

    #[test]
    fn a_snapshot_starts_a_new_journal() {
        let (mut auth, journal) = eventful();
        let mut fresh = vec![auth.snapshot()];
        let ram = auth
            .live()
            .model()
            .kinds()
            .iter()
            .position(|k| k.name == "ram")
            .unwrap();
        auth.submit(Job::new("after").needs(KindId(ram as u32), 1))
            .unwrap();
        let t = grant(&mut auth).last().unwrap().token;
        auth.heartbeat(t).unwrap();
        let tail = auth.take_journal();
        fresh.extend(tail.iter().cloned());
        assert_eq!(Authority::replay(through_text(&fresh)).unwrap(), auth);
        // The old journal with the snapshot and the tail appended gives the same.
        let mut long = journal;
        long.extend(fresh);
        assert_eq!(Authority::replay(long).unwrap(), auth);
    }

    #[test]
    fn replay_refuses_a_journal_it_cannot_trust() {
        let (_, journal) = eventful();
        assert_eq!(Authority::replay(vec![]), Err(ReplayError::Empty));
        assert_eq!(
            Authority::replay(journal[1..].to_vec()),
            Err(ReplayError::NoSnapshot)
        );

        let mut newer = journal.clone();
        if let Op::Snapshot { version, .. } = &mut newer[0].op {
            *version = JOURNAL_VERSION + 1;
        }
        assert!(matches!(
            Authority::replay(newer),
            Err(ReplayError::Version { index: 0, .. })
        ));

        // An entry lost from the middle: a later grant gets other tokens, or a later call is refused.
        let i = journal
            .iter()
            .position(|e| matches!(e.op, Op::Grant { .. }))
            .unwrap();
        let mut gap = journal.clone();
        gap.remove(i);
        assert!(matches!(
            Authority::replay(gap),
            Err(ReplayError::Refused { .. } | ReplayError::Diverged { .. })
        ));
        let mut tampered = journal;
        let g = tampered
            .iter_mut()
            .find_map(|e| match &mut e.op {
                Op::Grant { tokens, .. } => Some(tokens),
                _ => None,
            })
            .unwrap();
        g[0] = Token(77);
        assert_eq!(
            Authority::replay(tampered),
            Err(ReplayError::Diverged { index: i })
        );
    }

    /// Random runs (seeded): whatever happens, the journal rebuilds the same authority, and the leases held
    /// are exactly the running jobs.
    #[test]
    fn random_runs_rebuild_from_their_journal() {
        for seed in 0..40 {
            let Fleet {
                mut auth,
                ram,
                main,
                main_r,
            } = fleet(ttl30());
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let mut journal = auth.take_journal();
            let mut tokens: Vec<Token> = Vec::new();
            let mut n = 0;
            for _ in 0..200 {
                let pick =
                    |rng: &mut ChaCha8Rng, len: usize| (rng.next_u32() as usize) % len.max(1);
                let any_token = |rng: &mut ChaCha8Rng, ts: &[Token]| {
                    ts.get(pick(rng, ts.len())).copied().unwrap_or(Token(1))
                };
                let _ = match rng.next_u32() % 12 {
                    0 | 1 => {
                        n += 1;
                        let kind = if n % 5 == 0 { main } else { ram };
                        auth.submit(
                            Job::new(format!("j{n}"))
                                .needs(kind, 1 + u64::from(rng.next_u32() % 12)),
                        )
                        .map(drop)
                    }
                    2 | 3 => {
                        let ls = grant(&mut auth);
                        tokens.extend(ls.iter().map(|l| l.token));
                        Ok(())
                    }
                    4 | 5 => auth.heartbeat(any_token(&mut rng, &tokens)).map(drop),
                    6 => auth.advance(any_token(&mut rng, &tokens)).map(drop),
                    7 => {
                        let o =
                            [Outcome::Ok, Outcome::Failed, Outcome::Cancelled][pick(&mut rng, 3)];
                        auth.release(any_token(&mut rng, &tokens), o).map(drop)
                    }
                    8 => {
                        let t = auth.now() + Dur::secs(i64::from(rng.next_u32() % 20));
                        auth.set_now(t).unwrap();
                        auth.expire();
                        Ok(())
                    }
                    9 => auth.cancel(JobId(
                        pick(&mut rng, auth.live().model().jobs().len()) as u32
                    )),
                    10 => auth
                        .set_capacity(main_r, [None, Some(0)][pick(&mut rng, 2)])
                        .map(drop),
                    _ => {
                        let t = auth.now();
                        auth.compact(t);
                        Ok(())
                    }
                };
                let mut leased: Vec<JobId> = auth.leases().iter().map(|l| l.job).collect();
                leased.sort();
                let running: Vec<JobId> = auth.live().state().running().collect();
                assert_eq!(
                    leased, running,
                    "seed {seed}: a lease for every running job"
                );
            }
            journal.extend(auth.take_journal());
            let mut rebuilt = Authority::replay(through_text(&journal)).unwrap();
            // Time that passed after the last change is not journaled; the caller sets the clock.
            rebuilt.set_now(auth.now()).unwrap();
            assert_eq!(rebuilt, auth, "seed {seed}");
        }
    }
}
