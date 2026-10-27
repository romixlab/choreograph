//! The service's state and every call it answers, without IO of its own but the journal: the caller passes the
//! time in, so the same code runs under a real clock and under the paused clock of the tests.
//!
//! [`Service`] owns the [`Authority`] and keeps beside it what the library does not: a stable id per job
//! ([`JobRef`], never renumbered), the client that submitted it, when its client last asked about it, when
//! each node last reported, and the last plan's wait reasons. Every call that changes something plans again
//! ([`Service::schedule`]): expired leases end, jobs nobody waits for are cancelled, silent nodes are drained,
//! finished jobs are compacted, and what fits starts, each start a lease. Then the journal is written before
//! the call answers.

use std::collections::{BTreeMap, BTreeSet};

use choreograph::lease::Op;
use choreograph::model::{KindId, Labels, ModelError, NodeId, Phases, Resource, Selector, Unit};
use choreograph::{
    Authority, Dur, Estimate, Forecast, Job, JobId, JobStatus, LeaseError, ListScheduler, Live,
    ModelBuilder, Need, Outcome, ResourceId, Time, Token, WaitReason,
};

use choreograph::standing::{self, LocalJob, Standing};

use crate::api::{
    API_VERSION, BeatView, EstimateSpec, EtaView, HeldView, JobRef, JobState, JobView, LeaseView,
    NeedSpec, NodeReport, NodeView, QueueView, ResourceView, SubmitGroup, SubmitJob, TellJob,
    UsageView, UseReport, WaitView, WakeView,
};
use crate::config::{ClientConfig, Config, GoConfig, JobKindConfig, OptionConfig, StandingConfig};
use crate::store::{JobMeta, Line, STATE_VERSION, Store, StoreError};

/// Why a call was refused. Maps to an HTTP status and an [`ApiError`](crate::api::ApiError) code.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    #[error("no valid token")]
    Unauthorized,
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    StaleLease(String),
    #[error("{0}")]
    ExpiredLease(String),
    #[error("{0}")]
    Conflict(String),
    /// A tell between two holders that answer each other too often (the pair guard).
    #[error("{0}")]
    Loop(String),
    /// The journal could not be written: the service stops taking calls and exits.
    #[error("{0}")]
    Unavailable(String),
}

impl Refusal {
    pub fn code(&self) -> &'static str {
        match self {
            Refusal::Unauthorized => "unauthorized",
            Refusal::Forbidden(_) => "forbidden",
            Refusal::NotFound(_) => "not_found",
            Refusal::Invalid(_) => "invalid",
            Refusal::StaleLease(_) => "stale_lease",
            Refusal::ExpiredLease(_) => "expired_lease",
            Refusal::Conflict(_) => "conflict",
            Refusal::Loop(_) => "pair_loop",
            Refusal::Unavailable(_) => "unavailable",
        }
    }
}

impl From<LeaseError> for Refusal {
    fn from(e: LeaseError) -> Refusal {
        match e {
            LeaseError::Stale(_) => Refusal::StaleLease(e.to_string()),
            LeaseError::Expired { .. } => Refusal::ExpiredLease(e.to_string()),
            LeaseError::NotHeld { .. } => Refusal::Conflict(e.to_string()),
            LeaseError::Settings(_) | LeaseError::Model(_) | LeaseError::Quota(_) => {
                Refusal::Invalid(e.to_string())
            }
            LeaseError::State(_) => Refusal::Conflict(e.to_string()),
            LeaseError::PairLoop { .. } => Refusal::Loop(e.to_string()),
        }
    }
}

/// Why the service cannot start on its state directory. [`StartError::needs_a_person`] says whether to exit
/// with 78 (no restart helps).
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("journal: {0}")]
    Journal(String),
    #[error(
        "the journal was written by a newer choreographd (state version {0}, this one reads {STATE_VERSION})"
    )]
    Newer(u32),
    #[error(
        "kind {kind:?}: the configuration says unit {config:?}, the journal {journal:?}; a kind's unit cannot change"
    )]
    Unit {
        kind: String,
        config: Unit,
        journal: Unit,
    },
}

impl StartError {
    pub fn needs_a_person(&self) -> bool {
        match self {
            StartError::Store(e) => e.needs_a_person(),
            _ => true,
        }
    }
}

/// The scheduler's state and calls.
#[derive(Debug)]
pub struct Service {
    config: Config,
    auth: Authority,
    planner: ListScheduler,
    /// One per job of the model, in job id order (so ids ascend with it).
    jobs: Vec<JobMeta>,
    next_ref: JobRef,
    /// When each queued job's client last asked about it.
    seen: BTreeMap<JobRef, Time>,
    /// Jobs requeued by an expired lease whose client has not asked about them since: they do not start
    /// again before it does (nobody would take the lease).
    away: BTreeSet<JobRef>,
    /// When each node last reported (since this start).
    reported: BTreeMap<String, Time>,
    /// The jobs each node last reported running under its standing instruction.
    local: BTreeMap<String, Vec<LocalJob>>,
    /// The standing instruction last handed to each node (since this start).
    issued: BTreeMap<String, Standing>,
    /// The last plan's wait reasons.
    waits: BTreeMap<JobRef, WaitView>,
    /// When the last plan wants to be asked again.
    wake_at: Option<Time>,
    last_compact: Time,
    /// The forward plan for ETAs, made when first asked after a change.
    forecast: Option<Forecast>,
    store: Option<Store>,
    /// Service lines waiting to be written with the authority's next entries.
    pending: Vec<Line>,
    /// Set once a journal write failed: nothing is accepted any more.
    broken: Option<String>,
}

impl Service {
    /// Starts on a state directory: rebuilds from its journal or starts empty, adds the configured kinds and
    /// lease settings, and writes a fresh snapshot.
    pub fn open(config: Config, now: Time) -> Result<Service, StartError> {
        let (store, lines) = Store::open(&config.state_dir)?;
        Service::start(config, Some(store), lines, now)
    }

    /// A service without a journal on disk: everything is lost when it ends (for tests and trials).
    pub fn in_memory(config: Config, now: Time) -> Service {
        Service::start(config, None, Vec::new(), now).expect("an empty start cannot fail")
    }

    /// Checks that a journal can be rebuilt with this configuration, without writing anything: the number of
    /// jobs and nodes in it.
    pub fn check(config: &Config) -> Result<(usize, usize), StartError> {
        let lines = Store::read_only(&config.state_dir)?;
        let (auth, jobs, _) =
            rebuild(lines)?.unwrap_or_else(|| (empty(config, Time::ZERO), Vec::new(), 1));
        check_units(config, &auth)?;
        Ok((jobs.len(), auth.live().model().nodes().len()))
    }

    fn start(
        config: Config,
        store: Option<Store>,
        lines: Vec<Line>,
        now: Time,
    ) -> Result<Service, StartError> {
        let (mut auth, jobs, next_ref) = match rebuild(lines)? {
            Some(r) => r,
            None => (empty(&config, now), Vec::new(), 1),
        };
        check_units(&config, &auth)?;
        let now = now.max(auth.now());
        auth.set_now(now).expect("time moves forward");
        for k in &config.kinds {
            if auth.live().model().kind_by_name(&k.name).is_none() {
                auth.add_kind(k.name.clone(), k.unit)
                    .map_err(|e| StartError::Journal(e.to_string()))?;
            }
        }
        if auth.settings() != &config.lease {
            auth.set_settings(config.lease.clone())
                .map_err(|e| StartError::Journal(e.to_string()))?;
        }
        let shared = config.settings.shared_node.clone();
        let reported = auth
            .live()
            .model()
            .nodes()
            .iter()
            .filter(|n| n.name != shared)
            .map(|n| (n.name.clone(), now))
            .collect();
        let seen = auth
            .live()
            .state()
            .queued()
            .map(|j| (jobs[j.index()].id, now))
            .collect();
        let mut s = Service {
            planner: ListScheduler::new(config.policy.clone()),
            config,
            auth,
            jobs,
            next_ref,
            seen,
            away: BTreeSet::new(),
            reported,
            local: BTreeMap::new(),
            issued: BTreeMap::new(),
            waits: BTreeMap::new(),
            wake_at: None,
            last_compact: now,
            forecast: None,
            store,
            pending: Vec::new(),
            broken: None,
        };
        s.snapshot().map_err(StartError::Store)?;
        s.schedule(now);
        if let Some(e) = &s.broken {
            return Err(StartError::Journal(e.clone()));
        }
        Ok(s)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn authority(&self) -> &Authority {
        &self.auth
    }

    pub fn now(&self) -> Time {
        self.auth.now()
    }

    /// Why the service stopped taking calls, once a journal write failed.
    pub fn broken(&self) -> Option<&str> {
        self.broken.as_deref()
    }

    /// The client a token belongs to.
    /// Who a call comes from (see [`Config::caller`]).
    pub fn caller(
        &self,
        bearer: Option<&str>,
        named: Option<&str>,
    ) -> Result<ClientConfig, Refusal> {
        self.config
            .caller(bearer, named)
            .ok_or(Refusal::Unauthorized)
    }

    /// When the service wants to look again with nothing else happening: the next lease expiry, the plan's
    /// wake time, or one tick from now.
    pub fn next_wake(&self) -> Time {
        let now = self.now();
        [self.auth.next_expiry(), self.wake_at]
            .into_iter()
            .flatten()
            .fold(now + self.config.settings.tick, Time::min)
            .max(now)
    }

    /// Queues a job, plans, and answers with the job as it is now (it may already run).
    pub fn submit(
        &mut self,
        client: &ClientConfig,
        req: SubmitJob,
        now: Time,
    ) -> Result<JobView, Refusal> {
        self.begin(now)?;
        let job = self.job_from(req, now, self.next_ref, false)?;
        let refuse = self.refusal_for(&[&job]);
        let id = self.auth.submit(job).map_err(refuse)?;
        self.added(client, &[id], now);
        self.schedule(now);
        self.end()?;
        self.view_by_id(self.jobs.last().map(|m| m.id).expect("just added"))
    }

    /// Queues jobs that start together or not at all.
    pub fn submit_group(
        &mut self,
        client: &ClientConfig,
        req: SubmitGroup,
        now: Time,
    ) -> Result<Vec<JobView>, Refusal> {
        self.begin(now)?;
        if req.jobs.is_empty() {
            return Err(Refusal::Invalid("a group needs at least one job".into()));
        }
        let first = self.next_ref;
        let jobs = req
            .jobs
            .into_iter()
            .zip(first..)
            .map(|(j, r)| self.job_from(j, now, r, true))
            .collect::<Result<Vec<_>, _>>()?;
        let refuse = self.refusal_for(&jobs.iter().collect::<Vec<_>>());
        let (_, ids) = self
            .auth
            .submit_group(model_name(first, &req.name), jobs)
            .map_err(refuse)?;
        let refs = self.added(client, &ids, now);
        self.schedule(now);
        self.end()?;
        refs.into_iter().map(|r| self.view_by_id(r)).collect()
    }

    /// A job as it is now.
    pub fn job(&mut self, id: JobRef) -> Result<JobView, Refusal> {
        self.view_by_id(id)
    }

    /// The client asks about its job (a wait): it is still wanted. Answers with the job as it is now.
    pub fn touch(
        &mut self,
        client: &ClientConfig,
        id: JobRef,
        now: Time,
    ) -> Result<JobView, Refusal> {
        let j = self.own(client, id)?;
        if matches!(self.auth.live().state().status(j), JobStatus::Queued) {
            self.seen.insert(id, now.max(self.now()));
            self.away.remove(&id);
        }
        self.view_by_id(id)
    }

    /// Cancels a job: a queued one ends at once, a running one is told at its next heartbeat.
    pub fn cancel(
        &mut self,
        client: &ClientConfig,
        id: JobRef,
        now: Time,
    ) -> Result<JobView, Refusal> {
        self.begin(now)?;
        let j = self.own(client, id)?;
        if !matches!(self.auth.live().state().status(j), JobStatus::Done { .. }) {
            self.auth.cancel(j).map_err(Refusal::from)?;
            self.seen.remove(&id);
            self.away.remove(&id);
        }
        self.schedule(now);
        self.end()?;
        self.view_by_id(id)
    }

    /// Keeps a lease alive.
    pub fn heartbeat(
        &mut self,
        client: &ClientConfig,
        token: u64,
        now: Time,
    ) -> Result<BeatView, Refusal> {
        self.begin(now)?;
        self.own_lease(client, Token(token))?;
        let beat = self.auth.heartbeat(Token(token)).map_err(Refusal::from)?;
        self.end()?;
        Ok(BeatView {
            expires: beat.expires,
            stop: beat.stop,
            give_way: beat.give_way,
            over: beat.over,
            wakes: beat
                .wakes
                .into_iter()
                .map(|w| WakeView {
                    from: w.from.map(|j| self.jobs[j.index()].id),
                    at: w.at,
                    text: w.text,
                })
                .collect(),
        })
    }

    /// The holder reports what the current step of its lease has used; answers with the budgets it is over.
    /// A stopping budget's hard level cancels the job, which plans again.
    pub fn report_use(
        &mut self,
        client: &ClientConfig,
        token: u64,
        r: UseReport,
        now: Time,
    ) -> Result<UsageView, Refusal> {
        self.begin(now)?;
        let j = self.own_lease(client, Token(token))?;
        let over = self
            .auth
            .report_use(Token(token), r.used)
            .map_err(Refusal::from)?;
        let stop = matches!(
            self.auth.live().state().status(j),
            JobStatus::Running { ending, .. } if *ending != Outcome::Ok
        );
        let step = self
            .auth
            .lease(Token(token))
            .map_or(1, |l| l.usage.restarts + 1);
        self.end()?;
        Ok(UsageView { step, over, stop })
    }

    /// Restarts the lease's job in place: a fresh process of its holder under the same lease, token and
    /// resources, the next step. Not a new start: nothing is planned or woken.
    pub fn restart(
        &mut self,
        client: &ClientConfig,
        token: u64,
        now: Time,
    ) -> Result<JobView, Refusal> {
        self.begin(now)?;
        let j = self.own_lease(client, Token(token))?;
        let id = self.jobs[j.index()].id;
        self.auth.restart(Token(token)).map_err(Refusal::from)?;
        self.end()?;
        self.view_by_id(id)
    }

    /// Leaves a message for the holder of a running job, handed over with its next heartbeat. Any client may
    /// tell any running job; a sender that holds a lease names it (`from`, its own), and tells between two
    /// leases pass the pair guard.
    pub fn tell(
        &mut self,
        client: &ClientConfig,
        id: JobRef,
        t: TellJob,
        now: Time,
    ) -> Result<JobView, Refusal> {
        self.begin(now)?;
        let to = self.job_id(id)?;
        if let Some(f) = t.from {
            self.own_lease(client, Token(f))?;
        }
        if t.text.is_empty() {
            return Err(Refusal::Invalid("a tell needs a text".into()));
        }
        self.auth
            .tell(t.from.map(Token), to, t.text)
            .map_err(|e| match e {
                LeaseError::State(_) => Refusal::Conflict(format!(
                    "job {id} holds no lease: only a running job's holder can be told"
                )),
                LeaseError::PairLoop { from, to, bounces } => Refusal::Loop(format!(
                    "jobs {} and {} have told each other back and forth {bounces} times within the \
                     pair window: refused as a loop",
                    self.jobs[from.index()].id,
                    self.jobs[to.index()].id
                )),
                e => e.into(),
            })?;
        self.end()?;
        self.view_by_id(id)
    }

    /// Pauses a lease's job: it keeps its lease and what its kind keeps on pause, gives back the rest, and
    /// plans no longer count on it ending. Plans again, since what it gave back may start others.
    pub fn pause(
        &mut self,
        client: &ClientConfig,
        token: u64,
        now: Time,
    ) -> Result<JobView, Refusal> {
        self.begin(now)?;
        let j = self.own_lease(client, Token(token))?;
        let id = self.jobs[j.index()].id;
        self.auth.pause(Token(token)).map_err(Refusal::from)?;
        self.schedule(now);
        self.end()?;
        self.view_by_id(id)
    }

    /// A paused job goes on: it waits for what it gave back (a wait answers once it has it again) under the
    /// same lease, which its holder keeps alive meanwhile.
    pub fn resume(
        &mut self,
        client: &ClientConfig,
        token: u64,
        now: Time,
    ) -> Result<JobView, Refusal> {
        self.begin(now)?;
        let j = self.own_lease(client, Token(token))?;
        let id = self.jobs[j.index()].id;
        self.auth.resume(Token(token)).map_err(Refusal::from)?;
        self.schedule(now);
        self.end()?;
        self.view_by_id(id)
    }

    /// Ends a lease with the outcome of its job, frees what it held and plans again.
    pub fn release(
        &mut self,
        client: &ClientConfig,
        token: u64,
        outcome: Outcome,
        now: Time,
    ) -> Result<JobView, Refusal> {
        self.begin(now)?;
        let j = self.own_lease(client, Token(token))?;
        let id = self.jobs[j.index()].id;
        self.auth
            .release(Token(token), outcome)
            .map_err(Refusal::from)?;
        self.schedule(now);
        self.end()?;
        self.view_by_id(id)
    }

    /// A node reports itself: it is added on its first report, its labels, policy labels, state and the room
    /// per kind follow what it says. Only changes are journaled, so a steady node costs nothing there.
    pub fn report(
        &mut self,
        client: &ClientConfig,
        node: &str,
        r: NodeReport,
        now: Time,
    ) -> Result<NodeView, Refusal> {
        self.begin(now)?;
        if !client.may_report(node) {
            return Err(Refusal::Forbidden(format!(
                "client {:?} may not report node {node:?}",
                client.name
            )));
        }
        if node == self.config.settings.shared_node {
            return Err(Refusal::Forbidden(format!(
                "{node:?} is the shared node; no machine reports it"
            )));
        }
        if node.is_empty() || node.contains('/') {
            return Err(Refusal::Invalid(format!(
                "node {node:?}: a name without '/'"
            )));
        }
        let mut local: BTreeMap<&str, u64> = BTreeMap::new();
        for j in &r.local {
            if j.kind.is_empty() {
                return Err(Refusal::Invalid("a local job needs its kind".into()));
            }
            for (k, a) in &j.needs {
                let e = local.entry(k.as_str()).or_default();
                *e = e.saturating_add(*a);
            }
        }
        for k in r
            .room
            .keys()
            .map(String::as_str)
            .chain(local.keys().copied())
        {
            match self.config.kind(k) {
                None => return Err(Refusal::Invalid(format!("unknown kind {k:?}"))),
                Some(c) if c.shared => {
                    return Err(Refusal::Invalid(format!(
                        "kind {k:?} is shared: it belongs to no node"
                    )));
                }
                Some(_) => {}
            }
        }
        let n = match self.auth.live().model().node_by_name(node) {
            Some(n) => {
                let have = self.auth.live().model().node(n).clone();
                if have.labels != r.labels {
                    self.auth.set_node_labels(n, r.labels.clone())?;
                }
                if have.policy != r.policy {
                    self.auth.set_node_policy(n, r.policy.clone())?;
                }
                n
            }
            None => self
                .auth
                .add_node(node, r.labels.clone(), r.policy.clone())?,
        };
        if self.auth.live().state().node_up(n) != r.up {
            self.auth.set_node_up(n, r.up)?;
        }
        // Every non-shared kind: what it says, nothing for a kind it leaves out.
        let kinds: Vec<(String, KindId)> = self
            .config
            .kinds
            .iter()
            .filter(|k| !k.shared)
            .filter_map(|k| {
                Some((
                    k.name.clone(),
                    self.auth.live().model().kind_by_name(&k.name)?,
                ))
            })
            .collect();
        // Jobs run under the standing instruction hold their part of the room: what is left is planned with.
        for (name, kind) in kinds {
            let room = r.room.get(&name).copied().unwrap_or(0);
            let free = room.saturating_sub(local.get(name.as_str()).copied().unwrap_or(0));
            let rname = format!("{node}/{name}");
            let rid = match self.auth.live().model().resource_by_name(&rname) {
                None if room == 0 => continue,
                None => self.auth.add_resource(Resource {
                    name: rname,
                    kind,
                    node: n,
                    capacity: room,
                    labels: Labels::new(),
                    states: None,
                })?,
                Some(rid) => rid,
            };
            let st = self.auth.live().state();
            if st.measured(rid) != Some(free) {
                self.auth.set_capacity(rid, Some(free))?;
            }
        }
        self.reported.insert(node.to_string(), now.max(self.now()));
        if r.local.is_empty() {
            self.local.remove(node);
        } else {
            self.local.insert(node.to_string(), r.local);
        }
        self.schedule(now);
        self.end()?;
        self.issue(n);
        Ok(self.node_view(n))
    }

    /// A node's current standing instruction (made afresh from the configuration and the live view; a new
    /// serial only when it changed).
    pub fn standing(&mut self, node: &str) -> Result<Standing, Refusal> {
        let n = self
            .auth
            .live()
            .model()
            .node_by_name(node)
            .filter(|_| node != self.config.settings.shared_node)
            .ok_or_else(|| Refusal::NotFound(format!("no node {node:?}: it has not reported")))?;
        Ok(self.issue(n))
    }

    /// The whole picture.
    pub fn queue(&mut self) -> QueueView {
        let st = self.auth.live().state();
        let running: Vec<JobId> = st.running().collect();
        let order = self.planner.order(self.auth.live().model(), st);
        let mut done: Vec<(Time, JobId)> = self
            .auth
            .live()
            .model()
            .job_ids()
            .filter_map(|j| match st.status(j) {
                JobStatus::Done { at, .. } => Some((*at, j)),
                _ => None,
            })
            .collect();
        done.sort_by(|a, b| b.cmp(a));
        let nodes = (0..self.auth.live().model().nodes().len())
            .map(|i| self.node_view(NodeId(i as u32)))
            .collect();
        QueueView {
            now: self.now(),
            nodes,
            running: running.into_iter().map(|j| self.view(j)).collect(),
            queued: order.into_iter().map(|j| self.view(j)).collect(),
            done: done.into_iter().map(|(_, j)| self.view(j)).collect(),
        }
    }

    /// Plans with nothing else happening: expiries, stale nodes and jobs, aging, compaction.
    pub fn tick(&mut self, now: Time) -> Result<(), Refusal> {
        self.begin(now)?;
        self.schedule(now);
        self.end()
    }

    // ---- inside ----

    fn begin(&mut self, now: Time) -> Result<(), Refusal> {
        if let Some(e) = &self.broken {
            return Err(Refusal::Unavailable(e.clone()));
        }
        let now = now.max(self.now());
        self.auth.set_now(now).expect("time moves forward");
        Ok(())
    }

    /// Writes what changed; a failed write breaks the service.
    fn end(&mut self) -> Result<(), Refusal> {
        self.commit();
        match &self.broken {
            Some(e) => Err(Refusal::Unavailable(e.clone())),
            None => Ok(()),
        }
    }

    fn commit(&mut self) {
        let mut lines: Vec<Line> = self
            .auth
            .take_journal()
            .into_iter()
            .map(Line::Entry)
            .collect();
        // Service lines follow the authority entries they belong to: the order between them does not matter
        // on replay, only the order of the service lines among themselves.
        lines.append(&mut self.pending);
        if lines.is_empty() {
            return;
        }
        self.forecast = None;
        let Some(store) = &mut self.store else {
            return;
        };
        let res = store.append(&lines).and_then(|()| {
            if store.lines() >= self.config.settings.snapshot_every {
                store.replace(&[snapshot_line(&self.auth, &self.jobs, self.next_ref)])
            } else {
                Ok(())
            }
        });
        if let Err(e) = res {
            tracing::error!("journal write failed, stopping: {e}");
            self.broken = Some(format!("the journal could not be written: {e}"));
        }
    }

    fn snapshot(&mut self) -> Result<(), StoreError> {
        self.auth.take_journal();
        self.pending.clear();
        if let Some(store) = &mut self.store {
            store.replace(&[snapshot_line(&self.auth, &self.jobs, self.next_ref)])?;
        }
        Ok(())
    }

    /// Records new jobs: their ids, their client, seen now.
    fn added(&mut self, client: &ClientConfig, ids: &[JobId], now: Time) -> Vec<JobRef> {
        let mut metas = Vec::with_capacity(ids.len());
        for j in ids {
            debug_assert_eq!(j.index(), self.jobs.len());
            let m = JobMeta {
                id: self.next_ref,
                client: client.name.clone(),
            };
            self.next_ref += 1;
            self.seen.insert(m.id, now);
            self.jobs.push(m.clone());
            metas.push(m);
        }
        let refs = metas.iter().map(|m| m.id).collect();
        self.pending.push(Line::Jobs(metas));
        refs
    }

    /// Expire, cancel the jobs nobody waits for, drain silent nodes, compact, plan and start.
    fn schedule(&mut self, now: Time) {
        let now = now.max(self.now());
        self.auth.set_now(now).expect("time moves forward");
        let s = self.config.settings.clone();
        for l in self.auth.expire() {
            let id = self.jobs[l.job.index()].id;
            tracing::info!("lease {} of job {id} expired", l.token);
            if matches!(self.auth.live().state().status(l.job), JobStatus::Queued) {
                self.away.insert(id);
            }
        }
        // Expired jobs requeued: their clients get one more `queued_stale` to come back.
        let queued: Vec<JobId> = self.auth.live().state().queued().collect();
        for j in queued {
            let id = self.jobs[j.index()].id;
            let seen = *self.seen.entry(id).or_insert(now);
            if now - seen >= s.queued_stale {
                tracing::info!(
                    "job {id}: nobody asked about it for {}, cancelled",
                    now - seen
                );
                self.auth.cancel(j).expect("a queued job can be cancelled");
                self.seen.remove(&id);
                self.away.remove(&id);
            }
        }
        let stale: Vec<String> = self
            .reported
            .iter()
            .filter(|(_, t)| now - **t >= s.node_stale)
            .map(|(n, _)| n.clone())
            .collect();
        for name in stale {
            if let Some(n) = self.auth.live().model().node_by_name(&name)
                && self.auth.live().state().node_up(n)
            {
                tracing::warn!("node {name}: no report for {}, drained", s.node_stale);
                self.auth.set_node_up(n, false).expect("a known node");
            }
        }
        if now - self.last_compact >= s.tick {
            self.last_compact = now;
            self.compact(now - s.keep_done);
        }
        // A job whose client is away would start under a lease nobody takes: it is cancelled instead, and
        // the plan made again without it.
        let plan = loop {
            let plan = self.auth.plan(&mut self.planner);
            let gone: Vec<(JobId, JobRef)> = plan
                .starts
                .iter()
                .map(|st| (st.job, self.jobs[st.job.index()].id))
                .filter(|(_, id)| self.away.contains(id))
                .collect();
            if gone.is_empty() {
                break plan;
            }
            for (j, id) in gone {
                tracing::info!(
                    "job {id}: its client did not come back after its lease expired, cancelled"
                );
                self.auth.cancel(j).expect("a queued job can be cancelled");
                self.away.remove(&id);
                self.seen.remove(&id);
            }
        };
        if !plan.starts.is_empty() {
            match self.auth.grant(&plan.batch()) {
                Ok(leases) => {
                    for l in leases {
                        let id = self.jobs[l.job.index()].id;
                        self.seen.remove(&id);
                        tracing::info!("job {id} starts under lease {}", l.token);
                    }
                }
                Err(e) => tracing::error!("the plan's starts were refused: {e}"),
            }
        }
        for j in self.auth.ask_give_way(plan.give_way.keys().copied()) {
            let for_job = plan.give_way[&j];
            tracing::info!(
                "job {} is asked to give way for job {}",
                self.jobs[j.index()].id,
                self.jobs[for_job.index()].id
            );
        }
        self.wake_at = plan.wake_at;
        self.waits = plan
            .waits
            .iter()
            .map(|(j, w)| (self.jobs[j.index()].id, self.wait_view(w, *j)))
            .collect();
        self.forecast = None;
    }

    fn compact(&mut self, done_by: Time) {
        let live = self.auth.live();
        if live
            .state()
            .keep_after_compaction(live.model(), done_by)
            .iter()
            .all(|k| *k)
        {
            return;
        }
        let remap = self.auth.compact(done_by);
        let removed: Vec<JobRef> = remap.removed().map(|j| self.jobs[j.index()].id).collect();
        let mut i = 0;
        self.jobs.retain(|_| {
            i += 1;
            remap.job(JobId(i as u32 - 1)).is_some()
        });
        for r in &removed {
            self.seen.remove(r);
            self.away.remove(r);
        }
        self.pending.push(Line::Compacted { removed });
    }

    /// Turns the model's refusal of new jobs into one in the client's words: its job names, amounts in
    /// their unit.
    fn refusal_for(&self, jobs: &[&Job]) -> impl FnOnce(LeaseError) -> Refusal + use<> {
        let model = self.auth.live().model();
        let texts: Vec<(String, Vec<String>)> = jobs
            .iter()
            .map(|j| {
                let needs = j
                    .needs
                    .iter()
                    .map(|n| {
                        let k = model.kind(n.kind);
                        format!("{} {}", amount_text(n.amount, k.unit), k.name)
                    })
                    .collect();
                (j.name.clone(), needs)
            })
            .collect();
        move |e| match e {
            LeaseError::Model(ModelError::NoMatch { job, need }) => {
                let needs = texts.iter().find(|(n, _)| *n == job).map(|(_, n)| n);
                Refusal::Invalid(format!(
                    "job {:?}: no resource could ever give {}",
                    shown_name(&job),
                    needs
                        .and_then(|n| n.get(need))
                        .map_or("its need", |t| t.as_str()),
                ))
            }
            e => Refusal::Invalid(texts.iter().fold(e.to_string(), |t, (n, _)| {
                t.replace(n.as_str(), shown_name(n))
            })),
        }
    }

    /// The library job for a request; `id` is the job's [`JobRef`] to be.
    fn job_from(
        &mut self,
        req: SubmitJob,
        now: Time,
        id: JobRef,
        in_group: bool,
    ) -> Result<Job, Refusal> {
        if req.name.is_empty() {
            return Err(Refusal::Invalid("a job needs a name".into()));
        }
        let mut specs = req.shapes.into_iter();
        let first = if req.needs.is_empty() {
            specs.next().unwrap_or_default()
        } else {
            req.needs
        };
        let mut needs = Vec::with_capacity(first.len());
        for n in first {
            needs.push(self.need_from(n)?);
        }
        let mut shapes = Vec::with_capacity(specs.len());
        for (k, shape) in specs.enumerate() {
            if shape.is_empty() {
                return Err(Refusal::Invalid(format!("shape {} has no needs", k + 1)));
            }
            let mut s = Vec::with_capacity(shape.len());
            for n in shape {
                s.push(self.need_from(n)?);
            }
            shapes.push(s);
        }
        let mut after = Vec::with_capacity(req.after.len());
        for r in req.after {
            after.push(self.job_id(r).map_err(|_| {
                Refusal::Invalid(format!(
                    "job {r} it comes after is not known (never submitted, or finished and compacted)"
                ))
            })?);
        }
        let est = |e: Option<EstimateSpec>| {
            e.map_or(Estimate::default(), |e| Estimate {
                p50: e.p50,
                p90: e.p90.unwrap_or(e.p50),
            })
        };
        let kind = req.kind.as_deref().map(|k| {
            self.config.job_kind(k).cloned().unwrap_or(JobKindConfig {
                name: k.into(),
                ..JobKindConfig::default()
            })
        });
        if kind.as_ref().is_some_and(|k| k.name.is_empty()) {
            return Err(Refusal::Invalid("a job kind needs a name".into()));
        }
        // The run: the job's own guess, else what the kind has taught, else the kind's configured guess.
        let run = match req.run {
            Some(r) => est(Some(r)),
            None => kind
                .as_ref()
                .and_then(|k| self.auth.learned(&k.name))
                .unwrap_or_else(|| est(kind.as_ref().and_then(|k| k.run))),
        };
        let model = self.auth.live().model();
        let mut pause_keeps = Vec::new();
        for name in req
            .pause_keeps
            .as_ref()
            .or(kind.as_ref().map(|k| &k.pause_keeps))
            .into_iter()
            .flatten()
        {
            pause_keeps.push(
                model.kind_by_name(name).ok_or_else(|| {
                    Refusal::Invalid(format!("pause_keeps: unknown kind {name:?}"))
                })?,
            );
        }
        let mut job = Job::new(model_name(id, &req.name));
        job.needs = needs;
        job.phases = Phases {
            setup: est(req.setup),
            run,
            cleanup: est(req.cleanup),
        };
        job.after = after;
        job.class = req.class;
        job.weight = req.weight.unwrap_or(1);
        job.arrival = now;
        job.release = req.release;
        job.due = req.due;
        job.open = req.open.or(kind.as_ref().map(|k| k.open)).unwrap_or(false);
        job.preempt = req
            .preempt
            .or(kind.as_ref().map(|k| k.preempt))
            .unwrap_or_default();
        job.pause_keeps = pause_keeps;
        job.budgets = req
            .budgets
            .or(kind.as_ref().map(|k| k.budgets.clone()))
            .unwrap_or_default()
            .into_iter()
            .map(Into::into)
            .collect();
        job.shapes = shapes;
        // Start anyway: the job's own wait where its kind allows one, else its kind's for a job pinned to a
        // node outside a group.
        job.start_anyway_after = match req.start_anyway_after {
            Some(_) if !kind.as_ref().is_some_and(|k| k.allow_start_anyway) => {
                return Err(Refusal::Invalid(match &kind {
                    Some(k) => format!(
                        "jobs of kind {:?} may not set start_anyway_after (allow_start_anyway is false)",
                        k.name
                    ),
                    None => {
                        "start_anyway_after needs a kind that allows it (allow_start_anyway)".into()
                    }
                }));
            }
            Some(_) if in_group => {
                return Err(Refusal::Invalid(
                    "a job of a group starts with its group, not anyway".into(),
                ));
            }
            Some(_) if !job.pinned(job.smallest()) => {
                return Err(Refusal::Invalid(
                    "start_anyway_after needs every need to name a node or a resource".into(),
                ));
            }
            Some(d) => Some(d),
            None if in_group || !job.pinned(job.smallest()) => None,
            None => kind.as_ref().and_then(|k| k.start_anyway_after),
        };
        job.kind = kind.map(|k| k.name);
        Ok(job)
    }

    fn need_from(&mut self, n: NeedSpec) -> Result<Need, Refusal> {
        let model = self.auth.live().model();
        let kind = model
            .kind_by_name(&n.kind)
            .ok_or_else(|| Refusal::Invalid(format!("unknown kind {:?}", n.kind)))?;
        let shared = self.config.kind(&n.kind).is_some_and(|k| k.shared);
        let node = match &n.node {
            None => None,
            Some(name) => Some(
                model
                    .node_by_name(name)
                    .ok_or_else(|| Refusal::Invalid(format!("unknown node {name:?}")))?,
            ),
        };
        let resource = match &n.resource {
            None if shared => {
                return Err(Refusal::Invalid(format!(
                    "kind {:?} is shared: a need names its resource (\"resource\": \"repo/main\")",
                    n.kind
                )));
            }
            None => None,
            Some(name) => Some(match model.resource_by_name(name) {
                Some(r) if model.resource(r).kind == kind => r,
                Some(_) => {
                    return Err(Refusal::Invalid(format!(
                        "resource {name:?} is not of kind {:?}",
                        n.kind
                    )));
                }
                None if shared => self.add_shared(name, kind, &n.kind)?,
                None => return Err(Refusal::Invalid(format!("unknown resource {name:?}"))),
            }),
        };
        Ok(Need {
            kind,
            amount: n.amount,
            select: Selector {
                resource,
                node,
                labels: n.labels,
                node_labels: n.node_labels,
                tolerate: n.tolerate,
            },
            state: None,
            same_node: n.same_node,
        })
    }

    /// Makes a shared resource on first use, on the shared node.
    fn add_shared(&mut self, name: &str, kind: KindId, kname: &str) -> Result<ResourceId, Refusal> {
        if name.is_empty() {
            return Err(Refusal::Invalid("a resource needs a name".into()));
        }
        let cap = self.config.kind(kname).map_or(1, |k| k.capacity);
        let sn = self.config.settings.shared_node.clone();
        let node = match self.auth.live().model().node_by_name(&sn) {
            Some(n) => n,
            None => self.auth.add_node(sn, Labels::new(), Labels::new())?,
        };
        Ok(self.auth.add_resource(Resource {
            name: name.into(),
            kind,
            node,
            capacity: cap,
            labels: Labels::new(),
            states: None,
        })?)
    }

    fn job_id(&self, id: JobRef) -> Result<JobId, Refusal> {
        self.jobs
            .binary_search_by_key(&id, |m| m.id)
            .map(|i| JobId(i as u32))
            .map_err(|_| Refusal::NotFound(format!("no job {id}")))
    }

    fn own(&self, client: &ClientConfig, id: JobRef) -> Result<JobId, Refusal> {
        let j = self.job_id(id)?;
        if self.jobs[j.index()].client != client.name {
            return Err(Refusal::Forbidden(format!(
                "job {id} belongs to client {:?}",
                self.jobs[j.index()].client
            )));
        }
        Ok(j)
    }

    fn own_lease(&self, client: &ClientConfig, token: Token) -> Result<JobId, Refusal> {
        let l = self
            .auth
            .lease(token)
            .ok_or_else(|| Refusal::from(LeaseError::Stale(token)))?;
        let m = &self.jobs[l.job.index()];
        if m.client != client.name {
            return Err(Refusal::Forbidden(format!(
                "lease {token} belongs to client {:?}",
                m.client
            )));
        }
        Ok(l.job)
    }

    fn view_by_id(&mut self, id: JobRef) -> Result<JobView, Refusal> {
        let j = self.job_id(id)?;
        Ok(self.view(j))
    }

    fn view(&mut self, j: JobId) -> JobView {
        if self.forecast.is_none() {
            let live = self.auth.live();
            self.forecast = Some(self.planner.forecast(live.model(), live.state()));
        }
        let live = self.auth.live();
        let (model, st) = (live.model(), live.state());
        let meta = &self.jobs[j.index()];
        let mut v = JobView {
            id: meta.id,
            name: shown_name(&model.job(j).name).to_string(),
            client: meta.client.clone(),
            kind: model.job(j).kind.clone(),
            open: model.job(j).open,
            state: JobState::Queued,
            wait: None,
            eta: None,
            lease: None,
            shape: None,
            shapes: (model.job(j).shape_count() > 1).then(|| model.job(j).shape_count()),
            start_anyway_at: None,
            over: false,
            outcome: None,
            done_at: None,
        };
        let eta = self
            .forecast
            .as_ref()
            .and_then(|f| f.etas.get(&j))
            .map(|e| EtaView {
                start_p50: e.p50.map(|s| s.start),
                finish_p50: e.p50.map(|s| s.finish),
                start_p90: e.p90.map(|s| s.start),
                finish_p90: e.p90.map(|s| s.finish),
            });
        let wait = || {
            self.waits
                .get(&meta.id)
                .cloned()
                .unwrap_or_else(|| WaitView {
                    why: "planning".into(),
                    text: "not planned yet".into(),
                    job: None,
                    resource: None,
                    kind: None,
                    at: None,
                })
        };
        match st.status(j) {
            JobStatus::Pending | JobStatus::Queued => {
                v.wait = Some(wait());
                v.eta = eta;
                v.start_anyway_at = st.start_anyway_at(model, j);
            }
            JobStatus::Running {
                assignment,
                paused,
                over,
                ..
            } => {
                v.over = *over;
                v.shape = v.shapes.map(|_| assignment.shape);
                v.state = match paused {
                    Some(_) => JobState::Paused,
                    None => JobState::Running,
                };
                if paused.as_ref().is_some_and(|p| p.resume) {
                    v.wait = Some(wait());
                }
                v.eta = eta;
                v.lease = self.auth.lease_of(j).map(|l| LeaseView {
                    token: l.token.0,
                    granted: l.granted,
                    expires: l.expires,
                    held: assignment
                        .resources
                        .iter()
                        .zip(model.job(j).needs_for(assignment))
                        .enumerate()
                        .map(|(i, (r, n))| {
                            let res = model.resource(*r);
                            HeldView {
                                resource: res.name.clone(),
                                node: model.node(res.node).name.clone(),
                                kind: model.kind(res.kind).name.clone(),
                                amount: n.amount,
                                given_back: paused.as_ref().is_some_and(|p| !p.held[i]),
                            }
                        })
                        .collect(),
                    paused: paused.as_ref().map(|p| p.since),
                    give_way: l.give_way,
                    step: l.usage.restarts + 1,
                    over: l
                        .usage
                        .over(&model.job(j).budgets, l.granted, self.auth.now()),
                });
            }
            JobStatus::Done { at, outcome } => {
                v.state = JobState::Done;
                v.outcome = Some(*outcome);
                v.done_at = Some(*at);
            }
        }
        v
    }

    /// Makes node `n`'s standing instruction and keeps it; it gets a new serial when it differs from the last
    /// one handed out. A serial is the service's clock in milliseconds when it changed (one more than the last
    /// when that is not higher), so it grows across restarts without being stored.
    fn issue(&mut self, n: NodeId) -> Standing {
        let mut s = self.make_standing(n);
        let now = self.now();
        match self.issued.get(&s.node) {
            Some(old) if old.same_rules(&s) => old.clone(),
            old => {
                let next = old.map_or(0, |o| o.serial.saturating_add(1));
                s.serial = next.max(u64::try_from(now.ms()).unwrap_or(0));
                s.issued = now;
                self.issued.insert(s.node.clone(), s.clone());
                s
            }
        }
    }

    /// Node `n`'s standing instruction from the `[[standing]]` entries that select it (serial and time left
    /// for [`Service::issue`]).
    fn make_standing(&self, n: NodeId) -> Standing {
        let node = self.auth.live().model().node(n);
        let selected: Vec<(usize, &StandingConfig)> = self
            .config
            .standing
            .iter()
            .enumerate()
            .filter(|(_, e)| e.selects(&node.name, &node.labels, &node.policy))
            .collect();
        // The most specific entry for a kind: node selector first, then kind over "*", then file order.
        let best = |kind: &str| {
            selected
                .iter()
                .filter(|(_, e)| e.kind == kind || e.kind == "*")
                .max_by_key(|(i, e)| (e.specificity(), e.kind != "*", std::cmp::Reverse(*i)))
                .copied()
        };
        let default_valid = self.config.settings.standing_valid.as_ms().max(0) as u64;
        let other_entry = best("*");
        let other = match other_entry {
            Some((_, e)) => self.rule(e, n),
            None => standing::Rule {
                options: Vec::new(),
                valid_ms: None,
                why: format!("no standing instruction for this kind on {}", node.name),
            },
        };
        let mut kinds = BTreeMap::new();
        for (_, e) in &selected {
            if e.kind == "*" || kinds.contains_key(&e.kind) {
                continue;
            }
            let (i, chosen) = best(&e.kind).expect("the entry itself matches");
            if other_entry.is_some_and(|(o, _)| o == i) {
                continue;
            }
            kinds.insert(e.kind.clone(), self.rule(chosen, n));
        }
        let valid = kinds
            .values()
            .chain([&other])
            .filter_map(|r: &standing::Rule| r.valid_ms)
            .max()
            .unwrap_or(default_valid);
        Standing {
            node: node.name.clone(),
            serial: 0,
            api: API_VERSION.into(),
            issued: Time::ZERO,
            valid_ms: Some(valid),
            kinds,
            other,
        }
    }

    /// One `[[standing]]` entry as a rule for node `n`, its `go` lists filled from the live view.
    fn rule(&self, e: &StandingConfig, n: NodeId) -> standing::Rule {
        let valid = e.valid.unwrap_or(self.config.settings.standing_valid);
        standing::Rule {
            options: e
                .options
                .iter()
                .map(|o| match o {
                    OptionConfig::Local(l) => standing::Choice::Local(standing::Local {
                        max: l.max.unwrap_or(u32::MAX),
                        pool: l.pool.clone(),
                        share: l.share.clone(),
                        pressure: l.pressure,
                        max_run: l.max_run,
                        exclusive: l.exclusive.clone(),
                    }),
                    OptionConfig::Go(g) => standing::Choice::Go(standing::Go {
                        nodes: self.go_nodes(g, n),
                    }),
                })
                .collect(),
            valid_ms: Some(valid.as_ms().max(0) as u64),
            why: e.why.clone(),
        }
    }

    /// Where a `go` option sends jobs from node `n`: its named nodes, then up to `best` picked ones (up, with
    /// its labels, any policy label tolerated, most free `rank_by` first, then by name).
    fn go_nodes(&self, g: &GoConfig, n: NodeId) -> Vec<String> {
        let live = self.auth.live();
        let (model, st) = (live.model(), live.state());
        let me = &model.node(n).name;
        let mut out: Vec<String> = g.nodes.iter().filter(|x| *x != me).cloned().collect();
        if g.best == 0 {
            return out;
        }
        let free = |id: NodeId| -> u64 {
            let Some(kind) = &g.rank_by else { return 0 };
            model
                .resource_by_name(&format!("{}/{kind}", model.node(id).name))
                .map_or(0, |r| st.capacity(model, r).saturating_sub(st.used(r)))
        };
        let mut picks: Vec<(u64, &str)> = (0..model.nodes().len())
            .map(|i| NodeId(i as u32))
            .filter(|&id| id != n && st.node_up(id))
            .filter_map(|id| {
                let x = model.node(id);
                let fits = x.name != self.config.settings.shared_node
                    && !out.contains(&x.name)
                    && g.node_labels
                        .iter()
                        .all(|(k, v)| x.labels.get(k) == Some(v) || x.policy.get(k) == Some(v))
                    && x.policy.iter().all(|(k, v)| {
                        g.node_labels.get(k) == Some(v) || g.tolerate.get(k) == Some(v)
                    });
                fits.then(|| (free(id), x.name.as_str()))
            })
            .collect();
        picks.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(b.1)));
        out.extend(
            picks
                .into_iter()
                .take(g.best as usize)
                .map(|(_, x)| x.to_string()),
        );
        out
    }

    fn node_view(&self, n: NodeId) -> NodeView {
        let live = self.auth.live();
        let (model, st) = (live.model(), live.state());
        let node = model.node(n);
        NodeView {
            name: node.name.clone(),
            up: st.node_up(n),
            labels: node.labels.clone(),
            policy: node.policy.clone(),
            reported: self.reported.get(&node.name).copied(),
            local: self.local.get(&node.name).cloned().unwrap_or_default(),
            standing: self.issued.get(&node.name).cloned(),
            resources: model
                .resource_ids()
                .filter(|r| model.resource(*r).node == n)
                .map(|r| {
                    let res = model.resource(r);
                    ResourceView {
                        name: res.name.clone(),
                        kind: model.kind(res.kind).name.clone(),
                        capacity: st.capacity(model, r),
                        used: st.used(r),
                        local: self.local.get(&node.name).map_or(0, |js| {
                            let k = &model.kind(res.kind).name;
                            js.iter()
                                .filter_map(|j| j.needs.get(k))
                                .fold(0, |a, b| a.saturating_add(*b))
                        }),
                        holders: st
                            .holders(r)
                            .map(|(j, _)| self.jobs[j.index()].id)
                            .collect(),
                    }
                })
                .collect(),
        }
    }

    /// A wait reason with names and service ids, and a line for people.
    fn wait_view(&self, w: &WaitReason, j: JobId) -> WaitView {
        let model = self.auth.live().model();
        let now = self.now();
        let job_ref = |x: JobId| self.jobs[x.index()].id;
        let job_name =
            |x: JobId| format!("job {} ({})", job_ref(x), shown_name(&model.job(x).name));
        let need_kind = |need: usize| {
            // A resume waits on the shape it runs in, a queued job on its smallest.
            let job = model.job(j);
            let needs = match self.auth.live().state().status(j) {
                JobStatus::Running { assignment, .. } => job.needs_for(assignment),
                _ => job.shape(job.smallest()).unwrap_or_default(),
            };
            let k = needs[need].kind;
            model.kind(k).name.clone()
        };
        let mut v = WaitView {
            why: serde_json::to_value(w)
                .ok()
                .and_then(|x| x.get("why").and_then(|s| s.as_str()).map(String::from))
                .unwrap_or_default(),
            text: String::new(),
            job: None,
            resource: None,
            kind: None,
            at: None,
        };
        v.text = match w {
            WaitReason::Release { at } => {
                v.at = Some(*at);
                format!("not before its release time, in {}", *at - now)
            }
            WaitReason::Dep { job } => {
                v.job = Some(job_ref(*job));
                format!("after {}", job_name(*job))
            }
            WaitReason::DepFailed { job } => {
                v.job = Some(job_ref(*job));
                format!("{} it comes after did not end well", job_name(*job))
            }
            WaitReason::Group { member } => {
                v.job = Some(job_ref(*member));
                format!("{} of its group cannot start yet", job_name(*member))
            }
            WaitReason::Resource {
                resource,
                held_by,
                free_at,
                ..
            } => {
                let res = model.resource(*resource);
                v.resource = Some(res.name.clone());
                v.kind = Some(model.kind(res.kind).name.clone());
                v.job = held_by.first().map(|x| job_ref(*x));
                v.at = *free_at;
                let by: Vec<String> = held_by.iter().map(|x| job_name(*x)).collect();
                let mut t = format!("{} is held by {}", res.name, by.join(", "));
                if let Some(f) = free_at {
                    t.push_str(&format!(", free in about {}", (*f - now).max(Dur::ZERO)));
                }
                t
            }
            WaitReason::Capacity {
                kind,
                amount,
                most_free,
                ..
            } => {
                let k = model.kind(*kind);
                v.kind = Some(k.name.clone());
                format!(
                    "needs {} of {}, at most {} free on one resource",
                    amount_text(*amount, k.unit),
                    k.name,
                    amount_text(*most_free, k.unit)
                )
            }
            WaitReason::NodesDown { need } => {
                v.kind = Some(need_kind(*need));
                format!(
                    "every node that could take its {} is down",
                    need_kind(*need)
                )
            }
            WaitReason::Quota {
                resource,
                window,
                used,
                limit,
                reopens,
                ..
            } => {
                let res = model.resource(*resource);
                v.resource = Some(res.name.clone());
                v.kind = Some(model.kind(res.kind).name.clone());
                v.at = *reopens;
                let share = if *limit == 100 {
                    format!("{used}%")
                } else {
                    format!("{used} of {limit}")
                };
                let mut t = format!(
                    "waits for a {}: {} has used {share} of its {window} quota",
                    model.kind(res.kind).name,
                    res.name
                );
                match reopens {
                    Some(r) => t.push_str(&format!(", refills in {}", (*r - now).max(Dur::ZERO))),
                    None => t.push_str(", no refill known"),
                }
                t
            }
            WaitReason::SameNode { tag } => {
                format!("no node has room now for all its needs tagged {tag:?}")
            }
            WaitReason::NoMatch { need } => {
                v.kind = Some(need_kind(*need));
                format!("no resource can ever take its {} need", need_kind(*need))
            }
            WaitReason::ReservedFor { job } => {
                v.job = Some(job_ref(*job));
                format!("it would delay {}, which has a reservation", job_name(*job))
            }
            WaitReason::Interlock { rule } => format!("an interlock refuses it: {rule}"),
            WaitReason::Approval => "waits for an approval".into(),
        };
        v
    }
}

/// An amount in its unit, for people: `8 GiB`, `2.5 cores`, `3`.
pub fn amount_text(a: u64, unit: Unit) -> String {
    match unit {
        Unit::Count => a.to_string(),
        Unit::Millicores => {
            if a.is_multiple_of(1000) {
                format!("{} cores", a / 1000)
            } else {
                format!("{:.1} cores", a as f64 / 1000.0)
            }
        }
        Unit::Bytes => {
            const U: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
            let mut v = a as f64;
            let mut i = 0;
            while v >= 1024.0 && i < U.len() - 1 {
                v /= 1024.0;
                i += 1;
            }
            if i == 0 {
                format!("{a} B")
            } else {
                format!("{v:.1} {}", U[i])
            }
        }
    }
}

/// An authority with nothing in it yet: no nodes, no kinds (they are added from the configuration).
fn empty(config: &Config, now: Time) -> Authority {
    let model = ModelBuilder::new()
        .build()
        .expect("an empty model is valid");
    Authority::new(Live::new(model, now), config.lease.clone())
}

fn snapshot_line(auth: &Authority, jobs: &[JobMeta], next_ref: JobRef) -> Line {
    Line::Snapshot {
        version: STATE_VERSION,
        next_ref,
        jobs: jobs.to_vec(),
        entry: auth.snapshot(),
    }
}

type Rebuilt = (Authority, Vec<JobMeta>, JobRef);

/// Rebuilds the authority and the job table from journal lines; `None` for an empty journal.
fn rebuild(lines: Vec<Line>) -> Result<Option<Rebuilt>, StartError> {
    if lines.is_empty() {
        return Ok(None);
    }
    let mut entries = Vec::new();
    let mut jobs: Vec<JobMeta> = Vec::new();
    let mut next_ref = 1;
    for (i, l) in lines.into_iter().enumerate() {
        match l {
            Line::Snapshot {
                version,
                next_ref: n,
                jobs: j,
                entry,
            } => {
                if version > STATE_VERSION {
                    return Err(StartError::Newer(version));
                }
                entries.clear();
                entries.push(entry);
                jobs = j;
                next_ref = n;
            }
            _ if entries.is_empty() => {
                return Err(StartError::Journal(format!(
                    "line {}: the journal does not start with a snapshot",
                    i + 1
                )));
            }
            Line::Entry(e) => {
                if let Op::Snapshot { .. } = e.op {
                    return Err(StartError::Journal(format!(
                        "line {}: a bare snapshot entry",
                        i + 1
                    )));
                }
                entries.push(e);
            }
            Line::Jobs(m) => {
                for x in m {
                    if jobs.last().is_some_and(|l| l.id >= x.id) {
                        return Err(StartError::Journal(format!(
                            "line {}: job ids out of order",
                            i + 1
                        )));
                    }
                    next_ref = next_ref.max(x.id + 1);
                    jobs.push(x);
                }
            }
            Line::Compacted { removed } => jobs.retain(|m| !removed.contains(&m.id)),
        }
    }
    let auth = Authority::replay(entries).map_err(|e| match e {
        choreograph::lease::ReplayError::Version { version, .. } => StartError::Newer(version),
        e => StartError::Journal(e.to_string()),
    })?;
    if jobs.len() != auth.live().model().jobs().len() {
        return Err(StartError::Journal(format!(
            "{} jobs in the service's table, {} in the authority",
            jobs.len(),
            auth.live().model().jobs().len()
        )));
    }
    Ok(Some((auth, jobs, next_ref)))
}

fn check_units(config: &Config, auth: &Authority) -> Result<(), StartError> {
    let model = auth.live().model();
    for k in &config.kinds {
        if let Some(id) = model.kind_by_name(&k.name) {
            let have = model.kind(id).unit;
            if have != k.unit {
                return Err(StartError::Unit {
                    kind: k.name.clone(),
                    config: k.unit,
                    journal: have,
                });
            }
        }
    }
    Ok(())
}

/// The name of a job or group in the model: unique, since clients reuse names ("build") and the model does
/// not take two jobs of one name. It starts with the job's id: `7 build`.
fn model_name(id: JobRef, name: &str) -> String {
    format!("{id} {name}")
}

/// The client's name of a job or group from its [`model_name`].
fn shown_name(name: &str) -> &str {
    name.split_once(' ').map_or(name, |(_, n)| n)
}
