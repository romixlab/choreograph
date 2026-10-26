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

use crate::api::{
    BeatView, EstimateSpec, EtaView, HeldView, JobRef, JobState, JobView, LeaseView, NeedSpec,
    NodeReport, NodeView, QueueView, ResourceView, SubmitGroup, SubmitJob, WaitView,
};
use crate::config::{ClientConfig, Config};
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
            LeaseError::Settings(_) | LeaseError::Model(_) => Refusal::Invalid(e.to_string()),
            LeaseError::State(_) => Refusal::Conflict(e.to_string()),
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
        let job = self.job_from(req, now, self.next_ref)?;
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
            .map(|(j, r)| self.job_from(j, now, r))
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
        })
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
        for k in r.room.keys() {
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
        for (name, kind) in kinds {
            let room = r.room.get(&name).copied().unwrap_or(0);
            let rname = format!("{node}/{name}");
            match self.auth.live().model().resource_by_name(&rname) {
                None if room == 0 => {}
                None => {
                    self.auth.add_resource(Resource {
                        name: rname,
                        kind,
                        node: n,
                        capacity: room,
                        labels: Labels::new(),
                        states: None,
                    })?;
                }
                Some(rid) => {
                    let st = self.auth.live().state();
                    if st.capacity(self.auth.live().model(), rid) != room
                        || st.measured(rid).is_none()
                    {
                        self.auth.set_capacity(rid, Some(room))?;
                    }
                }
            }
        }
        self.reported.insert(node.to_string(), now.max(self.now()));
        self.schedule(now);
        self.end()?;
        Ok(self.node_view(n))
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
    fn job_from(&mut self, req: SubmitJob, now: Time, id: JobRef) -> Result<Job, Refusal> {
        if req.name.is_empty() {
            return Err(Refusal::Invalid("a job needs a name".into()));
        }
        let mut needs = Vec::with_capacity(req.needs.len());
        for n in req.needs {
            needs.push(self.need_from(n)?);
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
        let mut job = Job::new(model_name(id, &req.name));
        job.needs = needs;
        job.phases = Phases {
            setup: est(req.setup),
            run: est(req.run),
            cleanup: est(req.cleanup),
        };
        job.after = after;
        job.class = req.class;
        job.weight = req.weight.unwrap_or(1);
        job.arrival = now;
        job.release = req.release;
        job.due = req.due;
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
            state: JobState::Queued,
            wait: None,
            eta: None,
            lease: None,
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
        match st.status(j) {
            JobStatus::Pending | JobStatus::Queued => {
                v.wait = Some(
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
                        }),
                );
                v.eta = eta;
            }
            JobStatus::Running { assignment, .. } => {
                v.state = JobState::Running;
                v.eta = eta;
                v.lease = self.auth.lease_of(j).map(|l| LeaseView {
                    token: l.token.0,
                    granted: l.granted,
                    expires: l.expires,
                    held: assignment
                        .0
                        .iter()
                        .zip(&model.job(j).needs)
                        .map(|(r, n)| {
                            let res = model.resource(*r);
                            HeldView {
                                resource: res.name.clone(),
                                node: model.node(res.node).name.clone(),
                                kind: model.kind(res.kind).name.clone(),
                                amount: n.amount,
                            }
                        })
                        .collect(),
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
            let k = model.job(j).needs[need].kind;
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
