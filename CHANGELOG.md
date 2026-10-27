# Changelog

All notable changes to choreograph. The format follows [Keep a Changelog](https://keepachangelog.com/), versions
follow [Semantic Versioning](https://semver.org/). Feature IDs refer to [FEATURES.md](FEATURES.md).

## [Unreleased]

### Added

- Standing instructions (`standing` module: `Standing`, `Rule`, `Choice`, `Local`, `Go`, `LocalView`,
  `LocalJob`, `Verdict`): what a node may do while it can't reach the scheduler, handed out ahead of time, and
  the pure `standing::decide` that applies them (run here within a count and a share of the room, go to another
  node, wait, or ran out); `Standing::lone` for a node with no scheduler (SVC-3).

## [0.4.0] - 2026-10-27

### Added

- Open-ended jobs (`Job::open`) and job kinds (`Job::kind`): planned with their estimate while young and from
  their age once older (`theory::lognormal_conditional_quantile`, `theory::normal_cdf`,
  `theory::normal_quantile`), never sooner than `Policy::open_floor` from now (CORE-6).
- Pause and resume (`State::pause`, `State::resume`, `Live`, `Authority::pause`, `Authority::resume`): a paused
  job gives back what its `pause_keeps` does not name and claims no future time; a resume waits for the same
  resources and goes on under the same lease; trace events `paused` and `resumed` (CORE-6).
- `Preempt::Ask`: the planner asks a running job to give way for one at least `Policy::preempt_gap` classes
  higher (`Plan::give_way`, `Authority::ask_give_way`, `Lease::give_way`, `Beat::give_way`) (CORE-6).
- Durations learned per job kind from releases (`Authority::learned`, `Learner::observe_kind`,
  `Learner::estimate_kind`; `Learner` is now serializable) (CORE-6).
- Job-set keys `kind`, `open`, `pause_keeps`; the example `examples/hil_open.toml` (CORE-6).
- choreographd: `[[job_kind]]` in the configuration (`open`, `preempt`, `pause_keeps`, a first `run` guess);
  `SubmitJob` keys `kind`, `open`, `preempt`, `pause_keeps`, a missing `run` taken from what the kind has
  learned; `POST /v1/leases/{token}/pause` and `/resume` (`Client::pause`, `Client::resume`); `JobState::Paused`,
  `JobView::kind` and `open`, `LeaseView::paused` and `give_way`, `HeldView::given_back`, `BeatView::give_way`
  (CORE-6).
- `choreograph top` shows a job's kind, paused jobs with what they gave back, a resume's wait, and leases asked to
  give way (CORE-6).
- Budgets on a lease (`budget` module: `Budget`, `Meter`, `Scope`, `OnHard`, `Usage`, `Crossing`, `Level`;
  `Job::budgets`, the job-set key `budgets`): `Authority::report_use` and `Beat::over` tell the holder which
  levels it crossed, a `stop` budget's hard level cancels the job (SESS-1).
- Restart in place (`Authority::restart`): the next step of a job under the same lease, token and resources
  (SESS-1).
- Tells between holders (`Authority::tell`, `Tell`, `Beat::wakes`), delivered once with the next heartbeat,
  with a guard against loops between two holders (`LeaseSettings::pair_bounces`, `pair_window`,
  `LeaseError::PairLoop`) (SESS-1).
- choreographd: budgets per `[[job_kind]]` and per job (`SubmitJob::budgets`); `POST /v1/leases/{token}/usage`,
  `POST /v1/leases/{token}/restart`, `POST /v1/jobs/{id}/tell` (`Client::report_use`, `restart`, `tell`); the
  `pair_loop` refusal (429); `LeaseView::step` and `over`, `BeatView::over` and `wakes`; `[lease]`
  `pair_bounces` and `pair_window`. `choreograph top` shows the step and the budget levels crossed (SESS-1).

### Changed

- **Breaking:** `Beat` and `choreographd::api::BeatView` are no longer `Copy` (they carry `over` and `wakes`);
  `Lease` has `usage` and `mail`, `LeaseSettings` `pair_bounces` and `pair_window`, `ModelError` `BadBudget`
  (SESS-1).
- **Breaking:** `JobStatus::Running` has a `paused` field, `Beat` a `give_way` field, `Plan` a `give_way` map,
  `Policy` the settings `open_floor` and `preempt_gap` (CORE-6).
- A wait (`POST /v1/jobs/{id}/wait`, `Client::wait_running`) now answers when the job no longer waits, which
  includes a paused job (CORE-6).

## [0.3.4] - 2026-10-26

### Added

- `just dist OUT`: a static musl `choreographd` bundle for `tpm dist build` on a build node, so the PC that runs
  the service (core-1, which builds nothing) gets it with `tpm dist pull choreograph --from BUILD --rev SHA
  --install` and root's `tpm provision` upgrade step (tpm_cli PROV-72).

## [0.3.3] - 2026-10-26

### Added

- Regime-switching arrivals in the simulator: `Arrivals::Mmpp` (a Markov-modulated Poisson process over
  `Regime`s, each a rate and a mean stay) and `Arrivals::mmpp2` for busy and quiet stretches (SIM-4).
- `theory::mmpp2_rate` and `theory::mmpp2_idc` (the long-run rate and index of dispersion of a two-state MMPP)
  and `theory::batch_runs_per_entry` (gate runs per entry of batched gates with bisection) (THEORY-2).
- `theory::pick_slots`, `theory::batch_latency` and `theory::pick_batch` with `PickSettings`: the fewest gate
  slots under a utilisation cap and a wait target, and the batch size with the shortest time to result
  (THEORY-2).
- `recording`: `Recording` (model, trace and tags of real history, JSON), `Profile::measure` of a group of
  them (rates, burstiness, a two-regime fit, service, failures, waits, per lane), `Profile::predicted_wq`,
  `Profile::simulate` and `recording::pick`, the slots and batch size for the worst group (SIM-5).
- `choreograph theory FILES... --by TAG`: recordings per group, measured against Kingman and the simulator,
  and the pick for the worst group (`--gate-time DUR`: sized for gate runs of that mean length);
  `choreograph completions SHELL` (CLI-1).

## [0.3.2] - 2026-10-26

### Added

- `choreograph top`: a live view of a running `choreographd` (nodes, running jobs with their leases and
  ETAs, the queue in priority order with its wait reason and ETA, done jobs); `--once` for a single
  snapshot, `--json` for agents and the bot, `--color` (CLI-2).

## [0.3.1] - 2026-10-26

### Added

- `Live::add_kind` and `Authority::add_kind`: a resource kind added at runtime; `Authority::set_settings`
  replaces the lease settings (a new TTL after a restart). Both are journaled (`Op::AddKind`,
  `Op::SetSettings`) (CORE-5, EXEC-1).
- `choreographd`, the scheduler as a service: a JSON API to submit jobs, wait for them with a live wait
  reason and ETA, keep and release leases, report nodes and see the queue; a TOML config, a journal that
  survives restarts, a typed client (`choreographd::client`) and a systemd unit (SVC-1). It listens on a Unix
  socket by default, without tokens (callers name themselves; the socket's mode decides who may call), so a
  mesh proxy can carry its calls between machines: every call is one request and one answer, and a wait is a
  bounded long poll (`timeout=0`: a short poll). TLS and a token per client are optional, for a TCP listener;
  beyond loopback both are required. A job whose lease expired starts again only once its client asked for
  it; if it would start before, it is cancelled.

## [0.3.0] - 2026-10-26

### Added

- Metrics from a trace (`metrics`): utilisation per resource, queue length over time, waiting and sojourn
  times overall and per class, lateness and tardiness, makespan, throughput, setup share, cache hit rate,
  Little's law checked exactly; a warm-up cut, confidence intervals over replications and batch means;
  `Metrics::compare` puts each value next to the queueing formulas (SIM-2).
- `Live::record_events` and `Live::drain_events`: a live model's changes as trace events; `Live::measure` and
  `Live::metrics`: its metrics as it runs, kept across compactions (SIM-2).

### Changed

- **Breaking:** trace format version 2 with the events `requeued`, `capacity`, `node` and `compacted` (SIM-2).

## [0.2.1] - 2026-10-26

### Added

- Leases (`lease.rs`): `Authority` grants every start as a `Lease` with a fencing `Token`, a TTL and
  heartbeats (`Beat::stop` tells the holder of a cancel); `release` ends it with an outcome, `expire` gives an
  expired lease's resources back and requeues or fails its job (`LeaseSettings`, loadable from TOML); stale or
  expired tokens are refused; `check_holds` and `fence` guard work outside the authority (EXEC-1).
- The lease journal: every accepted change is an `Entry` (inputs and time, starting with a snapshot), taken
  with `take_journal`; `Authority::replay` rebuilds the same authority after a restart and refuses a newer
  version or a journal whose tokens diverge (EXEC-1).
- `State::finish` and `State::abandon` (with `Live` wrappers): a running job ends at once with an outcome, or
  is given up without its cleanup and requeued or failed (EXEC-1).

## [0.2.0] - 2026-10-26

### Added

- `Live`: a model and its state that change at runtime: `submit` and `submit_group` add jobs checked like a
  job set, `add_node` and `add_resource` grow the fleet, `set_node_labels` and `set_node_policy` change a
  node, `compact` removes finished jobs and renumbers the rest, returning a `Remap` (CORE-5).
- Measured capacities and node state in `State`: `set_capacity` (may drop below what is held: nothing new
  starts, running jobs keep their resources), `set_node_up` (a drain), `node_lost` with `LostJobs::Requeue`
  or `Fail`; `capacity`, `headroom`, `measured` and `node_up` to read them (CORE-5).
- Node policy labels (`Node::policy`, `policy = { builds = "false" }` in a job set): a need lands on such a
  node only when its selector names each policy label with the same value, in `node_labels` (asks for such a
  node) or in the new `Selector::tolerate` (accepts one) (CORE-5).

### Changed

- **Breaking:** `WaitReason::NodesDown` for a need whose every candidate is on a node that is down, and the
  new `Node::policy` and `Selector::tolerate` fields (CORE-5).
- The list scheduler plans from each resource's headroom, so a capacity measured below what is held delays
  reservations until enough is released, not just until the first job ends (CORE-5).

## [0.1.4] - 2026-10-25

### Added

- The result cache: results keyed by a hash of the job name and its inputs, validity windows and a max age,
  pass and fail verdicts reused, flaky runs and infrastructure errors never; lookups tell untested, stale (with
  what changed) and valid apart; `ResultCache::settle` marks queued jobs with a valid result as done without
  running them (CACHE-1).
- `Outcome::CachedFail` and `State::mark_cached_fail`: a job settled by a valid cached failure (CACHE-1).
- `SimConfig::cache`: the simulator settles jobs with a valid cached result without running them and records
  every run's verdict; `SimRun::cache` returns the cache, `SimStats::cached` counts the settled jobs (CACHE-1).

## [0.1.3] - 2026-10-25

### Added

- Duration estimates learned from history: an EWMA mean and streaming p50/p90/p99 per (job kind, phase,
  resource, node), blended with a prior per job kind and phase for a cold start, read through the new
  `Estimates` trait (`EST-1`).

## [0.1.2] - 2026-10-25

### Added

- Discrete-event simulator `sim::simulate` driving any planner on simulated time, with phase duration models
  (fixed, exponential, lognormal from p50/p90, gamma, hyperexponential, empirical), run failures and flaky
  retries, open workloads from job templates with Poisson, renewal (chosen c_a²) or replayed arrivals, and seeded
  per-purpose random streams so policies compare on the same sampled world; writes a trace (SIM-1).

### Changed

- `State` keeps an index of queued and running jobs: `queued()` and `running()` no longer scan every job, so
  long simulations stay fast (SIM-1).

## [0.1.1] - 2026-10-25

### Added

- `ListScheduler`, the main planner: priorities with aging and deadline urgency, shortest-first ties, placement
  by locality and dominant-resource fit, all-or-nothing starts for jobs and groups, EASY and conservative
  backfilling with reservations from p90 estimates, wait reasons with estimated free times (PLAN-1).
- `ListScheduler::forecast`: a forward plan with p50 and p90 ETAs for every queued and running job (PLAN-1).
- `Policy`, the planner's settings with documented defaults, loadable from TOML (PLAN-1).

## [0.1.0] - 2026-10-25

### Added

- Cargo workspace with the `choreograph` library and the `choreograph_cli` binary; `just test`, `just lint`,
  `just install`, `just deploy` (CORE-1).
- The model: nodes, resource kinds, resources with capacity, labels and optional states, jobs with needs,
  phases, estimates, dependencies, groups, priority class, release time and due date; validation (CORE-1).
- Runtime state with all-or-nothing starts and capacity checks, the `Planner` trait with typed wait reasons and
  the `FirstFit` baseline planner, the event trace format, the TOML job-set format (CORE-2).
- Queueing formulas for comparing measurements with theory (THEORY-1).
- `choreograph check FILE` and `choreograph --version` with the git SHA and commit time (CLI-1).
