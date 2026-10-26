# Changelog

All notable changes to choreograph. The format follows [Keep a Changelog](https://keepachangelog.com/), versions
follow [Semantic Versioning](https://semver.org/). Feature IDs refer to [FEATURES.md](FEATURES.md).

## [Unreleased]

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
