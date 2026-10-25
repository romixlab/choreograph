# Changelog

All notable changes to choreograph. The format follows [Keep a Changelog](https://keepachangelog.com/), versions
follow [Semantic Versioning](https://semver.org/). Feature IDs refer to [FEATURES.md](FEATURES.md).

## [Unreleased]

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
