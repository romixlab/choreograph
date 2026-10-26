# Features

The current state of choreograph: what works, what is planned, known bugs. Stable IDs per area; never renumbered
or reused. Status: ✅ done, 🚧 partial, 📋 planned, 🐛 bug, ❌ dropped.

## CORE: model, state, interfaces

- ✅ **CORE-1 workspace-and-model**: Cargo workspace (`choreograph` library, `choreograph_cli`), integer time
  (`time.rs`), the model of nodes, resource kinds, resources with capacity, labels and optional states (setup
  transitions, a safe state), jobs (needs with selectors and `same_node` tags, phases with p50/p90 estimates,
  deps, groups, class, weight, arrival, release, due date, cache inputs) and its validation (`model.rs`).
- ✅ **CORE-2 state-plan-trace**: runtime bookkeeping that refuses over-capacity or out-of-order starts and applies
  a batch all or nothing (`state.rs`); the `Planner` trait, `Plan`, typed `WaitReason`s, the exact assignment
  search and the `FirstFit` baseline planner (`plan.rs`); the event trace format (`trace.rs`); the TOML job-set
  format (`jobset.rs`).
- ✅ **EST-1 duration-estimates**: learns an EWMA mean and streaming p50/p90/p99 per (job kind, phase, resource,
  node), blended with a prior per job kind and phase for a cold start, behind the `Estimates` trait
  (`estimates.rs`). Settings for the EWMA weight, the quantile window and the prior weight. `PLAN-1`'s
  `ListScheduler` reserves and backfills from each job's own static `Phases` estimates today, not yet from a
  `Learner`; wiring it in is follow-up work for whichever piece needs learned estimates in the planner.
- ✅ **CORE-5 live-model**: a model that changes while it runs (`live.rs`): jobs and groups submitted at
  runtime (checked like a job set, all or nothing), nodes and resources added, node labels and policy labels
  (`builds = "false"`: only needs that ask for or tolerate the label land there) changed; capacities set from measurements,
  a drop below what is held stopping new starts while running jobs keep their place, nodes up, down (drained)
  or lost (their jobs requeued or failed), with `NodesDown` as a wait reason; finished jobs compacted with
  dense ids renumbered in order. `State` and both planners read the measured capacity and node state, and the
  list scheduler's profile counts a shortfall until it clears.
- 🚧 **EXEC-1 executor-state-machine**: done: leases (`lease.rs`): `Authority` owns a `Live` and grants every
  start as a lease with a fencing token (never reused, rising), a TTL and heartbeats (which also tell the holder
  of a cancel); a release with the outcome ends it, an expiry gives the job's resources back and requeues or
  fails it (`LeaseSettings`), and a stale or expired token is refused everywhere; `check_holds` and `fence` for
  work outside that must happen once (a push to `main`); every accepted change is journaled (append-only
  `Entry`s, starting with a snapshot) and `Authority::replay` rebuilds the same state after a restart, checking
  the recorded tokens. Planned: saga cleanup from every state and recovery of resources to safe states, a
  watchdog per phase, interlocks the planner cannot bypass.
- 📋 **EXEC-2 realtime-driver**: drive the executor state machine in real time (threads or an async runtime).
- ✅ **CACHE-1 result-cache**: results keyed by a SHA-256 hash of a canonical encoding of the job name and its
  inputs, validity windows (expiries such as a calibration, a max age), verdicts (pass and fail are results;
  flaky runs invalidate, infrastructure errors are ignored), lookups that tell untested, stale (with the changed
  inputs or the expiry named) and valid apart, and settling queued jobs with a valid result as `Cached` or
  `CachedFail` without running them (`cache.rs`); the simulator settles cached jobs and records every run
  (`SimConfig::cache`, `sim.rs`).

## THEORY: formulas

- ✅ **THEORY-1 queueing-formulas**: utilisation, Little's law, M/M/1, Pollaczek–Khinchine, Kingman, Erlang B and
  C, M/M/c, Allen–Cunneen, Cobham's priority formula, lognormal fit from p50/p90, speculation yield, aging
  overtake time (`theory.rs`).

## PLAN: planners

- ✅ **PLAN-1 list-scheduler**: `ListScheduler` (`list.rs`): effective priority = max(class + aging, deadline
  urgency from the p90 slack over a horizon), ties by shorter p50, arrival, id; placement over the assignments
  that fit (per node and without preference) by locality and dominant-resource best fit or least share;
  all-or-nothing starts including groups, no head-of-line blocking; EASY (default) and conservative backfilling
  on a free-capacity profile with reservations from p90 estimates; wait reasons with the estimated free time of
  a held resource; `forecast`, a forward plan with p50 and p90 ETAs per queued and running job; `Policy`
  (`policy.rs`) with documented defaults, loadable from TOML.
- 📋 **PLAN-2 setup-aware**: stateful resources in planning: setup transition times in durations and scoring,
  grouping jobs that share a setup state.
- 📋 **PLAN-3 merge-queue-policies**: speculative checks with a predicted failure probability per entry, batching
  with bisection.

## SOLVE: exact solving

- 📋 **SOLVE-1 cp-solver**: an exact constraint-programming model for small instances, to report the heuristic's
  optimality gap.

## SIM: simulation and metrics

- ✅ **SIM-1 discrete-event-sim**: discrete-event simulation on the library's state and any planner (`sim.rs`):
  an event queue on simulated time with a stated order within an instant, duration models (fixed, exponential,
  lognormal from p50/p90, gamma, hyperexponential, empirical; `sim/dist.rs`), run failures and flakes from the
  job's sim hints with in-place retries, cancelling jobs behind a failed dependency, open workloads from job
  templates with arrival processes (job list, Poisson, renewal with a chosen c_a², trace replay;
  `sim/workload.rs`), seeded ChaCha8 streams per purpose and job (common random numbers across policies), trace
  output. Long runs match M/M/1, M/M/c (Erlang C), M/D/1 and M/G/1 (Pollaczek–Khinchine) within stated
  tolerances (`tests/sim_theory.rs`).
- ✅ **SIM-2 trace-metrics**: metrics from a trace (`metrics.rs`): a streaming `Collector` that reads job details
  from the model as events come in (so it follows a live model through compactions), utilisation per resource
  against the measured capacity (0 while a node is down), L_q(t) as a step series with its mean and maximum, L,
  waiting and sojourn time overall and per class (mean, c², p50, p90, max), lateness, tardiness, late jobs,
  weighted tardiness, makespan, throughput, setup share, cache hit rate; Little's law checked in exact integers
  (`Little`), exact on runs that start and end empty; a warm-up cut, Student-t confidence intervals over
  replications and batch means; `Metrics::compare`, each measured value next to M/M/1, M/M/c, P-K, Kingman,
  Allen–Cunneen, Little and Cobham fed with the measured inputs. `Live::measure` measures a live model as it
  runs, `Live::record_events` records its trace (format version 2: requeues, capacities, node up and down,
  compactions).

## TEST: property tests

- ✅ **TEST-1 invariants**: property tests (`choreograph/tests/invariants.rs`, proptest with a fixed seed) over
  random fleets, job sets and seeds with `FirstFit` and `ListScheduler` (each backfill mode), in the batch
  simulator and in a `Live` model driven by a random script (submits, measured capacities, nodes down, up and
  lost, compactions), checked by an independent replay of the trace: no start over capacity or on a node that
  is down, every started job reaches cleanup and releases (only a lost node's jobs skip it), progress while work
  remains, Little's law exactly, determinism; on one exclusive slot the wait is bounded by the aging formula. A
  deliberately broken planner shows a failure shrinking to one job.

## CLI: command line

- 🚧 **CLI-1 cli**: `choreograph check` and `--version` with SHA and commit time work; planned: `plan`,
  `simulate`, `show`, `theory`, `sweep`, shell completion with live values, colour, progress bars.

## GUI: graphical interface

- 📋 **GUI-1 gui-timeline**: an egui app: load a job set or trace, a timeline per resource grouped by node.
- 📋 **GUI-2 gui-queue-playback**: the queue with wait reasons and ETA bands, deadline markers, playback of a
  trace.
- 📋 **GUI-3 gui-metrics-theory**: live metrics next to the theory's prediction, scenario presets.

## HIL: test benches

- 📋 **HIL-1 bench-plugs**: a `Plug` trait for instruments with safe states, a bench configuration, a first real
  bench run with recorded durations.
- 📋 **HIL-2 bench-leases**: instrument leases for interactive users and agents on top of the executor.

## MESH: several machines

- 📋 **MESH-1 distributed-leases**: one lease authority per resource, ordered acquisition for jobs over several
  nodes, a model-checked lease protocol.
- 📋 **MESH-2 merge-queue-adapter**: a merge queue running on choreograph.

## PY: Python

- 📋 **PY-1 python-wrapper**: Python bindings (PyO3, maturin).

## DOCS: documentation

- 📋 **DOCS-1 examples-and-theory**: example job sets and a docs page on the theory behind the planner.

## Dropped and superseded
