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
- 📋 **EST-1 duration-estimates**: learn durations from history: EWMA mean, streaming quantiles (p50, p90, p99),
  priors per kind for a cold start.
- 📋 **EXEC-1 executor-state-machine**: leases with fencing tokens, saga cleanup from every state, an append-only
  journal and recovery to safe states, a watchdog per phase, interlocks the planner cannot bypass.
- 📋 **EXEC-2 realtime-driver**: drive the executor state machine in real time (threads or an async runtime).
- 📋 **CACHE-1 result-cache**: results keyed by a canonical hash of every input, validity windows, verdicts
  (flaky and infrastructure errors are never valid results), untested vs stale.

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
- 📋 **SIM-2 trace-metrics**: utilisation, queue length, waiting and sojourn times per class, lateness and
  tardiness, makespan, Little's law self-check, and the matching theory predictions.

## TEST: property tests

- 📋 **TEST-1 invariants**: property tests over random job sets: no resource over capacity, cleanup always runs,
  progress while work remains, bounded waiting with aging, Little's law, determinism.

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
