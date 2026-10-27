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
- ✅ **CORE-6 open-ended-jobs**: a job may be open-ended (`Job::open`: no time limit, it
  holds its resources until released: an agent session, a person debugging on a bench, a long soak test) and
  carry a kind (`Job::kind`); the list scheduler plans it with its estimate while it is young and, once older,
  with the quantile of a lognormal fitted to its p50 and p90 given its age
  (`theory::lognormal_conditional_quantile`), never sooner than `Policy::open_floor` from now; a running job
  pauses (`State::pause`, `Authority::pause`) and gives back every need whose kind is not in its `pause_keeps`,
  with no claim on future time, and a resume waits for the same resources and goes on under the same lease
  and token (`resume`, before the queue, with a reservation as backfilling allows); a `Preempt::Ask` job is
  asked to give way (`Plan::give_way`, `Authority::ask_give_way`, `Beat::give_way`) for a job at least
  `Policy::preempt_gap` classes higher that fits only without it, and nothing is taken by force; the authority
  learns how long each kind holds its lease from releases that end well (`Authority::learned`, replayed from the
  journal); the job-set keys `kind`, `open`, `pause_keeps` and `preempt = "ask"`; the bench example
  `examples/hil_open.toml`. In `choreographd`: `[[job_kind]]` sets `open`, `preempt`, `pause_keeps` and a first
  `run` guess per kind, a job's own keys win, and a job without `run` takes what releases of its kind taught
  (`service.rs` `job_from`); `POST /v1/leases/{token}/pause` and `/resume`, `JobState::Paused`, a resumed
  job's wait answers once it has its resources back, `give_way` on the lease and the heartbeat; `choreograph
  top` shows paused jobs, what they gave back and who is asked to give way. Tests:
  `choreograph/tests/open_ended.rs`, `choreographd/tests/e2e.rs` (`an_open_session_...`).
- ✅ **CORE-8 start-anyway-and-moldable**: start-anyway: `Job::start_anyway_after`, how long a job pinned to
  a node or resource (every need of its smallest shape names one; outside a group) may wait before it starts
  anyway, counted from when it could first start (`State::ready_since`, `start_anyway_at`,
  `may_start_anyway`); `State` lets such a start go over capacity and marks it (`JobStatus::Running::over`),
  both planners start it on its smallest shape where it is pinned (`plan::anyway_assignment`) and ask to be
  woken at its time, and the list scheduler's profile counts what it holds. Moldable needs: `Job::shapes`,
  smaller alternatives to `needs` from larger to smaller; an `Assignment` names its shape (serialized as before
  for shape 0), `find_assignment` and the list scheduler grant the largest shape that fits now (for its whole
  estimated duration next to the reservations), and reservations, backfilling, pauses and the forecast count
  the shape a job got; a wait reason is that of the smallest shape. Job-set keys `shapes` and
  `start_anyway_after`; the example `examples/hil_moldable.toml`. Tests:
  `choreograph/tests/start_anyway_moldable.rs`. choreographd: `[[job_kind]]` `start_anyway_after` (applied to a
  job of the kind pinned to its node, outside a group) and `allow_start_anyway` (a job may set its own, `"0s"`
  for "start it now"; refused otherwise, or when the job is not pinned or in a group); `SubmitJob` keys `shapes`
  and `start_anyway_after`; `JobView` `shape`, `shapes`, `start_anyway_at` and `over`; `choreograph top` shows
  the shape a moldable job got, a start over capacity and when a queued job starts anyway. Tests:
  `choreographd/tests/e2e.rs` (`a_pinned_build_starts_anyway_past_its_wait_over_capacity`,
  `a_moldable_job_gets_the_largest_shape_that_fits`).
- 🚧 **CORE-9 quota-resources**: a resource whose use is limited by a quota that refills (a login's weekly
  cap and short window, an API key's rate limit, a bench's daily duty cycle), reported by a client as
  `Quota` windows (`quota.rs`: what is used, the limit, a soft level, the reset and an optional refill period);
  while a window is at or past its soft level (else its limit) the resource's capacity counts as 0
  (`State::capacity`, `State::spent`), so nothing new starts there and its holders keep running; at the reset
  the window counts as refilled; the list scheduler books the refill on its profile, so a waiting job gets a
  reservation and an ETA from the reset and is started then (`Plan::wake_at`); `WaitReason::Quota` names the
  resource, the window, its use and when it refills, in both planners; `Authority::report_quota` (checked,
  journaled, replayed) and `Beat::quota`, the windows at or past a level on what the holder holds (soft: plan
  the end; the limit: end now; nothing is taken). Tests: `choreograph/tests/quota.rs`. In `choreographd`: the
  wait line ("waits for a login: login-a has used 84% of its week quota, refills in 1d18h"). Planned: reports
  through `choreographd` (a route and which client may report which resource's quota), `Beat::quota` in
  its heartbeat answer; metrics that count a spent quota as no capacity (today utilisation is against the
  capacity as if refilled); a job's expected use of the quota in planning.
- 📋 **EST-3 learned-needs**: needs learned per job kind from the peaks holders report (an open-ended session's
  RAM), as its run length is learned today (CORE-6).
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

- ✅ **THEORY-2 slots-and-batches**: the two-state MMPP's rate and index of dispersion, gate runs per entry of
  batched gates with bisection, an entry's time to result through a batched lane (`batch_latency`), and the
  picks: `pick_slots` (fewest gate slots under a utilisation cap and a wait target, Allen–Cunneen) and
  `pick_batch` (the batch size with the shortest time to result), with `PickSettings` (`theory.rs`);
  `recording::pick` applies them to the worst of several traffic profiles, the one whose busy regime predicts
  the longest wait.

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

- ✅ **SIM-4 mmpp-arrivals**: regime-switching arrivals (`Arrivals::Mmpp`, `sim/workload.rs`): Poisson at the
  current regime's rate, exponential stays, the next regime drawn uniformly from the others; `Arrivals::mmpp2`
  for a busy and a quiet regime. A long run matches the theory's rate and index of dispersion
  (`theory::mmpp2_rate`, `mmpp2_idc`).
- ✅ **SIM-5 recordings-and-profiles**: `Recording` (model, trace, tags) of real history (`recording.rs`);
  `Profile::measure` of a group: λ per active hour, c_a² of gaps, the index of dispersion of binned counts, a
  two-regime MMPP fit (`Mmpp2Fit`), service, failures and aborts, measured waits, and the same per lane (the
  job name before `#`); `predicted_wq` (Kingman per lane), `simulate` (the lanes in the simulator under the
  fitted arrivals, empirical service, optional gate slots).
- 📋 **SIM-6 pause-metrics**: metrics that count a pause: what a paused job gave back as free time, what it
  keeps as busy time; today a paused job counts as holding all it held (`metrics.rs`, CORE-6).

## TEST: property tests

- ✅ **TEST-1 invariants**: property tests (`choreograph/tests/invariants.rs`, proptest with a fixed seed) over
  random fleets, job sets and seeds with `FirstFit` and `ListScheduler` (each backfill mode), in the batch
  simulator and in a `Live` model driven by a random script (submits, measured capacities, nodes down, up and
  lost, compactions), checked by an independent replay of the trace: no start over capacity or on a node that
  is down, every started job reaches cleanup and releases (only a lost node's jobs skip it), progress while work
  remains, Little's law exactly, determinism; on one exclusive slot the wait is bounded by the aging formula. A
  deliberately broken planner shows a failure shrinking to one job.

## CLI: command line

- 🚧 **CLI-1 cli**: `choreograph check`, `choreograph theory FILES... [--by TAG] [--json]` (recordings per
  group: measured, Kingman, Kingman with the regime fit's IDC, simulated waits, then the gate slots and batch
  size for the worst group, `--gate-time` for a planned gate length), `choreograph completions SHELL` and `--version` with SHA and commit time work;
  planned: `plan`, `simulate`, `show`, `sweep`, live values in completion, progress bars.
- ✅ **CLI-2 top**: `choreograph top`, a live view of a running `choreographd` over its public client/API
  (`choreograph_cli/src/top.rs`): nodes with their resources and labels, running jobs with their lease and
  ETA, the queue in priority order with its wait reason text and ETA, done jobs with their outcome; `--once`
  for a single snapshot, `--json` for the queue view as JSON (agents and the bot), `--color` (auto by
  default: a terminal without `NO_COLOR`). Stands in for the GUI (GUI-1..3) for now. Batches (PLAN-3a) and a
  node pressure signal (tpm_cli's memory sampler, not part of this generic service) aren't shown: nothing in
  `choreographd`'s API carries them yet.

## SVC: service

- ✅ **SVC-1 choreographd**: the scheduler as a service (`choreographd/`): tokio and axum on a Unix socket by
  default (callers name themselves, the socket's mode is the access control; every call one request and one
  answer, waits bounded, so a mesh proxy can carry them), or TCP over rustls on a pure-Rust crypto provider
  with one bearer token per client (required beyond loopback); a JSON API to submit jobs and groups, wait with
  a live wait reason and ETA, keep and release leases with fencing tokens, report nodes and their room, and
  see the queue; a TOML config for kinds (shared kinds made on first use), policy, lease and service settings;
  tokens stored as a hash; a journal replayed on restart; silent nodes drained, abandoned queued jobs
  cancelled; a typed client over the socket, HTTP, HTTPS or in process; a systemd unit
  (`choreographd/deploy/`) that restarts with a growing pause and stops on exit 78. Tests:
  `choreographd/tests/e2e.rs` (several clients on paused time), `choreographd/tests/tls.rs`,
  `choreographd/tests/unix.rs`.
- ✅ **SVC-3 standing-instructions**: what a node may do while it can't reach the service, handed out ahead of
  time. The library's `standing.rs`: `Standing` (per job kind a `Rule`, the options in order, then wait; a
  serial, the API it was made for, a validity counted from the node's own receipt, so a skewed clock neither
  shortens nor stretches it), `Choice::Local` (at most `max` at once per pool, a per-mille share of the room
  per resource kind, held back under pressure, `max_run`, an `exclusive` lock the client provides) and
  `Choice::Go` (other nodes in order), and the pure `decide` every node applies (`Run`, `Go`, `Wait` with the
  reasons, `Expired`); `Standing::lone` for a node with no scheduler, which never runs out. In `choreographd`:
  `[[standing]]` entries in the configuration (selected by `node` or `node_labels` and a job `kind` or `"*"`,
  the most specific winning; `local` and `go` options, `go.best` filled at issue from the live view: up, the
  labels, policy labels tolerated, most free `rank_by` first; `valid`, `why`; `settings.standing_valid`, a day by default),
  `NodeView::standing` on every report's answer and `GET /v1/nodes/{name}/standing` (`Client::standing`), a
  new serial only when the instruction changes (the service's clock at the change, so it grows across
  restarts), `NodeReport::local` (jobs run under a standing instruction, taken off the node's room until a
  report leaves them out; `ResourceView::local`); `choreograph top` shows each node's serial, when its
  instruction runs out and its local jobs. Examples: `choreographd/deploy/config.build-fleet.example.toml`,
  `config.hil-bench.example.toml`. Tests: `standing.rs` (proptests: never over `max` or a share, expired
  always waits, lone never runs out), `choreographd/tests/e2e.rs` (`a_report_answer_carries_...`,
  `local_jobs_are_counted_after_a_reconnect`, `the_most_specific_standing_entry_...`).
- ✅ **SVC-5 vouched-front**: the API for a proxy built into the same process that verifies its callers itself
  (`choreographd/src/front.rs`): `Front::call` takes one `ApiRequest` (method, path, query, body as text) and
  the identity the proxy vouches for, and answers with the router's `ApiAnswer` (status and body, byte for
  byte what a listener sends). The identity is matched exactly against a `[[client]]`'s `principal`
  (`"NODE"` or `"NODE/USER"`), with that client's rights (the nodes it may `report`, its own jobs and leases);
  no token and no header on that path, an unnamed identity is `401`. A client has a token, a principal or
  both; principal clients work with or without `tokens`. `Client::front` calls through it. Generic: the
  proxy (a mesh's listener) lives with the proxy. Tests: `choreographd/tests/front.rs` (a node reports only
  itself, a member never its node; a member touches only its own jobs and leases; only named principals get
  in; answers equal the token path's byte for byte), `config.rs` (`principal*`).

## SESS: long sessions on leases

- ✅ **SESS-1 session-leases**: leases for long work that decides alone within budgets (an agent session, a
  person on a bench, a soak test). A job carries budgets (`budget.rs`: a name, a meter `reported` by the holder
  or `wall` time from the authority's clock, a scope over the whole lease or the current step, soft and hard
  levels, `on_hard` `tell` or `stop`); the holder reports its step's use (`Authority::report_use`) and hears
  every budget at or past a level on that report and on each heartbeat (`Beat::over`): a lease event it acts
  on, not a guard, and only a `stop` budget's hard level cancels the job. A restart in place
  (`Authority::restart`: a fresh process under the same lease, token and resources, the next step; step
  budgets begin again, lease budgets add up) is not a new start and not a wake. Tells (`Authority::tell`)
  leave a message for a running job's holder, handed over once with its next heartbeat (`Beat::wakes`); the
  only loop guard is per pair of holders (`LeaseSettings::pair_bounces`, default 6, `pair_window`, default
  10m). All of it journaled and replayed. In `choreographd`: budgets per `[[job_kind]]` or per job (levels as
  numbers or durations), `POST /v1/leases/{token}/usage`, `/restart`, `POST /v1/jobs/{id}/tell`, the
  `pair_loop` refusal (429), `step` and `over` on the lease, `over` and `wakes` on the heartbeat; `choreograph
  top` shows the step and the levels crossed. Tests: `choreograph/tests/session_leases.rs`,
  `choreographd/tests/e2e.rs` (`sessions_have_budgets_...`).

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
