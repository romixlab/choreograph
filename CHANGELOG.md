# Changelog

All notable changes to choreograph. The format follows [Keep a Changelog](https://keepachangelog.com/), versions
follow [Semantic Versioning](https://semver.org/). Feature IDs refer to [FEATURES.md](FEATURES.md).

## [Unreleased]

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
