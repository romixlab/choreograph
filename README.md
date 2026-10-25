# choreograph

Plans and runs jobs over shared resources (instruments, connections, RAM, CPU, nodes) with priorities, deadlines,
setup and cleanup, without repeating work whose result is still valid.

A job needs some resources, goes through setup, run and cleanup, and carries a priority class, a release time and
a due date. choreograph keeps it safe (a job gets all it needs at once or nothing, nothing goes over capacity,
cleanup always runs), keeps it in order (dependencies, groups that start together) and says why each waiting job
waits. The same core serves a hardware-in-the-loop test bench (instruments, relay channels, fixtures) and a merge
queue on a few build machines (RAM, cores, checkouts): only the kinds of resources differ.

The library does no IO and reads no clock: the caller passes the time in and applies the decisions, so tests, the
simulator and a real executor all drive the same code.

Status: early. What works and what is planned: [FEATURES.md](FEATURES.md). Changes: [CHANGELOG.md](CHANGELOG.md).

## Try it

```sh
cargo run -p choreograph_cli -- check examples/bench.toml
```

## Crates

- `choreograph`: the library (model, state, planners, trace, queueing formulas, TOML job sets).
- `choreograph_cli`: the `choreograph` command.

## License

MIT, see [LICENSE](LICENSE).
