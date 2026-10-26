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
cargo run -p choreograph_cli -- top --socket /run/choreographd/api.sock   # a live view of choreographd
```

## Crates

- `choreograph`: the library (model, state, planners, trace, queueing formulas, TOML job sets).
- `choreograph_cli`: the `choreograph` command.
- `choreographd`: the scheduler as a service, with a typed client.

## The service

`choreographd` owns a fleet's resources and hands them out under leases. Nodes report their room (`ram`,
`cores`, test-gate slots) every few seconds; clients submit jobs with their needs, wait with a live wait reason
and ETA, keep the lease alive with heartbeats while the job runs and end it with a release. Every accepted
change is journaled before it is answered, so a restart picks up where it stopped, leases and fencing tokens
included.

```sh
choreographd check --config config.toml          # configuration, TLS files and journal, read-only
choreographd serve --config config.toml
```

The API is JSON over HTTP. By default the service listens on a Unix socket (`/run/choreographd/api.sock`)
and trusts whoever may open it, which is decided by the socket's file mode: a call names its client in the
`Choreograph-Client` header. Machines reach it through a proxy that carries single requests (a mesh): every
call is one request and one answer, and a wait is held at most its `timeout`, or answers at once with
`timeout=0`. Without such a proxy the service listens on TCP with TLS (rustls, no C) and one bearer token per
client (`tokens = true`; `choreographd token node-a --report node-a` makes a token and its `[[client]]`
block); a listener beyond loopback is refused without both.

| Call | Path |
|---|---|
| submit a job / a group | `POST /v1/jobs`, `POST /v1/groups` |
| look at a job | `GET /v1/jobs/{id}` |
| wait until it runs or ends | `POST /v1/jobs/{id}/wait?timeout=30s` |
| cancel it | `POST /v1/jobs/{id}/cancel` |
| keep / end its lease | `POST /v1/leases/{token}/heartbeat`, `POST /v1/leases/{token}/release` |
| report a node | `POST /v1/nodes/{name}` |
| the queue | `GET /v1/queue` |
| is it up (no token) | `GET /v1/health` |

Example configurations (`config.example.toml` on the socket, `config.tls.example.toml` with TLS and tokens)
and a systemd unit are in [`choreographd/deploy`](choreographd/deploy). The unit
restarts with a growing pause, gives up after a few fast failures, and does not restart on exit code 78, which
the service uses for what only a person can fix (a bad configuration, unusable TLS files, a journal that is
unreadable or written by a newer version).

## License

MIT, see [LICENSE](LICENSE).
