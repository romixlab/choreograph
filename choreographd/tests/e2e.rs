//! The service end to end: several clients over the HTTP routes (in process), on tokio's paused clock, so
//! minutes of leases, waits and ticks pass in milliseconds and every outcome is decided by the scheduler, not
//! by timing.

use std::collections::BTreeMap;
use std::path::Path;

use axum::Router;
use choreograph::{Dur, Outcome, Time};
use choreographd::api::{JobState, NeedSpec, NodeReport, SubmitGroup, SubmitJob};
use choreographd::client::Client;
use choreographd::config::{Config, token_hash};
use choreographd::daemon::{Clock, Daemon};
use tokio::task::JoinHandle;
use tokio::time::sleep;

const T0: Time = Time(1_800_000_000_000);

const CLIENTS: [(&str, &str); 5] = [
    ("a", r#"[]"#),
    ("b", r#"[]"#),
    ("c", r#"[]"#),
    ("node-1", r#"["node-1"]"#),
    ("node-2", r#"["node-2"]"#),
];

fn token(name: &str) -> String {
    format!("token-of-{name}")
}

/// A configuration with the kinds `gate` (slots), `ram` (bytes) and `main` (shared, one holder), the clients
/// above, and `extra` TOML appended (settings, lease).
fn config(state: &Path, extra: &str) -> Config {
    let mut t = format!(
        r#"
listen = "127.0.0.1:0"
state_dir = {state:?}
tokens = true
[tls]
cert = "cert.pem"
key = "key.pem"
[[kind]]
name = "gate"
[[kind]]
name = "ram"
unit = "bytes"
[[kind]]
name = "main"
shared = true
{extra}
"#
    );
    for (name, report) in CLIENTS {
        t.push_str(&format!(
            "[[client]]\nname = \"{name}\"\ntoken_sha256 = \"{}\"\nreport = {report}\n",
            token_hash(&token(name))
        ));
    }
    Config::parse(&t).unwrap()
}

struct Svc {
    daemon: Daemon,
    router: Router,
    ticker: JoinHandle<String>,
}

impl Svc {
    fn open(config: Config) -> Svc {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        Svc::with(Daemon::open(config, Clock::new(T0)).unwrap())
    }

    fn with(daemon: Daemon) -> Svc {
        let d = daemon.clone();
        let ticker = tokio::spawn(async move { d.run_ticker().await });
        Svc {
            router: choreographd::http::router(daemon.clone()),
            daemon,
            ticker,
        }
    }

    fn client(&self, name: &str) -> Client {
        Client::local(self.router.clone()).with_token(&token(name))
    }

    /// Stops the ticker and lets go of the state directory.
    async fn stop(self) {
        self.ticker.abort();
        let _ = self.ticker.await;
    }
}

fn need(kind: &str, amount: u64) -> NeedSpec {
    NeedSpec {
        kind: kind.into(),
        amount,
        ..Default::default()
    }
}

fn job(name: &str, needs: Vec<NeedSpec>, run: Dur) -> SubmitJob {
    serde_json::from_value(serde_json::json!({
        "name": name,
        "needs": needs,
        "run": {"p50": run.as_ms()},
    }))
    .unwrap()
}

fn gate(name: &str) -> SubmitJob {
    job(name, vec![need("gate", 1)], Dur::mins(10))
}

fn room(kinds: &[(&str, u64)]) -> NodeReport {
    NodeReport {
        up: true,
        room: kinds
            .iter()
            .map(|(k, n)| (k.to_string(), *n))
            .collect::<BTreeMap<_, _>>(),
        ..Default::default()
    }
}

fn code(e: choreographd::client::ClientError) -> String {
    e.code().unwrap_or("none").to_string()
}

#[tokio::test(start_paused = true)]
async fn clients_compete_for_gate_slots_and_a_release_hands_over() {
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(dir.path(), ""));
    let (a, b, c) = (svc.client("a"), svc.client("b"), svc.client("c"));
    svc.client("node-1")
        .report("node-1", &room(&[("gate", 2)]))
        .await
        .unwrap();

    let ja = a.submit(&gate("gate a")).await.unwrap();
    let jb = b.submit(&gate("gate b")).await.unwrap();
    let jc = c.submit(&gate("gate c")).await.unwrap();
    assert_eq!(ja.state, JobState::Running);
    assert_eq!(jb.state, JobState::Running);
    assert_eq!(jc.state, JobState::Queued);
    let wait = jc.wait.clone().unwrap();
    assert!(
        wait.text.contains("gate") || wait.resource.is_some(),
        "{wait:?}"
    );
    let eta = jc.eta.unwrap();
    assert_eq!(
        eta.start_p50,
        Some(T0 + Dur::mins(10)),
        "c starts when the first slot frees up: {eta:?}"
    );

    // A wait with nothing changing answers after its timeout, still queued, with the reason.
    let before = svc.daemon.now();
    let v = c.wait(jc.id, Some(Dur::secs(5))).await.unwrap();
    assert_eq!(v.state, JobState::Queued);
    assert_eq!(svc.daemon.now() - before, Dur::secs(5));
    assert!(v.wait.is_some());

    // c waits as long as it takes; a's release hands its slot over at once.
    let waiter = tokio::spawn({
        let c = c.clone();
        async move { c.wait_running(jc.id).await }
    });
    sleep(Dur::secs(20).to_std()).await;
    let la = ja.lease.unwrap();
    let done = a.release(la.token, Outcome::Ok).await.unwrap();
    assert_eq!(done.state, JobState::Done);
    assert_eq!(done.outcome, Some(Outcome::Ok));
    let vc = waiter.await.unwrap().unwrap();
    assert_eq!(vc.state, JobState::Running);
    let lc = vc.lease.unwrap();
    assert!(lc.token > la.token, "fencing tokens rise");
    assert_eq!(lc.held[0].node, "node-1");

    let q = b.queue().await.unwrap();
    assert_eq!(q.running.len(), 2);
    assert!(q.queued.is_empty());
    assert_eq!(q.done.len(), 1);
    let gate_res = &q.nodes[0].resources[0];
    assert_eq!((gate_res.capacity, gate_res.used), (2, 2));
    svc.stop().await;
}

trait ToStd {
    fn to_std(self) -> std::time::Duration;
}

impl ToStd for Dur {
    fn to_std(self) -> std::time::Duration {
        std::time::Duration::from_millis(self.as_ms() as u64)
    }
}

#[tokio::test(start_paused = true)]
async fn an_expired_lease_is_refused_and_its_job_runs_again() {
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(
        dir.path(),
        "[lease]\nttl = \"1m\"\n[settings]\nnode_stale = \"1h\"",
    ));
    let (a, b) = (svc.client("a"), svc.client("b"));
    svc.client("node-1")
        .report("node-1", &room(&[("gate", 1)]))
        .await
        .unwrap();
    let ja = a.submit(&gate("gate a")).await.unwrap();
    let la = ja.lease.clone().unwrap();
    assert_eq!(la.expires, T0 + Dur::mins(1));
    let jb = b.submit(&gate("gate b")).await.unwrap();
    assert_eq!(jb.state, JobState::Queued);

    // a keeps its lease for a while with heartbeats, then goes silent.
    sleep(Dur::secs(40).to_std()).await;
    let beat = a.heartbeat(la.token).await.unwrap();
    assert!(!beat.stop);
    assert_eq!(beat.expires, T0 + Dur::secs(100));
    let vb = b.wait_running(jb.id).await.unwrap();
    assert_eq!(
        svc.daemon.now(),
        T0 + Dur::secs(100),
        "b starts the moment a's lease expires"
    );
    let lb = vb.lease.unwrap();
    assert!(lb.token > la.token);

    // a's late heartbeat and release are refused: its token is no longer the holder's.
    let e = a.heartbeat(la.token).await.unwrap_err();
    assert!(
        ["stale_lease", "expired_lease"].contains(&code(e).as_str()),
        "late heartbeat refused"
    );
    let e = a.release(la.token, Outcome::Ok).await.unwrap_err();
    assert!(["stale_lease", "expired_lease"].contains(&code(e).as_str()));

    // a's job went back to the queue first in line, but a had not come back for it: rather than start it
    // under a lease nobody takes, the service cancelled it, and b went next.
    let va = a.job(ja.id).await.unwrap();
    assert_eq!(
        (va.state, va.outcome),
        (JobState::Done, Some(Outcome::Cancelled))
    );
    b.release(lb.token, Outcome::Ok).await.unwrap();
    svc.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_running_job_is_told_to_stop_at_its_heartbeat() {
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(dir.path(), ""));
    let a = svc.client("a");
    svc.client("node-1")
        .report("node-1", &room(&[("gate", 1)]))
        .await
        .unwrap();
    let ja = a.submit(&gate("gate a")).await.unwrap();
    let la = ja.lease.unwrap();
    assert!(!a.heartbeat(la.token).await.unwrap().stop);
    let v = a.cancel(ja.id).await.unwrap();
    assert_eq!(v.state, JobState::Running, "it runs until it cleans up");
    assert!(a.heartbeat(la.token).await.unwrap().stop);
    let v = a.release(la.token, Outcome::Cancelled).await.unwrap();
    assert_eq!(v.outcome, Some(Outcome::Cancelled));

    // A queued job ends at once.
    let j1 = a.submit(&gate("one")).await.unwrap();
    let j2 = a.submit(&gate("two")).await.unwrap();
    assert_eq!(j2.state, JobState::Queued);
    let v = a.cancel(j2.id).await.unwrap();
    assert_eq!(
        (v.state, v.outcome),
        (JobState::Done, Some(Outcome::Cancelled))
    );
    a.release(j1.lease.unwrap().token, Outcome::Failed)
        .await
        .unwrap();
    svc.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_restart_keeps_ids_leases_and_tokens() {
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(dir.path(), ""));
    let (a, b) = (svc.client("a"), svc.client("b"));
    svc.client("node-1")
        .report("node-1", &room(&[("gate", 1)]))
        .await
        .unwrap();
    let ja = a.submit(&gate("gate a")).await.unwrap();
    let jb = b.submit(&gate("gate b")).await.unwrap();
    let la = ja.lease.unwrap();
    sleep(Dur::secs(10).to_std()).await;
    drop((a, b));
    svc.stop().await;

    // A second service on the same directory while the first holds it is refused (checked in store tests);
    // after it stopped, the new one takes over where it was, even on a clock an hour behind.
    let behind = Clock::new(T0 - Dur::mins(60));
    let svc = Svc::with(Daemon::open(config(dir.path(), ""), behind).unwrap());
    let (a, b) = (svc.client("a"), svc.client("b"));
    assert!(
        svc.daemon.now() >= T0,
        "time does not go back behind the journal"
    );
    let va = a.job(ja.id).await.unwrap();
    assert_eq!(va.state, JobState::Running);
    assert_eq!(va.lease.as_ref().unwrap().token, la.token);
    assert!(!a.heartbeat(la.token).await.unwrap().stop);
    assert_eq!(b.job(jb.id).await.unwrap().state, JobState::Queued);
    let jc = b.submit(&gate("gate c")).await.unwrap();
    assert!(jc.id > jb.id, "ids continue");
    // The node has to report again to stay up, like after any restart.
    svc.client("node-1")
        .report("node-1", &room(&[("gate", 1)]))
        .await
        .unwrap();
    a.release(la.token, Outcome::Ok).await.unwrap();
    let vb = b.wait_running(jb.id).await.unwrap();
    assert!(vb.lease.unwrap().token > la.token);
    svc.stop().await;
}

#[tokio::test(start_paused = true)]
async fn tokens_decide_who_may_do_what() {
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(dir.path(), ""));
    let (a, b) = (svc.client("a"), svc.client("b"));
    let nobody = Client::local(svc.router.clone()).with_token("not a token");
    assert!(
        nobody
            .health()
            .await
            .unwrap()
            .version
            .starts_with(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(code(nobody.queue().await.unwrap_err()), "unauthorized");
    let e = nobody.submit(&gate("x")).await.unwrap_err();
    assert!(matches!(
        e,
        choreographd::client::ClientError::Api { status: 401, .. }
    ));

    assert_eq!(
        code(a.report("node-1", &room(&[("gate", 1)])).await.unwrap_err()),
        "forbidden"
    );
    assert_eq!(
        code(
            svc.client("node-1")
                .report("node-2", &room(&[("gate", 1)]))
                .await
                .unwrap_err()
        ),
        "forbidden"
    );
    svc.client("node-1")
        .report("node-1", &room(&[("gate", 1)]))
        .await
        .unwrap();
    let ja = a.submit(&gate("gate a")).await.unwrap();
    assert_eq!(code(b.cancel(ja.id).await.unwrap_err()), "forbidden");
    let la = ja.lease.unwrap();
    assert_eq!(code(b.heartbeat(la.token).await.unwrap_err()), "forbidden");
    assert_eq!(
        code(b.release(la.token, Outcome::Ok).await.unwrap_err()),
        "forbidden"
    );
    assert_eq!(code(b.wait(ja.id, None).await.unwrap_err()), "forbidden");
    // Looking is allowed: the queue is everyone's.
    assert_eq!(b.job(ja.id).await.unwrap().client, "a");
    assert_eq!(code(a.job(999).await.unwrap_err()), "not_found");
    assert_eq!(code(a.heartbeat(999).await.unwrap_err()), "stale_lease");
    assert_eq!(
        code(
            a.submit(&job("x", vec![need("bogus", 1)], Dur::ZERO))
                .await
                .unwrap_err()
        ),
        "invalid"
    );
    svc.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_silent_node_is_drained_until_it_reports_again() {
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(dir.path(), ""));
    let (a, n1) = (svc.client("a"), svc.client("node-1"));
    let gib = 1u64 << 30;
    n1.report("node-1", &room(&[("ram", 16 * gib)]))
        .await
        .unwrap();
    let big = job("build", vec![need("ram", 8 * gib)], Dur::mins(5));
    let j = a.submit(&big).await.unwrap();
    let l = j.lease.unwrap();
    a.release(l.token, Outcome::Ok).await.unwrap();

    // Two minutes without a report: drained at the next tick. A new job waits and says why.
    sleep((Dur::mins(2) + Dur::secs(10)).to_std()).await;
    let q = a.queue().await.unwrap();
    assert!(!q.nodes[0].up, "{:?}", q.nodes[0]);
    let j = a.submit(&big).await.unwrap();
    assert_eq!(j.state, JobState::Queued);
    assert_eq!(j.wait.as_ref().unwrap().why, "nodes_down", "{:?}", j.wait);

    // A job too big for any node there ever was is refused, in bytes people read.
    let e = a
        .submit(&job("huge", vec![need("ram", 64 * gib)], Dur::mins(5)))
        .await
        .unwrap_err();
    let msg = e.to_string();
    assert!(
        msg.contains("64.0 GiB") && msg.contains("\"huge\""),
        "{msg}"
    );
    n1.report("node-1", &room(&[("ram", 16 * gib)]))
        .await
        .unwrap();
    let v = a.wait_running(j.id).await.unwrap();
    assert_eq!(v.state, JobState::Running);
    svc.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_shared_main_has_one_holder_at_a_time() {
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(dir.path(), ""));
    let (a, b) = (svc.client("a"), svc.client("b"));
    let land = |name: &str| {
        job(
            name,
            vec![NeedSpec {
                resource: Some("repo/main".into()),
                ..need("main", 1)
            }],
            Dur::mins(3),
        )
    };
    let ja = a.submit(&land("land a")).await.unwrap();
    let jb = b.submit(&land("land b")).await.unwrap();
    let jo = b
        .submit(&job(
            "land other",
            vec![NeedSpec {
                resource: Some("other/main".into()),
                ..need("main", 1)
            }],
            Dur::mins(3),
        ))
        .await
        .unwrap();
    assert_eq!(ja.state, JobState::Running);
    assert_eq!(
        jo.state,
        JobState::Running,
        "another repository's main is free"
    );
    assert_eq!(jb.state, JobState::Queued);
    let w = jb.wait.unwrap();
    assert_eq!(w.resource.as_deref(), Some("repo/main"), "{w:?}");
    assert_eq!(w.job, Some(ja.id));
    let la = ja.lease.unwrap();
    assert_eq!(la.held[0].node, "shared");

    let waiter = tokio::spawn({
        let b = b.clone();
        async move { b.wait_running(jb.id).await }
    });
    sleep(Dur::secs(50).to_std()).await;
    a.heartbeat(la.token).await.unwrap();
    sleep(Dur::secs(50).to_std()).await;
    a.heartbeat(la.token).await.unwrap();
    a.release(la.token, Outcome::Ok).await.unwrap();
    let lb = waiter.await.unwrap().unwrap().lease.unwrap();
    assert!(lb.token > la.token);
    assert_eq!(
        code(
            a.submit(&job("bad", vec![need("main", 1)], Dur::ZERO))
                .await
                .unwrap_err()
        ),
        "invalid",
        "a shared need names its resource"
    );
    svc.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_queued_job_nobody_waits_for_is_cancelled() {
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(
        dir.path(),
        "[settings]\nnode_stale = \"1h\"\n[lease]\nttl = \"1h\"",
    ));
    let a = svc.client("a");
    svc.client("node-1")
        .report("node-1", &room(&[("gate", 1)]))
        .await
        .unwrap();
    let j1 = a.submit(&gate("runs")).await.unwrap();
    let kept = a.submit(&gate("kept")).await.unwrap();
    let left = a.submit(&gate("left")).await.unwrap();
    // The client keeps asking about one of them and forgets the other.
    for _ in 0..4 {
        assert_eq!(a.wait(kept.id, None).await.unwrap().state, JobState::Queued);
    }
    let v = a.job(left.id).await.unwrap();
    assert_eq!(
        (v.state, v.outcome),
        (JobState::Done, Some(Outcome::Cancelled))
    );
    assert_eq!(a.job(kept.id).await.unwrap().state, JobState::Queued);
    a.release(j1.lease.unwrap().token, Outcome::Ok)
        .await
        .unwrap();
    assert_eq!(
        a.wait_running(kept.id).await.unwrap().state,
        JobState::Running
    );
    svc.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_group_starts_together_or_not_at_all() {
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(dir.path(), ""));
    let a = svc.client("a");
    svc.client("node-1")
        .report("node-1", &room(&[("gate", 2)]))
        .await
        .unwrap();
    let blocker = a.submit(&gate("blocker")).await.unwrap();
    let g = a
        .submit_group(&SubmitGroup {
            name: "pair".into(),
            jobs: vec![gate("first"), gate("second")],
        })
        .await
        .unwrap();
    assert_eq!(g.len(), 2);
    assert!(
        g.iter().all(|j| j.state == JobState::Queued),
        "one slot is free, the pair needs two: {g:?}"
    );
    let waiter = tokio::spawn({
        let a = a.clone();
        let id = g[1].id;
        async move { a.wait_running(id).await }
    });
    sleep(Dur::secs(1).to_std()).await;
    a.release(blocker.lease.unwrap().token, Outcome::Ok)
        .await
        .unwrap();
    assert_eq!(waiter.await.unwrap().unwrap().state, JobState::Running);
    assert_eq!(a.job(g[0].id).await.unwrap().state, JobState::Running);
    svc.stop().await;
}

/// Without tokens (the default, behind a Unix socket or a mesh proxy) callers name themselves: jobs still
/// belong to the name that submitted them, and a wait can be a short poll.
#[tokio::test(start_paused = true)]
async fn without_tokens_callers_name_themselves() {
    let dir = tempfile::tempdir().unwrap();
    let c = Config::parse(&format!(
        "state_dir = {:?}\n[[kind]]\nname = \"gate\"\n",
        dir.path()
    ))
    .unwrap();
    let svc = Svc::with(Daemon::open(c, Clock::new(T0)).unwrap());
    let local = Client::local(svc.router.clone());
    let (a, b) = (local.named("core-1/a"), local.named("core-2/b"));
    // Any caller may report any node.
    b.report("node-9", &room(&[("gate", 1)])).await.unwrap();
    let ja = a.submit(&gate("gate a")).await.unwrap();
    assert_eq!(ja.client, "core-1/a");
    let jl = local.submit(&gate("gate local")).await.unwrap();
    assert_eq!(jl.client, "local");
    assert_eq!(code(b.cancel(ja.id).await.unwrap_err()), "forbidden");
    assert_eq!(
        code(local.named("two words").queue().await.unwrap_err()),
        "unauthorized"
    );

    // A short poll answers at once, with the reason and the ETA.
    let before = svc.daemon.now();
    let v = local.wait(jl.id, Some(Dur::ZERO)).await.unwrap();
    assert_eq!(svc.daemon.now(), before);
    assert_eq!(v.state, JobState::Queued);
    assert!(v.wait.is_some() && v.eta.is_some(), "{v:?}");
    a.release(ja.lease.unwrap().token, Outcome::Ok)
        .await
        .unwrap();
    let v = local.wait(jl.id, Some(Dur::ZERO)).await.unwrap();
    assert_eq!(v.state, JobState::Running);
    svc.stop().await;
}
