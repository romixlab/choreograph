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
    let (a, b) = (local.named("node-b/a"), local.named("node-c/b"));
    // Any caller may report any node.
    b.report("node-9", &room(&[("gate", 1)])).await.unwrap();
    let ja = a.submit(&gate("gate a")).await.unwrap();
    assert_eq!(ja.client, "node-b/a");
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

/// A session on a bench: open-ended, held until released, paused to let a gate run on its slot, resumed on
/// the same slot, asked to give way for a more important job, and its kind learns how long sessions last.
#[tokio::test(start_paused = true)]
async fn an_open_session_pauses_resumes_and_gives_way() {
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(
        dir.path(),
        r#"
[[job_kind]]
name = "session"
open = true
preempt = "ask"
pause_keeps = ["ram"]
run = { p50 = "1h", p90 = "3h" }
[lease]
ttl = "1h"
[settings]
node_stale = "2h"
queued_stale = "2h"
"#,
    ));
    let (a, b, c) = (svc.client("a"), svc.client("b"), svc.client("c"));
    svc.client("node-1")
        .report("node-1", &room(&[("gate", 1), ("ram", 8)]))
        .await
        .unwrap();
    let session = |name: &str| -> SubmitJob {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "kind": "session",
            "needs": [need("gate", 1), need("ram", 4)],
        }))
        .unwrap()
    };

    let ja = a.submit(&session("debug")).await.unwrap();
    assert_eq!(ja.state, JobState::Running);
    assert_eq!((ja.kind.as_deref(), ja.open), (Some("session"), true));
    let la = ja.lease.clone().unwrap();
    let jb = b.submit(&gate("gate b")).await.unwrap();
    assert_eq!(jb.state, JobState::Queued);

    // A pause gives the slot back (ram stays held) and the gate starts on it.
    sleep(Dur::mins(10).to_std()).await;
    a.heartbeat(la.token).await.unwrap();
    let pa = a.pause(la.token).await.unwrap();
    assert_eq!(pa.state, JobState::Paused);
    let pl = pa.lease.unwrap();
    assert_eq!(pl.paused, Some(T0 + Dur::mins(10)));
    let given_back: Vec<(&str, bool)> = pl
        .held
        .iter()
        .map(|h| (h.kind.as_str(), h.given_back))
        .collect();
    assert_eq!(given_back, [("gate", true), ("ram", false)]);
    let vb = b.wait_running(jb.id).await.unwrap();
    assert_eq!(vb.state, JobState::Running);

    // A resume waits for the slot under the same lease and gets it back once the gate is done.
    let ra = a.resume(la.token).await.unwrap();
    assert_eq!(ra.state, JobState::Paused);
    assert!(ra.wait.is_some(), "it waits for its slot: {ra:?}");
    let waiter = tokio::spawn({
        let a = a.clone();
        async move { a.wait_running(ja.id).await }
    });
    sleep(Dur::secs(20).to_std()).await;
    a.heartbeat(la.token).await.unwrap();
    b.release(vb.lease.unwrap().token, Outcome::Ok)
        .await
        .unwrap();
    let back = waiter.await.unwrap().unwrap();
    assert_eq!(back.state, JobState::Running);
    assert_eq!(
        back.lease.unwrap().token,
        la.token,
        "the same lease goes on"
    );

    // A more important job cannot start: the session is asked to give way, and nothing is taken from it.
    let mut urgent = gate("urgent");
    urgent.class = 5;
    let jc = c.submit(&urgent).await.unwrap();
    assert_eq!(jc.state, JobState::Queued);
    let beat = a.heartbeat(la.token).await.unwrap();
    assert!(beat.give_way && !beat.stop);
    assert!(a.job(ja.id).await.unwrap().lease.unwrap().give_way);

    // It ends; the urgent job starts, and sessions now have a learned length.
    sleep(Dur::mins(5).to_std()).await;
    let held = svc.daemon.now() - T0;
    a.release(la.token, Outcome::Ok).await.unwrap();
    let vc = c.wait_running(jc.id).await.unwrap();
    assert_eq!(vc.state, JobState::Running);
    let learned = svc
        .daemon
        .look(|s, _| s.authority().learned("session"))
        .unwrap();
    assert_eq!(learned.p50, held);
    c.release(vc.lease.unwrap().token, Outcome::Ok)
        .await
        .unwrap();
    svc.stop().await;
}

/// Two sessions with budgets: a crossing is heard on the report and the heartbeat, a restart in place keeps
/// the lease, the holders tell each other until the pair guard calls it a loop, steps survive a restart of
/// the service, and the wall budget's hard level stops a session.
#[tokio::test(start_paused = true)]
async fn sessions_have_budgets_restart_in_place_and_tell_each_other() {
    let dir = tempfile::tempdir().unwrap();
    let extra = r#"
[[job_kind]]
name = "session"
open = true
budgets = [
  { name = "context", scope = "step", soft = 150, hard = 190 },
  { name = "wall", meter = "wall", hard = "2h", on_hard = "stop" },
]
[lease]
ttl = "1h"
pair_bounces = 2
[settings]
node_stale = "4h"
"#;
    let svc = Svc::open(config(dir.path(), extra));
    let (a, b) = (svc.client("a"), svc.client("b"));
    svc.client("node-1")
        .report("node-1", &room(&[("gate", 2)]))
        .await
        .unwrap();
    let session: SubmitJob = serde_json::from_value(serde_json::json!({
        "name": "s", "kind": "session", "needs": [need("gate", 1)],
    }))
    .unwrap();
    let ja = a.submit(&session).await.unwrap();
    let jb = b.submit(&session).await.unwrap();
    let (la, lb) = (ja.lease.unwrap(), jb.lease.unwrap());
    assert_eq!(la.step, 1);

    // Past the context's soft level: heard on the report and in the lease.
    let used = |n: u64| BTreeMap::from([("context".to_string(), n)]);
    let u = a.report_use(la.token, used(160)).await.unwrap();
    assert_eq!((u.step, u.stop), (1, false));
    assert_eq!(
        u.over
            .iter()
            .map(|c| (c.budget.as_str(), c.level))
            .collect::<Vec<_>>(),
        [("context", choreograph::Level::Soft)]
    );
    assert_eq!(a.job(ja.id).await.unwrap().lease.unwrap().over, u.over);

    // A restart in place: same token, step 2, the context begins again.
    let r = a.restart(la.token).await.unwrap();
    let rl = r.lease.unwrap();
    assert_eq!(
        (r.state, rl.token, rl.step),
        (JobState::Running, la.token, 2)
    );
    assert!(
        a.report_use(la.token, used(10))
            .await
            .unwrap()
            .over
            .is_empty()
    );

    // Tells between the two: from outside a lease, then back and forth until the guard refuses.
    b.tell(ja.id, "main moved", None).await.unwrap();
    b.tell(ja.id, "your turn", Some(lb.token)).await.unwrap();
    let beat = a.heartbeat(la.token).await.unwrap();
    let wakes: Vec<(Option<u64>, &str)> = beat
        .wakes
        .iter()
        .map(|w| (w.from, w.text.as_str()))
        .collect();
    assert_eq!(wakes, [(None, "main moved"), (Some(jb.id), "your turn")]);
    assert!(
        a.heartbeat(la.token).await.unwrap().wakes.is_empty(),
        "once"
    );
    a.tell(jb.id, "no, yours", Some(la.token)).await.unwrap(); // bounce 1
    b.tell(ja.id, "no, yours", Some(lb.token)).await.unwrap(); // bounce 2
    let e = a
        .tell(jb.id, "no, yours", Some(la.token))
        .await
        .unwrap_err();
    assert_eq!(code(e), "pair_loop");
    // A sender names only its own lease; a job without a lease cannot be told.
    let e = a.tell(jb.id, "hi", Some(lb.token)).await.unwrap_err();
    assert_eq!(code(e), "forbidden");
    let jq = a.submit(&session).await.unwrap();
    assert_eq!(jq.state, JobState::Queued);
    let e = b.tell(jq.id, "hi", None).await.unwrap_err();
    assert_eq!(code(e), "conflict");
    a.cancel(jq.id).await.unwrap();

    // The step and the waiting tells survive a restart of the service.
    drop((a, b));
    svc.stop().await;
    let svc = Svc::open(config(dir.path(), extra));
    let (a, b) = (svc.client("a"), svc.client("b"));
    assert_eq!(a.job(ja.id).await.unwrap().lease.unwrap().step, 2);
    let beat = b.heartbeat(lb.token).await.unwrap();
    assert_eq!(beat.wakes.len(), 1, "a's tell before the restart");

    // Two hours of wall time: the stopping budget's hard level ends both sessions at their heartbeat.
    svc.client("node-1")
        .report("node-1", &room(&[("gate", 2)]))
        .await
        .unwrap();
    for _ in 0..2 {
        sleep(Dur::mins(50).to_std()).await;
        assert!(!a.heartbeat(la.token).await.unwrap().stop);
        b.heartbeat(lb.token).await.unwrap();
    }
    sleep(Dur::mins(20).to_std()).await;
    let beat = a.heartbeat(la.token).await.unwrap();
    assert!(beat.stop);
    assert!(
        beat.over
            .iter()
            .any(|c| c.budget == "wall" && c.level == choreograph::Level::Hard)
    );
    let v = a.release(la.token, Outcome::Cancelled).await.unwrap();
    assert_eq!(v.outcome, Some(Outcome::Cancelled));
    b.release(lb.token, Outcome::Ok).await.unwrap();
    svc.stop().await;
}

/// Standing instructions for the tests: builds on build nodes run there, gates go to the node with the most
/// free RAM, and node-2 waits for everything.
const STANDING: &str = r#"
[[standing]]
node_labels = { builds = "true" }
kind = "build"
options = [{ local = { max = 2, share = { ram = 800 }, pressure = true } }]
valid = "12h"
why = "builds wait for the scheduler on this node"

[[standing]]
kind = "gate"
options = [{ go = { best = 2, rank_by = "ram" } }]
valid = "2h"

[[standing]]
node = "node-2"
kind = "*"
why = "everything waits for the scheduler on node-2"
"#;

fn labelled(r: NodeReport, labels: &[(&str, &str)]) -> NodeReport {
    NodeReport {
        labels: labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ..r
    }
}

#[tokio::test(start_paused = true)]
async fn a_report_answer_carries_the_standing_instruction() {
    use choreograph::standing::{Choice, LocalView, Verdict, decide};
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(dir.path(), STANDING));
    let (n1, n2) = (svc.client("node-1"), svc.client("node-2"));
    let gib = 1u64 << 30;
    let r1 = labelled(room(&[("ram", 16 * gib)]), &[("builds", "true")]);
    n2.report("node-2", &room(&[("ram", 8 * gib)]))
        .await
        .unwrap();
    let v = n1.report("node-1", &r1).await.unwrap();
    let s = v.standing.expect("every report answer carries one");
    assert_eq!((s.node.as_str(), s.api.as_str()), ("node-1", "v1"));
    assert!(
        s.serial >= T0.ms() as u64,
        "a serial is at least the issue time"
    );
    assert_eq!(s.valid_ms, Some(Dur::hours(12).as_ms() as u64));
    let Choice::Local(l) = &s.kinds["build"].options[0] else {
        panic!("{s:?}")
    };
    assert_eq!((l.max, l.share["ram"], l.pressure), (2, 800, true));
    // The go list is filled from the live view: node-2, never node-1 itself.
    assert_eq!(
        s.kinds["gate"].options,
        vec![Choice::Go(choreograph::standing::Go {
            nodes: vec!["node-2".into()]
        })]
    );
    assert!(s.other.options.is_empty());

    // The node decides with it while the service is away; the hours count from its own receipt.
    let got = Time(5);
    let view = LocalView {
        room: [("ram".to_string(), 16 * gib)].into(),
        ..LocalView::default()
    };
    let needs = [("ram".to_string(), 4 * gib)].into();
    assert_eq!(
        decide(&s, got, Time(1000), "build", &needs, &view, &[]),
        Verdict::Run { choice: 0 }
    );
    assert!(matches!(
        decide(&s, got, Time(1000), "gate", &needs, &view, &[]),
        Verdict::Go { node, .. } if node == "node-2"
    ));
    assert_eq!(
        decide(&s, got, got + Dur::hours(3), "gate", &needs, &view, &[]),
        Verdict::Expired {
            at: got + Dur::hours(2)
        }
    );

    // Unchanged, it keeps its serial, in a report answer and on its own.
    sleep(Dur::secs(30).to_std()).await;
    let again = n1.report("node-1", &r1).await.unwrap().standing.unwrap();
    assert_eq!((again.serial, again.issued), (s.serial, s.issued));
    assert_eq!(n2.standing("node-1").await.unwrap(), s);
    assert_eq!(code(n1.standing("node-9").await.unwrap_err()), "not_found");

    // node-2's own "*" entry beats the gate entry for every node: it waits for everything.
    let s2 = n2.standing("node-2").await.unwrap();
    assert!(s2.kinds.is_empty(), "{s2:?}");
    assert!(matches!(
        decide(&s2, got, Time(1000), "gate", &needs, &view, &[]),
        Verdict::Wait { why, .. } if why == "everything waits for the scheduler on node-2"
    ));

    // A label changes: a new, higher serial, without the build rule.
    let changed = n1
        .report("node-1", &room(&[("ram", 16 * gib)]))
        .await
        .unwrap()
        .standing
        .unwrap();
    assert!(changed.serial > s.serial);
    assert!(!changed.kinds.contains_key("build"));
    assert!(matches!(
        decide(&changed, got, Time(1000), "build", &needs, &view, &[]),
        Verdict::Wait { .. }
    ));
    svc.stop().await;
}

#[tokio::test(start_paused = true)]
async fn local_jobs_are_counted_after_a_reconnect() {
    use choreograph::standing::LocalJob;
    let dir = tempfile::tempdir().unwrap();
    let gib = 1u64 << 30;
    let r1 = labelled(room(&[("ram", 16 * gib)]), &[("builds", "true")]);
    let svc = Svc::open(config(dir.path(), STANDING));
    let before = svc
        .client("node-1")
        .report("node-1", &r1)
        .await
        .unwrap()
        .standing
        .unwrap();
    svc.stop().await;

    // The service was away for an hour; the node ran a build under its standing meanwhile.
    let svc = Svc::with(
        Daemon::open(config(dir.path(), STANDING), Clock::new(T0 + Dur::hours(1))).unwrap(),
    );
    let (a, n1) = (svc.client("a"), svc.client("node-1"));
    let build = LocalJob {
        kind: "build".into(),
        needs: [("ram".to_string(), 10 * gib)].into(),
        since: T0 + Dur::mins(20),
    };
    let with_local = NodeReport {
        local: vec![build.clone()],
        ..r1.clone()
    };
    let v = n1.report("node-1", &with_local).await.unwrap();
    assert!(
        v.standing.unwrap().serial > before.serial,
        "serials grow across restarts"
    );
    assert_eq!(v.local, vec![build]);
    let ram = &v.resources[0];
    assert_eq!((ram.capacity, ram.local), (6 * gib, 10 * gib), "{ram:?}");

    // The room it holds is not handed out twice.
    let j = a
        .submit(&job("build", vec![need("ram", 8 * gib)], Dur::mins(5)))
        .await
        .unwrap();
    assert_eq!(j.state, JobState::Queued);
    assert_eq!(j.wait.as_ref().unwrap().why, "capacity", "{:?}", j.wait);

    // A local job of a kind the service doesn't know is refused, like room of one.
    let bad = NodeReport {
        local: vec![LocalJob {
            kind: "build".into(),
            needs: [("disk".to_string(), 1)].into(),
            since: T0,
        }],
        ..r1.clone()
    };
    assert_eq!(
        code(n1.report("node-1", &bad).await.unwrap_err()),
        "invalid"
    );

    // The local build ends: the next report leaves it out, and the queued job starts.
    let v = n1.report("node-1", &r1).await.unwrap();
    assert!(v.local.is_empty());
    assert_eq!(a.wait_running(j.id).await.unwrap().state, JobState::Running);
    svc.stop().await;
}

#[test]
fn the_most_specific_standing_entry_wins_and_go_picks_from_the_live_view() {
    use choreograph::standing::{Choice, Go};
    use choreographd::service::Service;
    let config = Config::parse(
        r#"
state_dir = "/nonexistent"
[[kind]]
name = "ram"
[[standing]]
kind = "build"
why = "every node"
[[standing]]
node_labels = { a = "1" }
kind = "build"
why = "one label"
[[standing]]
node_labels = { a = "1", b = "1" }
kind = "build"
why = "two labels, first"
[[standing]]
node_labels = { b = "1", c = "1" }
kind = "build"
why = "two labels, second"
[[standing]]
node = "n4"
kind = "*"
why = "n4 itself"
[[standing]]
kind = "gate"
options = [{ go = { nodes = ["n3"], best = 2, rank_by = "ram" } }]
"#,
    )
    .unwrap();
    let mut s = Service::in_memory(config, T0);
    let c = s.caller(None, None).unwrap();
    let report = |labels: &[(&str, &str)], policy: &[(&str, &str)], ram: u64, up: bool| {
        let mut r = labelled(room(&[("ram", ram)]), labels);
        r.policy = policy
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        r.up = up;
        r
    };
    let all = [("a", "1"), ("b", "1"), ("c", "1")];
    s.report(&c, "n1", report(&all, &[], 10, true), T0).unwrap();
    s.report(&c, "n2", report(&[("a", "1")], &[], 30, true), T0)
        .unwrap();
    s.report(&c, "n3", report(&[], &[], 50, true), T0).unwrap();
    s.report(&c, "n4", report(&[], &[], 90, true), T0).unwrap();
    // Most free RAM, but keeps work away unless asked for: never picked.
    s.report(&c, "n5", report(&[], &[("builds", "false")], 99, true), T0)
        .unwrap();
    // Drained: never picked.
    s.report(&c, "n6", report(&[], &[], 80, false), T0).unwrap();

    let why = |s: &mut Service, n: &str| s.standing(n).unwrap().rule("build").why.clone();
    assert_eq!(why(&mut s, "n1"), "two labels, first");
    assert_eq!(why(&mut s, "n2"), "one label");
    assert_eq!(why(&mut s, "n3"), "every node");
    assert_eq!(why(&mut s, "n4"), "n4 itself", "a node over its kind");
    // Named nodes first, then the most free RAM among the rest: n4 (90), n2 (30); never n1 itself.
    assert_eq!(
        s.standing("n1").unwrap().rule("gate").options,
        vec![Choice::Go(Go {
            nodes: vec!["n3".into(), "n4".into(), "n2".into()]
        })]
    );
}
