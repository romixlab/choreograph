//! The front end to end: a proxy in the same process calls as the principals it vouches for, on tokio's
//! paused clock. Each principal is its `[[client]]` with that client's rights, and the answers are the ones a
//! token caller gets, byte for byte.

use std::collections::BTreeMap;
use std::path::Path;

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use choreograph::{Dur, Outcome, Time};
use choreographd::api::{JobState, NeedSpec, NodeReport, SubmitJob};
use choreographd::client::{Client, ClientError};
use choreographd::config::{Config, token_hash};
use choreographd::daemon::{Clock, Daemon};
use choreographd::front::{ApiAnswer, ApiRequest, Front};
use http_body_util::BodyExt;
use tokio::task::JoinHandle;
use tower::ServiceExt;

const T0: Time = Time(1_800_000_000_000);

/// Two machines and a member on each; `node-1` may also call with its token over a listener.
fn config(state: &Path, tokens: bool) -> Config {
    let token = if tokens {
        format!("token_sha256 = \"{}\"\n", token_hash("token-1"))
    } else {
        String::new()
    };
    Config::parse(&format!(
        r#"
state_dir = {state:?}
tokens = {tokens}
[[kind]]
name = "gate"
[[client]]
name = "node-1"
{token}principal = "node-1"
report = ["node-1"]
[[client]]
name = "node-2"
principal = "node-2"
report = ["node-2"]
[[client]]
name = "alice"
principal = "node-1/alice"
[[client]]
name = "bob"
principal = "node-2/bob"
"#
    ))
    .unwrap()
}

struct Svc {
    router: Router,
    front: Front,
    ticker: JoinHandle<String>,
}

impl Svc {
    fn open(config: Config) -> Svc {
        let daemon = Daemon::open(config, Clock::new(T0)).unwrap();
        let d = daemon.clone();
        Svc {
            router: choreographd::http::router(daemon.clone()),
            front: Front::new(daemon),
            ticker: tokio::spawn(async move { d.run_ticker().await }),
        }
    }

    fn as_(&self, principal: &str) -> Client {
        Client::front(self.front.clone(), principal)
    }

    /// The same call over the listeners' router with `node-1`'s token: status and body bytes.
    async fn with_token(&self, req: &ApiRequest) -> ApiAnswer {
        let uri = if req.query.is_empty() {
            req.path.clone()
        } else {
            format!("{}?{}", req.path, req.query)
        };
        let http = Request::builder()
            .method(req.method.as_str())
            .uri(uri)
            .header("content-type", "application/json")
            .header("authorization", "Bearer token-1")
            .body(Body::from(req.body.clone()))
            .unwrap();
        let res = self.router.clone().oneshot(http).await.unwrap();
        ApiAnswer {
            status: res.status().as_u16(),
            body: String::from_utf8(res.into_body().collect().await.unwrap().to_bytes().to_vec())
                .unwrap(),
        }
    }

    async fn stop(self) {
        self.ticker.abort();
        let _ = self.ticker.await;
    }
}

fn gate(name: &str) -> SubmitJob {
    SubmitJob {
        name: name.into(),
        needs: vec![NeedSpec {
            kind: "gate".into(),
            amount: 1,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn room(n: u64) -> NodeReport {
    NodeReport {
        up: true,
        room: BTreeMap::from([("gate".to_string(), n)]),
        ..Default::default()
    }
}

fn code(e: ClientError) -> String {
    e.code().unwrap_or("none").to_string()
}

#[tokio::test(start_paused = true)]
async fn a_node_reports_only_itself_and_a_member_never_its_node() {
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(dir.path(), false));
    let (n1, n2, alice) = (
        svc.as_("node-1"),
        svc.as_("node-2"),
        svc.as_("node-1/alice"),
    );

    let v = n1.report("node-1", &room(1)).await.unwrap();
    assert_eq!(v.name, "node-1");
    assert_eq!(
        code(n1.report("node-2", &room(1)).await.unwrap_err()),
        "forbidden"
    );
    assert_eq!(
        code(n2.report("node-1", &room(9)).await.unwrap_err()),
        "forbidden"
    );
    assert_eq!(
        code(alice.report("node-1", &room(9)).await.unwrap_err()),
        "forbidden",
        "a member on a node is not the node"
    );
    n2.report("node-2", &room(1)).await.unwrap();
    svc.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_member_touches_only_its_own_jobs_and_leases() {
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(dir.path(), false));
    let (n1, alice, bob) = (
        svc.as_("node-1"),
        svc.as_("node-1/alice"),
        svc.as_("node-2/bob"),
    );
    n1.report("node-1", &room(1)).await.unwrap();

    let ja = alice.submit(&gate("alice's gate")).await.unwrap();
    assert_eq!(ja.client, "alice");
    let la = ja.lease.clone().expect("a free slot");
    let jb = bob.submit(&gate("bob's gate")).await.unwrap();
    assert_eq!((jb.client.as_str(), jb.state), ("bob", JobState::Queued));

    // Bob, and the node alice's job runs on, can look but not touch.
    for other in [&bob, &n1] {
        assert_eq!(
            code(other.heartbeat(la.token).await.unwrap_err()),
            "forbidden"
        );
        assert_eq!(
            code(other.release(la.token, Outcome::Ok).await.unwrap_err()),
            "forbidden"
        );
        assert_eq!(code(other.cancel(ja.id).await.unwrap_err()), "forbidden");
        assert_eq!(
            code(other.wait(ja.id, None).await.unwrap_err()),
            "forbidden"
        );
        assert_eq!(other.job(ja.id).await.unwrap().client, "alice");
    }
    assert_eq!(code(alice.cancel(jb.id).await.unwrap_err()), "forbidden");

    alice.heartbeat(la.token).await.unwrap();
    alice.release(la.token, Outcome::Ok).await.unwrap();
    let v = bob.wait(jb.id, Some(Dur::ZERO)).await.unwrap();
    assert_eq!(v.state, JobState::Running, "the query reaches the route");
    svc.stop().await;
}

#[tokio::test(start_paused = true)]
async fn only_a_principal_a_client_names_gets_in() {
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(dir.path(), true));
    for who in ["node-3", "node-1/carol", "alice", "", "node-1/"] {
        let a = svc.front.call(who, ApiRequest::get("/v1/queue")).await;
        assert_eq!(a.status, 401, "{who:?}: {}", a.body);
        assert!(a.body.contains("\"unauthorized\""), "{}", a.body);
    }
    // A token is no principal, and a listener's caller is never vouched for.
    assert_eq!(
        code(svc.as_("token-1").queue().await.unwrap_err()),
        "unauthorized"
    );
    let a = svc.with_token(&ApiRequest::get("/v1/queue")).await;
    assert_eq!(a.status, 200, "{}", a.body);

    for (req, why) in [
        (ApiRequest::get("v1/queue"), "no leading /"),
        (ApiRequest::get("/v1/queue?x=1"), "query in the path"),
        (
            ApiRequest {
                method: "G T".into(),
                ..ApiRequest::get("/v1/queue")
            },
            "bad method",
        ),
    ] {
        let a = svc.front.call("node-1", req).await;
        assert_eq!(a.status, 400, "{why}: {}", a.body);
        assert!(a.body.contains("\"invalid\""), "{why}: {}", a.body);
    }
    svc.stop().await;
}

#[tokio::test(start_paused = true)]
async fn answers_are_the_token_paths_byte_for_byte() {
    let dir = tempfile::tempdir().unwrap();
    let svc = Svc::open(config(dir.path(), true));
    let submit = serde_json::to_string(&gate("g")).unwrap();
    let report = serde_json::to_string(&room(2)).unwrap();
    // State-changing calls go through the front; after each, the reads must answer alike on both paths.
    let a = svc
        .front
        .call("node-1", ApiRequest::post("/v1/nodes/node-1", report))
        .await;
    assert_eq!(a.status, 200, "{}", a.body);
    let a = svc
        .front
        .call("node-1", ApiRequest::post("/v1/jobs", submit))
        .await;
    assert_eq!(a.status, 200, "{}", a.body);
    let id = serde_json::from_str::<serde_json::Value>(&a.body).unwrap()["id"]
        .as_u64()
        .unwrap();

    let reads = [
        ApiRequest::get("/v1/health"),
        ApiRequest::get("/v1/queue"),
        ApiRequest::get(format!("/v1/jobs/{id}")),
        ApiRequest::get("/v1/nodes/node-1/standing"),
        ApiRequest {
            query: "timeout=0".into(),
            ..ApiRequest::post(format!("/v1/jobs/{id}/wait"), "")
        },
        ApiRequest {
            query: "timeout=soon".into(),
            ..ApiRequest::post(format!("/v1/jobs/{id}/wait"), "")
        },
        ApiRequest::get("/v1/jobs/999"),
        ApiRequest::get("/v1/nope"),
        ApiRequest::post("/v1/jobs", "{not json"),
        ApiRequest::post("/v1/nodes/node-2", serde_json::to_string(&room(1)).unwrap()),
        ApiRequest::post("/v1/queue", ""),
    ];
    for req in reads {
        let front = svc.front.call("node-1", req.clone()).await;
        let token = svc.with_token(&req).await;
        assert_eq!(front, token, "{req:?}");
    }
    svc.stop().await;
}
