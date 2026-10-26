//! The service on a real Unix socket, plain HTTP, without tokens: the default setup.

use std::os::unix::fs::PermissionsExt;

use choreograph::Time;
use choreographd::api::JobState;
use choreographd::client::Client;
use choreographd::config::Config;
use choreographd::daemon::{Clock, Daemon};

#[tokio::test]
async fn the_service_answers_on_its_socket() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("api.sock");
    let config = Config::parse(&format!(
        "listen = {sock:?}\nsocket_mode = \"0600\"\nstate_dir = {:?}\n[[kind]]\nname = \"gate\"\n",
        dir.path().join("state")
    ))
    .unwrap();
    // A socket left by an earlier run is replaced.
    drop(std::os::unix::net::UnixListener::bind(&sock).unwrap());
    let bound = choreographd::listen::bind(&config.listen, config.socket_mode)
        .await
        .unwrap();
    assert_eq!(bound.path().as_deref(), Some(sock.as_path()));
    let mode = std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    let daemon = Daemon::open(config, Clock::new(Time(1_800_000_000_000))).unwrap();
    let server = tokio::spawn(bound.serve(
        None,
        choreographd::http::router(daemon),
        std::time::Duration::from_secs(10),
    ));

    let c = Client::unix(&sock).named("node-a");
    assert!(
        c.health()
            .await
            .unwrap()
            .version
            .starts_with(env!("CARGO_PKG_VERSION"))
    );
    let q = c.queue().await.unwrap();
    assert!(q.running.is_empty() && q.queued.is_empty());
    let mut r = choreographd::api::NodeReport {
        up: true,
        ..Default::default()
    };
    r.room.insert("gate".into(), 1);
    c.report("node-a", &r).await.unwrap();
    let j: choreographd::api::SubmitJob = serde_json::from_value(serde_json::json!({
        "name": "on the socket",
        "needs": [{"kind": "gate", "amount": 1}],
    }))
    .unwrap();
    let v = c.submit(&j).await.unwrap();
    assert_eq!((v.state, v.client.as_str()), (JobState::Running, "node-a"));
    server.abort();

    // Something else at the socket's path is not touched.
    let file = dir.path().join("not-a-socket");
    std::fs::write(&file, "keep me").unwrap();
    let e = choreographd::listen::bind(&choreographd::config::Listen::Unix(file.clone()), 0o660)
        .await
        .err()
        .unwrap();
    assert!(e.needs_a_person, "{e}");
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "keep me");
}
