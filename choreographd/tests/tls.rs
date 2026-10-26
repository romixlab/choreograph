//! The service over real TLS on a loopback port, with the typed HTTPS client.
//!
//! `tests/data` holds a synthetic test CA (`ca.pem`), a certificate it signed for `localhost` and 127.0.0.1
//! (`cert.pem`) and that certificate's key (`key.pem`), valid for 100 years; made with openssl for these tests
//! only, the CA's key was thrown away.

use std::path::Path;

use choreograph::Time;
use choreographd::api::{JobState, NeedSpec, NodeReport, SubmitJob};
use choreographd::client::Client;
use choreographd::config::{Config, token_hash};
use choreographd::daemon::{Clock, Daemon};

#[tokio::test]
async fn the_service_answers_over_tls() {
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    let state = tempfile::tempdir().unwrap();
    let config = Config::parse(&format!(
        r#"
listen = "127.0.0.1:0"
state_dir = {:?}
tokens = true
[tls]
cert = {:?}
key = {:?}
[[kind]]
name = "gate"
[[client]]
name = "a"
token_sha256 = "{}"
report = ["*"]
"#,
        state.path(),
        data.join("cert.pem"),
        data.join("key.pem"),
        token_hash("secret-a")
    ))
    .unwrap();
    let tls = choreographd::tls::server_config(config.tls.as_ref().unwrap()).unwrap();
    let daemon = Daemon::open(config, Clock::new(Time(1_800_000_000_000))).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(choreographd::tls::serve(
        listener,
        tls,
        choreographd::http::router(daemon),
        std::time::Duration::from_secs(10),
    ));

    let roots = choreographd::tls::certs(&data.join("ca.pem")).unwrap();
    for host in ["localhost", "127.0.0.1"] {
        let c = Client::https(&format!("{host}:{port}"), roots.clone()).unwrap();
        assert!(
            c.health()
                .await
                .unwrap()
                .version
                .starts_with(env!("CARGO_PKG_VERSION"))
        );
    }
    let c = Client::https(&format!("localhost:{port}"), roots)
        .unwrap()
        .with_token("secret-a");
    let mut r = NodeReport {
        up: true,
        ..Default::default()
    };
    r.room.insert("gate".into(), 1);
    c.report("n1", &r).await.unwrap();
    let j = c
        .submit(&SubmitJob {
            name: "over tls".into(),
            needs: vec![NeedSpec {
                kind: "gate".into(),
                amount: 1,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(j.state, JobState::Running);
    assert_eq!(
        c.with_token("wrong").queue().await.unwrap_err().code(),
        Some("unauthorized")
    );
    let unnamed = Client::https(
        &format!("localhost:{port}"),
        choreographd::tls::certs(&data.join("ca.pem")).unwrap(),
    )
    .unwrap()
    .named("a");
    assert_eq!(
        unnamed.queue().await.unwrap_err().code(),
        Some("unauthorized"),
        "with tokens, a name alone does not get in"
    );

    // A client that does not trust the service's CA does not get through.
    let own = choreographd::tls::certs(&data.join("cert.pem")).unwrap();
    let stranger = Client::https(&format!("localhost:{port}"), own)
        .unwrap()
        .with_token("secret-a");
    assert!(matches!(
        stranger.health().await.unwrap_err(),
        choreographd::client::ClientError::Transport(_)
    ));
    server.abort();
}
