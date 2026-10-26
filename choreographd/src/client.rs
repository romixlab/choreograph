//! A typed client of the service: every call of the [`api`](crate::api) table as a method.
//!
//! The transports, a new connection per call: [`Client::unix`] to the service's Unix socket, [`Client::http`]
//! to a plain TCP listener on loopback, [`Client::https`] to a TLS listener (the roots it trusts from a PEM
//! file), and [`Client::local`], straight into a [`Router`] in the same process (tests, an embedded service).
//! A client carries a token ([`Client::with_token`]) for a service with `tokens = true`, else the name it
//! calls as ([`Client::named`]; `local` when it gives none).
//!
//! ```no_run
//! # async fn f() -> Result<(), choreographd::client::ClientError> {
//! use choreographd::api::{NeedSpec, SubmitJob};
//! use choreographd::client::Client;
//!
//! let c = Client::unix("/run/choreographd/api.sock").named("build-1");
//! let job = c
//!     .submit(&SubmitJob {
//!         name: "gate".into(),
//!         needs: vec![NeedSpec { kind: "gate".into(), amount: 1, ..Default::default() }],
//!         ..Default::default()
//!     })
//!     .await?;
//! let job = c.wait_running(job.id).await?;
//! let lease = job.lease.expect("running");
//! // ... the work, with c.heartbeat(lease.token) now and then ...
//! c.release(lease.token, choreograph::Outcome::Ok).await?;
//! # Ok(())
//! # }
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use bytes::Bytes;
use choreograph::{Dur, Outcome};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::rt::{Read, Write};
use hyper::{Method, Request, StatusCode, header};
use hyper_util::rt::TokioIo;
use rustls::pki_types::{CertificateDer, ServerName};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_rustls::TlsConnector;
use tower::ServiceExt;

use crate::api::{
    ApiError, BeatView, Health, JobRef, JobState, JobView, NodeReport, NodeView, QueueView,
    Release, SubmitGroup, SubmitJob,
};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The service refused the call.
    #[error("{status}: {} ({})", .error.message, .error.error)]
    Api { status: u16, error: ApiError },
    /// The call did not reach the service or its answer did not come back.
    #[error("{0}")]
    Transport(String),
    /// The answer was not what the API says.
    #[error("unreadable answer: {0}")]
    Decode(String),
    #[error(transparent)]
    Tls(#[from] crate::tls::TlsError),
}

impl ClientError {
    /// The refusal's code (`stale_lease`, `forbidden`, ...), if the service refused.
    pub fn code(&self) -> Option<&str> {
        match self {
            ClientError::Api { error, .. } => Some(&error.error),
            _ => None,
        }
    }
}

#[derive(Clone)]
enum Transport {
    Https {
        /// `host:port`
        addr: String,
        name: ServerName<'static>,
        tls: TlsConnector,
    },
    /// `host:port`, plain HTTP.
    Http(String),
    Unix(PathBuf),
    Local(Router),
}

/// A client with its token or name. Cheap to clone.
#[derive(Clone)]
pub struct Client {
    transport: Transport,
    token: Option<String>,
    name: Option<String>,
}

impl Client {
    fn on(transport: Transport) -> Client {
        Client {
            transport,
            token: None,
            name: None,
        }
    }

    /// A client of the service on the Unix socket at `path`.
    pub fn unix(path: impl Into<PathBuf>) -> Client {
        Client::on(Transport::Unix(path.into()))
    }

    /// A client of the service at `addr` (`host:port`) over plain HTTP (a listener on loopback).
    pub fn http(addr: &str) -> Client {
        Client::on(Transport::Http(addr.into()))
    }

    /// A client of the service at `addr` (`host:port`) over TLS, trusting these roots; the host is the name
    /// its certificate is checked against.
    pub fn https(addr: &str, roots: Vec<CertificateDer<'static>>) -> Result<Client, ClientError> {
        let host = addr
            .rsplit_once(':')
            .map_or(addr, |(h, _)| h)
            .trim_start_matches('[')
            .trim_end_matches(']');
        let name = ServerName::try_from(host.to_string())
            .map_err(|e| ClientError::Transport(format!("{host}: {e}")))?;
        let mut store = rustls::RootCertStore::empty();
        for r in roots {
            store
                .add(r)
                .map_err(|e| ClientError::Transport(format!("a root certificate: {e}")))?;
        }
        let mut c = rustls::ClientConfig::builder_with_provider(crate::tls::provider())
            .with_safe_default_protocol_versions()
            .map_err(|e| ClientError::Transport(e.to_string()))?
            .with_root_certificates(store)
            .with_no_client_auth();
        c.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Client::on(Transport::Https {
            addr: addr.into(),
            name,
            tls: TlsConnector::from(Arc::new(c)),
        }))
    }

    /// A client calling the router in this process.
    pub fn local(router: Router) -> Client {
        Client::on(Transport::Local(router))
    }

    /// The same client with this token (a service with `tokens = true`).
    pub fn with_token(&self, token: &str) -> Client {
        Client {
            token: Some(token.into()),
            ..self.clone()
        }
    }

    /// The same client calling as `name` (a service without tokens).
    pub fn named(&self, name: &str) -> Client {
        Client {
            name: Some(name.into()),
            ..self.clone()
        }
    }

    pub async fn health(&self) -> Result<Health, ClientError> {
        self.call(Method::GET, "/v1/health", None::<&()>).await
    }

    pub async fn submit(&self, job: &SubmitJob) -> Result<JobView, ClientError> {
        self.call(Method::POST, "/v1/jobs", Some(job)).await
    }

    pub async fn submit_group(&self, group: &SubmitGroup) -> Result<Vec<JobView>, ClientError> {
        self.call(Method::POST, "/v1/groups", Some(group)).await
    }

    pub async fn job(&self, id: JobRef) -> Result<JobView, ClientError> {
        self.call(Method::GET, &format!("/v1/jobs/{id}"), None::<&()>)
            .await
    }

    /// Waits until the job runs or ends, or `timeout` passes (at most the service's `max_wait`; `None`:
    /// that; zero: answer at once), and answers with the job as it is then, its wait reason and ETA included.
    pub async fn wait(&self, id: JobRef, timeout: Option<Dur>) -> Result<JobView, ClientError> {
        let q = timeout.map_or(String::new(), |t| format!("?timeout={}", t.as_ms()));
        self.call(Method::POST, &format!("/v1/jobs/{id}/wait{q}"), None::<&()>)
            .await
    }

    /// Waits, as long as it takes, until the job runs or ends.
    pub async fn wait_running(&self, id: JobRef) -> Result<JobView, ClientError> {
        loop {
            let v = self.wait(id, None).await?;
            if v.state != JobState::Queued {
                return Ok(v);
            }
        }
    }

    pub async fn cancel(&self, id: JobRef) -> Result<JobView, ClientError> {
        self.call(Method::POST, &format!("/v1/jobs/{id}/cancel"), None::<&()>)
            .await
    }

    pub async fn heartbeat(&self, token: u64) -> Result<BeatView, ClientError> {
        self.call(
            Method::POST,
            &format!("/v1/leases/{token}/heartbeat"),
            None::<&()>,
        )
        .await
    }

    pub async fn release(&self, token: u64, outcome: Outcome) -> Result<JobView, ClientError> {
        self.call(
            Method::POST,
            &format!("/v1/leases/{token}/release"),
            Some(&Release { outcome }),
        )
        .await
    }

    pub async fn report(&self, node: &str, r: &NodeReport) -> Result<NodeView, ClientError> {
        self.call(Method::POST, &format!("/v1/nodes/{node}"), Some(r))
            .await
    }

    pub async fn queue(&self) -> Result<QueueView, ClientError> {
        self.call(Method::GET, "/v1/queue", None::<&()>).await
    }

    async fn call<B: Serialize, T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<T, ClientError> {
        let body = match body {
            Some(b) => Bytes::from(serde_json::to_vec(b).expect("requests serialise")),
            None => Bytes::new(),
        };
        let mut req = Request::builder()
            .method(method)
            .uri(path)
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(t) = &self.token {
            req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }
        if let Some(n) = &self.name {
            req = req.header(crate::http::CLIENT_HEADER, n.as_str());
        }
        let (status, bytes) = match &self.transport {
            Transport::Local(router) => {
                let req = req
                    .body(axum::body::Body::from(body))
                    .map_err(|e| ClientError::Transport(e.to_string()))?;
                let res = router
                    .clone()
                    .oneshot(req)
                    .await
                    .unwrap_or_else(|e| match e {});
                let status = res.status();
                let bytes = res
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| ClientError::Transport(e.to_string()))?
                    .to_bytes();
                (status, bytes)
            }
            Transport::Https { addr, name, tls } => {
                let t = |e: &dyn std::fmt::Display| ClientError::Transport(format!("{addr}: {e}"));
                let tcp = tokio::net::TcpStream::connect(addr.as_str())
                    .await
                    .map_err(|e| t(&e))?;
                let stream = tls.connect(name.clone(), tcp).await.map_err(|e| t(&e))?;
                let req = req.header(header::HOST, addr.as_str());
                exchange(TokioIo::new(stream), req, body)
                    .await
                    .map_err(|e| t(&e))?
            }
            Transport::Http(addr) => {
                let t = |e: &dyn std::fmt::Display| ClientError::Transport(format!("{addr}: {e}"));
                let tcp = tokio::net::TcpStream::connect(addr.as_str())
                    .await
                    .map_err(|e| t(&e))?;
                let req = req.header(header::HOST, addr.as_str());
                exchange(TokioIo::new(tcp), req, body)
                    .await
                    .map_err(|e| t(&e))?
            }
            Transport::Unix(path) => {
                let t = |e: &dyn std::fmt::Display| {
                    ClientError::Transport(format!("{}: {e}", path.display()))
                };
                let sock = tokio::net::UnixStream::connect(path)
                    .await
                    .map_err(|e| t(&e))?;
                let req = req.header(header::HOST, "localhost");
                exchange(TokioIo::new(sock), req, body)
                    .await
                    .map_err(|e| t(&e))?
            }
        };
        decode(status, &bytes)
    }
}

/// One request and its answer on a new HTTP/1.1 connection.
async fn exchange<IO>(
    io: IO,
    req: hyper::http::request::Builder,
    body: Bytes,
) -> Result<(StatusCode, Bytes), String>
where
    IO: Read + Write + Unpin + Send + 'static,
{
    let req = req.body(Full::new(body)).map_err(|e| e.to_string())?;
    let (mut send, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|e| e.to_string())?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let res: hyper::Response<Incoming> = send.send_request(req).await.map_err(|e| e.to_string())?;
    let status = res.status();
    let bytes = res
        .into_body()
        .collect()
        .await
        .map_err(|e| e.to_string())?
        .to_bytes();
    Ok((status, bytes))
}

fn decode<T: DeserializeOwned>(status: StatusCode, bytes: &[u8]) -> Result<T, ClientError> {
    if status.is_success() {
        return serde_json::from_slice(bytes).map_err(|e| ClientError::Decode(e.to_string()));
    }
    let error = serde_json::from_slice::<ApiError>(bytes).unwrap_or_else(|_| ApiError {
        error: "http".into(),
        message: String::from_utf8_lossy(bytes).into_owned(),
    });
    Err(ClientError::Api {
        status: status.as_u16(),
        error,
    })
}
