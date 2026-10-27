//! The HTTP layer: the routes of the [`api`](crate::api) table on a [`Daemon`], each call from a client: the
//! one its bearer token belongs to (`tokens = true`), else the one named in its [`CLIENT_HEADER`]. Every call
//! is one request and one answer; a wait is held at most its `timeout` (`0`: answer at once), so the calls fit
//! a proxy that carries single requests (a mesh). Refusals are an error status with an [`ApiError`] body:
//!
//! | Status | Code |
//! |---|---|
//! | 400 | `invalid` (a body or query that does not parse) |
//! | 401 | `unauthorized` |
//! | 403 | `forbidden` |
//! | 404 | `not_found` |
//! | 409 | `conflict`, `stale_lease` |
//! | 429 | `pair_loop` (a tell between two holders that answer each other too often) |
//! | 410 | `expired_lease` |
//! | 413 | `too_large` |
//! | 422 | `invalid` (a request that parses but cannot be done) |
//! | 503 | `unavailable` (the journal cannot be written; the service is stopping) |

use axum::Router;
use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, PathRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, FromRequestParts, Path, Query, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use choreograph::Dur;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::api::{
    ApiError, Health, NodeReport, Release, SubmitGroup, SubmitJob, TellJob, UseReport,
};
use crate::config::ClientConfig;
use crate::daemon::Daemon;
use crate::front::Vouched;
use crate::service::Refusal;

/// The service's routes on a daemon.
pub fn router(d: Daemon) -> Router {
    let limit = d.config().settings.max_body;
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/jobs", post(submit))
        .route("/v1/groups", post(submit_group))
        .route("/v1/jobs/{id}", get(job))
        .route("/v1/jobs/{id}/wait", post(wait))
        .route("/v1/jobs/{id}/cancel", post(cancel))
        .route("/v1/leases/{token}/heartbeat", post(heartbeat))
        .route("/v1/leases/{token}/release", post(release))
        .route("/v1/leases/{token}/pause", post(pause))
        .route("/v1/leases/{token}/resume", post(resume))
        .route("/v1/leases/{token}/usage", post(usage))
        .route("/v1/leases/{token}/restart", post(restart))
        .route("/v1/jobs/{id}/tell", post(tell))
        .route("/v1/nodes/{name}", post(report))
        .route("/v1/nodes/{name}/standing", get(standing))
        .route("/v1/queue", get(queue))
        .fallback(|| async { Fail::new(StatusCode::NOT_FOUND, "not_found", "no such route") })
        .layer(DefaultBodyLimit::max(limit))
        .with_state(d)
}

/// A refusal as an HTTP answer.
#[derive(Debug)]
pub struct Fail {
    status: StatusCode,
    body: ApiError,
}

impl Fail {
    pub(crate) fn new(status: StatusCode, code: &str, message: impl Into<String>) -> Fail {
        Fail {
            status,
            body: ApiError {
                error: code.into(),
                message: message.into(),
            },
        }
    }
}

impl From<Refusal> for Fail {
    fn from(r: Refusal) -> Fail {
        let status = match r {
            Refusal::Unauthorized => StatusCode::UNAUTHORIZED,
            Refusal::Forbidden(_) => StatusCode::FORBIDDEN,
            Refusal::NotFound(_) => StatusCode::NOT_FOUND,
            Refusal::Invalid(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Refusal::StaleLease(_) | Refusal::Conflict(_) => StatusCode::CONFLICT,
            Refusal::ExpiredLease(_) => StatusCode::GONE,
            Refusal::Loop(_) => StatusCode::TOO_MANY_REQUESTS,
            Refusal::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        };
        Fail::new(status, r.code(), r.to_string())
    }
}

impl From<BytesRejection> for Fail {
    fn from(r: BytesRejection) -> Fail {
        if r.status() == StatusCode::PAYLOAD_TOO_LARGE {
            Fail::new(r.status(), "too_large", r.body_text())
        } else {
            Fail::new(StatusCode::BAD_REQUEST, "invalid", r.body_text())
        }
    }
}

impl From<PathRejection> for Fail {
    fn from(r: PathRejection) -> Fail {
        Fail::new(StatusCode::NOT_FOUND, "not_found", r.body_text())
    }
}

impl From<QueryRejection> for Fail {
    fn from(r: QueryRejection) -> Fail {
        Fail::new(StatusCode::BAD_REQUEST, "invalid", r.body_text())
    }
}

impl IntoResponse for Fail {
    fn into_response(self) -> Response {
        let mut res = (self.status, axum::Json(self.body)).into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            res.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                "Bearer".parse().expect("a valid header"),
            );
        }
        res
    }
}

type Answer<T> = Result<axum::Json<T>, Fail>;

/// The header a caller names its client in when the service runs without tokens.
pub const CLIENT_HEADER: &str = "choreograph-client";

/// The client of a call: the one a [front](crate::front) vouches for, else from its `Authorization: Bearer`
/// header with tokens, else from its [`CLIENT_HEADER`].
struct Caller(ClientConfig);

impl FromRequestParts<Daemon> for Caller {
    type Rejection = Fail;

    async fn from_request_parts(parts: &mut Parts, d: &Daemon) -> Result<Caller, Fail> {
        if let Some(Vouched(c)) = parts.extensions.get::<Vouched>() {
            return Ok(Caller(c.clone()));
        }
        client(d, &parts.headers).map(Caller)
    }
}

/// The client a listener's call names: from its `Authorization: Bearer` header with tokens, else from its
/// [`CLIENT_HEADER`].
fn client(d: &Daemon, h: &HeaderMap) -> Result<ClientConfig, Fail> {
    let text = |name| {
        h.get(name)
            .map(|v| v.to_str().map_err(|_| Refusal::Unauthorized))
            .transpose()
    };
    let bearer = text(header::AUTHORIZATION.as_str())?
        .map(|v| v.strip_prefix("Bearer ").ok_or(Refusal::Unauthorized))
        .transpose()?
        .map(str::trim);
    Ok(d.caller(bearer, text(CLIENT_HEADER)?)?)
}

/// A JSON body; an empty one is `{}` for bodies whose fields are all optional.
fn body<T: DeserializeOwned>(b: Result<Bytes, BytesRejection>) -> Result<T, Fail> {
    let b = b?;
    let b: &[u8] = if b.iter().all(u8::is_ascii_whitespace) {
        b"{}"
    } else {
        &b
    };
    serde_json::from_slice(b)
        .map_err(|e| Fail::new(StatusCode::BAD_REQUEST, "invalid", e.to_string()))
}

async fn health(State(d): State<Daemon>) -> axum::Json<Health> {
    axum::Json(Health {
        version: crate::VERSION.into(),
        now: d.now(),
    })
}

async fn submit(
    State(d): State<Daemon>,
    Caller(c): Caller,
    b: Result<Bytes, BytesRejection>,
) -> Answer<crate::api::JobView> {
    let req: SubmitJob = body(b)?;
    Ok(axum::Json(d.call(|s, now| s.submit(&c, req, now))?))
}

async fn submit_group(
    State(d): State<Daemon>,
    Caller(c): Caller,
    b: Result<Bytes, BytesRejection>,
) -> Answer<Vec<crate::api::JobView>> {
    let req: SubmitGroup = body(b)?;
    Ok(axum::Json(d.call(|s, now| s.submit_group(&c, req, now))?))
}

async fn job(
    State(d): State<Daemon>,
    _: Caller,
    id: Result<Path<u64>, PathRejection>,
) -> Answer<crate::api::JobView> {
    let Path(id) = id?;
    Ok(axum::Json(d.look(|s, _| s.job(id))?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitQuery {
    /// `30s`, `2m` or milliseconds; at most the service's `max_wait`.
    timeout: Option<String>,
}

async fn wait(
    State(d): State<Daemon>,
    Caller(c): Caller,
    id: Result<Path<u64>, PathRejection>,
    q: Result<Query<WaitQuery>, QueryRejection>,
) -> Answer<crate::api::JobView> {
    let Path(id) = id?;
    let Query(q) = q?;
    let timeout = match q.timeout {
        None => None,
        Some(t) => Some(match t.parse::<i64>() {
            Ok(ms) => Dur(ms),
            Err(_) => t.parse::<Dur>().map_err(|e| {
                Fail::new(StatusCode::BAD_REQUEST, "invalid", format!("timeout: {e}"))
            })?,
        }),
    };
    Ok(axum::Json(d.wait(&c, id, timeout).await?))
}

async fn cancel(
    State(d): State<Daemon>,
    Caller(c): Caller,
    id: Result<Path<u64>, PathRejection>,
) -> Answer<crate::api::JobView> {
    let Path(id) = id?;
    Ok(axum::Json(d.call(|s, now| s.cancel(&c, id, now))?))
}

async fn heartbeat(
    State(d): State<Daemon>,
    Caller(c): Caller,
    token: Result<Path<u64>, PathRejection>,
) -> Answer<crate::api::BeatView> {
    let Path(token) = token?;
    Ok(axum::Json(d.call(|s, now| s.heartbeat(&c, token, now))?))
}

async fn release(
    State(d): State<Daemon>,
    Caller(c): Caller,
    token: Result<Path<u64>, PathRejection>,
    b: Result<Bytes, BytesRejection>,
) -> Answer<crate::api::JobView> {
    let Path(token) = token?;
    let r: Release = body(b)?;
    Ok(axum::Json(
        d.call(|s, now| s.release(&c, token, r.outcome, now))?,
    ))
}

async fn pause(
    State(d): State<Daemon>,
    Caller(c): Caller,
    token: Result<Path<u64>, PathRejection>,
) -> Answer<crate::api::JobView> {
    let Path(token) = token?;
    Ok(axum::Json(d.call(|s, now| s.pause(&c, token, now))?))
}

async fn resume(
    State(d): State<Daemon>,
    Caller(c): Caller,
    token: Result<Path<u64>, PathRejection>,
) -> Answer<crate::api::JobView> {
    let Path(token) = token?;
    Ok(axum::Json(d.call(|s, now| s.resume(&c, token, now))?))
}

async fn usage(
    State(d): State<Daemon>,
    Caller(c): Caller,
    token: Result<Path<u64>, PathRejection>,
    b: Result<Bytes, BytesRejection>,
) -> Answer<crate::api::UsageView> {
    let Path(token) = token?;
    let r: UseReport = body(b)?;
    Ok(axum::Json(
        d.call(|s, now| s.report_use(&c, token, r, now))?,
    ))
}

async fn restart(
    State(d): State<Daemon>,
    Caller(c): Caller,
    token: Result<Path<u64>, PathRejection>,
) -> Answer<crate::api::JobView> {
    let Path(token) = token?;
    Ok(axum::Json(d.call(|s, now| s.restart(&c, token, now))?))
}

async fn tell(
    State(d): State<Daemon>,
    Caller(c): Caller,
    id: Result<Path<u64>, PathRejection>,
    b: Result<Bytes, BytesRejection>,
) -> Answer<crate::api::JobView> {
    let Path(id) = id?;
    let t: TellJob = body(b)?;
    Ok(axum::Json(d.call(|s, now| s.tell(&c, id, t, now))?))
}

async fn report(
    State(d): State<Daemon>,
    Caller(c): Caller,
    name: Result<Path<String>, PathRejection>,
    b: Result<Bytes, BytesRejection>,
) -> Answer<crate::api::NodeView> {
    let Path(name) = name?;
    let r: NodeReport = body(b)?;
    Ok(axum::Json(d.call(|s, now| s.report(&c, &name, r, now))?))
}

async fn standing(
    State(d): State<Daemon>,
    _: Caller,
    name: Result<Path<String>, PathRejection>,
) -> Answer<crate::api::Standing> {
    let Path(name) = name?;
    Ok(axum::Json(d.look(|s, _| s.standing(&name))?))
}

async fn queue(State(d): State<Daemon>, _: Caller) -> Answer<crate::api::QueueView> {
    Ok(axum::Json(d.look(|s, _| s.queue())))
}
