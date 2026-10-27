//! A front in the same process: the service's API for a proxy that carries calls from elsewhere and has
//! verified who makes each one itself (a mesh, a message bus), with no HTTP listener and no token.
//!
//! The proxy hands over one [`ApiRequest`] (method, path, query, body) with the identity it vouches for, and
//! gets back the [`ApiAnswer`] of the same [router](crate::http::router) the listeners serve: its status and
//! its body, byte for byte. The identity is matched exactly against the `principal` of the `[[client]]`s
//! (`"NODE"` or `"NODE/USER"`), and the call is then that client's, with the same rights as over its token:
//! the nodes it may report, its own jobs and leases only. An identity no client names is refused
//! (`401 unauthorized`); headers never reach this path, so neither a token nor a client name in a header
//! counts here.
//!
//! The front trusts its embedder: whoever holds a [`Front`] may call as any principal. Building one is the
//! embedder's statement that it verified the caller before, and that it passes `"NODE"` only for the machine
//! itself (whoever administers it), never for another user on it, who is `"NODE/USER"`: a `report` right on
//! a node belongs to the node's principal alone.
//!
//! ```no_run
//! # async fn f(daemon: choreographd::daemon::Daemon) {
//! use choreographd::front::{ApiRequest, Front};
//!
//! let front = Front::new(daemon);
//! let answer = front
//!     .call("node-a", ApiRequest::get("/v1/nodes/node-a/standing"))
//!     .await;
//! assert_eq!(answer.status, 200);
//! # }
//! ```

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::response::IntoResponse;
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

use crate::config::ClientConfig;
use crate::daemon::Daemon;
use crate::http::Fail;
use crate::service::Refusal;

/// One call of the API as data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiRequest {
    /// `GET` or `POST`, as the API table says for the path.
    pub method: String,
    /// The route with its parameters, `/v1/jobs/7/wait`; no query, no fragment.
    pub path: String,
    /// The query without its `?`, `timeout=30s`; empty for none.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub query: String,
    /// The JSON body as text, exactly as a listener would read it; empty for none.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub body: String,
}

impl ApiRequest {
    pub fn get(path: impl Into<String>) -> ApiRequest {
        ApiRequest {
            method: "GET".into(),
            path: path.into(),
            query: String::new(),
            body: String::new(),
        }
    }

    pub fn post(path: impl Into<String>, body: impl Into<String>) -> ApiRequest {
        ApiRequest {
            method: "POST".into(),
            path: path.into(),
            query: String::new(),
            body: body.into(),
        }
    }
}

/// The router's answer: its status and its body, the JSON text the listeners send.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiAnswer {
    pub status: u16,
    pub body: String,
}

/// The identity a front vouches for, put on the request for the routes' caller extractor. Only this module
/// makes one; a listener's request never carries it.
#[derive(Clone, Debug)]
pub(crate) struct Vouched(pub(crate) ClientConfig);

/// The service's routes for an embedding proxy. Cheap to clone.
#[derive(Clone)]
pub struct Front {
    daemon: Daemon,
    router: Router,
}

impl Front {
    pub fn new(daemon: Daemon) -> Front {
        Front {
            router: crate::http::router(daemon.clone()),
            daemon,
        }
    }

    /// The call `req` made by `principal`, as the front vouches; the answer the router gives it.
    pub async fn call(&self, principal: &str, req: ApiRequest) -> ApiAnswer {
        let Some(client) = self.daemon.client_by_principal(principal) else {
            return answer(Fail::from(Refusal::Unauthorized).into_response()).await;
        };
        let mut http = match request(req) {
            Ok(r) => r,
            Err(e) => {
                return answer(Fail::new(StatusCode::BAD_REQUEST, "invalid", e).into_response())
                    .await;
            }
        };
        http.extensions_mut().insert(Vouched(client));
        let res = self
            .router
            .clone()
            .oneshot(http)
            .await
            .unwrap_or_else(|e| match e {});
        answer(res).await
    }
}

/// The HTTP request of `req`, with no headers but the body's type.
fn request(req: ApiRequest) -> Result<Request<Body>, String> {
    let method: Method = req
        .method
        .parse()
        .map_err(|_| format!("method {:?}", req.method))?;
    if !req.path.starts_with('/') || req.path.contains(['?', '#']) {
        return Err(format!(
            "path {:?}: a route starting with '/', without query or fragment",
            req.path
        ));
    }
    if req.query.contains('#') {
        return Err(format!("query {:?}: no fragment", req.query));
    }
    let uri = if req.query.is_empty() {
        req.path
    } else {
        format!("{}?{}", req.path, req.query)
    };
    Request::builder()
        .method(method)
        .uri(uri)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(req.body))
        .map_err(|e| e.to_string())
}

async fn answer(res: axum::response::Response) -> ApiAnswer {
    let status = res.status().as_u16();
    let bytes = match res.into_body().collect().await {
        Ok(b) => b.to_bytes(),
        Err(e) => {
            return ApiAnswer {
                status: 500,
                body: e.to_string(),
            };
        }
    };
    ApiAnswer {
        status,
        // the routes answer JSON, which is UTF-8
        body: String::from_utf8_lossy(&bytes).into_owned(),
    }
}
