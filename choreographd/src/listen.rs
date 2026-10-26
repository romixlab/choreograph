//! Where the service listens: a Unix socket (plain HTTP; the socket's file mode decides who may call) or TCP,
//! plain on loopback or over TLS (see [`tls`](crate::tls)).

use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::Router;
use rustls::ServerConfig;
use tokio::net::{TcpListener, UnixListener};

use crate::config::Listen;

/// Why the service cannot listen. [`ListenError::needs_a_person`]: no restart helps (exit 78).
#[derive(Debug, thiserror::Error)]
#[error("listen on {at}: {err}")]
pub struct ListenError {
    pub at: String,
    pub err: String,
    pub needs_a_person: bool,
}

/// A bound listener.
pub enum Bound {
    Unix(UnixListener),
    Tcp(TcpListener),
}

/// Binds the listener. A Unix socket left by an earlier run is replaced (the caller holds the state lock, so
/// no other instance serves it); anything else at its path is left alone and refused. The new socket gets
/// `mode`.
pub async fn bind(listen: &Listen, mode: u32) -> Result<Bound, ListenError> {
    let fail = |err: &dyn std::fmt::Display, needs_a_person| ListenError {
        at: listen.to_string(),
        err: err.to_string(),
        needs_a_person,
    };
    match listen {
        Listen::Tcp(a) => TcpListener::bind(a)
            .await
            .map(Bound::Tcp)
            .map_err(|e| fail(&e, false)),
        Listen::Unix(path) => {
            match std::fs::symlink_metadata(path) {
                Ok(m) if m.file_type().is_socket() => {
                    std::fs::remove_file(path).map_err(|e| fail(&e, false))?;
                }
                Ok(_) => return Err(fail(&"something that is not a socket is there", true)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(fail(&e, false)),
            }
            let l = UnixListener::bind(path).map_err(|e| fail(&e, false))?;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
                .map_err(|e| fail(&e, false))?;
            Ok(Bound::Unix(l))
        }
    }
}

impl Bound {
    /// The socket's path, for a Unix listener.
    pub fn path(&self) -> Option<PathBuf> {
        match self {
            Bound::Unix(l) => l
                .local_addr()
                .ok()
                .and_then(|a| a.as_pathname().map(Path::to_path_buf)),
            Bound::Tcp(_) => None,
        }
    }

    /// Serves the router until the task is dropped or the listener fails: over TLS when `tls` is given (TCP
    /// only), else plain HTTP/1.1.
    pub async fn serve(
        self,
        tls: Option<Arc<ServerConfig>>,
        router: Router,
        handshake: std::time::Duration,
    ) -> std::io::Error {
        let r = match (self, tls) {
            (Bound::Tcp(l), Some(tls)) => {
                return crate::tls::serve(l, tls, router, handshake).await;
            }
            (Bound::Tcp(l), None) => axum::serve(l, router).await,
            (Bound::Unix(l), None) => axum::serve(l, router).await,
            (Bound::Unix(_), Some(_)) => {
                return std::io::Error::other("TLS on a Unix socket");
            }
        };
        match r {
            Ok(()) => std::io::Error::other("the listener ended"),
            Err(e) => e,
        }
    }
}
