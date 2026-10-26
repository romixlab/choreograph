//! TLS without C: rustls on the pure-Rust graviola crypto provider, HTTP/1.1 through hyper, one task per
//! connection.
//!
//! The certificate and key paths of the configuration may be relative: they resolve against
//! `$CREDENTIALS_DIRECTORY` when it is set (systemd's `LoadCredential=`), else against the configuration
//! file's directory.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::Router;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use rustls::ServerConfig;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use crate::config::TlsFiles;

/// Why the TLS files cannot be used. Only a person can fix it (exit 78).
#[derive(Debug, thiserror::Error)]
#[error("{path}: {err}")]
pub struct TlsError {
    pub path: PathBuf,
    pub err: String,
}

/// The crypto provider every TLS config of this crate uses.
pub fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls_graviola::default_provider())
}

/// Where a TLS file of the configuration is: an absolute path as it is, a relative one in the credentials
/// directory or beside the configuration file.
pub fn resolve(path: &Path, config_dir: &Path, credentials: Option<&Path>) -> PathBuf {
    if path.is_absolute() {
        return path.into();
    }
    credentials.unwrap_or(config_dir).join(path)
}

/// Reads PEM certificates (a chain, leaf first, or the roots a client trusts).
pub fn certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let err = |e: &dyn std::fmt::Display| TlsError {
        path: path.into(),
        err: e.to_string(),
    };
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|e| err(&e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| err(&e))?;
    if certs.is_empty() {
        return Err(err(&"no certificate in it"));
    }
    Ok(certs)
}

/// The server's TLS settings from its certificate chain and key: TLS 1.2 and 1.3, HTTP/1.1.
pub fn server_config(files: &TlsFiles) -> Result<Arc<ServerConfig>, TlsError> {
    let chain = certs(&files.cert)?;
    let key = PrivateKeyDer::from_pem_file(&files.key).map_err(|e| TlsError {
        path: files.key.clone(),
        err: e.to_string(),
    })?;
    let mut c = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .and_then(|b| b.with_no_client_auth().with_single_cert(chain, key))
        .map_err(|e| TlsError {
            path: files.cert.clone(),
            err: e.to_string(),
        })?;
    c.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(c))
}

/// Serves the router on the listener over TLS until the task is dropped. A handshake that takes longer than
/// `handshake` is given up.
pub async fn serve(
    listener: TcpListener,
    tls: Arc<ServerConfig>,
    router: Router,
    handshake: std::time::Duration,
) -> std::io::Error {
    let acceptor = TlsAcceptor::from(tls);
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(c) => c,
            Err(e) if is_transient(&e) => {
                tracing::warn!("accept: {e}");
                // Out of file descriptors: give running connections a moment to end instead of spinning.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
            Err(e) => return e,
        };
        let acceptor = acceptor.clone();
        let svc = TowerToHyperService::new(router.clone());
        tokio::spawn(async move {
            let stream = match tokio::time::timeout(handshake, acceptor.accept(tcp)).await {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    tracing::debug!("{peer}: TLS handshake failed: {e}");
                    return;
                }
                Err(_) => {
                    tracing::debug!("{peer}: TLS handshake timed out");
                    return;
                }
            };
            let conn = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .serve_connection(TokioIo::new(stream), svc);
            if let Err(e) = conn.await {
                tracing::debug!("{peer}: {e}");
            }
        });
    }
}

/// Accept errors that concern one connection or a passing shortage (too many open files), not the listener.
fn is_transient(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;
    matches!(
        e.kind(),
        ConnectionAborted | ConnectionReset | Interrupted | WouldBlock
    ) || e.raw_os_error() == Some(24)
        || e.raw_os_error() == Some(23)
}
