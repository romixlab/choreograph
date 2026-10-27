//! `choreographd`: the choreograph scheduler as a service.
//!
//! Exit codes: 0 on a clean stop (SIGTERM, Ctrl-C), 78 when only a person can help (a bad configuration, TLS
//! files that cannot be read, a journal that is unreadable or written by a newer version, something other
//! than a socket at the socket's path), 1 for anything a restart may fix (the port is taken, the state
//! directory is locked, a journal write failed).

use std::io::{IsTerminal, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{CommandFactory, Parser, Subcommand, ValueHint};
use clap_complete::Shell;
use tracing_subscriber::EnvFilter;

use choreographd::VERSION;
use choreographd::config::{Config, new_token, token_hash};
use choreographd::daemon::{Clock, Daemon};
use choreographd::service::Service;
use choreographd::store::{STATE_VERSION, Store};

/// Only a person can fix it: systemd does not restart on this code (`RestartPreventExitStatus=78`).
const NEEDS_A_PERSON: u8 = 78;

const DEFAULT_CONFIG: &str = "/etc/choreographd/config.toml";

/// The choreograph scheduler as a service: a JSON API on a Unix socket (or TCP, with TLS and tokens) for
/// submitting jobs, waiting for them, leases and node reports.
#[derive(Parser)]
#[command(name = "choreographd", version = VERSION)]
struct Cli {
    /// Print the version and the state version of the journal as JSON, and exit with 78 when the journal is
    /// newer than this binary reads (for deploys checking a new binary before it starts).
    #[arg(long, hide = true)]
    self_check: bool,
    /// The configuration file for --self-check (default /etc/choreographd/config.toml).
    #[arg(long, value_hint = ValueHint::FilePath, requires = "self_check")]
    config: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Serve: listen, plan and journal until stopped.
    Serve {
        /// The configuration file.
        #[arg(long, default_value = DEFAULT_CONFIG, value_hint = ValueHint::FilePath)]
        config: PathBuf,
    },
    /// Check the configuration, the TLS files (if any) and the journal without changing anything.
    Check {
        /// The configuration file.
        #[arg(long, default_value = DEFAULT_CONFIG, value_hint = ValueHint::FilePath)]
        config: PathBuf,
    },
    /// Make a new client token: prints the token (give it to the client) and the `[[client]]` block for a
    /// configuration with `tokens = true` (it holds only the token's hash).
    Token {
        /// The client's name.
        name: String,
        /// The nodes it may report (repeat it; `*` for any).
        #[arg(long)]
        report: Vec<String>,
    },
    /// Print the `token_sha256` of a token read from standard input.
    HashToken,
    /// Print the shell completion script.
    Completions {
        /// bash, zsh, fish, elvish or powershell.
        shell: Shell,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if cli.self_check {
        return self_check(cli.config.as_deref().unwrap_or(Path::new(DEFAULT_CONFIG)));
    }
    match cli.cmd {
        None => {
            let _ = Cli::command().print_help();
            ExitCode::from(2)
        }
        Some(Cmd::Serve { config }) => serve(&config),
        Some(Cmd::Check { config }) => check(&config),
        Some(Cmd::Token { name, report }) => token(&name, &report),
        Some(Cmd::HashToken) => {
            let mut t = String::new();
            if let Err(e) = std::io::stdin().read_to_string(&mut t) {
                eprintln!("error: standard input: {e}");
                return ExitCode::FAILURE;
            }
            println!("{}", token_hash(t.trim()));
            ExitCode::SUCCESS
        }
        Some(Cmd::Completions { shell }) => {
            clap_complete::generate(
                shell,
                &mut Cli::command(),
                "choreographd",
                &mut std::io::stdout(),
            );
            ExitCode::SUCCESS
        }
    }
}

fn needs_a_person(what: impl std::fmt::Display) -> ExitCode {
    eprintln!("error: {what}");
    tracing::error!("{what}");
    ExitCode::from(NEEDS_A_PERSON)
}

fn failed(what: impl std::fmt::Display) -> ExitCode {
    eprintln!("error: {what}");
    tracing::error!("{what}");
    ExitCode::FAILURE
}

/// The configuration with its TLS paths resolved (see [`choreographd::tls`]).
fn load(path: &Path) -> Result<Config, ExitCode> {
    let mut c = Config::load(path).map_err(|e| needs_a_person(format!("configuration: {e}")))?;
    let dir = path.parent().unwrap_or(Path::new("."));
    let creds = std::env::var_os("CREDENTIALS_DIRECTORY").map(PathBuf::from);
    if let Some(tls) = &mut c.tls {
        tls.cert = choreographd::tls::resolve(&tls.cert, dir, creds.as_deref());
        tls.key = choreographd::tls::resolve(&tls.key, dir, creds.as_deref());
    }
    Ok(c)
}

/// The server's TLS settings, if the configuration has `[tls]`.
fn tls(config: &Config) -> Result<Option<std::sync::Arc<rustls::ServerConfig>>, ExitCode> {
    config
        .tls
        .as_ref()
        .map(choreographd::tls::server_config)
        .transpose()
        .map_err(|e| needs_a_person(format!("TLS: {e}")))
}

fn logging() {
    let colour = std::io::stderr().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("choreographd=info")),
        )
        .with_ansi(colour)
        .with_writer(std::io::stderr)
        .init();
}

fn serve(path: &Path) -> ExitCode {
    logging();
    let config = match load(path) {
        Ok(c) => c,
        Err(code) => return code,
    };
    let tls = match tls(&config) {
        Ok(t) => t,
        Err(code) => return code,
    };
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return failed(format!("runtime: {e}")),
    };
    rt.block_on(async move {
        let (listen, mode) = (config.listen.clone(), config.socket_mode);
        let handshake = std::time::Duration::from_millis(config.settings.handshake.as_ms().max(0) as u64);
        let daemon = match Daemon::open(config, Clock::wall()) {
            Ok(d) => d,
            Err(e) if e.needs_a_person() => return needs_a_person(format!("state: {e}")),
            Err(e) => return failed(format!("state: {e}")),
        };
        let listener = match choreographd::listen::bind(&listen, mode).await {
            Ok(l) => l,
            Err(e) if e.needs_a_person => return needs_a_person(e),
            Err(e) => return failed(e),
        };
        let how = if tls.is_some() { "TLS" } else { "plain HTTP" };
        tracing::info!("choreographd {VERSION} listening on {listen} ({how})");
        let router = choreographd::http::router(daemon.clone());
        tokio::select! {
            e = listener.serve(tls, router, handshake) => failed(format!("listen on {listen}: {e}")),
            why = daemon.run_ticker() => failed(why),
            () = stopped() => {
                tracing::info!("stopping");
                ExitCode::SUCCESS
            }
        }
    })
}

/// Resolves on SIGTERM or Ctrl-C.
async fn stopped() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("a SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

fn check(path: &Path) -> ExitCode {
    let config = match load(path) {
        Ok(c) => c,
        Err(code) => return code,
    };
    if let Err(code) = tls(&config) {
        return code;
    }
    match Service::check(&config) {
        Ok((jobs, nodes)) => {
            println!(
                "{}: listen on {}{}, {} kinds, {}; journal in {}: {jobs} jobs, {nodes} nodes",
                path.display(),
                config.listen,
                if config.tls.is_some() { " (TLS)" } else { "" },
                config.kinds.len(),
                clients(&config),
                config.state_dir.display()
            );
            ExitCode::SUCCESS
        }
        Err(e) if e.needs_a_person() => needs_a_person(format!("state: {e}")),
        Err(e) => failed(format!("state: {e}")),
    }
}

fn self_check(path: &Path) -> ExitCode {
    let data = Config::load(path)
        .map_err(|e| e.to_string())
        .and_then(|c| Store::data_version(&c.state_dir).map_err(|e| e.to_string()));
    let (data, err) = match data {
        Ok(v) => (v, None),
        Err(e) => (None, Some(e)),
    };
    let out = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "sha": env!("GIT_SHA"),
        "state_version": STATE_VERSION,
        "data": data,
        "error": err,
    });
    println!("{out}");
    if data.is_some_and(|v| v > STATE_VERSION) {
        eprintln!(
            "error: the journal has state version {}, this binary reads up to {STATE_VERSION}",
            data.unwrap_or_default()
        );
        return ExitCode::from(NEEDS_A_PERSON);
    }
    ExitCode::SUCCESS
}

fn token(name: &str, report: &[String]) -> ExitCode {
    let t = match new_token() {
        Ok(t) => t,
        Err(e) => return failed(format!("no randomness from the system: {e}")),
    };
    println!("token: {t}");
    println!();
    println!("# in a configuration with tokens = true");
    println!("[[client]]");
    println!("name = {}", toml_str(name));
    println!("token_sha256 = \"{}\"", token_hash(&t));
    if !report.is_empty() {
        let r: Vec<String> = report.iter().map(|n| toml_str(n)).collect();
        println!("report = [{}]", r.join(", "));
    }
    ExitCode::SUCCESS
}

fn toml_str(s: &str) -> String {
    toml::Value::String(s.into()).to_string()
}

/// The clients `check` names: how many call with a token, how many by principal.
fn clients(config: &Config) -> String {
    let count = |f: fn(&choreographd::config::ClientConfig) -> bool| {
        config.clients.iter().filter(|c| f(c)).count()
    };
    let tokens = count(|c| c.token_sha256.is_some());
    let principals = count(|c| c.principal.is_some());
    match (config.tokens, principals) {
        (true, 0) => format!("{tokens} clients with tokens"),
        (true, p) => format!("{tokens} clients with tokens, {p} by principal"),
        (false, 0) => "no tokens".into(),
        (false, p) => format!("no tokens, {p} clients by principal"),
    }
}
