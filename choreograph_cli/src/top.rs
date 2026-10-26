//! `choreograph top`: a live terminal view of a running `choreographd`, over its public client/API only
//! ([`choreographd::client::Client`]) — nothing here reaches into the service's own state.
//!
//! Redraws on an interval by default; `--once` prints a single snapshot, `--json` the queue view as JSON (for
//! agents and the bot) and implies `--once`. Colour is used only on a terminal, unless `--color` says
//! otherwise.

use std::fmt::Write as _;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration as StdDur;

use choreograph::{Dur, Outcome, Time};
use choreographd::api::{JobView, NodeView, QueueView, WaitView};
use choreographd::client::Client;
use clap::{Args, ValueHint};

#[derive(Args)]
pub struct TopArgs {
    /// The service's Unix socket.
    #[arg(long, default_value = "/run/choreographd/api.sock", value_hint = ValueHint::FilePath)]
    socket: PathBuf,
    /// The service at `host:port` over plain HTTP, instead of the socket.
    #[arg(long, conflicts_with = "https")]
    http: Option<String>,
    /// The service at `host:port` over TLS, instead of the socket.
    #[arg(long, requires = "ca", conflicts_with = "http")]
    https: Option<String>,
    /// The TLS roots to trust, PEM (with --https).
    #[arg(long, value_hint = ValueHint::FilePath)]
    ca: Option<PathBuf>,
    /// The client's token (a service with `tokens = true`).
    #[arg(long)]
    token: Option<String>,
    /// The client's name (a service without tokens calls itself `local` otherwise).
    #[arg(long = "as")]
    as_name: Option<String>,
    /// Redraw every this long (`1h30m`-style, or milliseconds).
    #[arg(long, default_value = "2s")]
    interval: String,
    /// One snapshot, then exit.
    #[arg(long)]
    once: bool,
    /// The queue, as JSON, once (for agents and the bot); implies --once.
    #[arg(long)]
    json: bool,
    /// Colour in the output: auto (a terminal without NO_COLOR), always or never.
    #[arg(long, value_enum, default_value_t = Color::Auto)]
    color: Color,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum Color {
    Auto,
    Always,
    Never,
}

pub fn run(args: TopArgs) -> ExitCode {
    let interval: Dur = match args.interval.parse() {
        Ok(d) => d,
        Err(e) => return failed(format!("--interval: {e}")),
    };
    let client = match connect(&args) {
        Ok(c) => c,
        Err(e) => return failed(e),
    };
    let once = args.once || args.json;
    let color = match args.color {
        Color::Always => true,
        Color::Never => false,
        Color::Auto => std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none(),
    };
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return failed(format!("starting: {e}")),
    };
    rt.block_on(async move {
        let stdout = std::io::stdout();
        let clear_each_time = !once && stdout.is_terminal();
        loop {
            let view = match client.queue().await {
                Ok(v) => v,
                Err(e) => return failed(format!("{e}")),
            };
            let mut out = std::io::stdout();
            if args.json {
                let Ok(json) = serde_json::to_string_pretty(&view) else {
                    return failed("the queue view would not serialise");
                };
                let _ = writeln!(out, "{json}");
            } else {
                if clear_each_time {
                    let _ = write!(out, "\x1b[2J\x1b[H");
                }
                let _ = write!(out, "{}", render(&view, color));
            }
            let _ = out.flush();
            if once {
                return ExitCode::SUCCESS;
            }
            tokio::time::sleep(StdDur::from_millis(interval.as_ms().max(0) as u64)).await;
        }
    })
}

fn connect(args: &TopArgs) -> Result<Client, String> {
    let mut c = if let Some(addr) = &args.https {
        let ca = args.ca.as_ref().expect("clap requires --ca with --https");
        let roots = choreographd::tls::certs(ca).map_err(|e| format!("{ca:?}: {e}"))?;
        Client::https(addr, roots).map_err(|e| e.to_string())?
    } else if let Some(addr) = &args.http {
        Client::http(addr)
    } else {
        Client::unix(&args.socket)
    };
    if let Some(t) = &args.token {
        c = c.with_token(t);
    }
    if let Some(n) = &args.as_name {
        c = c.named(n);
    }
    Ok(c)
}

fn failed(what: impl std::fmt::Display) -> ExitCode {
    eprintln!("error: {what}");
    ExitCode::FAILURE
}

/// The whole view, as text for a terminal. Pure, so a fixed [`QueueView`] renders the same every time
/// (`top_test.rs`-style snapshot tests).
pub fn render(view: &QueueView, color: bool) -> String {
    let mut out = String::new();
    let up = view.nodes.iter().filter(|n| n.up).count();
    let down = view.nodes.len() - up;
    let _ = writeln!(
        out,
        "{} — {} nodes ({up} up, {down} down), {} running, {} queued, {} done",
        bold(color, "choreograph top"),
        view.nodes.len(),
        view.running.len(),
        view.queued.len(),
        view.done.len(),
    );

    let _ = writeln!(out, "\n{}", bold(color, "NODES"));
    if view.nodes.is_empty() {
        let _ = writeln!(out, "  (none reported yet)");
    }
    for n in &view.nodes {
        render_node(&mut out, color, n);
    }

    let _ = writeln!(out, "\n{}", bold(color, "RUNNING"));
    if view.running.is_empty() {
        let _ = writeln!(out, "  (none)");
    }
    for j in &view.running {
        render_running(&mut out, j, view.now);
    }

    let _ = writeln!(out, "\n{}", bold(color, "QUEUED (priority order)"));
    if view.queued.is_empty() {
        let _ = writeln!(out, "  (none)");
    }
    for j in &view.queued {
        render_queued(&mut out, color, j, view.now);
    }

    if !view.done.is_empty() {
        let _ = writeln!(out, "\n{}", bold(color, "DONE"));
        for j in &view.done {
            render_done(&mut out, color, j, view.now);
        }
    }
    out
}

fn render_node(out: &mut String, color: bool, n: &NodeView) {
    let state = if n.up {
        paint(color, "32", "up")
    } else {
        paint(color, "31", "down")
    };
    let resources = if n.resources.is_empty() {
        "-".to_string()
    } else {
        n.resources
            .iter()
            .map(|r| format!("{} {}/{}", r.name, r.used, r.capacity))
            .collect::<Vec<_>>()
            .join("  ")
    };
    let labels = fmt_labels(&n.labels);
    let policy = fmt_labels(&n.policy);
    let _ = writeln!(
        out,
        "  {}  {state}  {resources}  labels={labels}  policy={policy}",
        n.name
    );
}

fn render_running(out: &mut String, j: &JobView, now: Time) {
    let lease = j.lease.as_ref().map_or("-".to_string(), |l| {
        let held = l
            .held
            .iter()
            .map(|h| format!("{} ({})", h.resource, h.kind))
            .collect::<Vec<_>>()
            .join(", ");
        format!("holds {held}, heartbeat within {}", l.expires - now)
    });
    let eta = j.eta.map_or("-".to_string(), |e| {
        format!(
            "finish p50 {} p90 {}",
            relative(now, e.finish_p50),
            relative(now, e.finish_p90)
        )
    });
    let _ = writeln!(
        out,
        "  #{}  {:?}  {}  {lease}  eta {eta}",
        j.id, j.name, j.client
    );
}

fn render_queued(out: &mut String, color: bool, j: &JobView, now: Time) {
    let wait = j
        .wait
        .as_ref()
        .map_or("-".to_string(), |w| paint(color, "33", &fmt_wait(w)));
    let eta = j.eta.map_or("-".to_string(), |e| {
        format!(
            "start p50 {} p90 {}",
            relative(now, e.start_p50),
            relative(now, e.start_p90)
        )
    });
    let _ = writeln!(
        out,
        "  #{}  {:?}  {}  waiting: {wait}  eta {eta}",
        j.id, j.name, j.client
    );
}

fn render_done(out: &mut String, color: bool, j: &JobView, now: Time) {
    let outcome = j.outcome.map_or("-", outcome_str);
    let painted = paint(
        color,
        if j.outcome == Some(Outcome::Ok) {
            "32"
        } else {
            "31"
        },
        outcome,
    );
    let when = j
        .done_at
        .map_or("-".to_string(), |t| format!("{} ago", now - t));
    let _ = writeln!(
        out,
        "  #{}  {:?}  {}  {painted}  {when}",
        j.id, j.name, j.client
    );
}

fn fmt_wait(w: &WaitView) -> String {
    w.text.clone()
}

fn fmt_labels(labels: &choreograph::model::Labels) -> String {
    if labels.is_empty() {
        return "-".to_string();
    }
    labels
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn outcome_str(o: Outcome) -> &'static str {
    match o {
        Outcome::Ok => "ok",
        Outcome::Failed => "failed",
        Outcome::Cancelled => "cancelled",
        Outcome::Cached => "cached",
        Outcome::CachedFail => "cached_fail",
    }
}

/// `now` relative to `at`: `in 5m`, `3m ago`, or `-` with nothing to show.
fn relative(now: Time, at: Option<Time>) -> String {
    match at {
        None => "-".to_string(),
        Some(t) if t >= now => format!("in {}", t - now),
        Some(t) => format!("{} ago", now - t),
    }
}

fn bold(color: bool, s: &str) -> String {
    paint(color, "1", s)
}

fn paint(color: bool, code: &str, s: &str) -> String {
    if color {
        format!("\x1b[{code}m{s}\x1b[0m")
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use choreographd::api::{EtaView, HeldView, JobState, LeaseView, ResourceView};

    fn job(id: u64, name: &str, client: &str, state: JobState) -> JobView {
        JobView {
            id,
            name: name.into(),
            client: client.into(),
            state,
            wait: None,
            eta: None,
            lease: None,
            outcome: None,
            done_at: None,
        }
    }

    fn fixed_view() -> QueueView {
        let now = Time(1_000_000);
        let node_a = NodeView {
            name: "node-a".into(),
            up: true,
            labels: [("arch".to_string(), "x86_64".to_string())].into(),
            policy: Default::default(),
            reported: Some(now),
            resources: vec![ResourceView {
                name: "gate".into(),
                kind: "gate".into(),
                capacity: 2,
                used: 1,
                holders: vec![1],
            }],
        };
        let node_b = NodeView {
            name: "node-b".into(),
            up: false,
            labels: Default::default(),
            policy: Default::default(),
            reported: Some(now),
            resources: vec![],
        };
        let mut running = job(1, "gate a", "node-a/a", JobState::Running);
        running.lease = Some(LeaseView {
            token: 7,
            granted: now - Dur::secs(30),
            expires: now + Dur::secs(30),
            held: vec![HeldView {
                resource: "node-a/gate".into(),
                node: "node-a".into(),
                kind: "gate".into(),
                amount: 1,
            }],
        });
        running.eta = Some(EtaView {
            start_p50: None,
            finish_p50: Some(now + Dur::mins(3)),
            start_p90: None,
            finish_p90: Some(now + Dur::mins(6)),
        });

        let mut queued = job(2, "gate b", "node-b/b", JobState::Queued);
        queued.wait = Some(WaitView {
            why: "capacity".into(),
            text: "needs 1 gate, none free".into(),
            job: None,
            resource: Some("gate".into()),
            kind: Some("gate".into()),
            at: None,
        });
        queued.eta = Some(EtaView {
            start_p50: Some(now + Dur::mins(1)),
            finish_p50: None,
            start_p90: Some(now + Dur::mins(4)),
            finish_p90: None,
        });

        let mut done = job(0, "gate z", "node-a/a", JobState::Done);
        done.outcome = Some(Outcome::Ok);
        done.done_at = Some(now - Dur::mins(1));

        QueueView {
            now,
            nodes: vec![node_a, node_b],
            running: vec![running],
            queued: vec![queued],
            done: vec![done],
        }
    }

    #[test]
    fn a_fixed_queue_renders_the_same_every_time() {
        let view = fixed_view();
        let rendered = render(&view, false);
        let expected = [
            "choreograph top — 2 nodes (1 up, 1 down), 1 running, 1 queued, 1 done",
            "",
            "NODES",
            "  node-a  up  gate 1/2  labels=arch=x86_64  policy=-",
            "  node-b  down  -  labels=-  policy=-",
            "",
            "RUNNING",
            "  #1  \"gate a\"  node-a/a  holds node-a/gate (gate), heartbeat within 30s  eta finish p50 in 3m p90 in 6m",
            "",
            "QUEUED (priority order)",
            "  #2  \"gate b\"  node-b/b  waiting: needs 1 gate, none free  eta start p50 in 1m p90 in 4m",
            "",
            "DONE",
            "  #0  \"gate z\"  node-a/a  ok  1m ago",
            "",
        ]
        .join("\n");
        assert_eq!(rendered, expected);
        // Rendering twice from the same view gives the same text: nothing here reads a clock.
        assert_eq!(rendered, render(&view, false));
    }

    #[test]
    fn an_empty_queue_says_so() {
        let view = QueueView {
            now: Time(0),
            nodes: vec![],
            running: vec![],
            queued: vec![],
            done: vec![],
        };
        let rendered = render(&view, false);
        assert!(rendered.contains("(none reported yet)"));
        assert!(rendered.contains("RUNNING\n  (none)"));
        assert!(rendered.contains("QUEUED (priority order)\n  (none)"));
        assert!(!rendered.contains("DONE"));
    }

    #[test]
    fn colour_wraps_without_changing_the_words() {
        let view = fixed_view();
        let plain = render(&view, false);
        let coloured = render(&view, true);
        assert_ne!(plain, coloured);
        assert!(coloured.contains("\x1b[32mup\x1b[0m"));
        assert!(coloured.contains("\x1b[31mdown\x1b[0m"));
    }
}
