//! `choreograph theory`: recordings grouped by a tag, measured against queueing theory and the simulator, and
//! the gate slots and batch size the worst group needs.

use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::PathBuf;

use choreograph::recording::{self, Pick, ReplaySettings};
use choreograph::theory::PickSettings;
use choreograph::{Dur, Profile, ProfileSettings, Recording};
use serde::Serialize;

#[derive(clap::Args)]
pub struct Args {
    /// Recordings, JSON files of `{tags, model, trace}`.
    #[arg(required = true, value_hint = clap::ValueHint::FilePath)]
    files: Vec<PathBuf>,
    /// Group the recordings by this tag (`kind`, `day`); without it, all are one group.
    #[arg(long)]
    by: Option<String>,
    /// Print JSON instead of the table.
    #[arg(long)]
    json: bool,
    /// The width of the bins arrivals are counted in (index of dispersion, regime fit).
    #[arg(long, default_value = "1h")]
    bin: Dur,
    /// The highest utilisation a pick of gate slots may run at.
    #[arg(long, default_value_t = 0.7)]
    max_util: f64,
    /// The mean wait in queue the gate slots must stay under.
    #[arg(long, default_value = "10m")]
    target_wait: Dur,
    /// Never pick more gate slots than this.
    #[arg(long, default_value_t = 16)]
    max_slots: u32,
    /// The largest batch size to consider.
    #[arg(long, default_value_t = 8)]
    max_batch: u32,
    /// Arrivals per simulated run (0: no simulation).
    #[arg(long, default_value_t = 5000)]
    sim_jobs: usize,
    /// The seed of the simulated runs.
    #[arg(long, default_value_t = 1)]
    seed: u64,
}

/// One group's numbers, measured, predicted and simulated; waits in seconds.
#[derive(Serialize)]
struct Group {
    name: String,
    profile: Profile,
    /// Kingman per lane with each lane's own c_a².
    predicted_wq: Option<f64>,
    /// The same with the regime fit's index of dispersion as c_a².
    predicted_wq_bursty: Option<f64>,
    /// Mean wait of the simulated lanes under the fitted arrivals.
    simulated_wq: Option<f64>,
}

#[derive(Serialize)]
struct Report {
    groups: Vec<Group>,
    pick: Option<Pick>,
    /// The simulated mean wait of the worst group with the picked gate slots on top of its lanes.
    pick_simulated_wq: Option<f64>,
}

pub fn run(a: Args) -> Result<(), String> {
    let mut groups: BTreeMap<String, Vec<Recording>> = BTreeMap::new();
    for f in &a.files {
        let text = std::fs::read_to_string(f).map_err(|e| format!("{}: {e}", f.display()))?;
        let rec: Recording =
            serde_json::from_str(&text).map_err(|e| format!("{}: {e}", f.display()))?;
        let key = match &a.by {
            Some(tag) => rec.tags.get(tag).cloned().unwrap_or_else(|| "-".into()),
            None => "all".into(),
        };
        groups.entry(key).or_default().push(rec);
    }
    let ps = ProfileSettings { bin: a.bin };
    let sim = ReplaySettings {
        jobs: a.sim_jobs,
        seed: a.seed,
        ..ReplaySettings::default()
    };
    let simulate = |p: &Profile, slots| -> Result<Option<f64>, String> {
        if a.sim_jobs == 0 {
            return Ok(None);
        }
        Ok(p.simulate(slots, &sim)
            .map_err(|e| e.to_string())?
            .map(|m| m.wait.mean))
    };
    let mut out = Vec::new();
    for (name, recs) in &groups {
        let profile = Profile::measure(recs, &ps);
        let idc = profile.mmpp.map(|m| m.idc());
        out.push(Group {
            name: name.clone(),
            predicted_wq: profile.predicted_wq(None),
            predicted_wq_bursty: idc.and_then(|i| profile.predicted_wq(Some(i))),
            simulated_wq: simulate(&profile, None)?,
            profile,
        });
    }
    let pick_settings = PickSettings {
        max_util: a.max_util,
        target_wq: a.target_wait.0 as f64 / 1000.0,
        max_slots: a.max_slots,
        max_batch: a.max_batch,
    };
    let pick = recording::pick(
        out.iter().map(|g| (g.name.as_str(), &g.profile)),
        &pick_settings,
    );
    let pick_simulated_wq = match &pick {
        Some(Pick {
            worst,
            slots: Some(c),
            ..
        }) => {
            let g = out.iter().find(|g| &g.name == worst).expect("picked group");
            simulate(&g.profile, Some(*c))?
        }
        _ => None,
    };
    let report = Report {
        groups: out,
        pick,
        pick_simulated_wq,
    };
    if a.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?
        );
    } else {
        print_table(&report, a.by.as_deref().unwrap_or("group"));
    }
    Ok(())
}

fn color() -> bool {
    std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty())
}

/// Seconds as a short duration (`4m10s`), `-` for none, `∞` for an overload.
fn secs(s: Option<f64>) -> String {
    match s {
        None => "-".into(),
        Some(v) if !v.is_finite() => "∞".into(),
        Some(v) => {
            let ms = (v * 1000.0).round() as i64;
            // Whole seconds above a minute keep the column short.
            let ms = if ms >= 60_000 { ms / 1000 * 1000 } else { ms };
            Dur(ms).to_string()
        }
    }
}

fn print_table(r: &Report, by: &str) {
    let (b, z) = if color() {
        ("\x1b[1m", "\x1b[0m")
    } else {
        ("", "")
    };
    let head = [
        by,
        "recs",
        "jobs",
        "hours",
        "λ/h",
        "c_a²",
        "IDC",
        "busy λ/h",
        "E[S]",
        "c_s²",
        "p",
        "W meas",
        "W p90",
        "Kingman",
        "K·IDC",
        "sim",
    ];
    let rows: Vec<Vec<String>> = r
        .groups
        .iter()
        .map(|g| {
            let p = &g.profile;
            vec![
                g.name.clone(),
                p.recordings.to_string(),
                p.arrivals.to_string(),
                format!("{:.0}", p.hours),
                format!("{:.2}", p.per_hour),
                format!("{:.2}", p.ca2),
                format!("{:.2}", p.idc),
                format!("{:.2}", p.busy_per_hour()),
                secs((p.service.n > 0).then_some(p.service.mean)),
                format!("{:.2}", p.service.cv2),
                format!("{:.2}", p.p_fail()),
                secs((p.wait.n > 0).then_some(p.wait.mean)),
                secs(p.wait.p90.map(|d| d.0 as f64 / 1000.0)),
                secs(g.predicted_wq),
                secs(g.predicted_wq_bursty),
                secs(g.simulated_wq),
            ]
        })
        .collect();
    let widths: Vec<usize> = (0..head.len())
        .map(|i| {
            rows.iter()
                .map(|r| r[i].chars().count())
                .chain([head[i].chars().count()])
                .max()
                .unwrap_or(0)
        })
        .collect();
    let line = |cells: Vec<String>| {
        cells
            .iter()
            .zip(&widths)
            .enumerate()
            .map(|(i, (c, w))| {
                let pad = w - c.chars().count();
                if i == 0 {
                    format!("{c}{}", " ".repeat(pad))
                } else {
                    format!("{}{c}", " ".repeat(pad))
                }
            })
            .collect::<Vec<_>>()
            .join("  ")
    };
    println!(
        "{b}{}{z}",
        line(head.iter().map(|s| s.to_string()).collect())
    );
    for row in rows {
        println!("{}", line(row));
    }
    println!();
    println!(
        "W meas: arrival to start as recorded; Kingman: per lane (one at a time), with each lane's c_a² and with the regime fit's IDC; sim: the lanes simulated under the fitted arrivals."
    );
    let Some(p) = &r.pick else {
        println!("No group with service times: nothing to pick.");
        return;
    };
    println!();
    println!(
        "{b}Pick{z} for the worst group, {b}{}{z} (busy {:.2}/h, c_a² {:.2}):",
        p.worst, p.busy_per_hour, p.ca2
    );
    match p.slots {
        Some(c) => println!(
            "  gate slots: {c} (simulated wait with them: {})",
            secs(r.pick_simulated_wq)
        ),
        None => println!("  gate slots: none within the limits"),
    }
    match (&p.lane, p.batch) {
        (Some(l), Some((size, lat))) => println!(
            "  batch size: {size} on the hottest lane {l} (busy {:.2}/h), mean time to result {}",
            p.lane_busy_per_hour,
            secs(Some(lat))
        ),
        (Some(l), None) => println!("  batch size: none keeps lane {l} stable"),
        _ => {}
    }
}
