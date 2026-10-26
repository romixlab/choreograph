//! Sets `GIT_SHA` (short, `-dirty` from the index) and `BUILD_TIME` (the commit's own time, not the clock) for
//! `choreographd --version`, so any installed build can be traced back to its commit. Both fall back to `unknown`
//! without git.
//!
//! Worktree-safe: in a git worktree `.git` is a file, so the real paths come from `git rev-parse` (`--git-dir`
//! for this worktree's `HEAD` and index, `--git-common-dir` for the branch ref and `packed-refs`). `BUILD_TIME`
//! is the commit's time, not the clock, so rebuilding an unchanged commit gives the same output.

use std::process::Command;

fn main() {
    let git_dir = git(&["rev-parse", "--git-dir"]).unwrap_or_else(|| ".git".into());
    let common_dir = git(&["rev-parse", "--git-common-dir"]).unwrap_or_else(|| git_dir.clone());
    println!("cargo:rerun-if-changed={git_dir}/HEAD");
    println!("cargo:rerun-if-changed={git_dir}/index");
    // HEAD here is this worktree's own, but "ref: refs/heads/X" only changes on a checkout; a commit on X
    // updates the ref file itself, shared in the common dir (or, once git packs it, packed-refs there).
    if let Ok(head) = std::fs::read_to_string(format!("{git_dir}/HEAD"))
        && let Some(r) = head.trim().strip_prefix("ref: ")
    {
        watch_if_there(&format!("{common_dir}/{r}"));
    }
    watch_if_there(&format!("{common_dir}/packed-refs"));
    println!("cargo:rustc-env=GIT_SHA={}", git_sha());
    println!("cargo:rustc-env=BUILD_TIME={}", build_time());
}

/// `rerun-if-changed` on a path that does not exist makes cargo treat build.rs as stale on every build ("the file
/// is missing"): a repo that never ran `git pack-refs` has no `packed-refs`, a packed branch no ref file. When
/// git creates or packs one, the other one disappears or changes, and that reruns build.rs.
fn watch_if_there(path: &str) {
    if std::path::Path::new(path).exists() {
        println!("cargo:rerun-if-changed={path}");
    }
}

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn git_sha() -> String {
    let Some(sha) = git(&["rev-parse", "--short", "HEAD"]) else {
        return "unknown".into();
    };
    // --quiet exits non-zero when the index differs from HEAD or the worktree from the index: dirty without
    // a full working-tree scan for untracked files (git() turns that non-zero exit into None).
    match git(&["diff-index", "--quiet", "HEAD", "--"]) {
        None => format!("{sha}-dirty"),
        Some(_) => sha,
    }
}

/// "3 Oct 2026 18:20 UTC", without a date crate; from the commit's own time (`git log -1 --format=%ct`), not
/// the clock, so this output doesn't change between builds of the same commit.
fn build_time() -> String {
    let Some(secs) = git(&["log", "-1", "--format=%ct"]).and_then(|s| s.parse::<i64>().ok()) else {
        return "unknown".into();
    };
    let (days, rest) = (secs.div_euclid(86400), secs.rem_euclid(86400));
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm)
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    format!(
        "{day} {} {year} {:02}:{:02} UTC",
        MONTHS[(month - 1) as usize],
        rest / 3600,
        rest % 3600 / 60
    )
}
