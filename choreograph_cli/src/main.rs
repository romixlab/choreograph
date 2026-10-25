//! `choreograph`: a thin command line over the choreograph library.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("GIT_SHA"),
    ", commit ",
    env!("BUILD_TIME"),
    ")"
);

/// Plans and runs jobs over shared resources.
#[derive(Parser)]
#[command(name = "choreograph", version = VERSION)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Load a TOML job set, validate it and print a summary.
    Check {
        /// The job set.
        file: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Check { file } => check(&file),
    }
}

fn check(file: &PathBuf) -> ExitCode {
    let text = match std::fs::read_to_string(file) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("error: {}: {e}", file.display());
            return ExitCode::FAILURE;
        }
    };
    match choreograph::jobset::from_toml(&text) {
        Ok(m) => {
            println!(
                "{}: {} nodes, {} kinds, {} resources, {} jobs, {} groups",
                file.display(),
                m.nodes().len(),
                m.kinds().len(),
                m.resources().len(),
                m.jobs().len(),
                m.groups().len()
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {}: {e}", file.display());
            ExitCode::FAILURE
        }
    }
}
