//! `choreograph`: a thin command line over the choreograph library.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::Shell;

mod theory;

mod top;

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
        #[arg(value_hint = clap::ValueHint::FilePath)]
        file: PathBuf,
    },
    /// A live view of a running choreographd: nodes, running jobs, the queue with wait reasons and ETAs.
    Top(top::TopArgs),
    /// Measure recordings of real history against queueing theory and the simulator, and pick gate slots and
    /// a batch size for the worst group.
    ///
    /// Examples:
    ///   choreograph theory days/*.json --by kind
    ///   choreograph theory days/*.json --by kind --target-wait 5m --json
    ///   choreograph theory days/*.json --by kind --gate-time 6m
    Theory(theory::Args),
    /// Print a shell completion script.
    Completions {
        /// bash, zsh, fish, elvish or powershell.
        shell: Shell,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Check { file } => check(&file),
        Cmd::Top(args) => top::run(args),
        Cmd::Theory(a) => match theory::run(a) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            }
        },
        Cmd::Completions { shell } => {
            clap_complete::generate(
                shell,
                &mut Cli::command(),
                "choreograph",
                &mut std::io::stdout(),
            );
            ExitCode::SUCCESS
        }
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
