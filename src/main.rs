//! moviedb: a self-hosted movie rating API.
//!
//! One binary with two subcommands: `serve` runs the API and `refresh`
//! re-fetches OMDB data for every stored movie. This started as a set of AWS
//! Lambdas, then became a `FastAPI` service, and this replaces both.
//!
//! - `http.rs`: the endpoints and error format
//! - `refresh.rs`: the refresh job
//! - `db.rs`: SQLite schema and connection setup
//! - `util.rs`: helpers both of them use

mod db;
mod http;
mod refresh;
mod util;

use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// One blocking thread runs database work at a time; the rest are for DNS
// lookups when calling OMDB. tokio's default is 512, and the unit's
// TasksMax=32 has to cover these plus the worker threads.
const MAX_BLOCKING_THREADS: usize = 4;

#[derive(Parser)]
#[command(name = "moviedb", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the API server
    Serve {
        #[arg(long, default_value = "0.0.0.0")]
        host: String,
        #[arg(long, default_value_t = 8000)]
        port: u16,
    },
    /// Re-pull OMDB data for all movies, preserving Personal ratings
    Refresh {
        /// SQLite database path (defaults to $`DB_PATH`, then
        /// /var/lib/moviedb/movies.db)
        db_path: Option<String>,
        /// Only process the N oldest-refreshed movies
        #[arg(long)]
        limit: Option<usize>,
        /// Seconds to sleep between OMDB requests
        #[arg(long, default_value = "0.5", value_parser = parse_seconds)]
        sleep: Duration,
        /// Print rating deltas without writing
        #[arg(long)]
        dry_run: bool,
    },
}

fn parse_seconds(s: &str) -> Result<Duration, String> {
    let secs: f64 = s.parse().map_err(|e| format!("{e}"))?;
    Duration::try_from_secs_f64(secs).map_err(|e| e.to_string())
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .max_blocking_threads(MAX_BLOCKING_THREADS)
        .build()
        .expect("failed to build tokio runtime")
        .block_on(async {
            match cli.command {
                Command::Serve { host, port } => {
                    http::serve(host, port).await;
                    ExitCode::SUCCESS
                }
                Command::Refresh {
                    db_path,
                    limit,
                    sleep,
                    dry_run,
                } => {
                    let db_path = db_path.unwrap_or_else(util::db_path);
                    refresh::refresh(db_path, limit, sleep, dry_run).await
                }
            }
        })
}
