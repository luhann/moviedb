//! moviedb
//!
//! Self-hosted movie rating API. Single binary: `serve` (the API)
//! and `refresh` (re-pull OMDB data, cron-able). Originally a set of AWS
//! Lambdas, then I migrated to a `FastAPI` service; this Rust binary is what replaced both.
//!
//! See `http.rs` for the endpoint list and error shape, `refresh.rs` for the
//! refresh job, `db.rs` for the SQLite schema/connection setup shared by
//! both, and `util.rs` for the handful of helpers/defaults they both need.

mod db;
mod http;
mod refresh;
mod util;

use std::env;
use std::process::exit;

use clap::{Parser, Subcommand};

use db::DB_POOL_SIZE;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// Headroom above DB_POOL_SIZE for non-DB blocking work. At most DB_POOL_SIZE blocking
// threads ever do DB work, so this headroom stays genuinely free for DNS. Sized for one user: POST / is
// the only handler that resolves DNS, so 4 is already generous, and total
// threads (workers + blocking pool) must stay comfortably under the systemd
// unit's TasksMax=32.
const BLOCKING_POOL_HEADROOM: usize = 4;

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
        /// SQLite database path (falls back to $`DB_PATH`)
        db_path: Option<String>,
        /// Only process the N oldest-refreshed movies
        #[arg(long)]
        limit: Option<usize>,
        /// Seconds to sleep between OMDB requests
        #[arg(long, default_value_t = 0.5)]
        sleep: f64,
        /// Print rating deltas without writing
        #[arg(long)]
        dry_run: bool,
    },
}

fn main() {
    let cli = Cli::parse();

    // Every DB-touching handler runs its query on a spawn_blocking thread,
    // admitted by with_conn's DB_POOL_SIZE-permit semaphore — but tokio's
    // *blocking thread pool itself* defaults to a cap of 512, independent of
    // DB_POOL_SIZE.
    tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .max_blocking_threads(DB_POOL_SIZE as usize + BLOCKING_POOL_HEADROOM)
        .build()
        .expect("failed to build tokio runtime")
        .block_on(async {
            match cli.command {
                Command::Serve { host, port } => http::serve(host, port).await,
                Command::Refresh {
                    db_path,
                    limit,
                    sleep,
                    dry_run,
                } => {
                    let db_path = db_path
                        .or_else(|| env::var("DB_PATH").ok().filter(|s| !s.is_empty()))
                        .unwrap_or_else(|| {
                            eprintln!("moviedb refresh: no db_path given and DB_PATH not set");
                            exit(2);
                        });
                    refresh::refresh(db_path, limit, sleep, dry_run).await;
                }
            }
        });
}
