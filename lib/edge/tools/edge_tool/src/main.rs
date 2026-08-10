//! `edge-tool` — create, seed, optimize, and upload minimal local Qdrant edge
//! collections, for benchmarking and manual testing of the edge write/read/
//! optimize paths without a running Qdrant server.
//!
//! ```sh
//! cargo run -p edge-tool -- create --dense 1024 --sparse --quantization turbo4 ./collection
//! cargo run -p edge-tool -- upsert -n 1000 ./collection
//! cargo run -p edge-tool -- optimize ./collection
//! cargo run -p edge-tool -- upload --bucket my-bucket ./collection my_collection/0
//! ```

mod args;
mod create;
mod optimize;
mod points;
mod quantization;
mod upload;
mod upsert;

use anyhow::Result;
use clap::Parser as _;

use crate::args::{Cli, Command};

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Create(args) => create::run(args),
        Command::Upsert(args) => upsert::run(args),
        Command::Optimize(args) => optimize::run(args),
        Command::Upload(args) => upload::run(args),
    }
}
