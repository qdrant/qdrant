//! Open a read-only edge shard over object storage and run one read request against it.
//!
//! The same binary runs natively and under a WASI runtime:
//!
//! ```sh
//! cargo run -p edge-wasm -- http://localhost:9000 collection/0 search --vector 1,2,3,4
//!
//! cargo build -p edge-wasm --target wasm32-wasip2 --release
//! wasmtime -S inherit-network -S allow-ip-name-lookup \
//!     target/wasm32-wasip2/release/edge-wasm.wasm \
//!     http://localhost:9000 collection/0 search --vector 1,2,3,4
//! ```
//!
//! Argument parsing is hand-rolled rather than `clap`-based to keep the wasm dependency graph — and
//! the resulting module — as small as the demonstration needs.

use std::process::ExitCode;

use edge_wasm::mem_fs::MemFs;
use edge_wasm::object_store;
use edge_wasm::shard::{
    MemEdgeShard, describe_open_error, info_json, record_json, scored_point_json, shard_files,
};

const USAGE: &str = "\
usage: edge-wasm <base-url> <prefix> <command> [options]

commands:
  info                       shard counters and derived config
  search --vector a,b,c      nearest-neighbour search
  scroll                     paginate over points

options:
  --vector <a,b,c>           query vector (search only, required)
  --vector-name <name>       named vector to query (default: the unnamed one)
  --limit <n>                max results (default: 10)
  --filter <json>            Qdrant filter as JSON
  --with-payload             include payloads in the output
";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let ([base_url, prefix, command], options) = match args.split_at_checked(3) {
        Some(([base, prefix, command], rest)) => {
            ([base.clone(), prefix.clone(), command.clone()], rest)
        }
        _ => return Err(format!("too few arguments\n\n{USAGE}")),
    };

    let opts = Options::parse(options)?;

    eprintln!("listing {base_url}/{prefix}");
    let listing = object_store::list(&base_url, &prefix)?;
    let base = base_url.trim_end_matches('/');

    let mut files = Vec::new();
    for (path, key) in shard_files(listing.into_iter().map(|object| object.key), &prefix) {
        files.push((path, object_store::get(&format!("{base}/{key}"))?));
    }

    let fs = MemFs::new(files);
    eprintln!(
        "loaded {} object(s), {} byte(s)",
        fs.paths().count(),
        fs.total_len(),
    );

    let shard = MemEdgeShard::open(fs, std::path::Path::new(""))
        .map_err(|err| describe_open_error(&err))?;

    let output = match command.as_str() {
        "info" => info_json(&shard.info().map_err(|err| err.to_string())?),
        "search" => {
            let vector = opts
                .vector
                .ok_or_else(|| format!("search needs --vector\n\n{USAGE}"))?;
            let hits = shard
                .search(
                    opts.vector_name.as_deref(),
                    vector,
                    opts.limit,
                    opts.filter,
                    opts.with_payload,
                )
                .map_err(|err| err.to_string())?;
            serde_json::Value::Array(hits.iter().map(scored_point_json).collect())
        }
        "scroll" => {
            let records = shard
                .scroll(opts.limit, opts.filter, opts.with_payload)
                .map_err(|err| err.to_string())?;
            serde_json::Value::Array(records.iter().map(record_json).collect())
        }
        other => return Err(format!("unknown command {other:?}\n\n{USAGE}")),
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&output).map_err(|err| err.to_string())?
    );
    Ok(())
}

#[derive(Default)]
struct Options {
    vector: Option<Vec<f32>>,
    vector_name: Option<String>,
    limit: usize,
    filter: Option<edge::Filter>,
    with_payload: bool,
}

impl Options {
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut opts = Options {
            limit: 10,
            ..Options::default()
        };
        let mut args = args.iter();

        while let Some(flag) = args.next() {
            let mut value = || {
                args.next()
                    .cloned()
                    .ok_or_else(|| format!("{flag} needs a value"))
            };

            match flag.as_str() {
                "--vector" => {
                    let raw = value()?;
                    let vector = raw
                        .split(',')
                        .map(|part| part.trim().parse::<f32>())
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|err| format!("invalid --vector {raw:?}: {err}"))?;
                    opts.vector = Some(vector);
                }
                "--vector-name" => opts.vector_name = Some(value()?),
                "--limit" => {
                    let raw = value()?;
                    opts.limit = raw
                        .parse()
                        .map_err(|err| format!("invalid --limit {raw:?}: {err}"))?;
                }
                "--filter" => {
                    let raw = value()?;
                    opts.filter = Some(
                        serde_json::from_str(&raw)
                            .map_err(|err| format!("invalid --filter: {err}"))?,
                    );
                }
                "--with-payload" => opts.with_payload = true,
                other => return Err(format!("unknown option {other:?}\n\n{USAGE}")),
            }
        }

        Ok(opts)
    }
}
