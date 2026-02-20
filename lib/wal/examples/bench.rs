use std::path::Path;
use std::str::FromStr;

use docopt::Docopt;
use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use regex::Regex;
use serde::Deserialize;

use wal::Segment;

static USAGE: &str = "
Usage:
  bench append [--batch=<n>] [--segment-size=<ss>] [--entry-size=<es>] <segment-path>

Options:
  --batch=<n>           Sync entries to disk in batches of size n, or 0 for async [default: 1].
  --segment-size=<ss>   Segment size (bytes) [default: 100MiB]
  --entry-size=<es>     Entry size (bytes) [default: 1KiB]
  -h --help             Show a help message.
";

#[derive(Debug, Deserialize)]
struct Args {
    cmd_append: bool,

    arg_segment_path: String,

    flag_batch: usize,
    flag_entry_size: String,
    flag_segment_size: String,
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());
    if args.cmd_append {
        append(&args);
    }
}

pub fn precise_time_ns() -> u64 {
    chrono::offset::Utc::now()
        .timestamp_nanos_opt()
        .expect("timestamp after year 2262") as u64
}

fn format_duration(n: u64) -> String {
    if n > 1_000_000_000 {
        format!("{:.2}s", n as f64 / 1_000_000_000f64)
    } else if n > 1_000_000 {
        format!("{:.2}ms", n as f64 / 1_000_000f64)
    } else if n > 1_000 {
        format!("{:.2}μs", n as f64 / 1_000f64)
    } else {
        format!("{n}ns")
    }
}

fn format_bytes(n: usize) -> String {
    if n > 1_073_741_824 {
        format!("{:.2}GiB", n as f64 / 1_073_741_824f64)
    } else if n > 1_048_576 {
        format!("{:.2}MiB", n as f64 / 1_048_576f64)
    } else if n > 1_024 {
        format!("{:.2}KiB", n as f64 / 1_024f64)
    } else {
        format!("{n}B")
    }
}

fn parse_bytes(s: &str) -> usize {
    let regex = Regex::new(r"(?i)^(\d+(?:\.\d+)?)\s?(k|m|g)?i?b?$").unwrap();
    let caps = regex
        .captures(s)
        .unwrap_or_else(|| panic!("unable to parse byte amount: {s}"));
    let n: usize = FromStr::from_str(caps.get(1).unwrap().as_str()).unwrap();

    match caps.get(2).map(|m| m.as_str()) {
        None => n,
        Some("k") | Some("K") => n * 1_024,
        Some("m") | Some("M") => n * 1_048_576,
        Some("g") | Some("G") => n * 1_073_741_824,
        _ => panic!("unable to parse byte amount: {s}"),
    }
}

fn append(args: &Args) {
    let path = Path::new(&args.arg_segment_path);

    let entry_size = parse_bytes(&args.flag_entry_size);
    let segment_size = parse_bytes(&args.flag_segment_size);

    println!(
        "entry size: {}, segment size: {}, batch size: {}",
        format_bytes(entry_size),
        format_bytes(segment_size),
        args.flag_batch
    );

    let mut segment = Segment::create(path, segment_size).unwrap();

    let mut buf = vec![0; entry_size];
    let mut small_rng = StdRng::from_os_rng();
    small_rng.fill_bytes(&mut buf);

    let mut append_hist = Histogram::<u64>::new_with_bounds(1, 60 * 60 * 1000, 2).unwrap();
    let mut sync_hist = Histogram::<u64>::new_with_bounds(1, 60 * 60 * 1000, 2).unwrap();

    let mut entries = 0usize;
    let mut time: u64 = precise_time_ns();
    let start_time: u64 = time;
    while segment.append(&buf).is_some() {
        entries += 1;
        if args.flag_batch != 0 && entries.is_multiple_of(args.flag_batch) {
            let start_sync = precise_time_ns();
            //future.await().unwrap();
            sync_hist.record(precise_time_ns() - start_sync).unwrap();
        }
        let new_time = precise_time_ns();
        append_hist.record(new_time - time).unwrap();
        time = new_time;
        small_rng.fill_bytes(&mut buf);
    }

    if args.flag_batch != 0 && !entries.is_multiple_of(args.flag_batch) {
        //segment.flush().await().unwrap();
        let new_time = precise_time_ns();
        append_hist.record(new_time - time).unwrap();
    }

    let end_time = precise_time_ns();

    if args.flag_batch == 0 {
        //segment.flush().await().unwrap();
        let flush_time = precise_time_ns() - end_time;
        println!("final sync latency: {}", format_duration(flush_time));
    }

    let time = end_time - start_time;
    let data = entries * entry_size;
    let rate = (data as f64 / (time as f64 / 1_000_000_000f64)) as usize;
    let overhead_amount = segment.len().saturating_sub(data);
    let overhead_rate = (overhead_amount as f64 / (time as f64 / 1_000_000_000f64)) as usize;

    println!(
        "time: {}, data: {} ({}), rate {}/s ({}/s), entries appended: {}",
        format_duration(time),
        format_bytes(data),
        format_bytes(overhead_amount),
        format_bytes(rate),
        format_bytes(overhead_rate),
        entries
    );
    println!(
        "append latency:\t\tp50: {:>7},\tp75: {:>7},\tp90: {:>7},\tp95: {:>7},\tp99: {:>7}",
        format_duration(append_hist.value_at_percentile(0.5)),
        format_duration(append_hist.value_at_percentile(0.75)),
        format_duration(append_hist.value_at_percentile(0.90)),
        format_duration(append_hist.value_at_percentile(0.95)),
        format_duration(append_hist.value_at_percentile(0.99)),
    );
    println!(
        "sync latency:\t\tp50: {:>7},\tp75: {:>7},\tp90: {:>7},\tp95: {:>7},\tp99: {:>7}",
        format_duration(sync_hist.value_at_percentile(0.5)),
        format_duration(sync_hist.value_at_percentile(0.75)),
        format_duration(sync_hist.value_at_percentile(0.90)),
        format_duration(sync_hist.value_at_percentile(0.95)),
        format_duration(sync_hist.value_at_percentile(0.99)),
    );
}
