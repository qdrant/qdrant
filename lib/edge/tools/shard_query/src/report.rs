//! Human-readable output: result diffs between live-reloads and disk-cache statistics.

use std::collections::HashMap;

use anyhow::Result;

use crate::request::Row;

/// Print the difference between the previous and the current run of the same
/// request: `+` rows whose id appeared, `-` rows whose id disappeared, `~` rows
/// whose rendered content changed for the same id (old -> new). A pure
/// reordering of unchanged rows prints nothing.
pub fn print_diff(previous: &[Row], current: &[Row]) -> Result<()> {
    let previous_by_id: HashMap<&str, &serde_json::Value> = previous
        .iter()
        .map(|(id, row)| (id.as_str(), row))
        .collect();
    let current_by_id: HashMap<&str, &serde_json::Value> =
        current.iter().map(|(id, row)| (id.as_str(), row)).collect();

    let mut added = 0usize;
    let mut removed = 0usize;
    let mut changed = 0usize;
    for (id, row) in current {
        match previous_by_id.get(id.as_str()) {
            None => {
                added += 1;
                println!("+ {}", serde_json::to_string(row)?);
            }
            Some(old) if **old != *row => {
                changed += 1;
                println!(
                    "~ {} -> {}",
                    serde_json::to_string(old)?,
                    serde_json::to_string(row)?
                );
            }
            Some(_) => {}
        }
    }
    for (id, row) in previous {
        if !current_by_id.contains_key(id.as_str()) {
            removed += 1;
            println!("- {}", serde_json::to_string(row)?);
        }
    }

    if added == 0 && removed == 0 && changed == 0 {
        println!("no changes");
    } else {
        println!("{added} added, {removed} removed, {changed} changed");
    }
    Ok(())
}

/// Print interval counters to stderr, leaving query result output unchanged.
pub fn print_cache_stats(phase: &str, stats: &common::universal_io::DiskCacheStatsSnapshot) {
    let mut fields = Vec::new();
    for (label, value) in [
        ("fetches", stats.remote_fetches_started),
        ("completed", stats.remote_fetches_completed),
        ("bytes", stats.downloaded_bytes),
        ("fetch_errors", stats.remote_fetch_errors),
        ("abandoned", stats.remote_fetches_abandoned),
    ] {
        if value != 0 {
            fields.push(format!("{label}={value}"));
        }
    }
    if let Some(average) = stats
        .avg_fetch_duration()
        .filter(|duration| !duration.is_zero())
    {
        fields.push(format!("avg={average:.3?}"));
    }
    let max_count = stats
        .fetch_duration_histogram
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    if fields.is_empty() && max_count == 0 {
        return;
    }
    eprintln!("Disk cache ({phase}): {}", fields.join(" "));
    if max_count == 0 {
        return;
    }
    eprintln!("  Fetch latency (ms, upper bounds exclusive):");
    let bounds = common::universal_io::DiskCacheStatsSnapshot::FETCH_DURATION_BUCKET_BOUNDS;
    for (i, &count) in stats.fetch_duration_histogram.iter().enumerate() {
        if count == 0 {
            continue;
        }
        let label = if i == 0 {
            format!("<{}", bounds[0].as_millis())
        } else if i == bounds.len() {
            format!(">={}", bounds[i - 1].as_millis())
        } else {
            format!("{}-{}", bounds[i - 1].as_millis(), bounds[i].as_millis())
        };
        // Scale to the busiest bucket; keep every non-empty bucket visible.
        let width = (u128::from(count) * 20).div_ceil(u128::from(max_count)) as usize;
        eprintln!("  {label:>9} | {:<20} {count}", "#".repeat(width));
    }
}
