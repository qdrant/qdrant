//! Human-readable output: result diffs between live-reloads and IO statistics.

use std::collections::HashMap;

use anyhow::Result;
use io_bridge_object_store::CachedBlobStatsSnapshot;

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

/// Print interval IO counters to stderr, leaving query result output unchanged.
pub fn print_io_stats(phase: &str, stats: &CachedBlobStatsSnapshot) {
    if let Some(compact) = stats.format_compact() {
        eprintln!("IO stats ({phase}):\n{compact}");
    }
}
