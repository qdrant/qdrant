use anyhow::{Context, Result};
use edge::EdgeShard;

use crate::args::OptimizeArgs;

pub fn run(args: OptimizeArgs) -> Result<()> {
    let shard = EdgeShard::load(&args.path, None)
        .with_context(|| format!("failed to open collection at {}", args.path.display()))?;

    let before = shard.info().context("failed to read collection info")?;
    log::info!(
        "optimizing {}: {} segment(s), ~{} point(s) before",
        args.path.display(),
        before.segments_count,
        before.points_count,
    );

    let optimized = shard.optimize().context("optimization failed")?;

    let after = shard.info().context("failed to read collection info")?;
    log::info!(
        "optimization {}: {} segment(s), ~{} point(s) after",
        if optimized {
            "made progress"
        } else {
            "was a no-op (already optimal)"
        },
        after.segments_count,
        after.points_count,
    );

    shard.flush().context("failed to flush the collection")?;
    Ok(())
}
