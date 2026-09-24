//! The dry run: resolve the batch against the shard and log what it would do.

use std::collections::HashMap;

use anyhow::{Context, Result};
use common::universal_io::UniversalAppendFs;
use edge::external::uuid::Uuid;
use edge::{FullyQualifiedPoint, PointAction, PointId, UpdateOnlyEdgeShard};

use crate::generate::generate_batch;
use crate::schema::ShardSchema;

fn log_preview_point(point: &edge::PointPreview) {
    let edge::PointPreview {
        id,
        current,
        slots,
        action,
    } = point;

    match current {
        Some(current) => log::info!(
            "point {id}: newest copy in segment {} slot {} at version {}",
            current.segment,
            current.internal_id,
            current.version,
        ),
        None => log::info!("point {id}: not stored in any segment (would be created)"),
    }
    for (segment, internal_id) in slots {
        log::info!("point {id}: occupies segment {segment} slot {internal_id}");
    }

    match action {
        PointAction::Store(resolved) => {
            let FullyQualifiedPoint {
                id: _,
                version,
                stored_vectors,
                updated_vectors,
                payload,
            } = resolved.as_ref();
            log::info!(
                "point {id}: would be stored at version {version}: \
                 {} vector(s) carried over as raw bytes {:?}, \
                 {} vector(s) supplied by the batch {:?}, payload {}",
                stored_vectors.len(),
                stored_vectors
                    .iter()
                    .map(|(name, bytes)| format!("{name}({} B)", bytes.len()))
                    .collect::<Vec<_>>(),
                updated_vectors.len(),
                updated_vectors.keys().collect::<Vec<_>>(),
                serde_json::to_string(payload).unwrap_or_else(|err| err.to_string()),
            );
            if !slots.is_empty() {
                log::info!(
                    "point {id}: its {} current slot(s) would be tombstoned",
                    slots.len()
                );
            }
        }
        PointAction::Delete => {
            log::info!(
                "point {id}: would be deleted, tombstoning {} slot(s)",
                slots.len()
            );
        }
        PointAction::Skip => log::info!(
            "point {id}: already at or beyond version — skipped (replay no-op); \
             re-run with a higher --op-num to overwrite",
        ),
        PointAction::Rejected => {
            log::info!("point {id}: already there — rejected by the upsert's update mode");
        }
        PointAction::Missing => {
            log::info!("point {id}: names a point no segment holds — nothing to do");
        }
    }
}

/// Generate the random batch, resolve it against the open shard, and log what
/// it would do. The backend is behind `S`, so this is the whole dry run for
/// local and object-storage shards alike.
pub fn dry_run<Fs: UniversalAppendFs>(
    shard: &UpdateOnlyEdgeShard<Fs>,
    schema: &ShardSchema,
    ids: &[PointId],
    op_num: u64,
    seed: u64,
) -> Result<()> {
    let operation = generate_batch(schema, ids, seed);

    let preview = shard
        .preview_batch([(op_num, operation)])
        .context("failed to resolve the batch")?;

    let mut stored = 0usize;
    let mut skipped = 0usize;
    let mut rejected = 0usize;
    let mut tombstones: HashMap<Uuid, usize> = HashMap::new();
    for point in &preview.points {
        log_preview_point(point);
        match &point.action {
            PointAction::Store(_) => {
                stored += 1;
                for (segment, _) in &point.slots {
                    *tombstones.entry(*segment).or_default() += 1;
                }
            }
            PointAction::Delete => {
                for (segment, _) in &point.slots {
                    *tombstones.entry(*segment).or_default() += 1;
                }
            }
            PointAction::Skip => skipped += 1,
            PointAction::Rejected => rejected += 1,
            PointAction::Missing => {}
        }
    }

    log::info!(
        "summary: {stored} point(s) would be appended to the write target, \
         {skipped} skipped, {rejected} rejected by their update mode",
    );
    for (segment, count) in &tombstones {
        log::info!("summary: segment {segment} would receive {count} tombstone(s)");
    }
    log::info!("dry run: nothing written — pass --apply to write");

    Ok(())
}
