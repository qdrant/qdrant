//! The real apply: write the batch and, interactively, the following rounds.

use anyhow::{Context, Result};
use common::universal_io::UniversalAppendFs;
use edge::external::uuid::Uuid;
use edge::{PointApplyKind, PointApplyRecord, PointId, UpdateOnlyEdgeShard};

use crate::generate::generate_batch;
use crate::parse::prompt_next_ids;
use crate::report::IoMeter;
use crate::schema::ShardSchema;

/// Generate the random batch and apply it for real: appends to the write
/// target, tombstones in every segment holding an older copy — durable on
/// the backend once this returns `Ok`. With `interactive`, keeps prompting
/// for the next round's ids and applies them through the writer the previous
/// `apply_batch` handed back, one op-num (and seed) higher per round.
pub fn apply_run<Fs: UniversalAppendFs>(
    mut shard: UpdateOnlyEdgeShard<Fs>,
    schema: &ShardSchema,
    ids: &[PointId],
    mut op_num: u64,
    mut seed: u64,
    interactive: bool,
    meter: &IoMeter,
) -> Result<()> {
    let mut ids = ids.to_vec();
    loop {
        let operation = generate_batch(schema, &ids, seed);

        let (returned, outcome) = meter.measure("apply", || {
            shard
                .apply_batch([(op_num, operation)])
                .context("failed to apply the batch")
        })?;
        shard = returned;

        log::info!(
            "applied at op-num {op_num}: {} stored, {} deleted, {} skipped, \
             {} rejected, {} missing",
            outcome.stored,
            outcome.deleted,
            outcome.skipped,
            outcome.rejected,
            outcome.missing,
        );
        for record in &outcome.points {
            log_apply_record(record);
        }

        if !interactive {
            return Ok(());
        }
        match prompt_next_ids()? {
            Some(next_ids) => ids = next_ids,
            None => return Ok(()),
        }
        op_num += 1;
        seed = seed.wrapping_add(1);
    }
}

/// One line per applied point: whether it was created fresh or overwrote
/// existing copies, and from which segments (and slots) the old copies were
/// removed.
fn log_apply_record(record: &PointApplyRecord) {
    let PointApplyRecord {
        id,
        kind,
        tombstoned,
        superseded,
    } = record;

    let slot = |(segment, internal_id): &(Uuid, _)| format!("segment {segment} slot {internal_id}");
    let tombstoned_list = tombstoned.iter().map(slot).collect::<Vec<_>>();

    match kind {
        PointApplyKind::Stored => {
            let mut retired = tombstoned_list;
            if let Some(superseded) = superseded {
                retired.push(format!("{} (superseded in place)", slot(superseded)));
            }
            if retired.is_empty() {
                log::info!("point {id}: created — no previous copy in any segment");
            } else {
                log::info!(
                    "point {id}: overwritten — old copies deleted from {}",
                    retired.join(", "),
                );
            }
        }
        PointApplyKind::Deleted => {
            log::info!("point {id}: deleted from {}", tombstoned_list.join(", "));
        }
        PointApplyKind::Skipped => {
            log::info!("point {id}: skipped — already at or beyond this op-num");
        }
        PointApplyKind::Rejected => {
            log::info!("point {id}: already there — rejected by its update mode");
        }
        PointApplyKind::Missing => {
            log::info!("point {id}: no segment holds it — nothing to do");
        }
    }
}
