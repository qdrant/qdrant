mod old_blocks;

use std::sync::atomic::AtomicBool;

use common::ambient;
use common::bitvec::BitSliceExt as _;
use common::progress_tracker::ProgressTracker;
use common::reason::reason;
use common::types::{DeferredBehavior, PointOffsetType};
use log::debug;

use self::old_blocks::old_range_blocks;
use super::block_filter::BlockSkip;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::index::PayloadIndexRead;
use crate::index::field_index::PayloadBlockCondition;
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::types::Condition::Field;
use crate::types::{FieldCondition, Filter, PayloadFieldSchema, PayloadKeyType};
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// The payload blocks of one indexed field.
pub(super) struct FieldBlocks<'a> {
    /// Progress of the field, started by the caller. The builder counts the points it links.
    pub progress: ProgressTracker,
    /// Which blocks to skip, besides those too large to build.
    pub skip: BlockSkip,
    /// The blocks, without their points: `loader` reads those, one block at a time, once the
    /// block is known to be small enough to build.
    pub blocks: Vec<PayloadBlockCondition>,
    pub loader: BlockLoader<'a>,
}

/// Reads the points of payload blocks.
#[derive(Clone, Copy)]
pub(super) struct BlockLoader<'a> {
    pub payload_index: &'a StructPayloadIndex,
    pub vector_storage: &'a VectorStorageEnum,
    pub config: &'a HnswGraphConfig,
    pub stopped: &'a AtomicBool,
}

impl BlockLoader<'_> {
    /// Read the non-deleted points of `block`. `None` if the block is mostly deleted.
    pub fn load(&self, block: PayloadBlockCondition) -> OperationResult<Option<PayloadBlock>> {
        let Self {
            payload_index,
            vector_storage,
            config,
            stopped,
        } = *self;
        load_payload_block(block, payload_index, vector_storage, config, stopped)
    }
}

/// One payload block, with its points.
pub(super) struct PayloadBlock {
    /// The payload index's estimate. The size limit is checked against this.
    pub cardinality: usize,
    /// The non-deleted points of the block.
    pub points: Vec<PointOffsetType>,
}

/// Indexed fields that get payload block graphs, in build order.
///
/// The order matters: every field after the first skips blocks the graph built so far already
/// connects well, so it decides which blocks get built. `indexed_fields` is a `HashMap`, whose
/// iteration order changes from one map to the next, so sort to make builds reproducible.
pub(super) fn payload_block_fields(
    payload_index: &StructPayloadIndex,
    payload_m: HnswM,
) -> Vec<(PayloadKeyType, PayloadFieldSchema)> {
    if payload_m.m == 0 {
        return Vec::new();
    }
    let mut fields = payload_index
        .with_view(|v| v.indexed_fields())
        .into_iter()
        .filter(|(field, payload_schema)| {
            let enable_hnsw = payload_schema.enable_hnsw();
            if !enable_hnsw {
                debug!("enable_hnsw=false. Skip building additional index for field {field}");
            }
            enable_hnsw
        })
        .collect::<Vec<_>>();
    fields.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
    fields
}

/// The payload blocks of `field`, without their points: [`BlockLoader`] reads those, one block
/// at a time.
///
/// With an `old_index`, a numeric field keeps the block ranges of the old index where it can,
/// see [`old_range_blocks`].
pub(super) fn field_payload_blocks(
    field: &PayloadKeyType,
    schema: &PayloadFieldSchema,
    payload_index: &StructPayloadIndex,
    old_index: Option<&OldIndex<'_>>,
    config: &HnswGraphConfig,
    stopped: &AtomicBool,
) -> OperationResult<Vec<PayloadBlockCondition>> {
    let mut blocks = Vec::new();
    let mut add_block = |payload_block: PayloadBlockCondition| {
        check_process_stopped(stopped)?;
        blocks.push(payload_block);
        Ok(())
    };

    let old_blocks = match old_index {
        Some(old_index) => old_range_blocks(old_index, field, schema, payload_index, config)?,
        None => None,
    };
    match old_blocks {
        Some(old_blocks) => {
            debug!(
                "healing {field} with the {} block ranges of the old index",
                old_blocks.len()
            );
            // Match blocks come from the new index, so a value that reached the threshold
            // since the old build gets one too. The new index yields them before its ranges.
            payload_index.with_view(|v| {
                v.for_each_payload_block(field, config.full_scan_threshold, &mut |block| {
                    if block.condition.range.is_some() {
                        return Ok(());
                    }
                    add_block(block)
                })
            })?;
            old_blocks.into_iter().try_for_each(&mut add_block)?;
        }
        None => payload_index.with_view(|v| {
            v.for_each_payload_block(field, config.full_scan_threshold, &mut add_block)
        })?,
    }
    Ok(blocks)
}

/// Read the non-deleted points of `payload_block`. `None` if the block is mostly deleted.
fn load_payload_block(
    payload_block: PayloadBlockCondition,
    payload_index: &StructPayloadIndex,
    vector_storage: &VectorStorageEnum,
    config: &HnswGraphConfig,
    stopped: &AtomicBool,
) -> OperationResult<Option<PayloadBlock>> {
    let points = condition_points(
        payload_block.condition,
        payload_index,
        vector_storage,
        stopped,
    )?;

    // This is a heuristic to skip building graph for mostly deleted blocks.
    // It might be, that majority of points do not actually have vectors
    // (vectors marked as deleted), so we can avoid building graph for such blocks.
    //
    // FYI: query heuristic does account
    // for deleted vectors via [`adjust_to_available_vectors`]
    const DELETED_POINTS_FACTOR: usize = 4; // allow block to have up to 75% of deleted points and still be indexed

    if points.len() > config.full_scan_threshold / DELETED_POINTS_FACTOR {
        Ok(Some(PayloadBlock {
            cardinality: payload_block.cardinality,
            points,
        }))
    } else {
        Ok(None)
    }
}

/// Get list of points for indexing, associated with payload block filtering condition
fn condition_points(
    condition: FieldCondition,
    payload_index: &StructPayloadIndex,
    vector_storage: &VectorStorageEnum,
    stopped: &AtomicBool,
) -> OperationResult<Vec<PointOffsetType>> {
    let filter = Filter::new_must(Field(condition));

    let _scope = ambient::unmeasured_guard(reason("Internal operation"));

    let deleted_bitslice = vector_storage.deleted_vector_bitslice();

    payload_index.with_view(|v| {
        let cardinality_estimation = v.estimate_cardinality(&filter)?;
        Ok(v.iter_filtered_points(
            &filter,
            &cardinality_estimation,
            stopped,
            DeferredBehavior::WithDeferred,
        )?
        .filter(|&point_id| !deleted_bitslice.get_bit(point_id as usize).unwrap_or(false))
        .collect())
    })
}
