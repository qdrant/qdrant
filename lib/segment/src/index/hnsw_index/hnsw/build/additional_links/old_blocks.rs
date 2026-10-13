use common::ambient;
use common::reason::reason;

use crate::common::operation_error::OperationResult;
use crate::index::PayloadIndexRead;
use crate::index::field_index::PayloadBlockCondition;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::types::Condition::Field;
use crate::types::{Filter, PayloadFieldSchema, PayloadKeyType, PayloadSchemaType};

/// Payload blocks of a numeric `field` with the ranges the old index used, and their
/// cardinalities in the new segment.
///
/// Numeric blocks are histogram ranges, so they move when points are added or removed, and a
/// block that straddles two old ones has few old links between its points. Blocks with the old
/// ranges can start from the old graph like keyword blocks do. Returns `None`, to compute new
/// blocks, if the field is not numeric in both indexes, if the threshold changed, if the old
/// index had no blocks for it, or if a range grew to more than twice its old size.
pub(super) fn old_range_blocks(
    old_index: &OldIndex<'_>,
    field: &PayloadKeyType,
    schema: &PayloadFieldSchema,
    payload_index: &StructPayloadIndex,
    config: &HnswGraphConfig,
) -> OperationResult<Option<Vec<PayloadBlockCondition>>> {
    let kind = schema.kind();
    if !matches!(
        kind,
        PayloadSchemaType::Integer | PayloadSchemaType::Float | PayloadSchemaType::Datetime
    ) || old_index.index.config.full_scan_threshold != config.full_scan_threshold
    {
        return Ok(None);
    }
    let old_payload_index = old_index.index.payload_index.borrow();
    let old_kind = old_payload_index.with_view(|v| v.indexed_fields().get(field).map(|s| s.kind()));
    if old_kind != Some(kind) {
        return Ok(None);
    }
    let mut blocks = Vec::new();
    old_payload_index.with_view(|v| {
        v.for_each_payload_block(field, config.full_scan_threshold, &mut |block| {
            blocks.push(block);
            Ok(())
        })
    })?;
    drop(old_payload_index);
    if blocks.is_empty() {
        return Ok(None);
    }

    let _scope = ambient::unmeasured_guard(reason("Internal operation"));
    for block in &mut blocks {
        let filter = Filter::new_must(Field(block.condition.clone()));
        let cardinality = payload_index
            .with_view(|v| v.estimate_cardinality(&filter))?
            .exp;
        if cardinality > 2 * block.cardinality.max(config.full_scan_threshold) {
            return Ok(None);
        }
        block.cardinality = cardinality;
    }
    Ok(Some(blocks))
}
