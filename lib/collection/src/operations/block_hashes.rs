//! On-demand block hashes. The public v1 byte contract and reference client are
//! in `tools/block_hashes.py`; shared test vectors are in `tests/fixtures/block_hashes.json`.

use std::sync::atomic::AtomicBool;

use schemars::JsonSchema;
use segment::common::operation_error::check_process_stopped;
use segment::json_path::{JsonPath, JsonPathItem};
use segment::types::{ExtendedPointId, Filter, Payload, slice_point_id_hash};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use validator::{Validate, ValidationError};

use super::types::{CollectionError, CollectionResult};

/// Compute deterministic block hashes of point IDs and one stored string payload field.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields)]
pub struct BlockHashesRequest {
    /// Object path to a stored fingerprint string. Array traversal is not supported.
    #[validate(custom(function = "validate_payload_key"))]
    pub payload_key: JsonPath,
    /// Number of blocks to return, including empty blocks (1..=65536).
    #[validate(range(min = 1, max = 65536))]
    pub block_count: u32,
    /// Only include points matching this filter.
    #[validate(nested)]
    pub filter: Option<Filter>,
}

fn validate_payload_key(key: &JsonPath) -> Result<(), ValidationError> {
    if key
        .rest
        .iter()
        .any(|part| !matches!(part, JsonPathItem::Key(_)))
    {
        return Err(ValidationError::new(
            "block hashes require an object path without array indices or wildcards",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BlockHash {
    pub block_id: u32,
    pub point_count: u64,
    /// Lowercase hexadecimal SHA-256 digest under the block-hashes v1 contract.
    pub hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BlockHashesResponse {
    /// All requested blocks, ordered by block_id.
    pub blocks: Vec<BlockHash>,
}

const POINT_DOMAIN: &[u8] = b"qdrant:point-fingerprint:v1\0";
const BLOCK_DOMAIN: &[u8] = b"qdrant:block-hashes:v1\0";

/// Consumes strictly increasing logical IDs, as returned by collection scroll.
/// Shard digests must never be concatenated: ordering is global, before hashing.
pub(crate) struct BlockHashAccumulator {
    blocks: Vec<(Sha256, u64)>,
    previous_id: Option<ExtendedPointId>,
}

impl BlockHashAccumulator {
    pub(crate) fn new(block_count: u32) -> Self {
        Self {
            blocks: (0..block_count)
                .map(|_| (Sha256::new_with_prefix(BLOCK_DOMAIN), 0))
                .collect(),
            previous_id: None,
        }
    }

    pub(crate) fn add(
        &mut self,
        id: ExtendedPointId,
        payload: Option<&Payload>,
        key: &JsonPath,
        stopped: &AtomicBool,
    ) -> CollectionResult<()> {
        check_process_stopped(stopped)?;
        if self.previous_id.is_some_and(|previous| previous >= id) {
            return Err(CollectionError::service_error(
                "Block hash scan returned unordered or duplicate point IDs",
            ));
        }
        let values = payload.map(|payload| key.value_get(&payload.0));
        let value = values
            .as_ref()
            .and_then(|values| match values.as_slice() {
                [serde_json::Value::String(value)] => Some(value.as_bytes()),
                _ => None,
            })
            .ok_or_else(|| {
                CollectionError::bad_request(format!(
                    "Point {id} must have a string at payload_key {key} for block hashes"
                ))
            })?;

        let mut point = Sha256::new_with_prefix(POINT_DOMAIN);
        match id {
            ExtendedPointId::NumId(num) => {
                point.update([0]);
                point.update(num.to_be_bytes());
            }
            ExtendedPointId::Uuid(uuid) => {
                point.update([1]);
                point.update(uuid.as_bytes());
            }
        }
        point.update((value.len() as u64).to_be_bytes());
        // Allow cancellation even for very large stored strings.
        for chunk in value.chunks(64 * 1024) {
            check_process_stopped(stopped)?;
            point.update(chunk);
        }
        let block_id = (slice_point_id_hash(id) % self.blocks.len() as u64) as usize;
        let (block, count) = &mut self.blocks[block_id];
        block.update(point.finalize());
        *count = count
            .checked_add(1)
            .ok_or_else(|| CollectionError::service_error("Block point count overflow"))?;
        self.previous_id = Some(id);
        Ok(())
    }

    pub(crate) fn finish(self, stopped: &AtomicBool) -> CollectionResult<BlockHashesResponse> {
        let blocks = self
            .blocks
            .into_iter()
            .enumerate()
            .map(|(block_id, (mut hash, point_count))| {
                check_process_stopped(stopped)?;
                hash.update(point_count.to_be_bytes());
                Ok(BlockHash {
                    block_id: block_id as u32,
                    point_count,
                    hash: hash
                        .finalize()
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect(),
                })
            })
            .collect::<CollectionResult<_>>()?;
        Ok(BlockHashesResponse { blocks })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[derive(Deserialize)]
    struct TestRecord {
        id: ExtendedPointId,
        value: String,
    }

    #[test]
    fn cross_language_vectors() {
        let vectors: serde_json::Value =
            serde_json::from_str(include_str!("../../../../tests/fixtures/block_hashes.json"))
                .unwrap();
        let stopped = AtomicBool::new(false);
        let mut points: Vec<TestRecord> =
            serde_json::from_value(vectors["records"].clone()).unwrap();
        points.sort_by_key(|point| point.id);
        let key: JsonPath = "sync.fingerprint".parse().unwrap();
        let mut accumulator = BlockHashAccumulator::new(16);
        for point in points {
            let payload =
                serde_json::from_value(json!({"sync": {"fingerprint": point.value}})).unwrap();
            accumulator
                .add(point.id, Some(&payload), &key, &stopped)
                .unwrap();
        }
        let result = accumulator.finish(&stopped).unwrap();
        assert_eq!(result.blocks.len(), 16);
        for (index, block) in result.blocks.iter().enumerate() {
            assert_eq!(block.block_id as usize, index);
            if block.point_count == 0 {
                assert_eq!(block.hash, vectors["empty_hash"].as_str().unwrap());
            }
        }
        let nonempty: Vec<_> = result
            .blocks
            .into_iter()
            .filter(|b| b.point_count > 0)
            .collect();
        assert_eq!(
            serde_json::to_value(nonempty).unwrap(),
            vectors["nonempty_blocks"]
        );
    }

    #[test]
    fn rejects_unordered_and_cancelled_records() {
        let key: JsonPath = "sync.fingerprint".parse().unwrap();
        let payload = serde_json::from_value(json!({"sync": {"fingerprint": "abc"}})).unwrap();
        let stopped = AtomicBool::new(false);
        let mut accumulator = BlockHashAccumulator::new(1);
        accumulator
            .add(42.into(), Some(&payload), &key, &stopped)
            .unwrap();
        for id in [42, 0] {
            assert!(
                accumulator
                    .add(id.into(), Some(&payload), &key, &stopped)
                    .is_err()
            );
        }
        stopped.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(
            accumulator
                .add(43.into(), Some(&payload), &key, &stopped)
                .is_err()
        );
        assert!(accumulator.finish(&stopped).is_err());
    }

    #[test]
    fn block_hashes_use_latest_duplicate_segment_version() {
        use std::time::Duration;

        use common::counter::hardware_accumulator::HwMeasurementAcc;
        use common::counter::hardware_counter::HardwareCounterCell;
        use common::types::DeferredBehavior;
        use segment::data_types::vectors::only_default_vector;
        use segment::entry::entry_point::SegmentEntry;
        use segment::types::{WithPayload, WithVector};
        use shard::fixtures::empty_segment;
        use shard::retrieve::retrieve_blocking::retrieve_blocking;
        use shard::segment_holder::SegmentHolder;
        use shard::segment_holder::locked::LockedSegmentHolder;

        let key: JsonPath = "sync.fingerprint".parse().unwrap();
        let stopped = AtomicBool::new(false);
        for versions in [[10, 20], [20, 10]] {
            let dir = tempfile::tempdir().unwrap();
            let counter = HardwareCounterCell::new();
            let mut holder = SegmentHolder::default();
            for version in versions {
                let mut segment = empty_segment(dir.path());
                segment
                    .upsert_point(version, 42.into(), only_default_vector(&[0.0; 4]), &counter)
                    .unwrap();
                let value = if version == 20 { "abc" } else { "stale" };
                let payload =
                    serde_json::from_value(json!({"sync": {"fingerprint": value}})).unwrap();
                segment
                    .set_payload(version, 42.into(), &payload, &None, &counter)
                    .unwrap();
                holder.add_new(segment);
            }
            let holder = LockedSegmentHolder::new(holder);
            // Scroll retrieves the newest payload once, even for duplicate IDs.
            let records = retrieve_blocking(
                holder,
                &[42.into(), 42.into()],
                &WithPayload::from(true),
                &WithVector::Bool(false),
                Duration::from_secs(5),
                &stopped,
                HwMeasurementAcc::new(),
                DeferredBehavior::VisibleOnly,
            )
            .unwrap();
            let mut accumulator = BlockHashAccumulator::new(1);
            assert_eq!(records.len(), 1);
            for record in records.values() {
                assert!(record.vector.is_none());
                accumulator
                    .add(record.id, record.payload.as_ref(), &key, &stopped)
                    .unwrap();
            }
            let vectors: serde_json::Value =
                serde_json::from_str(include_str!("../../../../tests/fixtures/block_hashes.json"))
                    .unwrap();
            let block = accumulator.finish(&stopped).unwrap().blocks.remove(0);
            assert_eq!(block.point_count, 1);
            assert_eq!(
                block.hash,
                vectors["nonempty_blocks"][0]["hash"].as_str().unwrap()
            );
        }
    }
}
