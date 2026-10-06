use std::collections::HashMap;

use segment::common::operation_error::OperationResult;
use segment::entry::{ReadSegmentEntry, VectorIndexInfo, VectorIndexInfoProvider};
use segment::types::{PayloadIndexInfo, PayloadKeyType, VectorNameBuf};

use crate::read_view::{EdgeReadView, ReadSegmentHandle};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub struct ShardInfo {
    /// Number of segments in shard.
    /// Each segment has independent vector as payload indexes
    pub segments_count: usize,
    /// Approximate number of points (vectors + payloads) in shard.
    /// Each point could be accessed by unique id.
    pub points_count: usize,
    /// Approximate number of indexed vectors in the shard.
    /// Indexed vectors in large segments are faster to query,
    /// as it is stored in vector index (HNSW).
    pub indexed_vectors_count: usize,
    /// Runtime index metadata for each named vector, with one entry per segment containing it.
    /// Empty storages are included. Entry order is unspecified.
    ///
    /// These are the indexes observed under each segment's read lock during this `info` call;
    /// a later call may observe different indexes after optimization or live reload.
    pub vector_indexes: HashMap<VectorNameBuf, Vec<VectorIndexInfo>>,
    /// Types of stored payload
    pub payload_schema: HashMap<PayloadKeyType, PayloadIndexInfo>,
}

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    pub(crate) fn info(&self) -> OperationResult<ShardInfo> {
        self.check_stopped()?;
        let mut segments_count = 0;
        let mut points_count = 0;
        let mut indexed_vectors_count = 0;
        let mut vector_indexes = HashMap::<VectorNameBuf, Vec<VectorIndexInfo>>::new();
        let mut payload_schema = HashMap::new();

        for segment in &self.segments {
            self.check_stopped()?;
            segments_count += 1;

            let segment = segment.read_segment();
            let segment_info = segment.info()?;

            for (name, index) in segment.vector_index_info() {
                self.check_stopped()?;
                vector_indexes.entry(name).or_default().push(index);
            }

            points_count += segment_info.num_points;
            indexed_vectors_count += segment_info.num_indexed_vectors;

            for (payload_key, payload_index) in segment_info.index_schema {
                payload_schema
                    .entry(payload_key)
                    .and_modify(|total: &mut PayloadIndexInfo| total.points += payload_index.points)
                    .or_insert(payload_index);
            }
        }

        self.check_stopped()?;
        Ok(ShardInfo {
            segments_count,
            points_count,
            indexed_vectors_count,
            vector_indexes,
            payload_schema,
        })
    }
}
