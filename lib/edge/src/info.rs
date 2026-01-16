use std::collections::HashMap;

use segment::types::{PayloadIndexInfo, PayloadKeyType};

use super::EdgeShard;

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
    /// Types of stored payload
    pub payload_schema: HashMap<PayloadKeyType, PayloadIndexInfo>,
}

impl EdgeShard {
    pub fn info(&self) -> ShardInfo {
        let mut segments_count = 0;
        let mut points_count = 0;
        let mut indexed_vectors_count = 0;
        let mut payload_schema = HashMap::new();

        for (_, segment) in self.segments.read().iter() {
            segments_count += 1;

            let segment_info = segment.get().read().info();

            points_count += segment_info.num_points;
            indexed_vectors_count += segment_info.num_indexed_vectors;

            for (payload_key, payload_index) in segment_info.index_schema {
                payload_schema
                    .entry(payload_key)
                    .and_modify(|total: &mut PayloadIndexInfo| total.points += payload_index.points)
                    .or_insert(payload_index);
            }
        }

        ShardInfo {
            segments_count,
            points_count,
            indexed_vectors_count,
            payload_schema,
        }
    }
}
