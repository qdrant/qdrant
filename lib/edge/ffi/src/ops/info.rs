//! [`EdgeShard::info`] — shard summary statistics.

use crate::EdgeShard;
use crate::error::Result;

#[uniffi::export]
impl EdgeShard {
    /// Returns summary information about the shard: number of segments,
    /// total points, and indexed vector count.
    ///
    /// Useful for debugging, UI "collection stats" screens, and sanity
    /// checks.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`](crate::error::EdgeError) if the
    /// shard is unloaded.
    pub fn info(&self) -> Result<ShardInfo> {
        self.with_shard(|shard| {
            let edge::ShardInfo {
                segments_count,
                points_count,
                indexed_vectors_count,
                payload_schema: _,
            } = shard.info()?;
            Ok(ShardInfo {
                segments_count: segments_count as u64,
                points_count: points_count as u64,
                indexed_vectors_count: indexed_vectors_count as u64,
            })
        })
    }
}

// ── ShardInfo ───────────────────────────────────────────────────────────────

/// Summary information about a shard's on-disk state.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ShardInfo {
    /// Number of segments in the shard.
    pub segments_count: u64,
    /// Total number of points across all segments.
    pub points_count: u64,
    /// Number of vectors that have been indexed (i.e. reachable via ANN).
    pub indexed_vectors_count: u64,
}
