//! Configuration for [`crate::EdgeShard`].
//!
//! User-facing structures only: no `SegmentConfig`, `payload_storage_type`, or
//! per-vector quantization. Use `on_disk_payload`, `on_disk` per vector, and
//! global `quantization_config` / `hnsw_config`.

mod optimizers;
mod shard;
mod vectors;

pub use optimizers::EdgeOptimizersConfig;
pub use shard::{EDGE_CONFIG_FILE, EdgeShardConfig};
#[allow(unused_imports)]
pub use vectors::{EdgeSparseVectorParams, EdgeVectorParams};
