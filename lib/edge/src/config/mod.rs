//! Configuration for [`crate::EdgeShard`].
//!
//! User-facing structures only: no `SegmentConfig`, `payload_storage_type`, or
//! per-vector quantization. Use `on_disk_payload`, `on_disk` per vector, and
//! global `quantization_config` / `hnsw_config`.

pub mod optimizers;
pub mod shard;
pub mod vectors;
