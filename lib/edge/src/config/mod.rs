//! Configuration for [`crate::EdgeShard`].
//!
//! User-facing structures only: no `SegmentConfig` or `payload_storage_type`.
//! Prefer `memory` / `payload_memory` over the deprecated `on_disk` /
//! `on_disk_payload` flags; use global `quantization_config` / `hnsw_config`.

pub mod optimizers;
pub mod shard;
pub mod vectors;
