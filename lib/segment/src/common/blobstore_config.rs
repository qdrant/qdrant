//! The storage mode every Blobstore-backed component is created with.

use blobstore::config::{GridstoreConfig, LogstoreConfig, StorageConfig};
use common::flags::feature_flags;

/// The creation config for a Blobstore-backed storage: the given mutable
/// (Gridstore) layout, or its append-only (Logstore) counterpart when the
/// [`append_only_storages`] feature flag is set.
///
/// Consulted at creation only — an existing storage keeps its persisted mode.
/// Page size and compression carry over; blocks and regions have no
/// append-only equivalent.
///
/// [`append_only_storages`]: common::flags::FeatureFlags::append_only_storages
pub fn blobstore_config(config_if_mutable: GridstoreConfig) -> StorageConfig {
    if !feature_flags().append_only_storages {
        return StorageConfig::Mutable(config_if_mutable);
    }

    let GridstoreConfig {
        page_size_bytes,
        block_size_bytes: _,
        region_size_blocks: _,
        compression,
    } = config_if_mutable;

    StorageConfig::AppendOnly(LogstoreConfig {
        page_capacity_bytes: page_size_bytes,
        compression,
    })
}
