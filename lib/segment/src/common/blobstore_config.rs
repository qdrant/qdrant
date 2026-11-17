//! The storage mode every Blobstore-backed component is created with.

use blobstore::config::{CreateOptions, StorageConfig};
use common::flags::feature_flags;

/// The [`StorageConfig`] a Blobstore-backed storage is created with: mutable,
/// or append-only when the [`append_only_storages`] feature flag is set.
///
/// Consulted at creation only — an existing storage keeps its persisted mode.
///
/// [`append_only_storages`]: common::flags::FeatureFlags::append_only_storages
pub fn storage_config(options: CreateOptions) -> StorageConfig {
    options.into_config(feature_flags().append_only_storages)
}
