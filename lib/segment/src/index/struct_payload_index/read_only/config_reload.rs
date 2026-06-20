use std::collections::HashMap;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::universal_io::UniversalRead;

use super::{ReadOnlyIndexesMap, ReadOnlyStructPayloadIndex};
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead;
use crate::index::field_index::ReadOnlyFieldIndex;
use crate::index::payload_config::PayloadConfig;
use crate::types::{PayloadKeyType, VectorName, VectorNameBuf};
use crate::vector_storage::read_only::VectorStorageReadEnum;

/// Difference between the in-memory payload field indexes and the on-disk
/// payload config, with every new or changed field index already loaded.
///
/// Produced by [`ReadOnlyStructPayloadIndex::config_reload_diff`] (heavy load,
/// shared `&self`) and consumed by [`ReadOnlyStructPayloadIndex::apply_config_reload`]
/// (cheap swap, `&mut self`).
pub struct PayloadIndexReloadDiff<S: UniversalRead> {
    /// Full on-disk payload config to install on apply.
    new_config: PayloadConfig,
    /// Field indexes that are new, or whose config changed — loaded, ready to
    /// install. Installing overwrites any existing entry for the same field; an
    /// empty entry means the field is no longer indexed and is dropped instead.
    added: ReadOnlyIndexesMap<S>,
    /// Fields that disappeared from the config entirely — to drop.
    removed: Vec<PayloadKeyType>,
}

impl<S: UniversalRead> PayloadIndexReloadDiff<S> {
    /// Whether the on-disk config matches the loaded field indexes (nothing to apply).
    pub fn is_empty(&self) -> bool {
        let Self {
            new_config: _,
            added,
            removed,
        } = self;
        added.is_empty() && removed.is_empty()
    }
}

impl<S: UniversalRead> ReadOnlyStructPayloadIndex<S> {
    /// Compute the difference between the loaded field indexes and `new_config`,
    /// eagerly loading every new or changed field index through `fs`.
    ///
    /// Uses shared `&self`, so the index keeps serving reads while the (heavy)
    /// loading happens. A field present in both configs but with a different
    /// schema/type set is reloaded — it lands in `added` and overwrites the old
    /// entry on apply.
    pub fn config_reload_diff(
        &self,
        fs: &S::Fs,
        new_config: PayloadConfig,
    ) -> OperationResult<PayloadIndexReloadDiff<S>> {
        let mut added: ReadOnlyIndexesMap<S> = HashMap::new();
        {
            let id_tracker = self.id_tracker.borrow();
            let total_point_count = id_tracker.total_point_count();
            let deleted_points = id_tracker.deleted_point_bitslice();

            for (field, indexed) in new_config.indices.iter() {
                // Unchanged fields keep their already-loaded indexes untouched.
                if self.config.indices.get(field) == Some(indexed) {
                    continue;
                }

                let mut indexes = Vec::with_capacity(indexed.types.len());
                for index_type in &indexed.types {
                    if let Some(index) = ReadOnlyFieldIndex::open(
                        fs,
                        &self.path,
                        field,
                        &indexed.schema,
                        index_type,
                        total_point_count,
                        deleted_points,
                    )? {
                        indexes.push(index);
                    }
                }
                // Keep empty vectors: apply turns them into a removal so the map
                // never holds an empty entry (mirrors `open`).
                added.insert(field.clone(), indexes);
            }
        }

        let removed = self
            .config
            .indices
            .keys()
            .filter(|field| !new_config.indices.contains_key(*field))
            .cloned()
            .collect();

        Ok(PayloadIndexReloadDiff {
            new_config,
            added,
            removed,
        })
    }

    /// Install a [`PayloadIndexReloadDiff`] produced by [`config_reload_diff`].
    ///
    /// Cheap, `&mut self`: no I/O, just swapping the already-loaded field indexes
    /// and the config into place.
    ///
    /// [`config_reload_diff`]: Self::config_reload_diff
    pub fn apply_config_reload(&mut self, diff: PayloadIndexReloadDiff<S>) {
        let PayloadIndexReloadDiff {
            new_config,
            added,
            removed,
        } = diff;

        for field in &removed {
            self.field_indexes.remove(field);
        }
        for (field, indexes) in added {
            // An empty index set means the field dropped all its index types.
            if indexes.is_empty() {
                self.field_indexes.remove(&field);
            } else {
                self.field_indexes.insert(field, indexes);
            }
        }

        self.config = new_config;
    }

    /// Register a vector storage in the `has_vector` lookup map.
    ///
    /// Must be called whenever a named vector is added to the segment, otherwise
    /// `has_vector` queries cannot see the new storage.
    pub fn register_vector_storage(
        &mut self,
        vector_name: VectorNameBuf,
        vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
    ) {
        self.vector_storages.insert(vector_name, vector_storage);
    }

    /// Drop a vector storage from the `has_vector` lookup map.
    ///
    /// Must be called whenever a named vector is removed from the segment,
    /// otherwise `has_vector` queries keep matching against the deleted storage.
    pub fn unregister_vector_storage(&mut self, vector_name: &VectorName) {
        self.vector_storages.remove(vector_name);
    }
}
