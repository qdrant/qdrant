use std::collections::HashMap;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::storage_version::StorageVersion;
use common::universal_io::{UniversalReadFs, read_json_via};

use super::{ReadOnlySegment, ReadOnlyVectorData};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::UniversalReadExt;
use crate::index::payload_config::PayloadConfig;
use crate::index::struct_payload_index::read_only::PayloadIndexReloadDiff;
use crate::segment::{SEGMENT_STATE_FILE, SegmentVersion};
use crate::segment_constructor::{get_payload_index_path, get_vector_storage_path};
use crate::types::{
    SegmentConfig, SegmentState, SegmentType, VectorDataConfig, VectorName, VectorNameBuf,
};
use crate::vector_storage::read_only::VectorStorageReadEnum;
use crate::vector_storage::sparse::read_only::ReadOnlySparseVectorStorage;

/// Vectors loaded by the diff phase, keyed by name, ready to install.
type AddedVectors<S> = HashMap<VectorNameBuf, ReadOnlyVectorData<S>>;

/// Split two config maps into the names to (re)load and the names to drop.
///
/// A name is loaded when it is new or its config changed; it is removed when it
/// vanished or its config changed. A changed name appears in *both* lists so the
/// stale component is dropped and the freshly loaded one installed.
fn diff_config_map<C: PartialEq>(
    old: &HashMap<VectorNameBuf, C>,
    new: &HashMap<VectorNameBuf, C>,
) -> (Vec<VectorNameBuf>, Vec<VectorNameBuf>) {
    let to_load = new
        .iter()
        .filter(|(name, config)| old.get(*name) != Some(config))
        .map(|(name, _)| name.clone())
        .collect();
    let to_remove = old
        .iter()
        .filter(|(name, config)| new.get(*name) != Some(config))
        .map(|(name, _)| name.clone())
        .collect();
    (to_load, to_remove)
}

/// Difference between a read-only segment's in-memory config and the current
/// on-disk config, with every newly added (or reloaded) vector already loaded.
///
/// Produced by [`ReadOnlySegment::config_reload_diff`] and consumed by
/// [`ReadOnlySegment::apply_config_reload`]. The split exists so the heavy
/// loading runs under a shared `&self` borrow (the segment keeps serving reads),
/// while the exclusive `&mut self` apply is a cheap swap.
///
/// A vector present in both configs but with a changed config is *reloaded*: its
/// name appears in both `removed_vectors` and `added_vectors`, so apply drops the
/// stale component and installs the freshly loaded one.
pub struct SegmentConfigReloadDiff<S: UniversalReadExt + 'static> {
    /// Full on-disk segment config to install on apply.
    new_config: SegmentConfig,
    /// Vectors that are new or changed — loaded, ready to install.
    added_vectors: AddedVectors<S>,
    /// Vectors that disappeared or changed — to drop before installing.
    removed_vectors: Vec<VectorNameBuf>,
    /// Payload field-index changes, with new indexes already loaded.
    payload: PayloadIndexReloadDiff<S>,
}

impl<S: UniversalReadExt + 'static> SegmentConfigReloadDiff<S> {
    /// Whether the on-disk config matches the in-memory state (nothing to apply).
    pub fn is_empty(&self) -> bool {
        let Self {
            new_config: _,
            added_vectors,
            removed_vectors,
            payload,
        } = self;
        added_vectors.is_empty() && removed_vectors.is_empty() && payload.is_empty()
    }
}

impl<S: UniversalReadExt + 'static> ReadOnlySegment<S> {
    /// Re-read the on-disk config and compute its difference against the
    /// in-memory config, eagerly loading every new or changed component.
    ///
    /// Heavy I/O (opening vector storages, indexes, quantized vectors and payload
    /// field indexes) happens here under a shared `&self` borrow, so the segment
    /// keeps answering queries while loading. Apply the result with
    /// [`apply_config_reload`](Self::apply_config_reload) under `&mut self`.
    ///
    /// Covers added/removed dense vectors, sparse vectors and payload field
    /// indexes. Vectors whose config changed are reloaded (drop + load).
    pub fn config_reload_diff(&self, fs: &S::Fs) -> OperationResult<SegmentConfigReloadDiff<S>> {
        // Component opens go straight to the raw backend — per-reload
        // caching/prefetch is a later iteration.
        let new_config = self.read_new_config(fs)?;
        let (added_vectors, removed_vectors) = self.diff_vectors(fs, &new_config)?;
        let payload = self.diff_payload(fs)?;

        Ok(SegmentConfigReloadDiff {
            new_config,
            added_vectors,
            removed_vectors,
            payload,
        })
    }

    /// Re-read the on-disk segment config, verifying the version file is present.
    fn read_new_config(&self, fs: &S::Fs) -> OperationResult<SegmentConfig> {
        let segment_path = &self.segment_path;
        if SegmentVersion::load_universal(fs, segment_path)?.is_none() {
            return Err(OperationError::service_error(format!(
                "Segment version file not found in segment: {}",
                segment_path.display(),
            )));
        }

        let SegmentState {
            initial_version: _,
            version: _,
            config,
        } = read_json_via(fs, segment_path.join(SEGMENT_STATE_FILE))?;
        Ok(config)
    }

    /// Compute the dense + sparse vector diff against `new_config`, eagerly
    /// loading every new or changed vector. Returns `(added, removed)`.
    fn diff_vectors(
        &self,
        fs: &impl UniversalReadFs<File = S>,
        new_config: &SegmentConfig,
    ) -> OperationResult<(AddedVectors<S>, Vec<VectorNameBuf>)> {
        let mut added = HashMap::new();
        let mut removed = Vec::new();

        let (load, drop) =
            diff_config_map(&self.segment_config.vector_data, &new_config.vector_data);
        for name in load {
            let config = &new_config.vector_data[&name];
            let data = self.load_dense_vector(fs, &name, config, new_config)?;
            added.insert(name, data);
        }
        removed.extend(drop);

        let (load, drop) = diff_config_map(
            &self.segment_config.sparse_vector_data,
            &new_config.sparse_vector_data,
        );
        for name in load {
            let data = self.load_sparse_vector(fs, &name)?;
            added.insert(name, data);
        }
        removed.extend(drop);

        Ok((added, removed))
    }

    /// Open one dense vector's storage, index and quantized vectors from disk.
    fn load_dense_vector(
        &self,
        fs: &impl UniversalReadFs<File = S>,
        name: &VectorName,
        config: &VectorDataConfig,
        new_config: &SegmentConfig,
    ) -> OperationResult<ReadOnlyVectorData<S>> {
        let path = get_vector_storage_path(&self.segment_path, name);
        // A config reload follows the new config alone: the request-specific
        // load profile of the original open (if any) does not outlive it.
        let storage = VectorStorageReadEnum::open(fs, config, &path, None)?.ok_or_else(|| {
            OperationError::service_error(format!(
                "Read-only dense vector storage '{name}' was not found, or is corrupted.",
            ))
        })?;
        let storage = Arc::new(AtomicRefCell::new(storage));
        ReadOnlyVectorData::open_dense(
            fs,
            &self.segment_path,
            name,
            config,
            new_config,
            self.id_tracker.clone(),
            self.payload_index.clone(),
            storage,
            None,
        )
    }

    /// Open one sparse vector's storage and index from disk (never quantized).
    fn load_sparse_vector(
        &self,
        fs: &impl UniversalReadFs<File = S>,
        name: &VectorName,
    ) -> OperationResult<ReadOnlyVectorData<S>> {
        let path = get_vector_storage_path(&self.segment_path, name);
        let storage =
            VectorStorageReadEnum::Sparse(Box::new(ReadOnlySparseVectorStorage::open(fs, &path)?));
        let storage = Arc::new(AtomicRefCell::new(storage));
        ReadOnlyVectorData::open_sparse(
            fs,
            &self.segment_path,
            name,
            self.id_tracker.clone(),
            self.payload_index.clone(),
            storage,
            None,
        )
    }

    /// Load the payload field-index diff (delegates to the payload index, which
    /// loads its own new/changed field indexes under a shared borrow).
    fn diff_payload(
        &self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<PayloadIndexReloadDiff<S>> {
        let payload_index_path = get_payload_index_path(&self.segment_path);
        let payload_config_path = PayloadConfig::get_config_path(&payload_index_path);
        let new_payload_config = PayloadConfig::load_universal(fs, &payload_config_path)?
            .ok_or_else(|| {
                OperationError::service_error(format!(
                    "Read-only payload index missing config at {}",
                    payload_config_path.display(),
                ))
            })?;
        self.payload_index
            .borrow()
            .config_reload_diff(fs, new_payload_config)
    }

    /// Install a [`SegmentConfigReloadDiff`] produced by
    /// [`config_reload_diff`](Self::config_reload_diff).
    ///
    /// Cheap, `&mut self`: no loading happens here — components were loaded while
    /// computing the diff. Removals run before insertions so a reloaded vector
    /// (present in both lists) ends up with its fresh component.
    pub fn apply_config_reload(&mut self, diff: SegmentConfigReloadDiff<S>) {
        let Self {
            uuid: _,
            segment_path: _,
            id_tracker: _,
            vector_data,
            payload_index,
            payload_storage: _,
            pending_reload: _,
            segment_type,
            segment_config,
        } = self;

        let SegmentConfigReloadDiff {
            new_config,
            added_vectors,
            removed_vectors,
            payload,
        } = diff;

        // Keep the payload index's `has_vector` map and field indexes in sync
        // under a single exclusive borrow.
        {
            let mut payload_index = payload_index.borrow_mut();
            for vector_name in &removed_vectors {
                payload_index.unregister_vector_storage(vector_name);
            }
            for (vector_name, data) in &added_vectors {
                payload_index
                    .register_vector_storage(vector_name.clone(), data.vector_storage.clone());
            }
            payload_index.apply_config_reload(payload);
        }

        for vector_name in &removed_vectors {
            vector_data.remove(vector_name);
        }
        for (vector_name, data) in added_vectors {
            vector_data.insert(vector_name, data);
        }

        *segment_type = if new_config.is_any_vector_indexed() {
            SegmentType::Indexed
        } else {
            SegmentType::Plain
        };
        *segment_config = new_config;
    }
}
