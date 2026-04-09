use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;

use super::Segment;
use crate::common::operation_error::OperationResult;
use crate::data_types::vector_name_config::VectorNameConfig;
use crate::id_tracker::IdTracker as _;
use crate::index::VectorIndexEnum;
use crate::index::plain_vector_index::PlainVectorIndex;
use crate::index::sparse_index::sparse_index_config::SparseIndexType;
use crate::index::sparse_index::sparse_vector_index::SparseVectorIndexOpenArgs;
use crate::segment::VectorData;
use crate::segment_constructor::{
    create_sparse_vector_index, create_sparse_vector_storage, get_vector_index_path,
    get_vector_storage_path, open_vector_storage,
};
use crate::types::{
    SeqNumberType, SparseVectorDataConfig, VectorDataConfig, VectorName, VectorStorageType,
};
use crate::vector_storage::dense::empty_dense_vector_storage::new_empty_dense_vector_storage;
use crate::vector_storage::sparse::empty_sparse_vector_storage::new_empty_sparse_vector_storage;

impl Segment {
    /// Core logic for creating a new named vector.
    /// Called from the `NonAppendableSegmentEntry::create_vector_name` trait impl in entry.rs.
    pub(super) fn create_vector_name_impl(
        &mut self,
        op_num: SeqNumberType,
        vector_name: &VectorName,
        config: &VectorNameConfig,
    ) -> OperationResult<bool> {
        // Idempotent: if vector already exists, return false
        if self.vector_data.contains_key(vector_name) {
            return Ok(false);
        }

        match config {
            VectorNameConfig::Dense(wrapper) => {
                let internal = wrapper.dense.to_internal(false);
                self.create_dense_vector(vector_name, &internal)
            }
            VectorNameConfig::Sparse(wrapper) => {
                let internal = wrapper.sparse.to_internal();
                self.create_sparse_vector(vector_name, &internal)
            }
        }?;

        // Persist and track version
        Segment::save_state(&self.get_state(), &self.segment_path)?;
        self.version_tracker
            .set_vector_names_schema(vector_name, Some(op_num));

        Ok(true)
    }

    fn create_dense_vector(
        &mut self,
        vector_name: &VectorName,
        config: &VectorDataConfig,
    ) -> OperationResult<()> {
        let num_points = self.id_tracker.borrow().total_point_count();

        let mut vector_storage = if self.appendable_flag {
            // Appendable segment: create real writable storage
            let storage_path = get_vector_storage_path(&self.segment_path, vector_name);
            // Use the configured storage type for appendable segments
            let config_for_open = VectorDataConfig {
                // Override storage type to appendable chunked mmap
                storage_type: VectorStorageType::from_on_disk(config.storage_type.is_on_disk()),
                // Use plain index for new vectors
                index: crate::types::Indexes::Plain {},
                ..config.clone()
            };
            open_vector_storage(&config_for_open, &storage_path)?
        } else {
            // Immutable segment: create empty placeholder
            new_empty_dense_vector_storage(
                config.size,
                config.distance,
                config.datatype.unwrap_or_default(),
                config.storage_type.is_on_disk(),
                config.multivector_config,
                num_points,
            )
        };

        // Fill storage with deleted entries for all existing points so that
        // total_vector_count matches the segment's point count.
        vector_storage.prefill_deleted_entries(num_points)?;

        let vector_storage = Arc::new(AtomicRefCell::new(vector_storage));
        let quantized_vectors = Arc::new(AtomicRefCell::new(None));

        // Create plain index for the new storage
        let vector_index = VectorIndexEnum::Plain(PlainVectorIndex::new(
            self.id_tracker.clone(),
            vector_storage.clone(),
            quantized_vectors.clone(),
            self.payload_index.clone(),
        ));

        // Register the new storage with the payload index so `has_vector`
        // filtering sees it immediately, not just after a restart.
        self.payload_index
            .borrow_mut()
            .register_vector_storage(vector_name.to_owned(), vector_storage.clone());

        self.vector_data.insert(
            vector_name.to_owned(),
            VectorData {
                vector_index: Arc::new(AtomicRefCell::new(vector_index)),
                vector_storage,
                quantized_vectors,
            },
        );
        self.segment_config
            .vector_data
            .insert(vector_name.to_owned(), config.clone());

        Ok(())
    }

    fn create_sparse_vector(
        &mut self,
        vector_name: &VectorName,
        config: &SparseVectorDataConfig,
    ) -> OperationResult<()> {
        let num_points = self.id_tracker.borrow().total_point_count();

        // Choose appropriate index type based on segment appendability
        let index_type = if self.appendable_flag {
            SparseIndexType::MutableRam
        } else if config.index.index_type.is_appendable() {
            // Immutable segment can't use MutableRam, upgrade to Mmap
            SparseIndexType::Mmap
        } else {
            config.index.index_type
        };

        let mut effective_config = *config;
        effective_config.index.index_type = index_type;

        let mut vector_storage = if self.appendable_flag {
            // Appendable: create real sparse mmap storage
            let storage_path = get_vector_storage_path(&self.segment_path, vector_name);
            create_sparse_vector_storage(&storage_path, &effective_config.storage_type)?
        } else {
            // Immutable: empty placeholder
            new_empty_sparse_vector_storage(num_points)
        };

        // Fill storage with deleted entries for all existing points so that
        // total_vector_count matches the segment's point count.
        vector_storage.prefill_deleted_entries(num_points)?;

        let vector_storage = Arc::new(AtomicRefCell::new(vector_storage));
        let quantized_vectors = Arc::new(AtomicRefCell::new(None));

        // Create sparse vector index
        let vector_index_path = get_vector_index_path(&self.segment_path, vector_name);
        let stopped = AtomicBool::new(false);
        let vector_index = create_sparse_vector_index(SparseVectorIndexOpenArgs {
            config: effective_config.index,
            id_tracker: self.id_tracker.clone(),
            vector_storage: vector_storage.clone(),
            payload_index: self.payload_index.clone(),
            path: &vector_index_path,
            stopped: &stopped,
            tick_progress: || (),
            deferred_internal_id: None,
        })?;

        // Register the new storage with the payload index so `has_vector`
        // filtering sees it immediately, not just after a restart.
        self.payload_index
            .borrow_mut()
            .register_vector_storage(vector_name.to_owned(), vector_storage.clone());

        self.vector_data.insert(
            vector_name.to_owned(),
            VectorData {
                vector_index: Arc::new(AtomicRefCell::new(vector_index)),
                vector_storage,
                quantized_vectors,
            },
        );
        self.segment_config
            .sparse_vector_data
            .insert(vector_name.to_owned(), effective_config);

        Ok(())
    }

    /// Core logic for deleting a named vector.
    /// Called from the `NonAppendableSegmentEntry::delete_vector_name` trait impl in entry.rs.
    pub(super) fn delete_vector_name_impl(
        &mut self,
        _op_num: SeqNumberType,
        vector_name: &VectorName,
    ) -> OperationResult<bool> {
        // Idempotent: if vector doesn't exist, return false
        if !self.vector_data.contains_key(vector_name) {
            return Ok(false);
        }

        // Drop the storage from the payload index lookup so `has_vector`
        // filtering stops matching against the deleted vector immediately.
        self.payload_index
            .borrow_mut()
            .unregister_vector_storage(vector_name);

        // Remove from runtime data (drops VectorData - releases storage/index/quantized)
        self.vector_data.remove(vector_name);

        // Remove from config (could be either dense or sparse)
        self.segment_config.vector_data.remove(vector_name);
        self.segment_config.sparse_vector_data.remove(vector_name);

        // Update version tracker
        self.version_tracker
            .set_vector_names_schema(vector_name, None);

        // Persist state
        Segment::save_state(&self.get_state(), &self.segment_path)?;

        // Clean up disk files (best-effort)
        let storage_path = get_vector_storage_path(&self.segment_path, vector_name);
        let index_path = get_vector_index_path(&self.segment_path, vector_name);
        if storage_path.exists()
            && let Err(e) = fs_err::remove_dir_all(&storage_path)
        {
            log::warn!(
                "Failed to remove vector storage at {}: {e}",
                storage_path.display()
            );
        }
        if index_path.exists()
            && let Err(e) = fs_err::remove_dir_all(&index_path)
        {
            log::warn!(
                "Failed to remove vector index at {}: {e}",
                index_path.display()
            );
        }

        Ok(true)
    }

    /// Reconcile named vectors in this segment to match the desired configuration.
    ///
    /// Creates vectors that are in `desired` but missing from the segment.
    /// Does NOT delete extra vectors (segment may have vectors from ongoing operations).
    ///
    /// This is similar to `update_all_field_indices` for payload indexes.
    pub fn update_all_vector_names(
        &mut self,
        desired: &[(crate::types::VectorNameBuf, VectorNameConfig)],
    ) -> OperationResult<()> {
        let version = self.version.unwrap_or(0);

        for (name, config) in desired {
            if self.vector_data.contains_key(name) {
                continue;
            }
            log::warn!("Segment is missing vector '{name}', creating it now");
            self.create_vector_name_impl(version, name, config)?;
        }

        Ok(())
    }
}
