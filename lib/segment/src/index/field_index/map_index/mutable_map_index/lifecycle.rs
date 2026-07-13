use std::path::PathBuf;

use blobstore::config::StorageOptions;
use blobstore::error::GridstoreError;
use blobstore::{Blob, Blobstore};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, Populate};

use super::super::MapIndexKey;
use super::MutableMapIndex;
use super::in_memory::InMemoryMapIndex;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};

/// Default options for Gridstore storage
const fn default_gridstore_options(block_size: usize) -> StorageOptions {
    StorageOptions {
        // Size dependent on map value type
        block_size_bytes: Some(block_size),
        compression: Some(blobstore::config::Compression::None),
        page_size_bytes: Some(block_size * 8192 * 32), // 4 to 8 MiB = block_size * region_blocks * regions,
        region_size_blocks: None,
        mode: None,
    }
}

impl<N: MapIndexKey + ?Sized> MutableMapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    /// Open and load mutable map index from Gridstore storage
    ///
    /// The `create_if_missing` parameter indicates whether to create a new Gridstore if it does
    /// not exist. If false and files don't exist, the load function will indicate nothing could be
    /// loaded.
    ///
    /// `prefix_index` enables in-memory prefix range scans; it is not
    /// persisted, so it must be re-supplied (from the payload schema) on every
    /// open.
    pub fn open_gridstore(
        path: PathBuf,
        create_if_missing: bool,
        prefix_index: bool,
    ) -> OperationResult<Option<Self>> {
        let store = if create_if_missing {
            let options = default_gridstore_options(N::gridstore_block_size());
            Blobstore::open_or_create(MmapFs, path, options, Populate::Blocking).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open mutable map index on gridstore: {err}"
                ))
            })?
        } else if path.exists() {
            Blobstore::open(MmapFs, path, Populate::Blocking).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open mutable map index on gridstore: {err}"
                ))
            })?
        } else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        // Load in-memory index from Gridstore
        let mut in_memory_index = InMemoryMapIndex::<N>::empty(prefix_index);

        let hw_counter = HardwareCounterCell::disposable();
        let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();
        store
            .iter::<_, GridstoreError>(
                |idx, values: Vec<_>| {
                    in_memory_index.add_many_to_map(idx, values);
                    Ok(true)
                },
                hw_counter_ref,
            )
            // unwrap safety: never returns an error
            .unwrap();

        Ok(Some(Self {
            in_memory_index,
            storage: store,
        }))
    }

    pub fn add_many_to_map<Q>(
        &mut self,
        idx: PointOffsetType,
        values: Vec<Q>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>
    where
        Q: Into<<N as MapIndexKey>::Owned> + Clone,
    {
        if values.is_empty() {
            return Ok(());
        }

        let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();

        let values = values.into_iter().map(Into::into).collect::<Vec<_>>();
        self.storage
            .put_value(idx, &values, hw_counter_ref)
            .map_err(|err| {
                OperationError::service_error(format!(
                    "failed to put value in mutable map index gridstore: {err}"
                ))
            })?;

        self.in_memory_index.add_many_to_map(idx, values);

        Ok(())
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        if !self.in_memory_index.remove_point(idx) {
            return Ok(());
        }

        self.storage.delete_value(idx)?;

        Ok(())
    }

    #[inline]
    pub(in super::super) fn clear(&mut self) -> OperationResult<()> {
        self.storage.clear().map_err(|err| {
            OperationError::service_error(format!("Failed to clear mutable map index: {err}"))
        })
    }

    #[inline]
    pub(in super::super) fn wipe(self) -> OperationResult<()> {
        self.storage.wipe().map_err(|err| {
            OperationError::service_error(format!("Failed to wipe mutable map index: {err}"))
        })
    }

    /// Clear gridstore disk cache. Does not affect the in-memory index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache().map_err(|err| {
            OperationError::service_error(format!(
                "Failed to clear mutable map index gridstore cache: {err}"
            ))
        })
    }

    #[inline]
    pub(in super::super) fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    #[inline]
    pub(in super::super) fn flusher(&self) -> Flusher {
        let storage_flusher = self.storage.flusher();
        Box::new(move || storage_flusher().map_err(OperationError::from))
    }
}
