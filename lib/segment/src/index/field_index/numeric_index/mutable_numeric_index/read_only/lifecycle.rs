use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::{CachedReadFs, OkNotFound, Populate, UniversalRead};
use gridstore::{Blob, GridstoreReader};

use super::super::InMemoryNumericIndex;
use super::ReadOnlyAppendableNumericIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::numeric_index::Encodable;
use crate::index::field_index::numeric_point::Numericable;

impl<T: Encodable + Numericable + Send + Sync + Default, S: UniversalRead>
    ReadOnlyAppendableNumericIndex<T, S>
where
    Vec<T>: Blob,
{
    /// Open the appendable (Gridstore) numeric index read-only, threading every
    /// file open through the filesystem handle `fs`.
    ///
    /// Opens a [`GridstoreReader`] over the generic filesystem object, then
    /// rebuilds the in-memory index by feeding every stored point through
    /// [`InMemoryNumericIndex::add_many_to_list`] — the exact reconstruction the
    /// writable [`MutableNumericIndex::open_gridstore`][1] performs over a
    /// writable `Gridstore`. No write path; the reader is retained for
    /// `files` / `clear_cache`.
    ///
    /// Returns [`Ok(None)`] when the on-disk directory doesn't exist, matching
    /// the `create_if_missing == false` branch of the writable counterpart —
    /// the read path never creates.
    ///
    /// [1]: super::super::MutableNumericIndex::open_gridstore
    pub fn open(fs: &CachedReadFs<S::Fs>, path: PathBuf) -> OperationResult<Option<Self>> {
        let Some(storage) =
            GridstoreReader::<Vec<T>, S>::open(fs, path, Populate::Blocking).ok_not_found()?
        else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        let mut in_memory_index = InMemoryNumericIndex::default();
        let hw_counter = HardwareCounterCell::disposable();
        storage
            .iter::<_, OperationError>(
                storage.max_point_offset(),
                |idx, values: Vec<T>| {
                    in_memory_index.add_many_to_list(idx, values);
                    Ok(true)
                },
                hw_counter.ref_payload_index_io_read_counter(),
            )
            .map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to load read-only appendable numeric index from gridstore: {err}"
                ))
            })?;

        Ok(Some(Self {
            in_memory_index,
            storage,
        }))
    }
}
