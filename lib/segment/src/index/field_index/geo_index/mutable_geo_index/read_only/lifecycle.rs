use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::UniversalRead;
use gridstore::GridstoreReader;

use super::super::inner::InMemoryGeoMapIndex;
use super::ReadOnlyAppendableGeoMapIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::RawGeoPoint;

impl<S: UniversalRead> ReadOnlyAppendableGeoMapIndex<S> {
    /// Open the appendable (Gridstore) geo index read-only, threading every
    /// file open through the filesystem handle `fs`.
    ///
    /// Opens a [`GridstoreReader`] over the generic filesystem object, then
    /// rebuilds the in-memory geohash buckets by iterating every stored point
    /// through [`InMemoryGeoMapIndex::ingest_raw_points`] — the exact
    /// reconstruction the writable [`MutableGeoMapIndex::open_gridstore`][1]
    /// performs over a writable `Gridstore`. No write path; the reader is
    /// retained for `files` / `clear_cache`.
    ///
    /// [1]: super::super::MutableGeoMapIndex::open_gridstore
    pub fn open(fs: &S::Fs, path: PathBuf) -> OperationResult<Self> {
        let storage = GridstoreReader::<Vec<RawGeoPoint>, S>::open(fs, path).map_err(|err| {
            OperationError::service_error(format!(
                "failed to open read-only appendable geo index on gridstore: {err}"
            ))
        })?;

        let mut in_memory_index = InMemoryGeoMapIndex::new();
        let hw_counter = HardwareCounterCell::disposable();
        storage
            .iter::<_, OperationError>(
                storage.max_point_offset(),
                |idx, values: Vec<RawGeoPoint>| {
                    in_memory_index.ingest_raw_points(idx, values)?;
                    Ok(true)
                },
                // Same counter the writable `open_gridstore` load uses; this is
                // a disposable counter, so the exact metric is unobservable.
                hw_counter.ref_payload_index_io_write_counter(),
            )
            .map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to load read-only appendable geo index from gridstore: {err}"
                ))
            })?;

        Ok(Self {
            in_memory_index,
            storage,
        })
    }
}
