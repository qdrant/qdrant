use std::path::PathBuf;

use common::universal_io::{OkNotFound, UniversalRead};
use gridstore::GridstoreReader;

use super::ReadOnlyPayloadStorage;
use crate::common::operation_error::OperationResult;
use crate::payload_storage::mmap_payload_storage::storage_dir;
use crate::types::Payload;

impl<S: UniversalRead> ReadOnlyPayloadStorage<S> {
    /// Open the payload storage read-only over the generic filesystem `fs` —
    /// the read-only counterpart of [`MmapPayloadStorage::open_or_create`][1].
    ///
    /// Resolves the `payload_storage` sub-directory (matching the writable
    /// storage's on-disk layout) and opens a [`GridstoreReader`] over it.
    /// `Payload` values are read straight from the reader, so unlike the
    /// read-only field indexes there is no in-memory index to rebuild. When
    /// `populate` is set the pages are loaded eagerly, exactly as the writable
    /// storage does; the flag is retained to answer
    /// [`is_on_disk`](super::super::PayloadStorageRead::is_on_disk).
    ///
    /// Returns [`Ok(None)`] when the on-disk directory doesn't exist — the read
    /// path never creates, mirroring the read-only field indexes.
    ///
    /// [1]: crate::payload_storage::mmap_payload_storage::MmapPayloadStorage::open_or_create
    pub fn open(fs: &S::Fs, path: PathBuf, populate: bool) -> OperationResult<Option<Self>> {
        let path = storage_dir(path);
        let Some(storage) = GridstoreReader::<Payload, S>::open(fs, path).ok_not_found()? else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        if populate {
            storage.populate()?;
        }

        Ok(Some(Self { storage, populate }))
    }
}
