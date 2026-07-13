use std::path::PathBuf;

use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs};
use gridstore::BlobstoreReader;

use super::ReadOnlyPayloadStorage;
use crate::common::operation_error::OperationResult;
use crate::payload_storage::payload_storage_impl::storage_dir;
use crate::types::Payload;

impl<S: UniversalRead> ReadOnlyPayloadStorage<S> {
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: PathBuf,
        populate: Populate,
    ) -> OperationResult<()> {
        let path = storage_dir(path);
        BlobstoreReader::<Payload, S>::preopen(fs, path, populate)?;
        Ok(())
    }

    /// Open the payload storage read-only over the generic filesystem `fs` —
    /// the read-only counterpart of [`PayloadStorageImpl::open_or_create`][1].
    ///
    /// [1]: crate::payload_storage::payload_storage_impl::PayloadStorageImpl::open_or_create
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: PathBuf,
        populate: Populate,
    ) -> OperationResult<Self> {
        let path = storage_dir(path);
        let storage = BlobstoreReader::<Payload, S>::open(fs, path, populate)?;

        Ok(Self { storage })
    }
}
