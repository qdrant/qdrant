//! The append-only value storage every update-only writer is built on.

use std::path::Path;

use blobstore::config::LogstoreConfig;
use blobstore::{Blob, Logstore};
use common::counter::referenced_counter::HwMetricRefCounter;
use common::types::PointOffsetType;
use common::universal_io::{Populate, UniversalAppend};

use crate::common::operation_error::OperationResult;

/// A short-lived append-only writer for a directory of values of type `V`,
/// opened for one batch of updates and dropped with it.
///
/// Always the append-only ([`Logstore`]) mode of the storage: a directory
/// created in mutable mode holds an incompatible file format and is refused
/// rather than opened. What it writes is read back through the ordinary
/// [`BlobstoreReader`], which selects the mode from the persisted config.
///
/// Values must be put at increasing slots, all above every slot the storage
/// already holds a value for; a slot skipped in between stays empty for good
/// and reads back as no value at all.
///
/// [`BlobstoreReader`]: blobstore::BlobstoreReader
pub struct UpdateOnlyBlobstore<V, S: UniversalAppend + 'static> {
    storage: Logstore<V, S>,
    buffered: bool,
}

impl<V: Blob, S: UniversalAppend + 'static> UpdateOnlyBlobstore<V, S> {
    /// Open the storage directory at `path` for appending, creating it if it is
    /// not there yet. An existing storage keeps the config it was created with,
    /// so `config_if_create` only decides the layout of a brand new one.
    pub fn open(
        fs: &S::Fs,
        path: &Path,
        config_if_create: LogstoreConfig,
    ) -> OperationResult<Self> {
        let storage = Logstore::open_or_create(
            fs,
            path.to_path_buf(),
            config_if_create,
            // Appends never read back, and on a remote backend populating would
            // fetch every page of the storage per batch.
            Populate::No,
        )?;

        Ok(Self {
            storage,
            buffered: false,
        })
    }

    /// Buffer `value` at `slot`. Nothing reaches the files until
    /// [`flush`](Self::flush); `fs` only opens the next page file on a
    /// rollover.
    pub fn put(
        &mut self,
        fs: &S::Fs,
        slot: PointOffsetType,
        value: &V,
        hw_counter: HwMetricRefCounter,
    ) -> OperationResult<()> {
        self.storage.put_value(fs, slot, value, hw_counter)?;
        self.buffered = true;
        Ok(())
    }

    /// Persist everything buffered since the last flush.
    ///
    /// Call this once per batch rather than per value: it syncs every page file
    /// of the storage, whether or not that page gained anything. A flush with
    /// nothing buffered is skipped for the same reason.
    pub fn flush(&mut self) -> OperationResult<()> {
        if !self.buffered {
            return Ok(());
        }

        self.storage.flusher()()?;
        self.buffered = false;
        Ok(())
    }
}
