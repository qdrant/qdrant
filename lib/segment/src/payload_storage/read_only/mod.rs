mod lifecycle;
mod live_reload;
mod payload_storage_read;

use blobstore::BlobstoreReader;
use common::universal_io::UniversalRead;

use crate::types::Payload;

/// Read-only counterpart to [`super::mmap_payload_storage::MmapPayloadStorage`].
///
/// Backed by a [`BlobstoreReader`] over generic [`UniversalRead`] instead of a
/// writable [`blobstore::Blobstore`]. Unlike the read-only field indexes there
/// is no in-memory index to rebuild: [`Payload`] values are read straight from
/// the reader, so [`PayloadStorageRead`](super::PayloadStorageRead) forwards
/// directly to it. Opened via [`Self::open`] (see [`lifecycle`]); provides no
/// mutation surface.
pub struct ReadOnlyPayloadStorage<S: UniversalRead> {
    storage: BlobstoreReader<Payload, S>,
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::universal_io::{MmapFile, Populate, ReadOnly, UniversalRead, UniversalReadFileOps};
    use rstest::rstest;
    use tempfile::TempDir;

    use super::ReadOnlyPayloadStorage;
    use crate::payload_json;
    use crate::payload_storage::payload_storage_impl::PayloadStorageImpl;
    use crate::payload_storage::{PayloadStorage, PayloadStorageRead};

    /// Write payloads through the writable mmap storage, then reopen the same
    /// files read-only through the write-enforced `ReadOnly<MmapFile>` backend
    /// and assert the reader returns what was written.
    #[rstest]
    fn read_only_payload_storage_round_trip(
        #[values(Populate::No, Populate::PreferBackground)] populate: Populate,
    ) {
        let dir = TempDir::with_prefix("read_only_payload").unwrap();
        let hw_counter = HardwareCounterCell::new();

        let payload = payload_json! {
            "a": "some text",
            "n": 42,
        };

        {
            let mut storage: PayloadStorageImpl =
                PayloadStorageImpl::open_or_create(dir.path().to_path_buf(), false).unwrap();
            for i in 0..5 {
                storage.set(i, &payload, &hw_counter).unwrap();
            }
            // Flush so the reader observes the data on disk.
            storage.flusher()().unwrap();
        }

        // `S = ReadOnly<MmapFile>` → the write-enforced backend (every open
        // asserted non-writable), usable because the gridstore reader opens
        // read-only.
        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();
        let storage: ReadOnlyPayloadStorage<ReadOnly<MmapFile>> =
            ReadOnlyPayloadStorage::open(&fs, dir.path().to_path_buf(), populate).unwrap();

        assert_eq!(storage.is_on_disk(), !populate.to_bool::<MmapFile>());
        for i in 0..5 {
            assert_eq!(storage.get(i, &hw_counter).unwrap(), payload);
        }
        // An unwritten point reads back as an empty payload.
        assert_eq!(storage.get(99, &hw_counter).unwrap(), payload_json! {});
    }
}
