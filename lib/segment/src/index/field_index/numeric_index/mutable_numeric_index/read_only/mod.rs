use blobstore::{Blob, BlobstoreReader};
use common::universal_io::UniversalRead;

use super::super::Encodable;
use super::InMemoryNumericIndex;
use crate::index::field_index::numeric_point::Numericable;

mod lifecycle;
mod live_reload;
mod read_ops;

/// Read-only counterpart to [`super::MutableNumericIndex`].
///
/// Owns the same in-memory state ([`InMemoryNumericIndex`]) but is backed by
/// [`BlobstoreReader`] over generic [`UniversalRead`] instead of a writable
/// [`blobstore::Blobstore`]. Implements
/// [`super::super::numeric_index_read::NumericIndexRead`] by forwarding to the
/// in-memory index; provides no mutation surface.
///
/// Opened via [`Self::open`], which rebuilds the in-memory state by iterating
/// the Gridstore on disk.
pub struct ReadOnlyAppendableNumericIndex<T: Encodable + Numericable, S: UniversalRead>
where
    Vec<T>: Blob,
{
    pub(super) in_memory_index: InMemoryNumericIndex<T>,
    /// Backing Blobstore reader, populated by [`Self::open`]. Held to keep the
    /// storage mapped; the `files` / `populate` / `clear_cache` wiring that
    /// reads it lands with the storage-variant enum lifecycle (it isn't part of
    /// the [`NumericIndexRead`](super::super::numeric_index_read::NumericIndexRead) surface).
    #[allow(dead_code)]
    pub(super) storage: BlobstoreReader<Vec<T>, S>,
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::universal_io::{MmapFile, ReadOnly, UniversalRead, UniversalReadFileOps};
    use serde_json::Value;
    use tempfile::TempDir;

    use super::ReadOnlyAppendableNumericIndex;
    use crate::index::field_index::FieldIndexBuilderTrait;
    use crate::index::field_index::numeric_index::{NumericIndex, NumericIndexRead};
    use crate::types::FloatPayloadType;

    /// Build an appendable (Gridstore) numeric index on disk, then open it
    /// read-only through the write-enforced `ReadOnly<MmapFile>` backend and
    /// assert the rebuilt in-memory index matches what was written.
    #[test]
    fn read_only_appendable_numeric_round_trip() {
        let dir = TempDir::with_prefix("read_only_numeric").unwrap();
        let hw_counter = HardwareCounterCell::new();

        {
            let mut builder = NumericIndex::<FloatPayloadType, FloatPayloadType>::builder_gridstore(
                dir.path().to_path_buf(),
            );
            builder.init().unwrap();
            builder
                .add_point(0, &[&Value::from(1.5)], &hw_counter)
                .unwrap();
            builder
                .add_point(1, &[&Value::from(2.5), &Value::from(3.5)], &hw_counter)
                .unwrap(); // 2 values
            builder
                .add_point(2, &[&Value::from(4.5)], &hw_counter)
                .unwrap();
            // `finalize` flushes the Gridstore to disk.
            builder.finalize().unwrap();
        }

        // `S = ReadOnly<MmapFile>` → the write-enforced backend (every open
        // asserted non-writable), now usable because the gridstore reader opens
        // read-only.
        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();
        let index: ReadOnlyAppendableNumericIndex<FloatPayloadType, ReadOnly<MmapFile>> =
            ReadOnlyAppendableNumericIndex::open(&fs, dir.path().to_path_buf())
                .unwrap()
                .unwrap();

        assert_eq!(index.get_points_count(), 3);
        assert_eq!(index.get_max_values_per_point(), 2);
        assert_eq!(index.values_count(0), Some(1));
        assert_eq!(index.values_count(1), Some(2));
        assert_eq!(index.get_values(0).unwrap().count(), 1);
        assert_eq!(index.get_values(1).unwrap().count(), 2);
        assert_eq!(index.total_unique_values_count().unwrap(), 4);
    }
}
