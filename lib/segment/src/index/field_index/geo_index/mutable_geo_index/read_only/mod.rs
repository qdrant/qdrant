use common::universal_io::UniversalRead;
use gridstore::GridstoreReader;

use super::inner::InMemoryGeoMapIndex;
use crate::types::RawGeoPoint;

mod lifecycle;
mod read_ops;

/// Read-only counterpart to [`super::MutableGeoMapIndex`].
///
/// Owns the same in-memory state ([`InMemoryGeoMapIndex`]) but is backed by
/// [`GridstoreReader`] over generic [`UniversalRead`] instead of a writable
/// [`gridstore::Gridstore`]. Implements
/// [`super::super::read_ops::GeoMapIndexRead`] by forwarding to the inner;
/// provides no mutation surface.
///
/// Opened via [`Self::open`], which rebuilds the in-memory state by iterating
/// the Gridstore on disk.
pub struct ReadOnlyAppendableGeoMapIndex<S: UniversalRead> {
    pub(super) in_memory_index: InMemoryGeoMapIndex,
    /// Backing Gridstore reader. Kept open after the in-memory state is built
    /// so `files` / `clear_cache` can drive the underlying storage.
    pub(super) storage: GridstoreReader<Vec<RawGeoPoint>, S>,
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::universal_io::{MmapFile, ReadOnly, UniversalRead, UniversalReadFileOps};
    use tempfile::TempDir;

    use super::super::MutableGeoMapIndex;
    use super::ReadOnlyAppendableGeoMapIndex;
    use crate::index::field_index::geo_index::GeoMapIndexRead;
    use crate::types::GeoPoint;

    /// Build an appendable (Gridstore) geo index on disk, then open it read-only
    /// through a write-prevented `ReadOnlyFs<MmapFs>` backend and assert the
    /// rebuilt in-memory index matches what was written.
    #[test]
    fn read_only_appendable_geo_round_trip() {
        let dir = TempDir::with_prefix("read_only_geo").unwrap();
        let hw_counter = HardwareCounterCell::new();

        {
            let mut mutable = MutableGeoMapIndex::open_gridstore(dir.path().to_path_buf(), true)
                .unwrap()
                .unwrap();
            // point 0: Berlin, 1 value
            mutable
                .add_many_geo_points(0, &[GeoPoint::new_unchecked(13.41, 52.52)], &hw_counter)
                .unwrap();
            // point 1: Paris, 1 value
            mutable
                .add_many_geo_points(1, &[GeoPoint::new_unchecked(2.35, 48.85)], &hw_counter)
                .unwrap();
            // point 2: two values
            mutable
                .add_many_geo_points(
                    2,
                    &[
                        GeoPoint::new_unchecked(1.0, 1.0),
                        GeoPoint::new_unchecked(2.0, 2.0),
                    ],
                    &hw_counter,
                )
                .unwrap();
            mutable.flusher()().unwrap();
        }

        // `S = ReadOnly<MmapFile>` → `S::Fs = ReadOnlyFs<MmapFs>`, the
        // write-enforced backend: every open is asserted non-writable, so this
        // only succeeds because `GridstoreReader::open` opens its pages and
        // tracker read-only.
        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();
        let index: ReadOnlyAppendableGeoMapIndex<ReadOnly<MmapFile>> =
            ReadOnlyAppendableGeoMapIndex::open(&fs, dir.path().to_path_buf())
                .unwrap()
                .unwrap();

        // Counts reconstructed from the Gridstore on open.
        assert_eq!(index.points_count(), 3);
        assert_eq!(index.points_values_count(), 4);
        assert_eq!(index.max_values_per_point(), 2);

        // Per-point values reconstructed into `point_to_values`.
        assert_eq!(index.values_count(0), 1);
        assert_eq!(index.values_count(2), 2);
        assert_eq!(index.get_values(0).unwrap().count(), 1);
        assert_eq!(index.get_values(2).unwrap().count(), 2);
    }
}
