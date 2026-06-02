use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use serde_json::Value;

use super::mmap_geo_index::StoredGeoMapIndex;
use super::mutable_geo_index::read_only::ReadOnlyAppendableGeoMapIndex;
use super::read_ops::GeoMapIndexRead;
use crate::common::utils::MultiValue;
use crate::index::payload_config::IndexMutability;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;

mod lifecycle;
mod read_ops;

/// Read-only counterpart of [`GeoMapIndex`][1].
///
/// Mirrors the writable enum's shape: an `Appendable` variant for the
/// Gridstore-backed mutable format (parallel to [`MutableGeoMapIndex`][2])
/// and an `Immutable` variant for the on-disk mmap format (parallel to
/// [`ImmutableGeoMapIndex`][3] / [`StoredGeoMapIndex`][4]). The backing
/// storage is bound to [`UniversalRead`] only — no buffer, no flusher, no
/// write path. Query logic (filter / cardinality / payload blocks /
/// condition checker) is shared with the writable variants via
/// [`super::read_ops`].
///
/// Opened via [`Self::open_gridstore`] / [`Self::open_mmap`], mirroring the
/// writable [`GeoMapIndex::new_gridstore`][5] / [`new_mmap`][6] pair.
///
/// [1]: super::GeoMapIndex
/// [2]: super::mutable_geo_index::MutableGeoMapIndex
/// [3]: super::immutable_geo_index::ImmutableGeoMapIndex
/// [4]: super::mmap_geo_index::StoredGeoMapIndex
/// [5]: super::GeoMapIndex::new_gridstore
/// [6]: super::GeoMapIndex::new_mmap
#[allow(clippy::large_enum_variant)]
pub enum ReadOnlyGeoMapIndex<S: UniversalRead> {
    /// Loads into RAM from appendable Gridstore storage format.
    Appendable(ReadOnlyAppendableGeoMapIndex<S>),
    /// Directly reads from storage in immutable mmap format.
    Immutable(StoredGeoMapIndex<S>),
}

impl<S: UniversalRead> ReadOnlyGeoMapIndex<S> {
    /// Produce a closure that maps a point id to its indexed geo values as
    /// JSON `Value`s. Mirrors `GeoMapIndex::value_retriever`.
    pub fn value_retriever<'a>(
        &'a self,
        _hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            GeoMapIndexRead::get_values(self, point_id)
                .into_iter()
                .flatten()
                .filter_map(|v| serde_json::to_value(v).ok())
                .collect()
        })
    }

    /// Reports the on-disk format's mutability, mirroring
    /// [`GeoMapIndex::get_mutability_type`][1].
    ///
    /// Reflects what the segment's payload-index config records about the
    /// storage format, NOT whether the runtime wrapper permits writes. The
    /// read-only wrapper always denies mutation; this value is what an
    /// equivalent writable open would report.
    ///
    /// - [`Self::Appendable`] mirrors the writable `Mutable` variant
    ///   (Gridstore-backed) → [`IndexMutability::Mutable`].
    /// - [`Self::Immutable`] mirrors the writable `Immutable` / `Storage`
    ///   variants (mmap-backed) → [`IndexMutability::Immutable`].
    ///
    /// [1]: super::GeoMapIndex::get_mutability_type
    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            Self::Appendable(_) => IndexMutability::Mutable,
            Self::Immutable(_) => IndexMutability::Immutable,
        }
    }
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::universal_io::{MmapFile, ReadOnly, UniversalRead, UniversalReadFileOps};
    use tempfile::TempDir;

    use super::super::GeoMapIndexRead;
    use super::super::mutable_geo_index::MutableGeoMapIndex;
    use super::ReadOnlyGeoMapIndex;
    use crate::types::GeoPoint;

    /// Build an appendable (Gridstore) geo index on disk, then open it via the
    /// parent enum's [`ReadOnlyGeoMapIndex::open_gridstore`] over the
    /// write-enforced `ReadOnly<MmapFile>` backend. Verifies the dispatcher
    /// wraps into [`ReadOnlyGeoMapIndex::Appendable`] and that the trait
    /// forwarders deliver the same counts as the writable index produced.
    #[test]
    fn parent_open_gridstore_round_trip() {
        let dir = TempDir::with_prefix("ro_geo_parent_gridstore").unwrap();
        let hw_counter = HardwareCounterCell::new();

        {
            let mut mutable = MutableGeoMapIndex::open_gridstore(dir.path().to_path_buf(), true)
                .unwrap()
                .unwrap();
            mutable
                .add_many_geo_points(0, &[GeoPoint::new_unchecked(13.41, 52.52)], &hw_counter)
                .unwrap();
            mutable
                .add_many_geo_points(1, &[GeoPoint::new_unchecked(2.35, 48.85)], &hw_counter)
                .unwrap();
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

        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();
        let index: ReadOnlyGeoMapIndex<ReadOnly<MmapFile>> =
            ReadOnlyGeoMapIndex::open_gridstore(&fs, dir.path().to_path_buf())
                .unwrap()
                .unwrap();

        // Dispatcher wraps the leaf into the right variant.
        assert!(matches!(index, ReadOnlyGeoMapIndex::Appendable(_)));

        // Trait dispatch on the parent enum forwards into the leaf.
        assert_eq!(GeoMapIndexRead::points_count(&index), 3);
        assert_eq!(GeoMapIndexRead::points_values_count(&index), 4);
        assert_eq!(GeoMapIndexRead::max_values_per_point(&index), 2);
        assert_eq!(GeoMapIndexRead::values_count(&index, 0), 1);
        assert_eq!(GeoMapIndexRead::values_count(&index, 2), 2);
    }
}
