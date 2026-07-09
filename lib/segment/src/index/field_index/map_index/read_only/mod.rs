use common::universal_io::UniversalRead;
use gridstore::Blob;

use crate::index::field_index::map_index::MapIndexKey;
use crate::index::field_index::map_index::immutable_map_index::ImmutableMapIndex;
use crate::index::field_index::map_index::mutable_map_index::read_only::ReadOnlyAppendableMapIndex;
use crate::index::field_index::map_index::on_disk_map_index::OnDiskMapIndex;

mod lifecycle;
mod live_reload;
mod read_ops;

/// Read-only counterpart of [`MapIndex`][1], parameterised by a
/// [`UniversalRead`] storage.
///
/// Dispatches the [`MapIndexRead`][2] / [`PayloadFieldIndexRead`][3] read
/// surface to one of two backing formats:
/// - [`Appendable`][Self::Appendable] — the in-RAM appendable index loaded
///   from the gridstore (write) format;
/// - [`Immutable`][Self::Immutable] — reads directly from the immutable mmap
///   format.
///
/// Constructed via [`Self::open_appendable`] and [`Self::open_immutable`]
/// (both generic over `S`); the upstream [`ReadOnlyFieldIndex`][4] wiring
/// follows in a separate PR.
///
/// [1]: super::MapIndex
/// [2]: super::read_ops::MapIndexRead
/// [3]: crate::index::field_index::PayloadFieldIndexRead
/// [4]: crate::index::field_index::field_index_base::read_only::ReadOnlyFieldIndex
pub enum ReadOnlyMapIndex<N: MapIndexKey + ?Sized, S: UniversalRead>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    /// Loads into RAM from appendable storage format
    Appendable(ReadOnlyAppendableMapIndex<N, S>),
    Immutable(ImmutableMapIndex<N, S>),
    /// Directly reads from storage in immutable format
    OnDisk(OnDiskMapIndex<N, S>),
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::types::PointOffsetType;
    use common::universal_io::{MmapFile, ReadOnly, UniversalRead, UniversalReadFileOps};
    use itertools::Itertools as _;
    use serde_json::Value;
    use tempfile::TempDir;

    use super::super::MapIndex;
    use super::ReadOnlyMapIndex;
    use crate::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndexRead};
    use crate::json_path::JsonPath;
    use crate::types::{FieldCondition, Match};

    /// Build an appendable (Gridstore) string map index on disk, then open it
    /// via the parent enum's [`ReadOnlyMapIndex::open_appendable`] over the
    /// write-enforced `ReadOnly<MmapFile>` backend. Verifies the dispatcher
    /// wraps into [`ReadOnlyMapIndex::Appendable`] and that the trait
    /// forwarders deliver the same hit set as the values inserted.
    #[test]
    fn parent_open_appendable_round_trip() {
        let dir = TempDir::with_prefix("ro_map_parent_gridstore").unwrap();
        let hw_counter = HardwareCounterCell::new();

        // Build via the writable gridstore builder (matches the existing map
        // tests' `IndexType::MutableGridstore` path).
        {
            let mut builder = MapIndex::<str>::builder_mutable(dir.path().to_path_buf(), false);
            builder.init().unwrap();
            let entries: &[(PointOffsetType, &[&str])] = &[
                (0, &["red", "green"]),
                (1, &["green"]),
                (2, &["blue", "red"]),
            ];
            for (idx, values) in entries {
                let values: Vec<Value> = values.iter().map(|v| Value::from(*v)).collect();
                let values_ref: Vec<_> = values.iter().collect();
                builder.add_point(*idx, &values_ref, &hw_counter).unwrap();
            }
            builder.finalize().unwrap();
        }

        // `S = ReadOnly<MmapFile>` → `S::Fs = ReadOnlyFs<MmapFs>`, named via the
        // associated-type projection since the wrapper type isn't exported.
        // The read-only filesystem context is `Default`.
        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();
        let index: ReadOnlyMapIndex<str, ReadOnly<MmapFile>> =
            ReadOnlyMapIndex::open_appendable(&fs, dir.path().to_path_buf())
                .unwrap()
                .unwrap();

        // Dispatcher wraps the leaf into the right variant.
        assert!(matches!(index, ReadOnlyMapIndex::Appendable(_)));

        // Trait dispatch on the parent enum forwards into the leaf:
        // every point with at least one value is counted, and `red` matches
        // the two that contain it while `blue` matches only the third.
        assert_eq!(index.count_indexed_points().unwrap(), 3);

        let key = JsonPath::new("color");
        let red = FieldCondition::new_match(key.clone(), Match::from("red".to_string()));
        let blue = FieldCondition::new_match(key, Match::from("blue".to_string()));

        assert_eq!(
            index
                .filter(&red, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![0, 2],
        );
        assert_eq!(
            index
                .filter(&blue, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![2],
        );
    }
}
