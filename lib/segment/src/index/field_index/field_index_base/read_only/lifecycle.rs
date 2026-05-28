use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::universal_io::{MmapFile, UniversalRead};

use super::ReadOnlyFieldIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::map_index::read_only::ReadOnlyMapIndex;
use crate::index::payload_config::{FullPayloadIndexType, PayloadIndexType};
use crate::json_path::JsonPath;

impl<S: UniversalRead> ReadOnlyFieldIndex<S> {
    /// Read-only mirror of [`IndexSelector::new_index_with_type`][1] for the
    /// Gridstore (appendable) storage path: dispatches on
    /// [`FullPayloadIndexType::index_type`] and forwards to the matching
    /// `ReadOnly*Index::open_gridstore` on each per-index parent enum.
    ///
    /// **Skeleton.** Only the four map-backed variants are wired in this
    /// draft — their `ReadOnlyMapIndex::open_gridstore` lives in the
    /// branch's base (`feat/readonly-map-index-open` →
    /// `feat/gridstore-readonly-open`). The remaining seven variants
    /// (`IntIndex`, `DatetimeIndex`, `FloatIndex`, `GeoIndex`,
    /// `FullTextIndex`, `BoolIndex`, `NullIndex`) need parent opens that
    /// live in still-open PRs; their arms are `todo!` placeholders and get
    /// filled in as each PR merges:
    /// - numeric (Int / Datetime / Float): #9213
    /// - geo: #9211
    /// - full-text: #9222
    /// - bool: #9200
    /// - null: #9197
    ///
    /// [1]: crate::index::field_index::index_selector::IndexSelector::new_index_with_type
    #[allow(dead_code)] // skeleton: no caller in the lib yet; exercised by tests
    pub fn open_gridstore(
        fs: &S::Fs,
        dir: &Path,
        field: &JsonPath,
        index_type: &FullPayloadIndexType,
    ) -> OperationResult<Option<Self>> {
        let index = match index_type.index_type {
            PayloadIndexType::KeywordIndex => {
                ReadOnlyMapIndex::<str, S>::open_gridstore(fs, map_dir(dir, field))?
                    .map(Self::KeywordIndex)
            }
            PayloadIndexType::IntMapIndex => {
                ReadOnlyMapIndex::<crate::types::IntPayloadType, S>::open_gridstore(
                    fs,
                    map_dir(dir, field),
                )?
                .map(Self::IntMapIndex)
            }
            // Matches the writable selector's `(PayloadIndexType::UuidIndex,
            // PayloadSchemaParams::Uuid(_))` arm, which constructs a
            // `MapIndex<UuidIntType>` and wraps it in
            // `FieldIndex::UuidMapIndex` — the `UuidIndex` discriminant is
            // historically map-backed.
            PayloadIndexType::UuidIndex | PayloadIndexType::UuidMapIndex => {
                ReadOnlyMapIndex::<crate::types::UuidIntType, S>::open_gridstore(
                    fs,
                    map_dir(dir, field),
                )?
                .map(Self::UuidMapIndex)
            }

            PayloadIndexType::IntIndex => todo!("blocked on #9213 (numeric parent opens)"),
            PayloadIndexType::DatetimeIndex => todo!("blocked on #9213 (numeric parent opens)"),
            PayloadIndexType::FloatIndex => todo!("blocked on #9213 (numeric parent opens)"),
            PayloadIndexType::GeoIndex => todo!("blocked on #9211 (geo parent opens)"),
            PayloadIndexType::FullTextIndex => todo!("blocked on #9222 (full-text parent opens)"),
            PayloadIndexType::BoolIndex => todo!("blocked on #9200 (bool parent opens)"),
            PayloadIndexType::NullIndex => {
                todo!("blocked on #9197 (null parent open + total_point_count threading)")
            }
        };
        Ok(index)
    }
}

impl ReadOnlyFieldIndex<MmapFile> {
    /// Read-only mirror of [`IndexSelector::new_index_with_type`][1] for the
    /// mmap (immutable) storage path: dispatches on
    /// [`FullPayloadIndexType::index_type`] and forwards to the matching
    /// `ReadOnly*Index::open_mmap` on each per-index parent enum.
    ///
    /// Specialised to `S = MmapFile` because both
    /// [`ReadOnlyMapIndex::open_mmap`] (which this skeleton wires) and the
    /// numeric counterpart fix `S` to `MmapFile` while the sub-opens
    /// (`UniversalHashMap` / `StoredPointToValues` / `MmapBitSlice` for map,
    /// `UniversalNumericIndex` for numeric) still hardcode [`MmapFs`][2].
    /// When those become fs-generic, this can fold into the generic
    /// `open_gridstore` impl block above.
    ///
    /// **Skeleton.** Same wiring shape as [`Self::open_gridstore`]: four
    /// map-backed arms wired, seven `todo!` arms blocked on the per-index
    /// parent-open PRs listed there. The geo and full-text mmap leaves are
    /// already fs-generic on their own and could be wired here even before
    /// the matching gridstore work merges — but their parent enums'
    /// `open_mmap` is still part of the same in-flight PRs, so the arms
    /// stay `todo!` until those land.
    ///
    /// [1]: crate::index::field_index::index_selector::IndexSelector::new_index_with_type
    /// [2]: common::universal_io::MmapFs
    #[allow(dead_code)] // skeleton: no caller in the lib yet; exercised by tests
    pub fn open_mmap(
        dir: &Path,
        field: &JsonPath,
        index_type: &FullPayloadIndexType,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let index = match index_type.index_type {
            PayloadIndexType::KeywordIndex => ReadOnlyMapIndex::<str, MmapFile>::open_mmap(
                &map_dir(dir, field),
                is_on_disk,
                deleted_points,
            )?
            .map(Self::KeywordIndex),
            PayloadIndexType::IntMapIndex => ReadOnlyMapIndex::<
                crate::types::IntPayloadType,
                MmapFile,
            >::open_mmap(
                &map_dir(dir, field), is_on_disk, deleted_points
            )?
            .map(Self::IntMapIndex),
            PayloadIndexType::UuidIndex | PayloadIndexType::UuidMapIndex => {
                ReadOnlyMapIndex::<crate::types::UuidIntType, MmapFile>::open_mmap(
                    &map_dir(dir, field),
                    is_on_disk,
                    deleted_points,
                )?
                .map(Self::UuidMapIndex)
            }

            PayloadIndexType::IntIndex => todo!("blocked on #9213 (numeric parent opens)"),
            PayloadIndexType::DatetimeIndex => todo!("blocked on #9213 (numeric parent opens)"),
            PayloadIndexType::FloatIndex => todo!("blocked on #9213 (numeric parent opens)"),
            PayloadIndexType::GeoIndex => todo!("blocked on #9211 (geo parent opens)"),
            PayloadIndexType::FullTextIndex => todo!("blocked on #9222 (full-text parent opens)"),
            PayloadIndexType::BoolIndex => todo!("blocked on #9200 (bool parent opens)"),
            PayloadIndexType::NullIndex => {
                todo!("blocked on #9197 (null parent open + total_point_count threading)")
            }
        };
        Ok(index)
    }
}

/// Per-field directory naming for map-backed indexes. Mirrors the private
/// [`index_selector::map_dir`][1] helper; duplicated here to keep the
/// skeleton PR self-contained — promotion to a shared `pub(crate)` lands
/// with the follow-up that wires the remaining variants.
///
/// [1]: crate::index::field_index::index_selector
#[allow(dead_code)] // skeleton: only `open_gridstore` / `open_mmap` (also dead) and tests reach this
fn map_dir(dir: &Path, field: &JsonPath) -> PathBuf {
    dir.join(format!("{}-map", field.filename()))
}

#[cfg(test)]
mod tests {
    use common::bitvec::BitVec;
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::types::PointOffsetType;
    use common::universal_io::{MmapFile, ReadOnly, UniversalRead, UniversalReadFileOps};
    use itertools::Itertools as _;
    use serde_json::Value;
    use tempfile::TempDir;

    use super::{ReadOnlyFieldIndex, map_dir};
    use crate::index::field_index::map_index::MapIndex;
    use crate::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndexRead};
    use crate::index::payload_config::{
        FullPayloadIndexType, IndexMutability, PayloadIndexType, StorageType,
    };
    use crate::json_path::JsonPath;
    use crate::types::{FieldCondition, Match};

    /// Generous deletion bitslice for mmap-side tests — must be longer than
    /// any point id under test. Mirrors `map_index::tests::TEST_DELETED_BITS`.
    const TEST_DELETED_BITS: usize = 4096;

    /// Shared test fixture: ids 0/1/2 with payloads {red, green} / {green} /
    /// {blue, red}, so `red` matches [0, 2] and `blue` matches [2].
    const KEYWORD_ENTRIES: &[(PointOffsetType, &[&str])] = &[
        (0, &["red", "green"]),
        (1, &["green"]),
        (2, &["blue", "red"]),
    ];

    fn add_keyword_entries(
        builder: &mut impl FieldIndexBuilderTrait,
        entries: &[(PointOffsetType, &[&str])],
        hw_counter: &HardwareCounterCell,
    ) {
        for (idx, values) in entries {
            let values: Vec<Value> = values.iter().map(|v| Value::from(*v)).collect();
            let values_ref: Vec<_> = values.iter().collect();
            builder.add_point(*idx, &values_ref, hw_counter).unwrap();
        }
    }

    fn keyword_index_type(
        storage_type: StorageType,
        mutability: IndexMutability,
    ) -> FullPayloadIndexType {
        FullPayloadIndexType {
            index_type: PayloadIndexType::KeywordIndex,
            mutability,
            storage_type,
        }
    }

    fn assert_keyword_filter_hits<S: UniversalRead>(
        leaf: &crate::index::field_index::map_index::read_only::ReadOnlyMapIndex<str, S>,
        field: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) {
        let red = FieldCondition::new_match(field.clone(), Match::from("red".to_string()));
        let blue = FieldCondition::new_match(field.clone(), Match::from("blue".to_string()));
        assert_eq!(
            leaf.filter(&red, hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![0, 2],
        );
        assert_eq!(
            leaf.filter(&blue, hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![2],
        );
    }

    /// End-to-end smoke for the gridstore branch of the dispatcher: build a
    /// writable `MapIndex<str>` on disk via the same per-field path the
    /// dispatcher uses, open through [`ReadOnlyFieldIndex::open_gridstore`]
    /// over `ReadOnly<MmapFile>`, and assert (a) the dispatcher picks the
    /// `KeywordIndex` arm with the `Appendable` leaf variant and (b) the
    /// downstream trait dispatch delivers the right hit set.
    #[test]
    fn open_gridstore_keyword_round_trip() {
        let base = TempDir::with_prefix("ro_fieldindex_kw_gridstore").unwrap();
        let field = JsonPath::new("color");
        let hw_counter = HardwareCounterCell::new();

        {
            let mut builder = MapIndex::<str>::builder_gridstore(map_dir(base.path(), &field));
            builder.init().unwrap();
            add_keyword_entries(&mut builder, KEYWORD_ENTRIES, &hw_counter);
            builder.finalize().unwrap();
        }

        // `S = ReadOnly<MmapFile>` → `S::Fs = ReadOnlyFs<MmapFs>`, named via
        // the associated-type projection since the wrapper type isn't
        // exported. The read-only filesystem context is `Default`.
        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();

        // Stored mutability label is irrelevant to gridstore dispatch (only
        // `index_type` is matched on); pin a value matching how the writable
        // selector would label this index.
        let index_type = keyword_index_type(StorageType::Gridstore, IndexMutability::Mutable);

        let index: ReadOnlyFieldIndex<ReadOnly<MmapFile>> =
            ReadOnlyFieldIndex::open_gridstore(&fs, base.path(), &field, &index_type)
                .unwrap()
                .unwrap();

        // Dispatcher routed the `KeywordIndex` discriminant; leaf chose
        // `Appendable` (gridstore-backed). Wildcard arm is intentional —
        // the assertion is "exactly this variant, not any of the other ten".
        #[allow(clippy::wildcard_enum_match_arm)]
        let leaf = match &index {
            ReadOnlyFieldIndex::KeywordIndex(leaf) => leaf,
            other => panic!(
                "expected KeywordIndex, got {:?}",
                std::mem::discriminant(other),
            ),
        };
        assert!(matches!(
            leaf,
            crate::index::field_index::map_index::read_only::ReadOnlyMapIndex::Appendable(_),
        ));
        assert_keyword_filter_hits(leaf, &field, &hw_counter);
    }

    /// End-to-end smoke for the mmap branch of the dispatcher: build a
    /// writable mmap-backed `MapIndex<str>` on disk, open through
    /// [`ReadOnlyFieldIndex::open_mmap`] (concrete to `MmapFile`), and
    /// assert dispatch + filter hits — symmetric to the gridstore test
    /// above but for the immutable variant.
    #[test]
    fn open_mmap_keyword_round_trip() {
        let base = TempDir::with_prefix("ro_fieldindex_kw_mmap").unwrap();
        let field = JsonPath::new("color");
        let hw_counter = HardwareCounterCell::new();
        let deleted_points = BitVec::repeat(false, TEST_DELETED_BITS);

        {
            let mut builder = MapIndex::<str>::builder_mmap(
                &map_dir(base.path(), &field),
                false,
                &deleted_points,
            );
            builder.init().unwrap();
            add_keyword_entries(&mut builder, KEYWORD_ENTRIES, &hw_counter);
            builder.finalize().unwrap();
        }

        let index_type = keyword_index_type(
            StorageType::Mmap { is_on_disk: false },
            IndexMutability::Immutable,
        );

        let index: ReadOnlyFieldIndex<MmapFile> =
            ReadOnlyFieldIndex::open_mmap(base.path(), &field, &index_type, false, &deleted_points)
                .unwrap()
                .unwrap();

        // Dispatcher routed the `KeywordIndex` discriminant; leaf chose
        // `Immutable` (mmap-backed). Wildcard arm intentional, same
        // rationale as the gridstore test above.
        #[allow(clippy::wildcard_enum_match_arm)]
        let leaf = match &index {
            ReadOnlyFieldIndex::KeywordIndex(leaf) => leaf,
            other => panic!(
                "expected KeywordIndex, got {:?}",
                std::mem::discriminant(other),
            ),
        };
        assert!(matches!(
            leaf,
            crate::index::field_index::map_index::read_only::ReadOnlyMapIndex::Immutable(_),
        ));
        assert_keyword_filter_hits(leaf, &field, &hw_counter);
    }
}
