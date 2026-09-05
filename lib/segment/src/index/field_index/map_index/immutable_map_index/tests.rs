use bitvec::vec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;
use serde_json::Value;
use tempfile::Builder;

use super::ImmutableMapIndex;
use crate::index::field_index::FieldIndexBuilderTrait as _;
use crate::index::field_index::map_index::MapIndex;
use crate::index::field_index::map_index::read_ops::MapIndexRead as _;
use crate::types::Memory;

/// Build an immutable keyword map index where every point in `points` carries
/// `value`, then hand back the concrete `ImmutableMapIndex` so the counter
/// bookkeeping can be exercised directly.
fn immutable_index_with(
    points: &[PointOffsetType],
    value: &str,
) -> (tempfile::TempDir, ImmutableMapIndex<str, MmapFile>) {
    let dir = Builder::new().prefix("immutable_map").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let deleted = BitVec::repeat(false, 1024);

    let mut builder = MapIndex::<str>::builder_immutable(dir.path(), false, &deleted, false);
    builder.init().unwrap();
    for &point in points {
        let json = Value::String(value.to_owned());
        builder.add_point(point, &[&json], &hw_counter).unwrap();
    }
    builder.finalize().unwrap();

    let index = MapIndex::<str>::new_immutable(dir.path(), Memory::Pinned, &deleted)
        .unwrap()
        .unwrap();
    match index {
        MapIndex::Immutable(index) => (dir, index),
        MapIndex::Mutable(_) | MapIndex::OnDisk(_) => panic!("expected an immutable map index"),
    }
}

fn postings(index: &ImmutableMapIndex<str, MmapFile>, value: &str) -> Vec<PointOffsetType> {
    let hw_counter = HardwareCounterCell::new();
    let mut ids: Vec<_> = index.get_iterator(value, &hw_counter).collect();
    ids.sort_unstable();
    ids
}

/// A removal that retires nothing must not shrink the value's live-posting
/// counter.
///
/// `ContainerSegment::count` is not a hint: `get_mut_point_ids_slice` refuses a
/// value once it hits zero, and reaching zero drops the value's key from
/// `value_to_points` altogether. So an over-removal that still decrements
/// retires a slot belonging to some *other*, still-live point, and enough of
/// them delete a whole value from the index — while the payload storage,
/// `point_to_values` and the posting container all still hold it.
///
/// That is the shape of the reported corruption: an unfiltered `scroll` returns
/// the points, a `MatchValue` on the indexed field returns none of them, and
/// rebuilding the index restores consistency.
/// See <https://github.com/qdrant/qdrant/issues/10302>.
#[test]
fn redundant_removal_does_not_retire_live_postings() {
    let (_dir, mut index) = immutable_index_with(&[0, 1, 2], "src");

    assert_eq!(postings(&index, "src"), vec![0, 1, 2]);

    // One genuine removal: point 0 goes, 1 and 2 stay.
    index.remove_point(0).unwrap();
    assert_eq!(postings(&index, "src"), vec![1, 2]);

    // Two removals that retire nothing: point 0's posting is already marked
    // deleted, and point 7 never had one. Neither may consume a live slot.
    for _ in 0..2 {
        ImmutableMapIndex::<str, MmapFile>::remove_idx_from_value_list(
            &mut index.value_to_points,
            &mut index.value_to_points_container,
            &mut index.deleted_value_to_points_container,
            "src",
            0,
        );
        ImmutableMapIndex::<str, MmapFile>::remove_idx_from_value_list(
            &mut index.value_to_points,
            &mut index.value_to_points_container,
            &mut index.deleted_value_to_points_container,
            "src",
            7,
        );
    }

    assert_eq!(
        postings(&index, "src"),
        vec![1, 2],
        "redundant removals retired live postings: the value became unreachable \
         through the index while its points still hold it",
    );
    let hw_counter = HardwareCounterCell::new();
    assert_eq!(
        index.get_count_for_value("src", &hw_counter),
        Some(2),
        "live-posting counter drifted below the number of live postings",
    );
}

/// `live_reload` replays a deleted-point set into an already-loaded index
/// (`live_reload.rs`), so the same point id can be presented more than once.
/// Replaying it must be idempotent rather than eroding unrelated postings.
#[test]
fn repeated_remove_point_is_idempotent() {
    let (_dir, mut index) = immutable_index_with(&[0, 1, 2, 3], "src");

    for _ in 0..3 {
        index.remove_point(1).unwrap();
    }

    assert_eq!(postings(&index, "src"), vec![0, 2, 3]);
    let hw_counter = HardwareCounterCell::new();
    assert_eq!(index.get_count_for_value("src", &hw_counter), Some(3));
}
