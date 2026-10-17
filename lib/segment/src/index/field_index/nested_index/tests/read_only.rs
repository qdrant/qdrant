//! Per-element indexes of read-only segments, right after open and after live reloads
//! that follow the writer.
//!
//! Reads that see only live points (filters, order-by) must match the writable segment.
//! Facets and unique values must match the read-only segment with flattened indexes over
//! the same data: like the flattened indexes, the per-element ones may still count points
//! the writer deleted, and in the same way.

use std::sync::atomic::AtomicBool;

use common::universal_io::{MmapFile, MmapFs};

use super::*;
use crate::data_types::order_by::OrderValue;
use crate::entry::entry_point::ReadSegmentEntry;
use crate::index::field_index::ReadOnlyFieldIndex;
use crate::segment::read_only::ReadOnlySegment;

fn open_read_only(segment: &Segment) -> ReadOnlySegment<MmapFile> {
    segment.flush(true).unwrap();
    ReadOnlySegment::<MmapFile>::open(&MmapFs, &segment.data_path(), segment.uuid, None, None)
        .unwrap()
}

/// Staged preload, then the exclusive apply, as the edge shard drives a reload.
fn reload(read_only: &mut ReadOnlySegment<MmapFile>, writer: &Segment) {
    writer.flush(true).unwrap();
    let max_committed_id =
        futures::executor::block_on(read_only.live_preload(&AtomicBool::new(false))).unwrap();
    read_only.live_reload(max_committed_id).unwrap();
}

fn read_filtered(segment: &impl ReadSegmentEntry, filter: &Filter) -> Vec<ExtendedPointId> {
    segment
        .read_filtered(
            None,
            None,
            Some(filter),
            &AtomicBool::new(false),
            DeferredBehavior::WithDeferred,
        )
        .unwrap()
}

/// Points in the order of `order_by`, ties sorted by id: they are in no particular order.
fn ordered(
    segment: &impl ReadSegmentEntry,
    order_by: &OrderBy,
) -> Vec<(OrderValue, ExtendedPointId)> {
    let mut points = segment
        .read_ordered_filtered(
            None,
            None,
            order_by,
            &AtomicBool::new(false),
            DeferredBehavior::WithDeferred,
        )
        .unwrap();
    points.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    points
}

/// The same data in a writable segment and in two read-only ones: one with per-element
/// indexes, one with flattened indexes.
struct ReadOnlyPair<'a> {
    writable: &'a Segment,
    nested: &'a ReadOnlySegment<MmapFile>,
    flat: &'a ReadOnlySegment<MmapFile>,
    /// Fields whose facets are not compared
    skip_facets: &'a [&'a str],
}

fn assert_read_only_matches(pair: ReadOnlyPair<'_>) {
    let ReadOnlyPair {
        writable,
        nested,
        flat,
        skip_facets,
    } = pair;
    let stopped = AtomicBool::new(false);
    let _scope = ambient::test_guard();

    {
        let payload_index = nested.payload_index.borrow();
        for &(field, _) in FIELDS {
            let indexes = &payload_index.field_indexes[&JsonPath::from_str(field).unwrap()];
            assert!(
                indexes
                    .iter()
                    .all(|index| matches!(index, ReadOnlyFieldIndex::Nested(_))),
                "{field} must be indexed per element in the read-only segment",
            );
        }
    }

    assert_eq!(
        writable.available_point_count(),
        nested.available_point_count()
    );

    let mut matched_any = false;
    for filter in filters().into_iter().chain(nested_emptiness_filters()) {
        let points = read_filtered(writable, &filter);
        matched_any |= !points.is_empty();
        assert_eq!(points, read_filtered(nested, &filter), "filter {filter:?}");
    }
    assert!(matched_any, "filters must select something to compare");

    for key in ["offers[].price", "offers[].rating"] {
        for direction in [Direction::Asc, Direction::Desc] {
            let order_by = OrderBy {
                key: JsonPath::from_str(key).unwrap(),
                direction: Some(direction),
                start_from: None,
            };
            assert_eq!(
                ordered(writable, &order_by),
                ordered(nested, &order_by),
                "order by {order_by:?}",
            );
        }
    }

    for key in [
        "offers[].shop",
        "offers[].price",
        "offers[].available",
        "offers[].tags",
    ] {
        if skip_facets.contains(&key) {
            continue;
        }
        for filter in [None, Some(filters()[5].clone())] {
            let request = FacetParams {
                key: JsonPath::from_str(key).unwrap(),
                limit: 1000,
                filter,
                exact: true,
            };
            assert_eq!(
                flat.facet(&request, &stopped).unwrap(),
                nested.facet(&request, &stopped).unwrap(),
                "facet {request:?}",
            );
        }
        let key = JsonPath::from_str(key).unwrap();
        assert_eq!(
            flat.unique_values(&key, None, &stopped).unwrap(),
            nested.unique_values(&key, None, &stopped).unwrap(),
            "unique values of {key}",
        );
    }
}

fn run_read_only(seed: u64, memory: Option<Memory>) {
    let dir = Builder::new().prefix("nested_read_only").tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(seed);
    let _scope = ambient::test_guard();

    let mut pair = Pair::new(dir.path());
    pair.upsert(&mut rng, 0..NUM_POINTS / 2);
    pair.create_indexes(memory);
    pair.upsert(&mut rng, NUM_POINTS / 2..NUM_POINTS);
    pair.mutate(&mut rng, NUM_POINTS as usize);

    // Mutable element maps
    assert_read_only_matches(ReadOnlyPair {
        writable: &pair.nested,
        nested: &open_read_only(&pair.nested),
        flat: &open_read_only(&pair.flat),
        skip_facets: &[],
    });

    // Immutable element maps, with deleted points
    let mut flat = optimize(&pair.flat, &dir.path().join("flat_optimized"));
    let mut nested = optimize(&pair.nested, &dir.path().join("nested_optimized"));
    for id in (0..NUM_POINTS).step_by(7) {
        pair.op_num += 1;
        flat.delete_point(pair.op_num, id.into()).unwrap();
        nested.delete_point(pair.op_num, id.into()).unwrap();
    }
    assert_read_only_matches(ReadOnlyPair {
        writable: &nested,
        nested: &open_read_only(&nested),
        flat: &open_read_only(&flat),
        skip_facets: &[],
    });
}

#[test]
fn read_only_segment_matches_nested_indexes() {
    for seed in 0..2 {
        run_read_only(seed, None);
    }
}

#[test]
fn read_only_segment_matches_nested_indexes_on_disk() {
    run_read_only(42, Some(Memory::Cold));
}

/// A read-only segment follows a writer that keeps updating per-element indexes: every
/// reload catches up with the element maps and the values of the changed elements.
#[test]
fn read_only_segment_live_reloads_nested_indexes() {
    let dir = Builder::new()
        .prefix("nested_live_reload")
        .tempdir()
        .unwrap();
    let mut rng = StdRng::seed_from_u64(5);
    let _scope = ambient::test_guard();

    let mut pair = Pair::new(dir.path());
    // Updates move points to new offsets, the mode read-only followers reload from
    pair.flat.append_only_mutations = true;
    pair.nested.append_only_mutations = true;
    pair.upsert(&mut rng, 0..NUM_POINTS / 2);
    pair.create_indexes(None);

    let mut nested = open_read_only(&pair.nested);
    let mut flat = open_read_only(&pair.flat);
    for round in 0..5 {
        if round > 0 {
            if round % 2 == 1 {
                pair.upsert(&mut rng, NUM_POINTS / 2..NUM_POINTS);
            }
            pair.mutate(&mut rng, NUM_POINTS as usize / 2);
            reload(&mut nested, &pair.nested);
            reload(&mut flat, &pair.flat);
        }
        assert_read_only_matches(ReadOnlyPair {
            writable: &pair.nested,
            nested: &nested,
            flat: &flat,
            // A point the writer inserts and moves again between two reloads is in no
            // reload's changes. The flattened bool index rereads its flags whole and
            // counts such a deleted point, the other indexes don't
            skip_facets: &["offers[].available"],
        });
    }
}
