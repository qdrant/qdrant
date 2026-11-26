use std::cell::Cell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use common::universal_io::{MmapFs, UniversalReadFs as _};
use segment::index::payload_config::PayloadConfig;
use segment::json_path::JsonPath;
use segment::segment_constructor::get_payload_index_path;
use segment::types::{PayloadFieldSchema, PayloadSchemaType};
use shard::files::SEGMENTS_PATH;
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
use shard::operations::point_ops::PointOperations::UpsertPoints;
use shard::optimizers::config::TEMP_SEGMENTS_PATH;
use tempfile::TempDir;

use super::store::store_batch;
use crate::read_only::tests::{
    delete as leader_delete, exact_count, open_follower, tag_facet_total, tagged_points,
    test_config,
};
use crate::{
    EdgeConfig, EdgeOptimizersConfig, EdgeShard, PayloadIndexOutcome, UpdateOnlyEdgeShard,
};

/// 301 to 1000 in the immutable segment, 1001 to 1010 but 1005 in the appendable one.
const LIVE: usize = 709;
/// A tag is `"t0"`, `"t1"` or `"t2"`: four JSON bytes.
const TAG_BYTES: u64 = 4;

fn tagged_leader(prefix: &str) -> TempDir {
    let dir = tempfile::Builder::new().prefix(prefix).tempdir().unwrap();
    let config = EdgeConfig {
        optimizers: Some(EdgeOptimizersConfig {
            deleted_threshold: Some(0.2),
            vacuum_min_vector_number: Some(1),
            indexing_threshold: Some(1),
            ..EdgeOptimizersConfig::default()
        }),
        ..test_config()
    };
    let leader = EdgeShard::new(dir.path(), config).unwrap();
    leader
        .update(PointOperation(UpsertPoints(PointsList(tagged_points(
            1..=1000,
        )))))
        .unwrap();
    leader_delete(&leader, 1..=300);
    leader.flush().unwrap();
    assert!(leader.optimize().unwrap(), "expected a vacuum to run");
    leader
        .update(PointOperation(UpsertPoints(PointsList(tagged_points(
            1001..=1010,
        )))))
        .unwrap();
    leader_delete(&leader, [1005]);
    leader.flush().unwrap();
    dir
}

fn tag() -> JsonPath {
    "tag".parse().unwrap()
}

fn schema(kind: PayloadSchemaType) -> PayloadFieldSchema {
    PayloadFieldSchema::FieldType(kind)
}

fn create(
    writer: UpdateOnlyEdgeShard<MmapFs>,
    dir: &Path,
    kind: PayloadSchemaType,
) -> (UpdateOnlyEdgeShard<MmapFs>, PayloadIndexOutcome) {
    writer
        .create_payload_index(&tag(), &schema(kind), &dir.join(TEMP_SEGMENTS_PATH), || {
            false
        })
        .unwrap()
}

/// The leader's appendable keeps a mutable payload storage, which an append-only writer refuses.
fn with_fresh_target(
    writer: UpdateOnlyEdgeShard<MmapFs>,
    dir: &Path,
) -> UpdateOnlyEdgeShard<MmapFs> {
    let source = writer.write_target().unwrap();
    let (writer, _) = writer
        .create_appendable_from(source, &HashMap::new(), &dir.join(TEMP_SEGMENTS_PATH))
        .unwrap();
    writer
}

fn segment_dirs(dir: &Path) -> Vec<PathBuf> {
    fs_err::read_dir(dir.join(SEGMENTS_PATH))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| get_payload_index_path(path).is_dir())
        .collect()
}

fn config_path(segment: &Path) -> PathBuf {
    PayloadConfig::get_config_path(&get_payload_index_path(segment))
}

fn recorded(dir: &Path) -> Vec<Option<PayloadFieldSchema>> {
    segment_dirs(dir)
        .iter()
        .map(|segment| {
            let config = PayloadConfig::load(&config_path(segment)).unwrap();
            config
                .indices
                .get(&tag())
                .map(|indexed| indexed.schema.clone())
        })
        .collect()
}

fn tag_files(dir: &Path, suffix: &str) -> usize {
    segment_dirs(dir)
        .iter()
        .map(|segment| {
            let prefix =
                get_payload_index_path(segment).join(format!("{}-{suffix}", tag().filename()));
            MmapFs.list_files(&prefix).unwrap().len()
        })
        .sum()
}

#[test]
fn create_indexes_every_segment_and_meters_live_values() {
    let dir = tagged_leader("edge-pi-create");
    let writer = UpdateOnlyEdgeShard::open_mmap(dir.path()).unwrap();
    let segments = writer.segments_count();
    assert!(
        segments >= 2,
        "expected an immutable and an appendable segment"
    );

    let (_writer, outcome) = create(writer, dir.path(), PayloadSchemaType::Keyword);
    assert_eq!(
        outcome,
        PayloadIndexOutcome {
            changed: segments,
            remaining: 0,
            indexed_bytes: LIVE as u64 * TAG_BYTES,
        },
    );
    assert!(
        recorded(dir.path())
            .iter()
            .all(|recorded| recorded == &Some(schema(PayloadSchemaType::Keyword))),
    );

    // An exact facet is served from the field index alone.
    let follower = open_follower(dir.path());
    assert_eq!(exact_count(&follower), LIVE);
    assert_eq!(tag_facet_total(&follower), LIVE);
}

/// The writer opens its field indexes on its first store, so one store comes before the create.
#[test]
fn points_stored_after_create_are_indexed() {
    let dir = tagged_leader("edge-pi-append");
    let writer = with_fresh_target(
        UpdateOnlyEdgeShard::open_mmap(dir.path()).unwrap(),
        dir.path(),
    );
    let (writer, _) = writer
        .apply_batch(store_batch(2000, tagged_points(2001..=2002)))
        .unwrap();
    let (writer, _) = create(writer, dir.path(), PayloadSchemaType::Keyword);

    let (_writer, outcome) = writer
        .apply_batch(store_batch(2001, tagged_points(2003..=2005)))
        .unwrap();
    assert_eq!(outcome.stored, 3);

    let follower = open_follower(dir.path());
    assert_eq!(exact_count(&follower), LIVE + 5);
    assert_eq!(tag_facet_total(&follower), LIVE + 5);
}

/// A crash between the upload and the config save leaves files no config records.
#[test]
fn leftovers_of_an_unrecorded_build_are_replaced() {
    let dir = tagged_leader("edge-pi-leftovers");
    let writer = UpdateOnlyEdgeShard::open_mmap(dir.path()).unwrap();
    let (writer, _) = create(writer, dir.path(), PayloadSchemaType::Keyword);
    drop(writer);

    let segment = segment_dirs(dir.path()).remove(0);
    let path = config_path(&segment);
    let mut config = PayloadConfig::load(&path).unwrap();
    config.indices.remove(&tag());
    config.save(&path).unwrap();
    let stale = get_payload_index_path(&segment)
        .join(format!("{}-map", tag().filename()))
        .join("stale.dat");
    fs_err::write(&stale, b"stale").unwrap();

    let writer = UpdateOnlyEdgeShard::open_mmap(dir.path()).unwrap();
    let (_writer, outcome) = create(writer, dir.path(), PayloadSchemaType::Keyword);
    assert_eq!(outcome.changed, 1);
    assert!(!stale.exists());

    let follower = open_follower(dir.path());
    assert_eq!(tag_facet_total(&follower), LIVE);
}

#[test]
fn a_yielded_call_is_finished_by_the_next() {
    let dir = tagged_leader("edge-pi-yield");
    let writer = UpdateOnlyEdgeShard::open_mmap(dir.path()).unwrap();
    let segments = writer.segments_count();

    let asked = Cell::new(0);
    let (writer, first) = writer
        .create_payload_index(
            &tag(),
            &schema(PayloadSchemaType::Keyword),
            &dir.path().join(TEMP_SEGMENTS_PATH),
            || {
                asked.set(asked.get() + 1);
                asked.get() > 1
            },
        )
        .unwrap();
    assert_eq!(first.changed, 1);
    assert_eq!(first.remaining, segments - 1);

    let (_writer, second) = create(writer, dir.path(), PayloadSchemaType::Keyword);
    assert_eq!(second.changed, segments - 1);
    assert_eq!(
        first.indexed_bytes + second.indexed_bytes,
        LIVE as u64 * TAG_BYTES,
    );
}

#[test]
fn another_schema_replaces_the_index() {
    let dir = tagged_leader("edge-pi-replace");
    let writer = UpdateOnlyEdgeShard::open_mmap(dir.path()).unwrap();
    let segments = writer.segments_count();
    let (writer, _) = create(writer, dir.path(), PayloadSchemaType::Keyword);

    let (_writer, outcome) = create(writer, dir.path(), PayloadSchemaType::Text);
    assert_eq!(outcome.changed, segments);
    assert!(
        recorded(dir.path())
            .iter()
            .all(|recorded| recorded == &Some(schema(PayloadSchemaType::Text))),
    );
    assert_eq!(
        tag_files(dir.path(), "map"),
        0,
        "the keyword index must be gone"
    );

    let follower = open_follower(dir.path());
    assert_eq!(exact_count(&follower), LIVE);
}

#[test]
fn delete_removes_the_index_and_writes_do_not_bring_it_back() {
    let dir = tagged_leader("edge-pi-delete");
    let writer = with_fresh_target(
        UpdateOnlyEdgeShard::open_mmap(dir.path()).unwrap(),
        dir.path(),
    );
    let segments = writer.segments_count();
    let (writer, _) = create(writer, dir.path(), PayloadSchemaType::Keyword);
    // The writer now holds the index open, as it does after any store.
    let (writer, _) = writer
        .apply_batch(store_batch(2000, tagged_points(2001..=2002)))
        .unwrap();
    let follower = open_follower(dir.path());
    assert_eq!(tag_facet_total(&follower), LIVE + 2);

    let (writer, outcome) = writer.delete_payload_index(&tag(), || false).unwrap();
    assert_eq!(outcome.changed, segments);
    assert!(recorded(dir.path()).iter().all(Option::is_none));
    assert_eq!(tag_files(dir.path(), ""), 0);

    follower.live_reload().unwrap();
    assert_eq!(exact_count(&follower), LIVE + 2);

    let (_writer, stored) = writer
        .apply_batch(store_batch(2001, tagged_points(2003..=2005)))
        .unwrap();
    assert_eq!(stored.stored, 3);
    assert_eq!(
        tag_files(dir.path(), ""),
        0,
        "a write recreated the deleted index"
    );
}
