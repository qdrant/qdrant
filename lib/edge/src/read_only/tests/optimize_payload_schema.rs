use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use common::budget::ResourceBudget;
use segment::index::payload_config::PayloadConfig;
use segment::json_path::JsonPath;
use segment::segment_constructor::get_payload_index_path;
use segment::types::{PayloadFieldSchema, PayloadSchemaType};
use shard::files::SEGMENTS_PATH;
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;

use super::{delete, tag_index_operation, tagged_points, test_config};
use crate::PointOperations::UpsertPoints;
use crate::{EdgeConfig, EdgeOptimizersConfig, EdgeShard};

fn tag() -> JsonPath {
    "tag".parse().unwrap()
}

fn leader_due_a_vacuum(dir: &Path, indexed: bool) -> EdgeShard {
    let config = EdgeConfig {
        optimizers: Some(EdgeOptimizersConfig {
            deleted_threshold: Some(0.2),
            vacuum_min_vector_number: Some(1),
            indexing_threshold: Some(1),
            ..EdgeOptimizersConfig::default()
        }),
        ..test_config()
    };
    let leader = EdgeShard::new(dir, config).unwrap();
    leader
        .update(PointOperation(UpsertPoints(PointsList(tagged_points(
            1..=1000,
        )))))
        .unwrap();
    if indexed {
        leader.update(tag_index_operation(true)).unwrap();
    }
    delete(&leader, 1..=300);
    leader.flush().unwrap();
    leader
}

fn segment_dirs(dir: &Path) -> HashSet<PathBuf> {
    fs_err::read_dir(dir.join(SEGMENTS_PATH))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| get_payload_index_path(path).is_dir())
        .collect()
}

/// `tag`'s schema on every segment the optimize built, the empty appendable it leaves included.
fn built_schemas(
    leader: &EdgeShard,
    dir: &Path,
    payload_schema: HashMap<JsonPath, PayloadFieldSchema>,
) -> Vec<Option<PayloadFieldSchema>> {
    let before = segment_dirs(dir);
    assert!(
        leader
            .optimize_with_payload_schema(ResourceBudget::new(1, 1), payload_schema)
            .unwrap()
    );
    leader.flush().unwrap();
    segment_dirs(dir)
        .difference(&before)
        .map(|segment| {
            let path = PayloadConfig::get_config_path(&get_payload_index_path(segment));
            PayloadConfig::load(&path)
                .unwrap()
                .indices
                .get(&tag())
                .map(|indexed| indexed.schema.clone())
        })
        .collect()
}

#[test]
fn a_rebuild_drops_an_index_missing_from_the_schema() {
    let dir = tempfile::Builder::new()
        .prefix("edge-opt-drop")
        .tempdir()
        .unwrap();
    let leader = leader_due_a_vacuum(dir.path(), true);

    let built = built_schemas(&leader, dir.path(), HashMap::new());
    assert!(!built.is_empty());
    assert!(built.iter().all(Option::is_none), "{built:?}");
}

#[test]
fn a_rebuild_adds_an_index_its_sources_lack() {
    let dir = tempfile::Builder::new()
        .prefix("edge-opt-add")
        .tempdir()
        .unwrap();
    let leader = leader_due_a_vacuum(dir.path(), false);
    let keyword = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);
    let built = built_schemas(
        &leader,
        dir.path(),
        HashMap::from([(tag(), keyword.clone())]),
    );
    assert!(!built.is_empty());
    assert!(
        built.iter().all(|schema| schema == &Some(keyword.clone())),
        "{built:?}"
    );
}
