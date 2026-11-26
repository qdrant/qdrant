use segment::index::payload_config::PayloadConfig;
use segment::json_path::JsonPath;
use segment::segment_constructor::get_payload_index_path;
use shard::files::SEGMENTS_PATH;
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;

use super::{delete, open_follower, tag_index_operation, tagged_points, test_config};
use crate::PointOperations::UpsertPoints;
use crate::{EdgeConfig, EdgeOptimizersConfig, EdgeShard, EdgeShardRead as _};

#[test]
fn a_field_indexed_on_one_segment_only_is_not_fully_indexed() {
    let dir = tempfile::Builder::new()
        .prefix("edge-ro-fully-indexed")
        .tempdir()
        .unwrap();
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
    delete(&leader, 1..=300);
    leader.flush().unwrap();
    assert!(leader.optimize().unwrap(), "expected a vacuum to run");
    leader
        .update(PointOperation(UpsertPoints(PointsList(tagged_points(
            1001..=1010,
        )))))
        .unwrap();
    leader.update(tag_index_operation(true)).unwrap();
    leader.flush().unwrap();
    drop(leader);

    let tag: JsonPath = "tag".parse().unwrap();
    let follower = open_follower(dir.path());
    assert!(follower.fully_indexed_fields().unwrap().contains_key(&tag));

    // What a partly applied index change leaves behind.
    let segment = fs_err::read_dir(dir.path().join(SEGMENTS_PATH))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| get_payload_index_path(path).is_dir())
        .unwrap();
    let path = PayloadConfig::get_config_path(&get_payload_index_path(&segment));
    let mut payload_config = PayloadConfig::load(&path).unwrap();
    payload_config.indices.remove(&tag);
    payload_config.save(&path).unwrap();

    let follower = open_follower(dir.path());
    assert!(!follower.fully_indexed_fields().unwrap().contains_key(&tag));
}
