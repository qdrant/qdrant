//! End-to-end check of the in-memory backend: write a real edge shard to disk with the ordinary
//! read-write path, slurp every file it produced into [`MemFs`], and serve reads out of that.
//!
//! This is the native stand-in for the browser flow. The wasm build fills `MemFs` from `fetch`
//! instead of from the filesystem, but everything downstream — segment open, HNSW/plain search,
//! filtering, payload retrieval — is the same code over the same [`MemFile`] backend, so a green
//! run here means the wasm target has a working read path and not just a clean compile.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use common::flags::{FeatureFlags, init_feature_flags};
use edge::{
    Condition, Distance, EdgeConfig, EdgeShard, EdgeVectorParams, FieldCondition, Filter, JsonPath,
    Match, PointId, PointInsertOperations, PointOperations, UpdateOperation, ValueVariants,
};
use edge_wasm::mem_fs::MemFs;
use edge_wasm::shard::MemEdgeShard;
use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
use shard::files::SEGMENT_MANIFEST_FILE;
use shard::operations::point_ops::{PointStructPersisted, VectorStructPersisted};

const VECTOR_NAME: &str = "wasm-test-vector";
const DIM: usize = 4;
const POINTS: u64 = 64;

/// A deterministic, well-separated vector for point `id`: a one-hot-ish ramp so nearest-neighbour
/// order is unambiguous and the assertions do not depend on index tuning.
fn vector_for(id: u64) -> Vec<f32> {
    let base = id as f32;
    (0..DIM).map(|i| base + i as f32 * 0.01).collect()
}

fn config() -> EdgeConfig {
    EdgeConfig {
        on_disk_payload: Some(false),
        vectors: HashMap::from([(
            VECTOR_NAME.to_string(),
            EdgeVectorParams {
                size: DIM,
                distance: Distance::Euclid,
                quantization_config: None,
                multivector_config: None,
                datatype: None,
                on_disk: None,
                hnsw_config: None,
            },
        )]),
        sparse_vectors: HashMap::new(),
        hnsw_config: Default::default(),
        quantization_config: None,
        optimizers: Default::default(),
        wal_options: None,
        max_search_threads: Some(1),
    }
}

fn point(id: u64) -> PointStructPersisted {
    PointStructPersisted {
        id: PointId::NumId(id),
        vector: VectorStructPersisted::from(VectorStructInternal::Named(HashMap::from([(
            VECTOR_NAME.to_string(),
            VectorInternal::from(vector_for(id)),
        )]))),
        payload: Some(
            serde_json::from_value(serde_json::json!({
                "parity": if id % 2 == 0 { "even" } else { "odd" },
            }))
            .unwrap(),
        ),
    }
}

/// Recursively read every file under `root` into `(shard-relative path, bytes)` pairs — the
/// filesystem equivalent of listing an object-store prefix and downloading each key.
fn slurp(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut files = Vec::new();

    for entry in walkdir::WalkDir::new(root) {
        let entry = entry.unwrap();
        if !entry.file_type().is_file() {
            continue;
        }

        let relative = entry.path().strip_prefix(root).unwrap().to_path_buf();
        files.push((relative, std::fs::read(entry.path()).unwrap()));
    }

    files
}

fn parity_filter(parity: &str) -> Filter {
    let path: JsonPath = "parity".parse().unwrap();
    let value = ValueVariants::String(parity.to_string());

    Filter::new_must(Condition::Field(FieldCondition::new_match(
        path,
        Match::from(value),
    )))
}

#[test]
fn search_over_in_memory_segments_matches_the_leader() {
    // The read-only follower discovers segments through the leader's manifest.
    let mut flags = FeatureFlags::default();
    flags.write_segment_manifest = true;
    init_feature_flags(flags);

    let dir = tempfile::Builder::new()
        .prefix("edge-wasm-round-trip")
        .tempdir()
        .unwrap();

    let leader = EdgeShard::new(dir.path(), config()).unwrap();
    leader
        .update(UpdateOperation::PointOperation(
            PointOperations::UpsertPoints(PointInsertOperations::PointsList(
                (1..=POINTS).map(point).collect(),
            )),
        ))
        .unwrap();
    leader.flush().unwrap();

    let files = slurp(dir.path());
    assert!(
        files
            .iter()
            .any(|(path, _)| path == Path::new(SEGMENT_MANIFEST_FILE)),
        "leader did not write a segment manifest; discovery would find nothing",
    );

    let fs = MemFs::new(files);
    let total_len = fs.total_len();
    assert!(total_len > 0, "no bytes were loaded");

    let shard = MemEdgeShard::open(fs, Path::new("")).unwrap();

    let info = shard.info().unwrap();
    assert_eq!(info.points_count, POINTS as usize);

    // Nearest neighbours of point 10's own vector must start with point 10 itself.
    let hits = shard
        .search(Some(VECTOR_NAME), vector_for(10), 5, None, true)
        .unwrap();
    assert_eq!(hits.len(), 5);
    assert_eq!(hits[0].id, PointId::NumId(10));
    assert_eq!(
        hits[0].payload.as_ref().unwrap().0.get("parity").unwrap(),
        &serde_json::json!("even"),
    );

    // `Distance::Euclid` scores are squared distances, so the ranking runs ascending; the exact
    // match above scored 0. Assert the order holds and that the neighbourhood is the ids around 10.
    let returned: Vec<_> = hits.iter().map(|hit| (hit.id, hit.score)).collect();
    for pair in hits.windows(2) {
        assert!(
            pair[0].score <= pair[1].score,
            "results out of order: {returned:?}",
        );
    }
    assert_eq!(
        hits[0].score, 0.0,
        "exact match did not score 0: {returned:?}"
    );
    for id in [8u64, 9, 10, 11, 12] {
        assert!(
            returned.iter().any(|(hit, _)| *hit == PointId::NumId(id)),
            "expected {id} among the 5 nearest to 10, got {returned:?}",
        );
    }

    // A filtered search must only return matching points.
    let odd = shard
        .search(
            Some(VECTOR_NAME),
            vector_for(10),
            5,
            Some(parity_filter("odd")),
            false,
        )
        .unwrap();
    assert_eq!(odd.len(), 5);
    for hit in &odd {
        let PointId::NumId(id) = hit.id else {
            panic!("unexpected point id kind: {:?}", hit.id);
        };
        assert_eq!(id % 2, 1, "filter leaked an even point: {returned:?}");
    }

    // Scroll must see every point.
    let scrolled = shard.scroll(POINTS as usize, None, false).unwrap();
    assert_eq!(scrolled.len(), POINTS as usize);
}
