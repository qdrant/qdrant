mod common;

use std::fmt::Display;

use collection::collection::Collection;
use collection::grouping::GroupBy;
use collection::operations::point_ops::{PointOperations, PointStruct};
use collection::operations::types::{BaseGroupRequest, SearchGroupsRequest};
use collection::operations::{CollectionUpdateOperations, CreateIndex, FieldIndexOperations};
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use itertools::Itertools;
use rand::thread_rng;
use segment::fixtures::payload_fixtures::random_vector;
use segment::types::{Payload, PayloadFieldSchema, PayloadSchemaType};
use serde_json::Map;
use tempfile::Builder;
use tokio::runtime::Runtime;

const VECTOR_SIZE: usize = 256;
const NUM_POINTS: usize = 200000;

fn create_rnd_batch() -> CollectionUpdateOperations {
    let mut rng: rand::rngs::SmallRng = rand::SeedableRng::seed_from_u64(42);
    let num_points = NUM_POINTS as u64;
    let dim = VECTOR_SIZE;
    let mut points = Vec::new();
    for i in 0..num_points {
        let mut payload_map = Map::new();
        payload_map.insert("a".to_string(), (i % 5).into());
        payload_map.insert("group_id".to_string(), (i % 200).into());
        let vector = random_vector(&mut rng, dim);
        let point = PointStruct {
            id: i.into(),
            vector: vector.into(),
            payload: Some(Payload(payload_map)),
        };
        points.push(point);
    }
    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(points.into()))
}

fn create_int_payload_index(key: impl Into<String>) -> CollectionUpdateOperations {
    CollectionUpdateOperations::FieldIndexOperation(FieldIndexOperations::CreateIndex(
        CreateIndex {
            field_name: key.into(),
            field_schema: Some(PayloadFieldSchema::FieldType(PayloadSchemaType::Integer)),
        },
    ))
}

fn search_groups_bench(c: &mut Criterion) {
    // Prepare collection
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();

    let storage_dir = Builder::new().prefix("storage").tempdir().unwrap();
    let storage_path = storage_dir.path();
    let collection = handle.block_on(common::custom_collection_fixture(storage_path, 256));

    // Upsert points
    let rnd_batch = create_rnd_batch();

    handle
        .block_on(collection.update_from_client(rnd_batch, true, Default::default()))
        .unwrap();

    for i in 0..30 {
        if i == 30 {
            panic!("Indexing timed out");
        }
        let info = handle.block_on(collection.info(None)).unwrap();
        if info.indexed_vectors_count == NUM_POINTS {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    
    let group_sizes = [1, 10, 100].into_iter().map(|x| x as u32);
    
    let limits = [1, 10, 100, 1000].into_iter().map(|x| x as u32);
    
    #[derive(Clone, Copy)]
    struct Params {
        group_size: u32,
        limit: u32,
    }
    impl Display for Params {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "group_size={}, limit={}", self.group_size, self.limit)
        }
    }
    
    let param_combinations = group_sizes.cartesian_product(limits).map(|(group_size, limit)| Params{group_size, limit}).collect_vec();
    
    let mut group = c.benchmark_group("unindexed-search-groups");
    for params in param_combinations.clone() {
        group.bench_with_input(
            BenchmarkId::from_parameter(params),
            &params,
            |bencher, Params { group_size, limit}| {
                bencher
                    .to_async(&runtime)
                    .iter(|| search_groups(*limit, *group_size, &collection))
            },
        );
    }
    group.finish();

    // Now create index on group_id
    handle
        .block_on(collection.update_from_client(
            create_int_payload_index("group_id"),
            true,
            Default::default(),
        ))
        .unwrap();

    for i in 0..=30 {
        if i == 30 {
            panic!("Payload indexing timed out");
        }
        let info = handle.block_on(collection.info(None)).unwrap();
        if info.payload_schema.get("group_id").unwrap().points == NUM_POINTS {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    let mut group = c.benchmark_group("indexed-search-groups");
    for params in param_combinations {
        group.bench_with_input(
            BenchmarkId::from_parameter(params),
            &params,
            |bencher, Params { group_size, limit } | {
                bencher
                    .to_async(&runtime)
                    .iter(|| search_groups(*limit, *group_size, &collection))
            },
        );
    }

    group.finish();
}

async fn search_groups(limit: u32, group_size: u32, collection: &Collection) {
    let mut rng = thread_rng();
    let query = random_vector(&mut rng, VECTOR_SIZE);
    let search_groups_query = SearchGroupsRequest {
        vector: query.into(),
        filter: None,
        params: None,
        group_request: BaseGroupRequest {
            limit,
            group_size,
            group_by: "group_id".to_string(),
        },
        with_payload: None,
        with_vector: None,
        score_threshold: None,
    };

    let group_by = GroupBy::new(search_groups_query.into(), collection, |_| async {
        unreachable!()
    });

    let result = group_by.execute().await.unwrap();
    assert!(!result.is_empty());
}

criterion_group!{
    name = benches;
    config = Criterion::default()
        .measurement_time(core::time::Duration::from_secs(30))
        .warm_up_time(core::time::Duration::from_secs(10));
    targets = search_groups_bench
}

criterion_main!(benches);
