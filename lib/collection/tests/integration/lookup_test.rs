use collection::collection::Collection;
use collection::lookup::types::PseudoId;
use collection::lookup::{lookup_ids, WithLookup};
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::point_ops::{Batch, WriteOrdering};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::shards::shard::ShardId;
use itertools::Itertools;
use rand::rngs::SmallRng;
use rand::{self, Rng, SeedableRng};
use rstest::*;
use segment::data_types::vectors::VectorStruct;
use segment::types::{Payload, PointIdType};
use serde_json::json;
use tempfile::Builder;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::common::simple_collection_fixture;

const SEED: u64 = 42;

struct Resources {
    request: WithLookup,
    collection: RwLock<Collection>,
    read_consistency: Option<ReadConsistency>,
    shard_selection: Option<ShardId>,
}

async fn setup() -> Resources {
    let request = WithLookup {
        collection_name: "test".to_string(),
        with_payload: None,
        with_vectors: None,
    };

    let collection_dir = Builder::new().prefix("storage").tempdir().unwrap();

    let collection = simple_collection_fixture(collection_dir.path(), 1).await;

    let int_ids = (0..1000).map(PointIdType::from);

    let mut rng = SmallRng::seed_from_u64(SEED);
    let uuids = (0..1000).map(|_| PointIdType::Uuid(Uuid::from_u128(rng.gen())));

    let ids = int_ids.chain(uuids).collect_vec();

    let mut rng = SmallRng::seed_from_u64(SEED);
    let vectors = (0..2000)
        .map(|_| rng.gen::<[f32; 4]>().to_vec())
        .collect_vec();

    let payloads = ids
        .iter()
        .map(|i| Some(Payload::from(json!({ "foo": format!("bar {}", i) }))))
        .collect_vec();

    let upsert_points = collection::operations::CollectionUpdateOperations::PointOperation(
        Batch {
            ids,
            vectors: vectors.into(),
            payloads: Some(payloads),
        }
        .into(),
    );

    collection
        .update_from_client_simple(upsert_points, true, WriteOrdering::default())
        .await
        .unwrap();

    let read_consistency = None;

    let shard_selection = None;

    Resources {
        request,
        collection: RwLock::new(collection),
        read_consistency,
        shard_selection,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn happy_lookup_ids() {
    let Resources {
        mut request,
        collection,
        read_consistency,
        shard_selection,
    } = setup().await;

    let collection = collection.read().await;

    let collection_by_name = |_: String| async { Some(collection) };

    let n = 100u64;
    let ints = (0..n).map_into();

    let mut rng = SmallRng::seed_from_u64(SEED);
    let uuids = (0..n)
        .map(|_| Uuid::from_u128(rng.gen()).to_string())
        .map_into();

    let values = ints.chain(uuids).collect_vec();
    request.with_payload = Some(true.into());
    request.with_vectors = Some(true.into());

    let shard_selection = match shard_selection {
        Some(shard_id) => ShardSelectorInternal::ShardId(shard_id),
        None => ShardSelectorInternal::All,
    };

    let result = lookup_ids(
        request.clone(),
        values.clone(),
        collection_by_name,
        read_consistency,
        &shard_selection,
    )
    .await;

    assert!(result.is_ok());

    let result = result.unwrap();

    assert_eq!(result.len(), (n * 2) as usize);

    let mut rng = SmallRng::seed_from_u64(SEED);

    // use points 0..n and 1000..1000+n as expected vectors
    let expected_vectors = (0..1000 + n)
        .map(|i| (i, rng.gen::<[f32; 4]>().to_vec()))
        .filter(|(i, _)| !(&n..&1000).contains(&i))
        .map(|(_, v)| v)
        .map(VectorStruct::from);

    for (id_value, vector) in values.into_iter().zip(expected_vectors) {
        let record = result
            .get(&id_value)
            .unwrap_or_else(|| panic!("Expected to find record for id {}", id_value));

        assert_eq!(record.id, PointIdType::try_from(id_value.clone()).unwrap());
        assert_eq!(
            record.payload,
            Some(Payload::from(json!({ "foo": format!("bar {}", id_value) })))
        );
        assert_eq!(record.vector, Some(vector));
    }
}

fn first_uuid() -> String {
    let mut rng = SmallRng::seed_from_u64(SEED);
    Uuid::from_u128(rng.gen()).to_string()
}

#[rstest]
#[case::existing_uuid(first_uuid())]
#[case::zero_int(0i64)]
#[case::positive_int(1i64)]
#[case::existing_uint(999u64)]
fn parsable_pseudo_id_to_point_id(#[case] value: impl Into<PseudoId>) {
    let value = value.into();
    assert!(PointIdType::try_from(value).is_ok());
}

#[rstest]
#[case::negative_int(-1i64)]
#[case::non_uuid_string("not a uuid")]
fn non_parsable_pseudo_id_to_point_id(#[case] value: impl Into<PseudoId>) {
    let value = value.into();
    assert!(PointIdType::try_from(value).is_err());
}

#[rstest]
#[case::uuid(Uuid::new_v4().to_string())]
#[case::int(1001u64)]
#[tokio::test(flavor = "multi_thread")]
async fn nonexistent_lookup_ids_are_ignored(#[case] value: impl Into<PseudoId>) {
    let value = value.into();

    let Resources {
        mut request,
        collection,
        read_consistency,
        shard_selection,
    } = setup().await;

    let shard_selection = match shard_selection {
        Some(shard_id) => ShardSelectorInternal::ShardId(shard_id),
        None => ShardSelectorInternal::All,
    };

    let collection = collection.read().await;

    let collection_by_name = |_: String| async { Some(collection) };

    let values = vec![value];
    request.with_payload = Some(true.into());
    request.with_vectors = Some(true.into());

    let result = lookup_ids(
        request,
        values,
        collection_by_name,
        read_consistency,
        &shard_selection,
    )
    .await;

    assert!(result.is_ok());

    let result = result.unwrap();

    assert_eq!(result.len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn err_when_collection_by_name_returns_none() {
    let Resources {
        request,
        read_consistency,
        shard_selection,
        ..
    } = setup().await;

    let shard_selection = match shard_selection {
        Some(shard_id) => ShardSelectorInternal::ShardId(shard_id),
        None => ShardSelectorInternal::All,
    };

    let collection_by_name = |_: String| async { None };

    let result = lookup_ids(
        request,
        vec![],
        collection_by_name,
        read_consistency,
        &shard_selection,
    )
    .await;

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Collection test not found".to_string()
    );
}
