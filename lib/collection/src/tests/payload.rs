use std::str::FromStr;
use std::sync::Arc;

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use ordered_float::OrderedFloat;
use segment::json_path::JsonPath;
use segment::types::{
    Condition, FieldCondition, Filter, GeoPoint, GeoRadius, PayloadFieldSchema, PayloadSchemaType,
    Range,
};
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::collection::payload_index_schema::{self, PayloadIndexSchema};
use crate::operations::{CollectionUpdateOperations, CreateIndex, FieldIndexOperations};
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::tests::fixtures::{create_collection_config, upsert_operation};

#[tokio::test(flavor = "multi_thread")]
async fn test_payload_missing_index_check() {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();

    let collection_name = "test".to_string();

    let current_runtime: Handle = Handle::current();

    let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
    let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
    let payload_index_schema =
        Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file.clone()).unwrap());

    let shard = LocalShard::build(
        0,
        collection_name.clone(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        Arc::new(Default::default()),
        payload_index_schema.clone(),
        current_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    let upsert_ops = upsert_operation();

    shard
        .update(upsert_ops.into(), true, HwMeasurementAcc::new())
        .await
        .unwrap();

    let geo_filter = Filter::new_must(Condition::Field(FieldCondition::new_geo_radius(
        JsonPath::from_str("location").unwrap(),
        GeoRadius {
            center: GeoPoint::new(12.0, 34.0).ok().unwrap(),
            radius: OrderedFloat(50.0),
        },
    )));

    // No index yet => Filter has unindexed field
    assert_eq!(
        payload_index_schema::one_unindexed_filter_key(
            &shard.payload_index_schema.read(),
            &geo_filter
        )
        .map(|(x, _)| x),
        Some(JsonPath::from_str("location").unwrap())
    );

    // Create unnested index
    create_index(
        &shard,
        &payload_index_schema,
        "location",
        PayloadSchemaType::Geo,
    )
    .await;

    // Index created => Filter shouldn't have any unindexed field anymore
    assert_eq!(
        payload_index_schema::one_unindexed_filter_key(
            &shard.payload_index_schema.read(),
            &geo_filter
        ),
        None
    );

    // Create nested filter
    let condition = Condition::new_nested(
        JsonPath::new("location"),
        Filter::new_must(Condition::Field(FieldCondition::new_range(
            JsonPath::new("lat"),
            Range {
                gt: Some(12.into()),
                ..Default::default()
            },
        ))),
    );
    let num_filter = Filter::new_must(condition);

    // Index only exists for 'location' but not 'location.lat'
    // so we expect it to be detected as unindexed
    assert_eq!(
        payload_index_schema::one_unindexed_filter_key(
            &shard.payload_index_schema.read(),
            &num_filter
        )
        .map(|(x, _)| x),
        Some("location[].lat".parse().unwrap())
    );

    // Create index for nested field
    create_index(
        &shard,
        &payload_index_schema,
        "location[].lat",
        PayloadSchemaType::Float,
    )
    .await;

    // Nested field also gets detected as indexed and unindexed fields in the query are empty.
    assert_eq!(
        payload_index_schema::one_unindexed_filter_key(
            &shard.payload_index_schema.read(),
            &num_filter
        ),
        None,
    );

    // Filters combined also completely indexed!
    let combined_filter = geo_filter.merge(&num_filter);
    assert_eq!(
        payload_index_schema::one_unindexed_filter_key(
            &shard.payload_index_schema.read(),
            &combined_filter
        ),
        None,
    );
}

async fn create_index(
    shard: &LocalShard,
    payload_index_schema: &Arc<SaveOnDisk<PayloadIndexSchema>>,
    name: &str,
    field_type: PayloadSchemaType,
) {
    payload_index_schema
        .write(|schema| {
            schema.schema.insert(
                name.parse().unwrap(),
                PayloadFieldSchema::FieldType(field_type),
            );
        })
        .unwrap();
    let create_index = CollectionUpdateOperations::FieldIndexOperation(
        FieldIndexOperations::CreateIndex(CreateIndex {
            field_name: name.parse().unwrap(),
            field_schema: Some(PayloadFieldSchema::FieldType(field_type)),
        }),
    );
    shard
        .update(create_index.into(), true, HwMeasurementAcc::new())
        .await
        .unwrap();
}
