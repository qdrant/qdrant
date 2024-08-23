use std::str::FromStr;
use std::sync::Arc;

use common::cpu::CpuBudget;
use segment::json_path::JsonPath;
use segment::types::{
    Condition, FieldCondition, Filter, GeoPoint, GeoRadius, PayloadFieldSchema, PayloadSchemaType,
    Range,
};
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::operations::{CollectionUpdateOperations, CreateIndex, FieldIndexOperations};
use crate::save_on_disk::SaveOnDisk;
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
        CpuBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    let upsert_ops = upsert_operation();

    shard.update(upsert_ops.into(), true).await.unwrap();

    let geo_filter = Filter::new_must(Condition::Field(FieldCondition::new_geo_radius(
        JsonPath::from_str("location").unwrap(),
        GeoRadius {
            center: GeoPoint::new(12.0, 34.0).ok().unwrap(),
            radius: 50.0,
        },
    )));

    // We didn't create a payload yet
    assert_eq!(
        shard
            .payload_index_schema
            .read()
            .filter_without_index(&geo_filter, &collection_name),
        Some(JsonPath::from_str("location").unwrap())
    );
    create_index(
        &shard,
        &payload_index_schema,
        "location",
        PayloadSchemaType::Geo,
    )
    .await;

    assert_eq!(
        shard
            .payload_index_schema
            .read()
            .filter_without_index(&geo_filter, &collection_name),
        None
    );

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

    assert_eq!(
        shard
            .payload_index_schema
            .read()
            .filter_without_index(&num_filter, &collection_name),
        Some("location.lat".parse().unwrap())
    );

    create_index(
        &shard,
        &payload_index_schema,
        "location.lat",
        PayloadSchemaType::Float,
    )
    .await;

    assert_eq!(
        shard
            .payload_index_schema
            .read()
            .filter_without_index(&num_filter, &collection_name),
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
    shard.update(create_index.into(), true).await.unwrap();
}
