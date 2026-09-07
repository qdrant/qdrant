use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use common::types::DeferredBehavior;
use ordered_float::OrderedFloat;
use segment::data_types::order_by::OrderBy;
use segment::json_path::JsonPath;
use segment::types::{
    Condition, FieldCondition, Filter, GeoPoint, GeoRadius, PayloadFieldSchema, PayloadSchemaType,
    Range, WithPayload,
};
use serde_json::json;
use shard::scroll::ScrollRequestInternal;
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::collection::payload_index_schema::{self, PayloadIndexSchema};
use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::{CollectionUpdateOperations, CreateIndex, FieldIndexOperations};
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::{ShardOperation, WaitUntil};
use crate::tests::fixtures::{create_collection_config, upsert_operation};

#[tokio::test(flavor = "multi_thread")]
async fn test_internal_scroll_prefers_payload_index() {
    let dir = Builder::new().prefix("indexed_scroll").tempdir().unwrap();
    let config = create_collection_config();
    let runtime = AdaptiveSearchHandle::current_for_tests();
    let schema =
        Arc::new(SaveOnDisk::load_or_init_default(dir.path().join("schema.json")).unwrap());
    let shard = LocalShard::build(
        0,
        "test".into(),
        dir.path(),
        Arc::new(RwLock::new(config.clone())),
        Arc::new(Default::default()),
        schema.clone(),
        Handle::current(),
        runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config,
    )
    .await
    .unwrap();
    let upsert = CollectionUpdateOperations::PointOperation(
        crate::operations::point_ops::PointOperations::UpsertPoints(
            serde_json::from_value(json!({"points": [
                {"id": 1, "vector": [1.0, 2.0, 3.0, 4.0], "payload": {"num": 20}},
                {"id": 2, "vector": [1.0, 2.0, 3.0, 4.0], "payload": {"num": 10}}
            ]}))
            .unwrap(),
        ),
    );
    shard
        .update(
            upsert.into(),
            WaitUntil::Visible,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap();
    create_index(&shard, &schema, "num", PayloadSchemaType::Integer).await;

    for order_by in [None, Some("num")] {
        // Public request conversion must keep exact retrieval, even if a client
        // sends the internal hint as an unknown JSON field.
        let request: ScrollRequestInternal = serde_json::from_value(json!({
            "limit": 1, "with_payload": ["num"], "order_by": order_by,
            "prefer_payload_index": true
        }))
        .unwrap();
        let mut with_payload = WithPayload::from(request.with_payload.as_ref().unwrap());
        assert!(!with_payload.prefer_payload_index);
        with_payload.prefer_payload_index = true;
        let hw = HwMeasurementAcc::new();
        let indexed = if let Some(order_by) = request.order_by.clone() {
            shard
                .internal_scroll_by_field(
                    1,
                    &with_payload,
                    &false.into(),
                    None,
                    &runtime,
                    &OrderBy::from(order_by),
                    Duration::from_secs(30),
                    hw.clone(),
                    DeferredBehavior::VisibleOnly,
                )
                .await
                .unwrap()
        } else {
            shard
                .internal_scroll_by_id(
                    None,
                    1,
                    &with_payload,
                    &false.into(),
                    None,
                    &runtime,
                    Duration::from_secs(30),
                    hw.clone(),
                    DeferredBehavior::VisibleOnly,
                )
                .await
                .unwrap()
        };
        assert_eq!(hw.get_payload_io_read(), 0);
        let exact = shard
            .scroll_by(Arc::new(request), &runtime, None, HwMeasurementAcc::new())
            .await
            .unwrap();
        assert_eq!(indexed.len(), 1);
        assert_eq!(exact.len(), 1);
        assert_eq!(indexed[0].id, exact[0].id);
        let expected = if order_by.is_some() { 10 } else { 20 };
        assert_eq!(
            indexed[0].payload.as_ref().unwrap().0["num"],
            json!([expected])
        );
        assert_eq!(exact[0].payload.as_ref().unwrap().0["num"], json!(expected));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_payload_missing_index_check() {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();

    let collection_name = "test".to_string();

    let update_runtime = Handle::current();
    let search_runtime: AdaptiveSearchHandle = AdaptiveSearchHandle::current_for_tests();

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
        update_runtime.clone(),
        search_runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    let upsert_ops = upsert_operation();

    shard
        .update(
            upsert_ops.into(),
            WaitUntil::Visible,
            None,
            HwMeasurementAcc::new(),
        )
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

pub async fn create_index(
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
        .update(
            create_index.into(),
            WaitUntil::Visible,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap();
}
