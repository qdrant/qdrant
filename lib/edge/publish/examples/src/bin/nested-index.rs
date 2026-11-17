// See lib/edge/python/examples/nested-index.py for the equivalent Python example.
//
// Per-element payload indexes: index every object of an array as its own entry, so a
// `nested` filter is answered from the indexes, matching conditions on the same object.

use std::error::Error;

use examples::load_new_shard;
use qdrant_edge::external::ordered_float::OrderedFloat;
use qdrant_edge::external::serde_json::json;
use qdrant_edge::{
    Condition, CountRequest, CreateIndex, FieldCondition, FieldIndexOperations, Filter,
    IntegerIndexParamsBuilder, JsonPath, KeywordIndexParamsBuilder, Match, Nested, NestedCondition,
    PayloadFieldSchema, PayloadSchemaParams, PointInsertOperations, PointOperations, PointStruct,
    Range, UpdateOperation, ValueVariants,
};

fn main() -> Result<(), Box<dyn Error>> {
    let shard = load_new_shard()?;

    shard.update(UpdateOperation::PointOperation(
        PointOperations::UpsertPoints(PointInsertOperations::PointsList(vec![
            PointStruct::new(
                1,
                vec![0.1, 0.2, 0.3, 0.4],
                json!({"offers": [{"shop": "a", "price": 10}]}),
            )
            .into(),
            // Shop "a" and a price under 20, but in different offers
            PointStruct::new(
                2,
                vec![0.4, 0.3, 0.2, 0.1],
                json!({"offers": [{"shop": "a", "price": 30}, {"shop": "b", "price": 5}]}),
            )
            .into(),
        ])),
    ))?;

    // `nested` names the array: "offers" and "offers[]" are the same
    let offers: JsonPath = "offers".try_into().unwrap();
    for (field, params) in [
        (
            "offers[].shop",
            PayloadSchemaParams::Keyword(
                KeywordIndexParamsBuilder::new()
                    .nested(offers.clone())
                    .build(),
            ),
        ),
        (
            "offers[].price",
            PayloadSchemaParams::Integer(
                IntegerIndexParamsBuilder::new()
                    .nested(offers.clone())
                    .build(),
            ),
        ),
    ] {
        shard.update(UpdateOperation::FieldIndexOperation(
            FieldIndexOperations::CreateIndex(CreateIndex {
                field_name: field.try_into().unwrap(),
                field_schema: Some(PayloadFieldSchema::FieldParams(params)),
            }),
        ))?;
    }

    for (field, index) in &shard.info()?.payload_schema {
        let nested = index.params.as_ref().and_then(PayloadSchemaParams::nested);
        assert_eq!(nested.map(ToString::to_string).as_deref(), Some("offers[]"));
        println!("{field}: nested={}", nested.unwrap());
    }

    // An offer of shop "a" under 20: both conditions on the same offer
    let cheap_offer_of_a = Filter::new_must(Condition::Nested(NestedCondition::new(Nested {
        key: offers,
        filter: Filter {
            should: None,
            min_should: None,
            must: Some(vec![
                Condition::Field(FieldCondition::new_match(
                    "shop".try_into().unwrap(),
                    Match::new_value(ValueVariants::String("a".to_string())),
                )),
                Condition::Field(FieldCondition::new_range(
                    "price".try_into().unwrap(),
                    Range {
                        lt: Some(OrderedFloat(20.0)),
                        gt: None,
                        gte: None,
                        lte: None,
                    },
                )),
            ]),
            must_not: None,
        },
    })));
    let count = shard.count(CountRequest {
        filter: Some(cheap_offer_of_a),
        exact: true,
    })?;
    println!("Points with a cheap offer of shop a: {count}");
    assert_eq!(count, 1);

    Ok(())
}
