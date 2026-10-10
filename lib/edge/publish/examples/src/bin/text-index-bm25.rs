// See lib/edge/python/examples/text-index-bm25.py for the equivalent Python example.

use std::error::Error;

use examples::load_new_shard;
use qdrant_edge::external::serde_json::json;
use qdrant_edge::{
    Bm25ParamsBuilder, CreateIndex, FieldIndexOperations, PayloadFieldSchema, PayloadSchemaParams,
    PointInsertOperations, PointOperations, PointStruct, QueryRequestBuilder,
    TextIndexParamsBuilder, TextQueryBuilder, TextQueryScoring, TextScoringParams, UpdateOperation,
};

fn main() -> Result<(), Box<dyn Error>> {
    let shard = load_new_shard()?;

    // `scoring` makes the index record document lengths, which BM25 needs.
    shard.update(UpdateOperation::FieldIndexOperation(
        FieldIndexOperations::CreateIndex(CreateIndex {
            field_name: "text".try_into().unwrap(),
            field_schema: Some(PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(
                TextIndexParamsBuilder::new()
                    .scoring(TextScoringParams::default())
                    .build(),
            ))),
        }),
    ))?;

    shard.update(UpdateOperation::PointOperation(
        PointOperations::UpsertPoints(PointInsertOperations::PointsList(vec![
            PointStruct::new(
                1,
                vec![0.1, 0.2, 0.3, 0.4],
                json!({"text": "the quick brown fox"}),
            )
            .into(),
            PointStruct::new(
                2,
                vec![0.4, 0.3, 0.2, 0.1],
                json!({"text": "a lazy dog sleeps"}),
            )
            .into(),
            PointStruct::new(
                3,
                vec![0.5, 0.5, 0.5, 0.5],
                json!({"text": "fox fox fox, a very clever fox"}),
            )
            .into(),
        ])),
    ))?;

    let result = shard.query(
        QueryRequestBuilder::new(10)
            .query(TextQueryBuilder::new("text".try_into().unwrap(), "clever fox").build())
            .build(),
    )?;

    for point in &result {
        println!("{point:?}");
    }
    let ids: Vec<_> = result.iter().map(|point| point.id).collect();
    assert_eq!(ids, [3.into(), 1.into()]);

    // Without length normalization the long document loses its length penalty.
    let flat = shard.query(
        QueryRequestBuilder::new(10)
            .query(
                TextQueryBuilder::new("text".try_into().unwrap(), "fox")
                    .scoring(TextQueryScoring::Bm25(
                        Bm25ParamsBuilder::new().b(0.0).build(),
                    ))
                    .build(),
            )
            .build(),
    )?;
    let ids: Vec<_> = flat.iter().map(|point| point.id).collect();
    assert_eq!(ids, [3.into(), 1.into()]);

    Ok(())
}
