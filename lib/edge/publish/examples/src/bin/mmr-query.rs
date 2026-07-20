// See lib/edge/python/examples/mmr-query.py for the equivalent Python example.

use std::error::Error;

use examples::{fill_dummy_data, load_new_shard};
use qdrant_edge::external::ordered_float::OrderedFloat;
use qdrant_edge::{
    DEFAULT_VECTOR_NAME, Mmr, QueryRequestBuilder, ScoringQuery, WithPayloadInterface, WithVector,
};

fn main() -> Result<(), Box<dyn Error>> {
    let shard = load_new_shard()?;
    fill_dummy_data(&shard)?;

    let result = shard.query(
        QueryRequestBuilder::new(10)
            .query(ScoringQuery::Mmr(Mmr {
                vector: vec![6.0, 9.0, 4.0, 2.0].into(),
                using: DEFAULT_VECTOR_NAME.to_string(),
                lambda: OrderedFloat(0.9),
                candidates_limit: 100,
            }))
            .with_vector(WithVector::Bool(true))
            .with_payload(WithPayloadInterface::Bool(true))
            .build(),
    )?;

    for point in &result {
        println!("{point:?}");
    }

    Ok(())
}
