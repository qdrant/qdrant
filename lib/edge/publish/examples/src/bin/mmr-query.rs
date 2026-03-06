// See lib/edge/python/examples/mmr-query.py for the equivalent Python example.

use std::error::Error;

use examples::{fill_dummy_data, load_new_shard};
use ordered_float::OrderedFloat;
use qdrant_edge::segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use qdrant_edge::segment::types::{WithPayloadInterface, WithVector};
use qdrant_edge::shard::query::{MmrInternal, ScoringQuery, ShardQueryRequest};

const DATA_DIR: &str = "./data/mmr-query";

fn main() -> Result<(), Box<dyn Error>> {
    let shard = load_new_shard(DATA_DIR)?;
    fill_dummy_data(&shard)?;

    let result = shard.query(ShardQueryRequest {
        prefetches: vec![],
        query: Some(ScoringQuery::Mmr(MmrInternal {
            vector: vec![6.0, 9.0, 4.0, 2.0].into(),
            using: DEFAULT_VECTOR_NAME.to_string(),
            lambda: OrderedFloat(0.9),
            candidates_limit: 100,
        })),
        filter: None,
        score_threshold: None,
        limit: 10,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(true),
        with_payload: WithPayloadInterface::Bool(true),
    })?;

    for point in &result {
        println!("{point:?}");
    }

    Ok(())
}
