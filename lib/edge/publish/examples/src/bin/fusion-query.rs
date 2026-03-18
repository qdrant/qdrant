// See lib/edge/python/examples/fusion-query.py for the equivalent Python example.

use std::error::Error;

use examples::{fill_dummy_data, load_new_shard};
use ordered_float::OrderedFloat;
use qdrant_edge::segment::data_types::vectors::{DEFAULT_VECTOR_NAME, NamedQuery, VectorInternal};
use qdrant_edge::segment::types::{
    Condition, FieldCondition, Filter, Match, ValueVariants, WithPayloadInterface, WithVector,
};
use qdrant_edge::shard::query::query_enum::QueryEnum;
use qdrant_edge::shard::query::{FusionInternal, ScoringQuery, ShardPrefetch, ShardQueryRequest};

fn main() -> Result<(), Box<dyn Error>> {
    let shard = load_new_shard()?;
    fill_dummy_data(&shard)?;

    let search_filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        "color".try_into().unwrap(),
        Match::new_value(ValueVariants::String("red".to_string())),
    )));

    // Basic RRF fusion (equal weights)
    println!("=== Basic RRF Fusion ===");
    let result = shard.query(ShardQueryRequest {
        prefetches: vec![
            ShardPrefetch {
                prefetches: vec![],
                query: Some(ScoringQuery::Vector(nearest([6.0, 9.0, 4.0, 2.0]))),
                limit: 5,
                params: None,
                filter: None,
                score_threshold: None,
            },
            ShardPrefetch {
                prefetches: vec![],
                query: Some(ScoringQuery::Vector(nearest([1.0, -3.0, 2.0, 8.0]))),
                limit: 5,
                params: None,
                filter: Some(search_filter.clone()),
                score_threshold: None,
            },
        ],
        query: Some(ScoringQuery::Fusion(FusionInternal::Rrf {
            k: 2,
            weights: None,
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

    // Weighted RRF fusion - first prefetch has 3x weight
    println!("\n=== Weighted RRF Fusion (3:1) ===");
    let result = shard.query(ShardQueryRequest {
        prefetches: vec![
            ShardPrefetch {
                prefetches: vec![],
                query: Some(ScoringQuery::Vector(nearest([6.0, 9.0, 4.0, 2.0]))),
                limit: 5,
                params: None,
                filter: None,
                score_threshold: None,
            },
            ShardPrefetch {
                prefetches: vec![],
                query: Some(ScoringQuery::Vector(nearest([1.0, -3.0, 2.0, 8.0]))),
                limit: 5,
                params: None,
                filter: Some(search_filter),
                score_threshold: None,
            },
        ],
        query: Some(ScoringQuery::Fusion(FusionInternal::Rrf {
            k: 2,
            weights: Some(vec![OrderedFloat(3.0), OrderedFloat(1.0)]), // First prefetch has 3x weight
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

fn nearest(vec: [f32; 4]) -> QueryEnum {
    QueryEnum::Nearest(NamedQuery {
        query: VectorInternal::from(vec.to_vec()),
        using: Some(DEFAULT_VECTOR_NAME.to_string()),
    })
}
