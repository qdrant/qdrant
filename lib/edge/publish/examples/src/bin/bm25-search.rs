// See lib/edge/python/examples/bm25-search.py for the equivalent Python example.
//
// Native BM25 sparse search: embed text with `EdgeBm25`, then upsert and query
// the produced sparse vectors through the regular update/query path.

use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

use examples::TMP_DIR;
use qdrant_edge::bm25_embed::{EdgeBm25, EdgeBm25Config};
use qdrant_edge::external::serde_json::json;
use qdrant_edge::{
    EdgeConfig, EdgeShard, EdgeSparseVectorParams, Modifier, NamedQuery, PointInsertOperations,
    PointOperations, PointStruct, QueryEnum, QueryRequest, ScoringQuery, UpdateOperation,
    VectorInternal, Vectors, WithPayloadInterface, WithVector,
};

const SPARSE_VECTOR_NAME: &str = "text";

fn main() -> Result<(), Box<dyn Error>> {
    let path = Path::new(TMP_DIR).join("qdrant_edge_bm25");
    if path.exists() {
        fs_err::remove_dir_all(&path)?;
    }
    fs_err::create_dir_all(&path)?;

    let config = EdgeConfig {
        sparse_vectors: HashMap::from([(
            SPARSE_VECTOR_NAME.to_string(),
            EdgeSparseVectorParams {
                modifier: Some(Modifier::Idf),
                ..Default::default()
            },
        )]),
        ..Default::default()
    };
    let shard = EdgeShard::new(&path, config)?;

    let bm25 = EdgeBm25::new(EdgeBm25Config {
        language: Some("english".to_string()),
        ..Default::default()
    })?;

    let docs = [
        (1u64, "the quick brown fox", "1"),
        (2, "a lazy dog sleeps", "2"),
        (3, "foxes are clever", "3"),
    ];

    let points = docs
        .iter()
        .map(|(id, text, doc)| {
            PointStruct::new(
                *id,
                Vectors::new_named([(SPARSE_VECTOR_NAME, bm25.embed_document(text))]),
                json!({ "doc": doc }),
            )
            .into()
        })
        .collect();

    shard.update(UpdateOperation::PointOperation(
        PointOperations::UpsertPoints(PointInsertOperations::PointsList(points)),
    ))?;
    shard.optimize()?;
    println!("Info: {:?}", shard.info());

    let query = bm25.embed_query("clever fox");
    let results = shard.query(QueryRequest {
        prefetches: vec![],
        query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery {
            query: VectorInternal::from(query),
            using: Some(SPARSE_VECTOR_NAME.to_string()),
        }))),
        filter: None,
        score_threshold: None,
        limit: 3,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(false),
        with_payload: WithPayloadInterface::Bool(true),
    })?;

    println!("Got {} results", results.len());
    for r in &results {
        println!(
            "  id={:?} score={:.4} payload={:?}",
            r.id, r.score, r.payload
        );
    }

    Ok(())
}
