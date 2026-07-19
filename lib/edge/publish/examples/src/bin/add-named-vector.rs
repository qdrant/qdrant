// See lib/edge/python/examples/add-named-vector.py for the equivalent Python example.
//
// Example: Adding a named vector to an existing Edge shard.
//
// Demonstrates the workflow for migrating to a new embedding model
// or adding hybrid search by creating a new named vector after the
// shard is already populated with data.

use std::error::Error;
use std::path::Path;

use examples::TMP_DIR;
use qdrant_edge::external::serde_json::json;
use qdrant_edge::{
    CreateVectorName, DeleteVectorName, DenseVectorConfig, Distance, EdgeConfig, EdgeShard,
    EdgeVectorParams, Modifier, NamedQuery, PointInsertOperations, PointOperations, PointStruct,
    QueryEnum, QueryRequestBuilder, ScoringQuery, SparseVector, SparseVectorConfig,
    UpdateOperation, Vector, VectorInternal, VectorNameConfig, VectorNameOperations, Vectors,
    WithPayloadInterface,
};

fn main() -> Result<(), Box<dyn Error>> {
    let path = Path::new(TMP_DIR).join("qdrant_edge_add_vector");
    if path.exists() {
        fs_err::remove_dir_all(&path)?;
    }
    fs_err::create_dir_all(&path)?;

    // ------------------------------------------------------------------
    // 1. Create shard with a single dense vector and populate it
    // ------------------------------------------------------------------
    println!("---- Create shard with initial vector ----");

    let config = EdgeConfig::builder()
        .vector("", EdgeVectorParams::builder(4, Distance::Cosine).build())
        .build();
    let shard = EdgeShard::new(&path, config)?;

    shard.update(UpdateOperation::PointOperation(
        PointOperations::UpsertPoints(PointInsertOperations::PointsList(vec![
            PointStruct::new(
                1u64,
                vec![0.1, 0.2, 0.3, 0.4],
                json!({ "text": "first document" }),
            )
            .into(),
            PointStruct::new(
                2u64,
                vec![0.5, 0.6, 0.7, 0.8],
                json!({ "text": "second document" }),
            )
            .into(),
            PointStruct::new(
                3u64,
                vec![0.9, 0.1, 0.2, 0.3],
                json!({ "text": "third document" }),
            )
            .into(),
        ])),
    ))?;

    println!(
        "Points after initial insert: {}",
        shard.info()?.points_count
    );

    // Verify search works on the default vector
    let results = shard.query(
        QueryRequestBuilder::new(3)
            .query(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery {
                query: vec![0.1, 0.2, 0.3, 0.4].into(),
                using: None,
            })))
            .with_payload(WithPayloadInterface::Bool(true))
            .build(),
    )?;
    println!("Search on default vector: {} results", results.len());
    assert_eq!(results.len(), 3);

    // ------------------------------------------------------------------
    // 2. Add a new dense vector (e.g., a new embedding model)
    // ------------------------------------------------------------------
    println!("\n---- Add new dense vector 'v2' ----");

    shard.update(UpdateOperation::VectorNameOperation(
        VectorNameOperations::CreateVectorName(CreateVectorName {
            vector_name: "v2".to_string(),
            config: VectorNameConfig::dense(DenseVectorConfig {
                size: 8,
                distance: Distance::Dot,
                multivector_config: None,
                datatype: None,
            }),
        }),
    ))?;

    // Existing points don't have 'v2' yet - insert new points with both vectors
    shard.update(UpdateOperation::PointOperation(
        PointOperations::UpsertPoints(PointInsertOperations::PointsList(vec![
            PointStruct::new(
                4u64,
                Vectors::new_named([
                    ("", Vector::new_dense(vec![0.4, 0.3, 0.2, 0.1])),
                    (
                        "v2",
                        Vector::new_dense(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]),
                    ),
                ]),
                json!({ "text": "fourth document" }),
            )
            .into(),
            PointStruct::new(
                5u64,
                Vectors::new_named([
                    ("", Vector::new_dense(vec![0.8, 0.7, 0.6, 0.5])),
                    (
                        "v2",
                        Vector::new_dense(vec![8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]),
                    ),
                ]),
                json!({ "text": "fifth document" }),
            )
            .into(),
        ])),
    ))?;

    println!("Points after adding v2: {}", shard.info()?.points_count);

    // Search on the new vector - only points 4 and 5 have 'v2'
    let results = shard.query(
        QueryRequestBuilder::new(5)
            .query(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery {
                query: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0].into(),
                using: Some("v2".to_string()),
            })))
            .with_payload(WithPayloadInterface::Bool(true))
            .build(),
    )?;
    println!("Search on 'v2': {} results", results.len());
    assert!(!results.is_empty(), "Should find at least point 4 or 5");

    // ------------------------------------------------------------------
    // 3. Add a sparse vector (e.g., for hybrid search with BM25/SPLADE)
    // ------------------------------------------------------------------
    println!("\n---- Add sparse vector 'keywords' ----");

    shard.update(UpdateOperation::VectorNameOperation(
        VectorNameOperations::CreateVectorName(CreateVectorName {
            vector_name: "keywords".to_string(),
            config: VectorNameConfig::sparse(SparseVectorConfig {
                modifier: Some(Modifier::Idf),
                datatype: None,
            }),
        }),
    ))?;

    // Insert a point with sparse keyword data
    shard.update(UpdateOperation::PointOperation(
        PointOperations::UpsertPoints(PointInsertOperations::PointsList(vec![
            PointStruct::new(
                6u64,
                Vectors::new_named([
                    ("", Vector::new_dense(vec![0.3, 0.3, 0.3, 0.3])),
                    (
                        "keywords",
                        Vector::new_sparse(vec![10, 25, 42], vec![0.8, 0.5, 0.3])?,
                    ),
                ]),
                json!({ "text": "sixth document with keywords" }),
            )
            .into(),
        ])),
    ))?;

    println!(
        "Points after adding keywords: {}",
        shard.info()?.points_count
    );

    // Search on sparse vector
    let results = shard.query(
        QueryRequestBuilder::new(5)
            .query(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery {
                query: VectorInternal::Sparse(SparseVector::new(vec![10, 25], vec![1.0, 0.5])?),
                using: Some("keywords".to_string()),
            })))
            .with_payload(WithPayloadInterface::Bool(true))
            .build(),
    )?;
    println!("Search on 'keywords': {} results", results.len());
    assert!(!results.is_empty());

    // ------------------------------------------------------------------
    // 4. Delete a named vector
    // ------------------------------------------------------------------
    println!("\n---- Delete vector 'v2' ----");

    shard.update(UpdateOperation::VectorNameOperation(
        VectorNameOperations::DeleteVectorName(DeleteVectorName {
            vector_name: "v2".to_string(),
        }),
    ))?;

    println!("Points after deleting v2: {}", shard.info()?.points_count);

    // Search on the default vector still works
    let results = shard.query(
        QueryRequestBuilder::new(5)
            .query(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery {
                query: vec![0.1, 0.2, 0.3, 0.4].into(),
                using: None,
            })))
            .with_payload(WithPayloadInterface::Bool(true))
            .build(),
    )?;
    println!(
        "Search on default vector after deleting v2: {} results",
        results.len()
    );
    assert!(results.len() >= 3);

    // ------------------------------------------------------------------
    // 5. Verify persistence - close and reopen
    // ------------------------------------------------------------------
    println!("\n---- Close and reopen ----");

    drop(shard);
    let shard = EdgeShard::load(&path, None)?;

    let info = shard.info()?;
    println!("Reopened shard: {} points", info.points_count);

    let results = shard.query(
        QueryRequestBuilder::new(5)
            .query(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery {
                query: VectorInternal::Sparse(SparseVector::new(vec![10, 25], vec![1.0, 0.5])?),
                using: Some("keywords".to_string()),
            })))
            .with_payload(WithPayloadInterface::Bool(true))
            .build(),
    )?;
    println!(
        "Search on 'keywords' after reopen: {} results",
        results.len()
    );
    assert!(!results.is_empty());

    println!("\nDone!");

    Ok(())
}
