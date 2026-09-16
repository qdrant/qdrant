//! Managing a collection's HNSW **training vectors**.
//!
//! Training vectors are a build input for the query-aware projection edges
//! (`hnsw_config.projection`): a set of vectors that look like the queries the index will
//! really be asked, used at build time to decide which stored points should be linked. They
//! are not points, they are not searchable and they are never returned by a query.
//!
//! They are stored per collection and per vector name, under
//! `<storage>/collections/<collection>/hnsw_training_vectors/`; see
//! [`segment::index::hnsw_index::training_vectors`] for the exact file layout. Every segment
//! HNSW build of that collection on this node reads them, so an optimizer-triggered rebuild
//! keeps the feature.
//!
//! **Node-local.** The files live next to the collection's own data and are not replicated
//! through consensus, so in a distributed deployment the upload must be repeated against every
//! node that holds a shard of the collection.

use collection::operations::types::CollectionError;
use schemars::JsonSchema;
use segment::common::operation_error::OperationError;
use segment::index::hnsw_index::training_vectors::{
    HnswTrainingVectors, HnswTrainingVectorsHeader, MAX_TRAINING_VECTOR_DIM, training_vectors_dir,
};
use segment::types::VectorName;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::content_manager::errors::StorageError;
use crate::content_manager::toc::TableOfContent;
use crate::rbac::CollectionPass;

/// Upload training vectors for the query-aware HNSW projection edges of one vector name.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct SetHnswTrainingVectors {
    /// Name of the vector these training vectors belong to. Omit for the unnamed (default)
    /// vector of the collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_name: Option<String>,
    /// Training vectors, one row per vector. Every row must have the collection's configured
    /// size for this vector name.
    pub vectors: Vec<Vec<f32>>,
    /// Append to the training vectors already uploaded (default), or replace them.
    #[serde(default = "default_append")]
    pub append: bool,
}

const fn default_append() -> bool {
    true
}

/// Which vector name to act on, for the endpoints that take no body.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct HnswTrainingVectorsSelector {
    /// Name of the vector. Omit for the unnamed (default) vector of the collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_name: Option<String>,
}

fn storage_err(err: OperationError) -> StorageError {
    StorageError::from(CollectionError::from(err))
}

impl TableOfContent {
    /// Append (or replace) the training vectors of one vector name of a collection.
    ///
    /// `vectors` is a list of rows, all of the same length, which must match the collection's
    /// configured size for `vector_name`.
    pub async fn set_hnsw_training_vectors(
        &self,
        collection: &CollectionPass<'_>,
        vector_name: &VectorName,
        vectors: Vec<Vec<f32>>,
        append: bool,
    ) -> Result<HnswTrainingVectorsHeader, StorageError> {
        let expected_dim = self.collection_vector_dim(collection, vector_name).await?;

        if vectors.is_empty() && append {
            // Nothing to add; report the current state rather than writing an empty file.
            let dir = training_vectors_dir(&self.get_collection_path(collection.name()));
            return Ok(HnswTrainingVectors::info(&dir, vector_name)
                .map_err(storage_err)?
                .unwrap_or(HnswTrainingVectorsHeader {
                    vector_name: vector_name.to_string(),
                    dim: expected_dim,
                    num_vectors: 0,
                }));
        }

        let mut flat = Vec::with_capacity(vectors.len() * expected_dim);
        for (i, row) in vectors.iter().enumerate() {
            if row.len() != expected_dim {
                return Err(StorageError::bad_input(format!(
                    "HNSW training vector {i} has dimension {}, but vector `{vector_name}` of \
                     collection `{}` has dimension {expected_dim}",
                    row.len(),
                    collection.name(),
                )));
            }
            flat.extend_from_slice(row);
        }

        let dir = training_vectors_dir(&self.get_collection_path(collection.name()));
        let header = HnswTrainingVectors::store(&dir, vector_name, expected_dim, &flat, append)
            .map_err(storage_err)?;
        log::info!(
            "HNSW training vectors for collection `{}` vector `{vector_name}`: \
             {} rows of dim {} stored ({} added)",
            collection.name(),
            header.num_vectors,
            header.dim,
            vectors.len(),
        );
        Ok(header)
    }

    /// Drop the training vectors of one vector name. Returns true if anything was removed.
    pub async fn delete_hnsw_training_vectors(
        &self,
        collection: &CollectionPass<'_>,
        vector_name: &VectorName,
    ) -> Result<bool, StorageError> {
        // Resolve the collection so a typo does not silently succeed.
        self.get_collection(collection).await?;
        let dir = training_vectors_dir(&self.get_collection_path(collection.name()));
        HnswTrainingVectors::clear(&dir, vector_name).map_err(storage_err)
    }

    /// Header (row count and dimension) of the training vectors of one vector name.
    pub async fn get_hnsw_training_vectors_info(
        &self,
        collection: &CollectionPass<'_>,
        vector_name: &VectorName,
    ) -> Result<Option<HnswTrainingVectorsHeader>, StorageError> {
        self.get_collection(collection).await?;
        let dir = training_vectors_dir(&self.get_collection_path(collection.name()));
        HnswTrainingVectors::info(&dir, vector_name).map_err(storage_err)
    }

    /// Configured dimension of a dense vector of a collection.
    async fn collection_vector_dim(
        &self,
        collection: &CollectionPass<'_>,
        vector_name: &VectorName,
    ) -> Result<usize, StorageError> {
        let collection_obj = self.get_collection(collection).await?;
        let vectors_config = collection_obj.vectors_config().await;
        let params = vectors_config.get_params(vector_name).ok_or_else(|| {
            StorageError::bad_input(format!(
                "Collection `{}` has no dense vector named `{vector_name}`",
                collection.name(),
            ))
        })?;
        let dim = params.size.get() as usize;
        if dim == 0 || dim > MAX_TRAINING_VECTOR_DIM {
            return Err(StorageError::bad_input(format!(
                "Unsupported vector dimension {dim} for HNSW training vectors",
            )));
        }
        Ok(dim)
    }
}
