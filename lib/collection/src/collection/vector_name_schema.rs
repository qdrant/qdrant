use std::num::NonZeroU64;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::vector_name_config::{
    DenseVectorConfig, SparseVectorConfig, VectorNameConfig,
};
use segment::types::VectorNameBuf;
use shard::operations::{
    CollectionUpdateOperations, CreateVectorName, DeleteVectorName, VectorNameOperations,
};

use crate::collection::Collection;
use crate::operations::types::{
    CollectionError, CollectionResult, SparseVectorParams, VectorParams,
};
use crate::shards::shard_trait::WaitUntil;

impl Collection {
    pub async fn create_named_vector(
        &self,
        vector_name: VectorNameBuf,
        config: VectorNameConfig,
        hw_acc: HwMeasurementAcc,
    ) -> CollectionResult<()> {
        self.update_collection_vector_config(|params| {
            add_vector_to_config(params, &vector_name, &config)
        })
        .await?;

        let operation = CollectionUpdateOperations::VectorNameOperation(
            VectorNameOperations::CreateVectorName(CreateVectorName {
                vector_name,
                config,
            }),
        );

        self.update_all_local(operation, WaitUntil::from(false), hw_acc, true)
            .await?;

        Ok(())
    }

    pub async fn delete_named_vector(&self, vector_name: VectorNameBuf) -> CollectionResult<()> {
        self.update_collection_vector_config(|params| {
            remove_vector_from_config(params, &vector_name);
            Ok(())
        })
        .await?;

        let operation = CollectionUpdateOperations::VectorNameOperation(
            VectorNameOperations::DeleteVectorName(DeleteVectorName { vector_name }),
        );

        self.update_all_local(
            operation,
            WaitUntil::from(true),
            HwMeasurementAcc::disposable(),
            true, // Delete even in dead shards
        )
        .await?;

        Ok(())
    }

    /// Apply a mutation to collection params and persist.
    async fn update_collection_vector_config(
        &self,
        mutate: impl FnOnce(&mut crate::config::CollectionParams) -> CollectionResult<()>,
    ) -> CollectionResult<()> {
        let mut config = self.collection_config.write().await;
        mutate(&mut config.params)?;
        config.save(&self.path)?;
        Ok(())
    }
}

fn dense_config_to_params(config: &DenseVectorConfig) -> VectorParams {
    let DenseVectorConfig {
        size,
        distance,
        multivector_config,
        datatype,
    } = config;

    VectorParams {
        size: NonZeroU64::new(*size as u64).unwrap_or(NonZeroU64::MIN),
        distance: *distance,
        hnsw_config: None,
        quantization_config: None,
        on_disk: None,
        datatype: datatype.map(storage_datatype_to_collection),
        multivector_config: *multivector_config,
    }
}

fn sparse_config_to_params(config: &SparseVectorConfig) -> SparseVectorParams {
    let SparseVectorConfig { modifier, datatype } = config;

    SparseVectorParams {
        index: datatype.map(|dt| crate::operations::types::SparseIndexParams {
            full_scan_threshold: None,
            on_disk: None,
            datatype: Some(storage_datatype_to_collection(dt)),
        }),
        modifier: *modifier,
    }
}

/// Insert a new named vector into the stored collection params.
///
/// Returns `Ok(true)` if the vector was newly added, `Ok(false)` if a vector with the
/// same name and an identical config already exists (idempotent no-op), or
/// `Err(BadInput)` if a vector with the same name exists with a conflicting config or
/// a conflicting kind (dense vs. sparse). This guards against silently overwriting the
/// stored schema with a different one while shards keep the old one — shard-level
/// `CreateVectorName` is idempotent and will not re-apply the new config.
fn add_vector_to_config(
    params: &mut crate::config::CollectionParams,
    vector_name: &VectorNameBuf,
    config: &VectorNameConfig,
) -> CollectionResult<()> {
    match config {
        VectorNameConfig::Dense(wrapper) => {
            if let Some(existing) = params.vectors.get_params(vector_name.as_str()) {
                if !dense_schema_matches(existing, &wrapper.dense) {
                    return Err(CollectionError::bad_input(format!(
                        "Vector `{vector_name}` already exists with a different configuration",
                    )));
                }
                return Ok(());
            }
            if params
                .sparse_vectors
                .as_ref()
                .is_some_and(|sv| sv.contains_key(vector_name))
            {
                return Err(CollectionError::bad_input(format!(
                    "Vector `{vector_name}` already exists as a sparse vector",
                )));
            }
            params
                .vectors
                .insert(vector_name.clone(), dense_config_to_params(&wrapper.dense));
            Ok(())
        }
        VectorNameConfig::Sparse(wrapper) => {
            if let Some(existing) = params
                .sparse_vectors
                .as_ref()
                .and_then(|sv| sv.get(vector_name))
            {
                if !sparse_schema_matches(existing, &wrapper.sparse) {
                    return Err(CollectionError::bad_input(format!(
                        "Sparse vector `{vector_name}` already exists with a different configuration",
                    )));
                }
                return Ok(());
            }
            if params.vectors.get_params(vector_name.as_str()).is_some() {
                return Err(CollectionError::bad_input(format!(
                    "Vector `{vector_name}` already exists as a dense vector",
                )));
            }
            params
                .sparse_vectors
                .get_or_insert_with(Default::default)
                .insert(
                    vector_name.clone(),
                    sparse_config_to_params(&wrapper.sparse),
                );
            Ok(())
        }
    }
}

/// Compare the schema-defining subset of `VectorParams` against a `DenseVectorConfig`.
///
/// `VectorParams` carries fields that can be tuned independently after creation
/// (`hnsw_config`, `quantization_config`, `on_disk`); those must be ignored here so
/// repeated `create_named_vector` calls don't spuriously conflict with a vector whose
/// HNSW/quantization/on_disk settings were updated since.
fn dense_schema_matches(existing: &VectorParams, config: &DenseVectorConfig) -> bool {
    let DenseVectorConfig {
        size,
        distance,
        multivector_config,
        datatype,
    } = config;

    existing.size.get() == *size as u64
        && existing.distance == *distance
        && existing.multivector_config == *multivector_config
        && existing.datatype == datatype.map(storage_datatype_to_collection)
}

/// Compare the schema-defining subset of `SparseVectorParams` against a
/// `SparseVectorConfig`. Index tuning fields (`full_scan_threshold`, `on_disk`) are
/// ignored for the same reason as in [`dense_schema_matches`].
fn sparse_schema_matches(existing: &SparseVectorParams, config: &SparseVectorConfig) -> bool {
    let SparseVectorConfig { modifier, datatype } = config;

    let existing_datatype = existing.index.as_ref().and_then(|index| index.datatype);
    existing.modifier == *modifier
        && existing_datatype == datatype.map(storage_datatype_to_collection)
}

fn remove_vector_from_config(
    params: &mut crate::config::CollectionParams,
    vector_name: &VectorNameBuf,
) {
    params.vectors.remove(vector_name);

    if let Some(sparse) = &mut params.sparse_vectors {
        sparse.remove(vector_name);
    }
}

fn storage_datatype_to_collection(
    dt: segment::types::VectorStorageDatatype,
) -> crate::operations::types::Datatype {
    match dt {
        segment::types::VectorStorageDatatype::Float32 => {
            crate::operations::types::Datatype::Float32
        }
        segment::types::VectorStorageDatatype::Float16 => {
            crate::operations::types::Datatype::Float16
        }
        segment::types::VectorStorageDatatype::Uint8 => crate::operations::types::Datatype::Uint8,
    }
}
