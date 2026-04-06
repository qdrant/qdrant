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
use crate::operations::types::{CollectionResult, SparseVectorParams, VectorParams};
use crate::shards::shard_trait::WaitUntil;

impl Collection {
    pub async fn create_named_vector(
        &self,
        vector_name: VectorNameBuf,
        config: VectorNameConfig,
        hw_acc: HwMeasurementAcc,
    ) -> CollectionResult<()> {
        self.update_collection_vector_config(|params| {
            add_vector_to_config(params, &vector_name, &config);
        })
        .await?;

        let operation = CollectionUpdateOperations::VectorNameOperation(
            VectorNameOperations::CreateVectorName(CreateVectorName {
                vector_name,
                config,
            }),
        );

        self.update_all_local(operation, WaitUntil::from(false), hw_acc)
            .await?;

        Ok(())
    }

    pub async fn delete_named_vector(&self, vector_name: VectorNameBuf) -> CollectionResult<()> {
        self.update_collection_vector_config(|params| {
            remove_vector_from_config(params, &vector_name);
        })
        .await?;

        let operation = CollectionUpdateOperations::VectorNameOperation(
            VectorNameOperations::DeleteVectorName(DeleteVectorName { vector_name }),
        );

        self.update_all_local(
            operation,
            WaitUntil::from(false),
            HwMeasurementAcc::disposable(),
        )
        .await?;

        Ok(())
    }

    /// Apply a mutation to collection params and persist.
    async fn update_collection_vector_config(
        &self,
        mutate: impl FnOnce(&mut crate::config::CollectionParams),
    ) -> CollectionResult<()> {
        let mut config = self.collection_config.write().await;
        mutate(&mut config.params);
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

fn add_vector_to_config(
    params: &mut crate::config::CollectionParams,
    vector_name: &VectorNameBuf,
    config: &VectorNameConfig,
) {
    match config {
        VectorNameConfig::Dense(wrapper) => {
            let vector_params = dense_config_to_params(&wrapper.dense);
            params.vectors.insert(vector_name.clone(), vector_params);
        }
        VectorNameConfig::Sparse(wrapper) => {
            let sparse_params = sparse_config_to_params(&wrapper.sparse);
            params
                .sparse_vectors
                .get_or_insert_with(Default::default)
                .insert(vector_name.clone(), sparse_params);
        }
    }
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
