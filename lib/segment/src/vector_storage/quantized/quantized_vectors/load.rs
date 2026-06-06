use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::fs::read_json;

use super::{QuantizedVectors, QuantizedVectorsConfig, QuantizedVectorsStorageType};
use crate::common::operation_error::OperationResult;
use crate::types::{
    BinaryQuantization, ProductQuantization, QuantizationConfig, ScalarQuantization,
    TurboQuantization,
};
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

impl QuantizedVectors {
    pub fn load(
        quantization_config: &QuantizationConfig,
        vector_storage: &VectorStorageEnum,
        path: &Path,
        stopped: &AtomicBool,
    ) -> OperationResult<Option<Self>> {
        let config_path = Self::get_config_path(path);
        if config_path.exists() {
            let config: QuantizedVectorsConfig = read_json(&config_path)?;
            return Ok(Some(Self::load_impl(config, vector_storage, path)?));
        }

        // If we don't have an appendable quantization feature, do not create a new one.
        if !common::flags::feature_flags().appendable_quantization {
            return Ok(None);
        }

        // Only quantization methods that support appendable storage can be auto-created
        // here with `Mutable` storage type. Other methods (Scalar, Product) would fail
        // inside `create_impl`/`create_multi_impl` because their `create_*` helpers reject
        // `Mutable`.
        if !quantization_config.supports_appendable() {
            return Ok(None);
        }

        // Auto-create only initializes an empty quantized container for
        // a fresh appendable segment — `count == 0` short-circuits the
        // pre-pass, so the thread budget here is irrelevant. Any
        // re-quantization with actual data goes through
        // `SegmentBuilder::build` under a permit-bounded `max_threads`.
        let max_threads = 1;
        let quantized_vectors = Self::create(
            vector_storage,
            quantization_config,
            QuantizedVectorsStorageType::Mutable,
            path,
            max_threads,
            stopped,
        )?;
        Ok(Some(quantized_vectors))
    }

    pub fn load_impl(
        config: QuantizedVectorsConfig,
        vector_storage: &VectorStorageEnum,
        path: &Path,
    ) -> OperationResult<Self> {
        let quantized_store = if let Some(multivector_config) =
            vector_storage.try_multi_vector_config()
        {
            match &config.quantization_config {
                QuantizationConfig::Scalar(ScalarQuantization { scalar }) => {
                    Self::load_scalar_multi(
                        vector_storage,
                        path,
                        &config,
                        scalar,
                        multivector_config,
                    )?
                }
                QuantizationConfig::Product(ProductQuantization { product }) => {
                    Self::load_pq_multi(vector_storage, path, &config, product, multivector_config)?
                }
                QuantizationConfig::Binary(BinaryQuantization { binary }) => {
                    Self::load_binary_multi(
                        vector_storage,
                        path,
                        &config,
                        binary,
                        multivector_config,
                    )?
                }
                QuantizationConfig::Turbo(TurboQuantization { turbo }) => Self::load_turbo_multi(
                    vector_storage,
                    path,
                    &config,
                    turbo,
                    multivector_config,
                )?,
            }
        } else {
            match &config.quantization_config {
                QuantizationConfig::Scalar(ScalarQuantization { scalar }) => {
                    Self::load_scalar(vector_storage, path, &config, scalar)?
                }
                QuantizationConfig::Product(ProductQuantization { product }) => {
                    Self::load_pq(vector_storage, path, &config, product)?
                }
                QuantizationConfig::Binary(BinaryQuantization { binary }) => {
                    Self::load_binary(vector_storage, path, &config, binary)?
                }
                QuantizationConfig::Turbo(TurboQuantization { turbo }) => {
                    Self::load_turbo(vector_storage, path, &config, turbo)?
                }
            }
        };

        let distance = vector_storage.distance();
        let datatype = vector_storage.datatype();
        Ok(QuantizedVectors {
            storage_impl: quantized_store,
            config,
            path: path.to_path_buf(),
            distance,
            datatype,
        })
    }
}
