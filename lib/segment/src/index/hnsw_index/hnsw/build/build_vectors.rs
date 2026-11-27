//! MEASUREMENT REFERENCE, NOT FOR MERGE: build the graph from vectors other than the segment's
//! own, chosen at runtime, while the saved graph is searched with the segment's own vectors.
//!
//! `QDRANT_BUILD_VECTORS` selects the vectors every graph build of the process scores:
//! - unset, empty or `float32`: the segment's own vectors and quantization, as without this file.
//! - `float16`, `uint8`, `turbo4`, `turbo8`, `turbo16`: a temporary copy of the vectors in that
//!   storage datatype, the storage a collection of that datatype uses, without quantization.
//! - `sq8` (or `scalar`): temporary scalar int8 quantized vectors (full value range, in RAM).
//! - a quantization config as JSON, as in the collection API, such as
//!   `{"turbo":{"bits":"bits4"}}`: temporary quantized vectors of that config.
//!
//! Both the main graph (HNSW or PiPNN, see the `pipnn_graph_building` feature flag) and the
//! payload blocks are built from them. The copy or the quantized vectors are deleted when the
//! build ends: nothing of them is saved, and searches score the segment's own vectors.
//! Only dense `float32` storages are replaced; any other storage is built from as it is.
//!
//! `QDRANT_BUILD_SEED`: a `u64` that replaces the random seed of every graph build (point
//! levels, PiPNN partitions), so that builds of the same vectors can be paired across datatypes.

use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use common::generic_consts::Sequential;
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use tempfile::TempDir;

use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::vectors::VectorRef;
use crate::types::{
    Memory, QuantizationConfig, ScalarQuantization, ScalarQuantizationConfig, ScalarType,
    VectorStorageDatatype,
};
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::open_appendable_memmap_vector_storage;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use crate::vector_storage::{VectorStorage as _, VectorStorageEnum, VectorStorageRead as _};

const BUILD_VECTORS_ENV: &str = "QDRANT_BUILD_VECTORS";
const BUILD_SEED_ENV: &str = "QDRANT_BUILD_SEED";

/// Vectors a graph build scores in place of the segment's own. Each variant keeps its temporary
/// directory last, so that it is deleted after the vectors in it are closed.
pub(super) enum BuildVectors {
    /// The vectors copied into another storage datatype, without quantization.
    Datatype {
        storage: VectorStorageEnum,
        _dir: TempDir,
    },
    /// The segment's own vectors with temporary quantized vectors.
    Quantized {
        quantized: Box<QuantizedVectors>,
        _dir: TempDir,
    },
}

/// What `QDRANT_BUILD_VECTORS` asks for in place of the segment's own vectors.
enum BuildVectorsSpec {
    Datatype(VectorStorageDatatype),
    Quantization(Box<QuantizationConfig>),
}

impl BuildVectors {
    /// The vectors `QDRANT_BUILD_VECTORS` selects for building the graph of `storage`, prepared
    /// in a temporary directory next to `index_path`. `None` to build from the segment's own.
    pub fn from_env(
        index_path: &Path,
        storage: &VectorStorageEnum,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Option<Self>> {
        let Some((value, spec)) = spec_from_env()? else {
            return Ok(None);
        };
        let total = storage.total_vector_count();
        let datatype = storage.datatype();
        let dim = match (total, datatype) {
            (0, _) => None,
            (_, VectorStorageDatatype::Float32) => {
                match storage.get_vector::<Sequential>(0).as_vec_ref() {
                    VectorRef::Dense(vector) => Some(vector.len()),
                    VectorRef::Sparse(_) | VectorRef::MultiDense(_) => None,
                }
            }
            (_, _) => None,
        };
        let Some(dim) = dim else {
            log::warn!(
                "{BUILD_VECTORS_ENV}={value}: not a dense float32 storage with vectors \
                 ({datatype:?}, {total} vectors), building from its own vectors",
            );
            return Ok(None);
        };

        let timer = Instant::now();
        let parent = index_path.parent().unwrap_or(index_path);
        let dir = TempDir::with_prefix_in("build_vectors_", parent)?;
        let build_vectors = match spec {
            BuildVectorsSpec::Datatype(datatype) => {
                let storage = copy_storage(storage, datatype, dim, dir.path(), stopped)?;
                Self::Datatype { storage, _dir: dir }
            }
            BuildVectorsSpec::Quantization(config) => {
                let quantized = QuantizedVectors::create(
                    storage,
                    &config,
                    QuantizedVectorsStorageType::Immutable,
                    dir.path(),
                    max_threads,
                    stopped,
                )?;
                Self::Quantized {
                    quantized: Box::new(quantized),
                    _dir: dir,
                }
            }
        };
        log::info!(
            "{BUILD_VECTORS_ENV}={value}: building the graph of {total} {dim}d vectors from {}, \
             prepared in {:.3} s",
            build_vectors.description(),
            timer.elapsed().as_secs_f64(),
        );
        Ok(Some(build_vectors))
    }

    /// The vector storage and quantized vectors a build scores: these in place of `storage`
    /// and `quantized`, or those if `build_vectors` is `None`. A copy replaces the quantized
    /// vectors too, so that its own vectors are scored.
    pub fn select<'a>(
        build_vectors: Option<&'a Self>,
        storage: &'a VectorStorageEnum,
        quantized: Option<&'a QuantizedVectors>,
    ) -> (&'a VectorStorageEnum, Option<&'a QuantizedVectors>) {
        match build_vectors {
            None => (storage, quantized),
            Some(Self::Datatype { storage, _dir: _ }) => (storage, None),
            Some(Self::Quantized { quantized, _dir: _ }) => (storage, Some(quantized.as_ref())),
        }
    }

    fn description(&self) -> String {
        match self {
            Self::Datatype { storage, _dir: _ } => {
                format!("a {:?} copy", storage.datatype())
            }
            Self::Quantized { quantized, _dir: _ } => {
                format!(
                    "quantized vectors {:?}",
                    quantized.config().quantization_config
                )
            }
        }
    }
}

/// The seed `QDRANT_BUILD_SEED` sets for every graph build, if any.
pub(super) fn seed_from_env() -> OperationResult<Option<u64>> {
    let Some(value) = env_value(BUILD_SEED_ENV)? else {
        return Ok(None);
    };
    let seed = value.parse::<u64>().map_err(|err| {
        OperationError::service_error(format!("{BUILD_SEED_ENV}={value}: not a u64: {err}"))
    })?;
    log::info!("{BUILD_SEED_ENV}={seed}: building the graph with this seed");
    Ok(Some(seed))
}

/// The value of `name`, `None` if unset or empty.
fn env_value(name: &str) -> OperationResult<Option<String>> {
    match std::env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value.trim().to_owned())),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err @ std::env::VarError::NotUnicode(_)) => {
            Err(OperationError::service_error(format!("{name}: {err}")))
        }
    }
}

/// The value of `QDRANT_BUILD_VECTORS` and what it asks for, `None` for the segment's own vectors.
fn spec_from_env() -> OperationResult<Option<(String, BuildVectorsSpec)>> {
    let Some(value) = env_value(BUILD_VECTORS_ENV)? else {
        return Ok(None);
    };
    let spec = match value.as_str() {
        "float32" => return Ok(None),
        "float16" => BuildVectorsSpec::Datatype(VectorStorageDatatype::Float16),
        "uint8" => BuildVectorsSpec::Datatype(VectorStorageDatatype::Uint8),
        "turbo4" => BuildVectorsSpec::Datatype(VectorStorageDatatype::Turbo4),
        "turbo8" => BuildVectorsSpec::Datatype(VectorStorageDatatype::Turbo8),
        "turbo16" => BuildVectorsSpec::Datatype(VectorStorageDatatype::Turbo16),
        "sq8" | "scalar" => BuildVectorsSpec::Quantization(Box::new(scalar_int8())),
        json if json.starts_with('{') => {
            let config = serde_json::from_str::<QuantizationConfig>(json).map_err(|err| {
                OperationError::service_error(format!(
                    "{BUILD_VECTORS_ENV}={json}: not a quantization config: {err}"
                ))
            })?;
            BuildVectorsSpec::Quantization(Box::new(config))
        }
        other => {
            return Err(OperationError::service_error(format!(
                "{BUILD_VECTORS_ENV}={other}: expected float32, float16, uint8, turbo4, turbo8, \
                 turbo16, sq8 or a quantization config as JSON"
            )));
        }
    };
    Ok(Some((value, spec)))
}

/// Scalar int8 quantization over the whole value range, in RAM.
fn scalar_int8() -> QuantizationConfig {
    #[expect(deprecated, reason = "`always_ram` must be set, `memory` replaces it")]
    let scalar = ScalarQuantizationConfig {
        r#type: ScalarType::Int8,
        quantile: None,
        always_ram: None,
        memory: Some(Memory::Pinned),
    };
    QuantizationConfig::Scalar(ScalarQuantization { scalar })
}

/// Copy the vectors of `source` into a new appendable storage of `datatype` in `dir`. Deleted
/// vectors are copied and deleted too, although a build reads its deleted flags elsewhere.
fn copy_storage(
    source: &VectorStorageEnum,
    datatype: VectorStorageDatatype,
    dim: usize,
    dir: &Path,
    stopped: &AtomicBool,
) -> OperationResult<VectorStorageEnum> {
    let mut copy = open_appendable_memmap_vector_storage(
        datatype,
        dir,
        dim,
        source.distance(),
        AdviceSetting::Global,
        true,
    )?;
    for key in 0..source.total_vector_count() as PointOffsetType {
        check_process_stopped(stopped)?;
        copy.insert_vector(key, source.get_vector::<Sequential>(key).as_vec_ref())?;
        if source.is_deleted_vector(key) {
            copy.delete_vector(key)?;
        }
    }
    Ok(copy)
}
