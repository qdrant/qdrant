use std::alloc::Layout;
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use common::fs::atomic_save_json;
use common::mmap::MmapFlusher;
use common::typelevel::True;
use common::types::PointOffsetType;
use fs_err as fs;
use serde::{Deserialize, Serialize};

use crate::encoded_storage::{EncodedStorage, EncodedStorageBuilder};
use crate::encoded_vectors::{EncodedVectors, VectorParameters, validate_vector_parameters};
use crate::{DistanceType, EncodingError};

pub const DEFAULT_TURBO_QUANT_BITS: usize = 4;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum TqCorrection {
    NoCorrection,
    Qjl,
    #[default]
    Normalization,
    QjlNormalization,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum TqRotation {
    NoRotation,
    #[default]
    Hadamard,
    Random,
}

pub struct EncodedVectorsTQ<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    metadata_path: Option<PathBuf>,
}

/// Encoded query type.
/// Just original vector as example.
pub struct EncodedQueryTQ {
    query: Vec<f32>,
}

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    pub vector_parameters: VectorParameters,
    pub bits: usize,
    pub correction: TqCorrection,
    pub rotation: TqRotation,
    pub hadamard_chunk: Option<usize>,
}

impl<TStorage: EncodedStorage> EncodedVectorsTQ<TStorage> {
    pub fn storage(&self) -> &TStorage {
        &self.encoded_vectors
    }

    /// Encode vector data
    ///
    /// # Arguments
    /// * `data` - iterator over original vector data
    /// * `storage_builder` - encoding result storage builder
    /// * `vector_parameters` - parameters of original vector data (dimension, distance, etc)
    /// * `count` - number of vectors in `data` iterator, used for progress bar
    /// * `bits` - number of bits for quantization (default: 4, range: 1-6)
    /// * `correction` - correction method
    /// * `rotation` - rotation method
    /// * `hadamard_chunk` - number of Hadamard rotations (only when rotation is Hadamard)
    /// * `meta_path` - optional path to save metadata, if `None`, metadata will not be saved
    /// * `stopped` - Atomic bool that indicates if encoding should be stopped
    #[allow(clippy::too_many_arguments)]
    pub fn encode<'a>(
        data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
        mut storage_builder: impl EncodedStorageBuilder<Storage = TStorage>,
        vector_parameters: &VectorParameters,
        _count: usize,
        bits: usize,
        correction: TqCorrection,
        rotation: TqRotation,
        hadamard_chunk: Option<usize>,
        meta_path: Option<&Path>,
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        debug_assert!(validate_vector_parameters(data.clone(), vector_parameters).is_ok());

        for vector in data {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let encoded_vector: Vec<u8> = Self::encode_vector(vector.as_ref());

            storage_builder
                .push_vector_data(&encoded_vector)
                .map_err(|e| {
                    EncodingError::EncodingError(format!("Failed to push encoded vector: {e}",))
                })?;
        }

        let encoded_vectors = storage_builder
            .build()
            .map_err(|e| EncodingError::EncodingError(format!("Failed to build storage: {e}",)))?;

        let metadata = Metadata {
            vector_parameters: vector_parameters.clone(),
            bits,
            correction,
            rotation,
            hadamard_chunk,
        };
        if let Some(meta_path) = meta_path {
            meta_path
                .parent()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Path must have a parent directory",
                    )
                })
                .and_then(fs::create_dir_all)
                .map_err(|e| {
                    EncodingError::EncodingError(format!(
                        "Failed to create metadata directory: {e}",
                    ))
                })?;
            atomic_save_json(meta_path, &metadata).map_err(|e| {
                EncodingError::EncodingError(format!("Failed to save metadata: {e}",))
            })?;
        }

        Ok(Self {
            encoded_vectors,
            metadata,
            metadata_path: meta_path.map(PathBuf::from),
        })
    }

    pub fn load(encoded_vectors: TStorage, meta_path: &Path) -> std::io::Result<Self> {
        let contents = fs::read_to_string(meta_path)?;
        let metadata: Metadata = serde_json::from_str(&contents)?;
        let result = Self {
            encoded_vectors,
            metadata,
            metadata_path: Some(meta_path.to_path_buf()),
        };
        Ok(result)
    }

    // Get quantized vector size in bytes
    pub fn get_quantized_vector_size(vector_parameters: &VectorParameters, _bits: usize) -> usize {
        vector_parameters.dim * std::mem::size_of::<f32>()
    }

    /// Encode single vector from `&[f32]` into `&[u8]`.
    fn encode_vector(vector_data: &[f32]) -> Vec<u8> {
        // Just convert vector data into bytes using ne_bytes:
        vector_data.iter().flat_map(|&x| x.to_ne_bytes()).collect()
    }

    fn score_point_simple(&self, query: &EncodedQueryTQ, vector: &[u8]) -> f32 {
        let mut result = 0f32;

        for (q, v) in query
            .query
            .iter()
            .zip(vector.chunks_exact(std::mem::size_of::<f32>()))
        {
            let v = f32::from_ne_bytes(v.try_into().unwrap());

            match self.metadata.vector_parameters.distance_type {
                DistanceType::Dot => {
                    result += q * v;
                }
                DistanceType::L1 => {
                    result += (q - v).abs();
                }
                DistanceType::L2 => {
                    let diff = q - v;
                    result += diff * diff;
                }
            }
        }

        if self.metadata.vector_parameters.invert {
            -result
        } else {
            result
        }
    }

    pub fn get_quantized_vector(&self, i: PointOffsetType) -> Cow<'_, [u8]> {
        self.encoded_vectors.get_vector_data(i)
    }

    pub fn layout(&self) -> Layout {
        Layout::from_size_align(self.quantized_vector_size(), align_of::<f32>()).unwrap()
    }

    pub fn get_metadata(&self) -> &Metadata {
        &self.metadata
    }
}

impl<TStorage: EncodedStorage> EncodedVectors for EncodedVectorsTQ<TStorage> {
    type EncodedQuery = EncodedQueryTQ;

    fn is_on_disk(&self) -> bool {
        self.encoded_vectors.is_on_disk()
    }

    fn encode_query(&self, query: &[f32]) -> EncodedQueryTQ {
        EncodedQueryTQ {
            query: query.to_vec(),
        }
    }

    fn score_point(
        &self,
        query: &EncodedQueryTQ,
        i: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let centroids = self.encoded_vectors.get_vector_data(i);

        self.score_bytes(True, query, &centroids, hw_counter)
    }

    /// Score two points inside endoded data by their indexes
    fn score_internal(
        &self,
        i: PointOffsetType,
        j: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let mut result = 0f32;

        let v1 = self.encoded_vectors.get_vector_data(i);
        let v2 = self.encoded_vectors.get_vector_data(j);

        hw_counter.vector_io_read().incr_delta(v1.len() + v2.len());

        for (v1, v2) in v1
            .chunks_exact(std::mem::size_of::<f32>())
            .zip(v2.chunks_exact(std::mem::size_of::<f32>()))
        {
            let v1 = f32::from_ne_bytes(v1.try_into().unwrap());
            let v2 = f32::from_ne_bytes(v2.try_into().unwrap());

            match self.metadata.vector_parameters.distance_type {
                DistanceType::Dot => {
                    result += v1 * v2;
                }
                DistanceType::L1 => {
                    result += (v1 - v2).abs();
                }
                DistanceType::L2 => {
                    let diff = v1 - v2;
                    result += diff * diff;
                }
            }
        }

        if self.metadata.vector_parameters.invert {
            -result
        } else {
            result
        }
    }

    fn quantized_vector_size(&self) -> usize {
        self.metadata.vector_parameters.dim * std::mem::size_of::<f32>()
    }

    fn encode_internal_vector(&self, _id: PointOffsetType) -> Option<EncodedQueryTQ> {
        // Keep here just `None`, for prototype it's OK
        None
    }

    fn upsert_vector(
        &mut self,
        _id: PointOffsetType,
        _vector: &[f32],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        debug_assert!(false, "TurboQuant does not support upsert_vector",);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "TurboQuant does not support upsert_vector",
        ))
    }

    fn vectors_count(&self) -> usize {
        // `vector_division` size is equal to quantized vector size because each chunk is replaced by one `u8` centroid index.
        self.encoded_vectors.vectors_count()
    }

    fn flusher(&self) -> MmapFlusher {
        self.encoded_vectors.flusher()
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.encoded_vectors.files();
        if let Some(meta_path) = &self.metadata_path {
            files.push(meta_path.clone());
        }
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = self.encoded_vectors.immutable_files();
        if let Some(meta_path) = &self.metadata_path {
            files.push(meta_path.clone());
        }
        files
    }

    type SupportsBytes = True;
    fn score_bytes(
        &self,
        _: Self::SupportsBytes,
        query: &Self::EncodedQuery,
        bytes: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        hw_counter.cpu_counter().incr_delta(bytes.len());

        self.score_point_simple(query, bytes)
    }
}
