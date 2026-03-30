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
use turboquant::turbo_mse::{MseQuantized, TurboMse};

use crate::encoded_storage::{EncodedStorage, EncodedStorageBuilder};
use crate::encoded_vectors::{EncodedVectors, VectorParameters, validate_vector_parameters};
use crate::{DistanceType, EncodingError};

pub const DEFAULT_TURBO_QUANT_LEVELS: usize = 4;

const DEFAULT_SEED: u64 = 42;

/// Pack indices into a byte buffer using `bit_width` bits per index.
fn pack_indices(indices: &[u8], bit_width: u8) -> Vec<u8> {
    let total_bits = indices.len() * bit_width as usize;
    let num_bytes = (total_bits + 7) / 8;
    let mut packed = vec![0u8; num_bytes];
    for (i, &idx) in indices.iter().enumerate() {
        let bit_offset = i * bit_width as usize;
        let byte_offset = bit_offset / 8;
        let bit_shift = bit_offset % 8;
        packed[byte_offset] |= idx << bit_shift;
        if bit_shift + bit_width as usize > 8 {
            packed[byte_offset + 1] |= idx >> (8 - bit_shift);
        }
    }
    packed
}

/// Unpack indices from a byte buffer, reading `bit_width` bits per index.
fn unpack_indices(packed: &[u8], dim: usize, bit_width: u8) -> Vec<u8> {
    let mask = (1u16 << bit_width) - 1;
    let mut indices = Vec::with_capacity(dim);
    for i in 0..dim {
        let bit_offset = i * bit_width as usize;
        let byte_offset = bit_offset / 8;
        let bit_shift = bit_offset % 8;
        let mut val = packed[byte_offset] as u16 >> bit_shift;
        if bit_shift + bit_width as usize > 8 {
            val |= (packed[byte_offset + 1] as u16) << (8 - bit_shift);
        }
        indices.push((val & mask) as u8);
    }
    indices
}

/// Number of bytes needed to store `dim` indices at `levels` bits each.
fn packed_indices_size(dim: usize, levels: usize) -> usize {
    (dim * levels + 7) / 8
}

pub struct EncodedVectorsTQ<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    metadata_path: Option<PathBuf>,
    turbo_mse: TurboMse,
}

/// Encoded query type - stores the original query vector.
pub struct EncodedQueryTQ {
    query: Vec<f32>,
}

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    pub vector_parameters: VectorParameters,
    pub levels: usize,
    pub seed: u64,
}

impl<TStorage: EncodedStorage> EncodedVectorsTQ<TStorage> {
    pub fn storage(&self) -> &TStorage {
        &self.encoded_vectors
    }

    /// Encode vector data using TurboQuant MSE quantization.
    ///
    /// # Arguments
    /// * `data` - iterator over original vector data
    /// * `storage_builder` - encoding result storage builder
    /// * `vector_parameters` - parameters of original vector data (dimension, distance, etc)
    /// * `count` - number of vectors in `data` iterator, used for progress bar
    /// * `levels` - bit width per coordinate for TurboQuant quantization (default: 4, range: 1-4)
    /// * `meta_path` - optional path to save metadata, if `None`, metadata will not be saved
    /// * `stopped` - Atomic bool that indicates if encoding should be stopped
    ///
    /// # Panics
    /// Panics if L1 distance type is used (not supported by TurboQuant).
    #[allow(clippy::too_many_arguments)]
    pub fn encode<'a>(
        data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
        mut storage_builder: impl EncodedStorageBuilder<Storage = TStorage>,
        vector_parameters: &VectorParameters,
        _count: usize,
        levels: usize,
        meta_path: Option<&Path>,
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        debug_assert!(validate_vector_parameters(data.clone(), vector_parameters).is_ok());

        if vector_parameters.distance_type == DistanceType::L1 {
            panic!("TurboQuant does not support L1 distance metric");
        }

        let seed = DEFAULT_SEED;
        let bit_width = levels as u8;
        let turbo_mse = TurboMse::new(vector_parameters.dim, bit_width, Some(seed));

        for vector in data {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let vector_f64: Vec<f64> = vector.as_ref().iter().map(|&x| f64::from(x)).collect();
            let quantized = turbo_mse.quantize(&vector_f64);

            // Layout: norm (8 bytes as f64) + bit-packed indices (levels bits per coordinate)
            let packed = pack_indices(&quantized.indices, bit_width);
            let mut encoded = Vec::with_capacity(std::mem::size_of::<f64>() + packed.len());
            encoded.extend_from_slice(&quantized.norm.to_ne_bytes());
            encoded.extend_from_slice(&packed);

            storage_builder
                .push_vector_data(&encoded)
                .map_err(|e| {
                    EncodingError::EncodingError(format!("Failed to push encoded vector: {e}"))
                })?;
        }

        let encoded_vectors = storage_builder
            .build()
            .map_err(|e| EncodingError::EncodingError(format!("Failed to build storage: {e}")))?;

        let metadata = Metadata {
            vector_parameters: vector_parameters.clone(),
            levels,
            seed,
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
                EncodingError::EncodingError(format!("Failed to save metadata: {e}"))
            })?;
        }

        Ok(Self {
            encoded_vectors,
            metadata,
            metadata_path: meta_path.map(PathBuf::from),
            turbo_mse,
        })
    }

    pub fn load(encoded_vectors: TStorage, meta_path: &Path) -> std::io::Result<Self> {
        let contents = fs::read_to_string(meta_path)?;
        let metadata: Metadata = serde_json::from_str(&contents)?;
        let turbo_mse = TurboMse::new(
            metadata.vector_parameters.dim,
            metadata.levels as u8,
            Some(metadata.seed),
        );
        Ok(Self {
            encoded_vectors,
            metadata,
            metadata_path: Some(meta_path.to_path_buf()),
            turbo_mse,
        })
    }

    /// Get quantized vector size in bytes: f64 norm + bit-packed indices.
    pub fn get_quantized_vector_size(
        vector_parameters: &VectorParameters,
        levels: usize,
    ) -> usize {
        std::mem::size_of::<f64>() + packed_indices_size(vector_parameters.dim, levels)
    }

    /// Dequantize a stored vector from its byte representation.
    fn dequantize_bytes(&self, bytes: &[u8]) -> Vec<f64> {
        let bit_width = self.metadata.levels as u8;
        let dim = self.metadata.vector_parameters.dim;
        let norm = f64::from_ne_bytes(bytes[..8].try_into().unwrap());
        let indices = unpack_indices(&bytes[8..], dim, bit_width);
        let quantized = MseQuantized {
            indices,
            bit_width,
            norm,
        };
        self.turbo_mse.dequantize(&quantized)
    }

    fn score_point_simple(&self, query: &EncodedQueryTQ, vector_bytes: &[u8]) -> f32 {
        let dequantized = self.dequantize_bytes(vector_bytes);

        let mut result = 0f64;
        for (&q, &v) in query.query.iter().zip(dequantized.iter()) {
            let q = f64::from(q);
            match self.metadata.vector_parameters.distance_type {
                DistanceType::Dot => result += q * v,
                DistanceType::L1 => panic!("TurboQuant does not support L1 distance metric"),
                DistanceType::L2 => {
                    let diff = q - v;
                    result += diff * diff;
                }
            }
        }

        let result = result as f32;
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
        Layout::from_size_align(self.quantized_vector_size(), align_of::<f64>()).unwrap()
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
        let vector_data = self.encoded_vectors.get_vector_data(i);
        self.score_bytes(True, query, &vector_data, hw_counter)
    }

    fn score_internal(
        &self,
        i: PointOffsetType,
        j: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let v1_bytes = self.encoded_vectors.get_vector_data(i);
        let v2_bytes = self.encoded_vectors.get_vector_data(j);

        hw_counter
            .vector_io_read()
            .incr_delta(v1_bytes.len() + v2_bytes.len());

        let v1 = self.dequantize_bytes(&v1_bytes);
        let v2 = self.dequantize_bytes(&v2_bytes);

        let mut result = 0f64;
        for (&a, &b) in v1.iter().zip(v2.iter()) {
            match self.metadata.vector_parameters.distance_type {
                DistanceType::Dot => result += a * b,
                DistanceType::L1 => panic!("TurboQuant does not support L1 distance metric"),
                DistanceType::L2 => {
                    let diff = a - b;
                    result += diff * diff;
                }
            }
        }

        let result = result as f32;
        if self.metadata.vector_parameters.invert {
            -result
        } else {
            result
        }
    }

    fn quantized_vector_size(&self) -> usize {
        std::mem::size_of::<f64>()
            + packed_indices_size(self.metadata.vector_parameters.dim, self.metadata.levels)
    }

    fn encode_internal_vector(&self, _id: PointOffsetType) -> Option<EncodedQueryTQ> {
        None
    }

    fn upsert_vector(
        &mut self,
        _id: PointOffsetType,
        _vector: &[f32],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        debug_assert!(false, "TurboQuant does not support upsert_vector");
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "TurboQuant does not support upsert_vector",
        ))
    }

    fn vectors_count(&self) -> usize {
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
