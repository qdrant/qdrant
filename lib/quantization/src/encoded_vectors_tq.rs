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
use turboquant::turbo_mse::MseQuantized;
use turboquant::turbo_prod::{ProdQuantized, TurboProd};

use crate::encoded_storage::{EncodedStorage, EncodedStorageBuilder};
use crate::encoded_vectors::{EncodedVectors, VectorParameters, validate_vector_parameters};
use crate::{DistanceType, EncodingError};

pub const DEFAULT_TURBO_QUANT_LEVELS: usize = 4;

const DEFAULT_SEED: u64 = 42;

/// Pack indices into a byte buffer using `bit_width` bits per index.
fn pack_indices(indices: &[u8], bit_width: u8) -> Vec<u8> {
    let total_bits = indices.len() * bit_width as usize;
    let num_bytes = total_bits.div_ceil(8);
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
        let mut val = u16::from(packed[byte_offset]) >> bit_shift;
        if bit_shift + bit_width as usize > 8 {
            val |= u16::from(packed[byte_offset + 1]) << (8 - bit_shift);
        }
        indices.push((val & mask) as u8);
    }
    indices
}

/// Number of bytes needed to store `dim` indices at `levels` bits each.
fn packed_indices_size(dim: usize, levels: usize) -> usize {
    (dim * levels).div_ceil(8)
}

/// Pack i8 signs (+1/-1) into bits: +1 → 1, -1 → 0.
fn pack_signs(signs: &[i8]) -> Vec<u8> {
    let num_bytes = signs.len().div_ceil(8);
    let mut packed = vec![0u8; num_bytes];
    for (i, &sign) in signs.iter().enumerate() {
        if sign > 0 {
            packed[i / 8] |= 1 << (i % 8);
        }
    }
    packed
}

/// Unpack bits into i8 signs: 1 → +1, 0 → -1.
fn unpack_signs(packed: &[u8], count: usize) -> Vec<i8> {
    let mut signs = Vec::with_capacity(count);
    for i in 0..count {
        let bit = (packed[i / 8] >> (i % 8)) & 1;
        signs.push(if bit == 1 { 1 } else { -1 });
    }
    signs
}

/// Number of bytes needed to store `count` sign bits.
fn packed_signs_size(count: usize) -> usize {
    count.div_ceil(8)
}

pub struct EncodedVectorsTQ<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    metadata_path: Option<PathBuf>,
    turbo_prod: TurboProd,
}

/// Encoded query type - stores the original query vector.
pub struct EncodedQueryTQ {
    query_f64: Vec<f64>,
    query_norm_sq: f64,
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
    /// * `levels` - bit width per coordinate for TurboQuant quantization (default: 4, range: 2-4)
    /// * `meta_path` - optional path to save metadata, if `None`, metadata will not be saved
    /// * `stopped` - Atomic bool that indicates if encoding should be stopped
    ///
    /// # Panics
    /// Panics if L1 distance type is used (not supported by TurboQuant).
    /// Panics if `levels` < 2 (TurboProd requires at least 2 bits).
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
        assert!(
            levels >= 2,
            "TurboProd requires at least 2 quantization levels"
        );

        let seed = DEFAULT_SEED;
        let bit_width = levels as u8;
        let mse_bit_width = bit_width - 1;
        let turbo_prod = TurboProd::new(vector_parameters.dim, bit_width, Some(seed));

        for vector in data {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let vector_f64: Vec<f64> = vector.as_ref().iter().map(|&x| f64::from(x)).collect();
            let quantized = turbo_prod.quantize(&vector_f64);

            // Layout: mse_norm (f64) + residual_norm (f64) + packed MSE indices + packed QJL signs
            let packed_indices = pack_indices(&quantized.mse_part.indices, mse_bit_width);
            let packed_signs = pack_signs(&quantized.qjl_signs);
            let mut encoded = Vec::with_capacity(
                2 * std::mem::size_of::<f64>() + packed_indices.len() + packed_signs.len(),
            );
            encoded.extend_from_slice(&quantized.mse_part.norm.to_ne_bytes());
            encoded.extend_from_slice(&quantized.residual_norm.to_ne_bytes());
            encoded.extend_from_slice(&packed_indices);
            encoded.extend_from_slice(&packed_signs);

            storage_builder.push_vector_data(&encoded).map_err(|e| {
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
            turbo_prod,
        })
    }

    pub fn load(encoded_vectors: TStorage, meta_path: &Path) -> std::io::Result<Self> {
        let contents = fs::read_to_string(meta_path)?;
        let metadata: Metadata = serde_json::from_str(&contents)?;
        let turbo_prod = TurboProd::new(
            metadata.vector_parameters.dim,
            metadata.levels as u8,
            Some(metadata.seed),
        );
        Ok(Self {
            encoded_vectors,
            metadata,
            metadata_path: Some(meta_path.to_path_buf()),
            turbo_prod,
        })
    }

    /// Get quantized vector size in bytes: two f64 values + bit-packed MSE indices + packed QJL signs.
    pub fn get_quantized_vector_size(vector_parameters: &VectorParameters, levels: usize) -> usize {
        2 * std::mem::size_of::<f64>()
            + packed_indices_size(vector_parameters.dim, levels - 1)
            + packed_signs_size(vector_parameters.dim)
    }

    /// Deserialize stored bytes into a ProdQuantized struct.
    fn deserialize_prod_quantized(&self, bytes: &[u8]) -> ProdQuantized {
        let mse_bit_width = (self.metadata.levels - 1) as u8;
        let dim = self.metadata.vector_parameters.dim;
        let mse_norm = f64::from_ne_bytes(bytes[..8].try_into().unwrap());
        let residual_norm = f64::from_ne_bytes(bytes[8..16].try_into().unwrap());
        let mse_packed_size = packed_indices_size(dim, self.metadata.levels - 1);
        let indices = unpack_indices(&bytes[16..16 + mse_packed_size], dim, mse_bit_width);
        let signs = unpack_signs(&bytes[16 + mse_packed_size..], dim);
        ProdQuantized {
            mse_part: MseQuantized {
                indices,
                bit_width: mse_bit_width,
                norm: mse_norm,
            },
            qjl_signs: signs,
            residual_norm,
        }
    }

    /// Dequantize a stored vector (MSE part only) from its byte representation.
    fn dequantize_bytes(&self, bytes: &[u8]) -> Vec<f64> {
        let prod = self.deserialize_prod_quantized(bytes);
        self.turbo_prod.turbo_mse.dequantize(&prod.mse_part)
    }

    fn score_point_simple(&self, query: &EncodedQueryTQ, vector_bytes: &[u8]) -> f32 {
        let prod_quantized = self.deserialize_prod_quantized(vector_bytes);
        let ip = self
            .turbo_prod
            .estimate_inner_product(&query.query_f64, &prod_quantized);

        let result = match self.metadata.vector_parameters.distance_type {
            DistanceType::Dot => ip,
            DistanceType::L1 => panic!("TurboQuant does not support L1 distance metric"),
            DistanceType::L2 => {
                let vec_norm_sq = prod_quantized.mse_part.norm * prod_quantized.mse_part.norm;
                query.query_norm_sq - 2.0 * ip + vec_norm_sq
            }
        } as f32;

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
        let query_f64: Vec<f64> = query.iter().map(|&x| f64::from(x)).collect();
        let query_norm_sq = query_f64.iter().map(|&x| x * x).sum();
        EncodedQueryTQ {
            query_f64,
            query_norm_sq,
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

        let v1_deq = self.dequantize_bytes(&v1_bytes);
        let v2_prod = self.deserialize_prod_quantized(&v2_bytes);
        let ip = self.turbo_prod.estimate_inner_product(&v1_deq, &v2_prod);

        let result = match self.metadata.vector_parameters.distance_type {
            DistanceType::Dot => ip,
            DistanceType::L1 => panic!("TurboQuant does not support L1 distance metric"),
            DistanceType::L2 => {
                let v1_norm_sq: f64 = v1_deq.iter().map(|&x| x * x).sum();
                let v2_norm_sq = v2_prod.mse_part.norm * v2_prod.mse_part.norm;
                v1_norm_sq - 2.0 * ip + v2_norm_sq
            }
        } as f32;

        if self.metadata.vector_parameters.invert {
            -result
        } else {
            result
        }
    }

    fn quantized_vector_size(&self) -> usize {
        2 * std::mem::size_of::<f64>()
            + packed_indices_size(
                self.metadata.vector_parameters.dim,
                self.metadata.levels - 1,
            )
            + packed_signs_size(self.metadata.vector_parameters.dim)
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
