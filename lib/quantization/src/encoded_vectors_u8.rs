use std::alloc::Layout;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use io::file_operations::atomic_save_json;
use memory::mmap_type::MmapFlusher;
use serde::{Deserialize, Serialize};

use crate::EncodingError;
use crate::encoded_storage::{EncodedStorage, EncodedStorageBuilder};
use crate::encoded_vectors::{
    DistanceType, EncodedVectors, EncodedVectorsBytes, VectorParameters, validate_vector_parameters,
};
use crate::quantile::{find_min_max_from_iter, find_quantile_interval};

pub const ALIGNMENT: usize = 16;

pub struct EncodedVectorsU8<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
}

pub struct EncodedQueryU8 {
    offset: f32,
    encoded_query: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    actual_dim: usize,
    alpha: f32,
    offset: f32,
    multiplier: f32,
    vector_parameters: VectorParameters,
}

impl<TStorage: EncodedStorage> EncodedVectorsU8<TStorage> {
    pub fn storage(&self) -> &TStorage {
        &self.encoded_vectors
    }

    pub fn encode<'a>(
        orig_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
        mut storage_builder: impl EncodedStorageBuilder<Storage = TStorage>,
        vector_parameters: &VectorParameters,
        count: usize,
        quantile: Option<f32>,
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        let actual_dim = Self::get_actual_dim(vector_parameters);

        if count == 0 {
            return Ok(EncodedVectorsU8 {
                encoded_vectors: storage_builder.build().map_err(|e| {
                    EncodingError::EncodingError(format!("Failed to build storage: {e}",))
                })?,
                metadata: Metadata {
                    actual_dim,
                    alpha: 0.0,
                    offset: 0.0,
                    multiplier: 0.0,
                    vector_parameters: vector_parameters.clone(),
                },
            });
        }

        debug_assert!(validate_vector_parameters(orig_data.clone(), vector_parameters).is_ok());
        let (alpha, offset) = Self::find_alpha_offset_size_dim(orig_data.clone());
        let (alpha, offset) = if let Some(quantile) = quantile {
            if let Some((min, max)) =
                find_quantile_interval(orig_data.clone(), vector_parameters.dim, count, quantile)
            {
                Self::alpha_offset_from_min_max(min, max)
            } else {
                (alpha, offset)
            }
        } else {
            (alpha, offset)
        };

        for vector in orig_data {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let mut encoded_vector = Vec::with_capacity(actual_dim + std::mem::size_of::<f32>());
            encoded_vector.extend_from_slice(&f32::default().to_ne_bytes());
            for &value in vector.as_ref() {
                let encoded = Self::f32_to_u8(value, alpha, offset);
                encoded_vector.push(encoded);
            }
            if vector_parameters.dim % ALIGNMENT != 0 {
                for _ in 0..(ALIGNMENT - vector_parameters.dim % ALIGNMENT) {
                    let placeholder = match vector_parameters.distance_type {
                        DistanceType::Dot => 0.0,
                        DistanceType::L1 | DistanceType::L2 => offset,
                    };
                    let encoded = Self::f32_to_u8(placeholder, alpha, offset);
                    encoded_vector.push(encoded);
                }
            }
            let vector_offset = match vector_parameters.distance_type {
                DistanceType::Dot => {
                    actual_dim as f32 * offset * offset
                        + encoded_vector.iter().map(|&x| f32::from(x)).sum::<f32>() * alpha * offset
                }
                DistanceType::L1 => 0.0,
                DistanceType::L2 => {
                    actual_dim as f32 * offset * offset
                        + encoded_vector
                            .iter()
                            .map(|&x| f32::from(x) * f32::from(x))
                            .sum::<f32>()
                            * alpha
                            * alpha
                }
            };
            let vector_offset = if vector_parameters.invert {
                -vector_offset
            } else {
                vector_offset
            };
            encoded_vector[0..std::mem::size_of::<f32>()]
                .copy_from_slice(&vector_offset.to_ne_bytes());
            storage_builder.push_vector_data(&encoded_vector);
        }
        let multiplier = match vector_parameters.distance_type {
            DistanceType::Dot => alpha * alpha,
            DistanceType::L1 => alpha,
            DistanceType::L2 => -2.0 * alpha * alpha,
        };
        let multiplier = if vector_parameters.invert {
            -multiplier
        } else {
            multiplier
        };

        Ok(EncodedVectorsU8 {
            encoded_vectors: storage_builder.build().map_err(|e| {
                EncodingError::EncodingError(format!("Failed to build storage: {e}",))
            })?,
            metadata: Metadata {
                actual_dim,
                alpha,
                offset,
                multiplier,
                vector_parameters: vector_parameters.clone(),
            },
        })
    }

    pub fn score_point_simple(&self, query: &EncodedQueryU8, i: u32) -> f32 {
        let (vector_offset, v_ptr) = self.get_vec_ptr(i);

        let score = match self.metadata.vector_parameters.distance_type {
            DistanceType::Dot | DistanceType::L2 => impl_score_dot(
                query.encoded_query.as_ptr(),
                v_ptr,
                self.metadata.actual_dim,
            ),
            DistanceType::L1 => impl_score_l1(
                query.encoded_query.as_ptr(),
                v_ptr,
                self.metadata.actual_dim,
            ),
        };

        self.metadata.multiplier * score as f32 + query.offset + vector_offset
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    pub fn score_point_neon(&self, query: &EncodedQueryU8, i: u32) -> f32 {
        unsafe {
            let (vector_offset, v_ptr) = self.get_vec_ptr(i);
            let score = match self.metadata.vector_parameters.distance_type {
                DistanceType::Dot | DistanceType::L2 => impl_score_dot_neon(
                    query.encoded_query.as_ptr(),
                    v_ptr,
                    self.metadata.actual_dim as u32,
                ),
                DistanceType::L1 => impl_score_l1_neon(
                    query.encoded_query.as_ptr(),
                    v_ptr,
                    self.metadata.actual_dim as u32,
                ),
            };
            self.metadata.multiplier * score + query.offset + vector_offset
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    pub fn score_point_sse(&self, query: &EncodedQueryU8, i: u32) -> f32 {
        unsafe {
            let (vector_offset, v_ptr) = self.get_vec_ptr(i);
            let score = match self.metadata.vector_parameters.distance_type {
                DistanceType::Dot | DistanceType::L2 => impl_score_dot_sse(
                    query.encoded_query.as_ptr(),
                    v_ptr,
                    self.metadata.actual_dim as u32,
                ),
                DistanceType::L1 => impl_score_l1_sse(
                    query.encoded_query.as_ptr(),
                    v_ptr,
                    self.metadata.actual_dim as u32,
                ),
            };
            self.metadata.multiplier * score + query.offset + vector_offset
        }
    }

    #[cfg(target_arch = "x86_64")]
    pub fn score_point_avx(&self, query: &EncodedQueryU8, i: u32) -> f32 {
        unsafe {
            let (vector_offset, v_ptr) = self.get_vec_ptr(i);
            let score = match self.metadata.vector_parameters.distance_type {
                DistanceType::Dot | DistanceType::L2 => impl_score_dot_avx(
                    query.encoded_query.as_ptr(),
                    v_ptr,
                    self.metadata.actual_dim as u32,
                ),
                DistanceType::L1 => impl_score_l1_avx(
                    query.encoded_query.as_ptr(),
                    v_ptr,
                    self.metadata.actual_dim as u32,
                ),
            };
            self.metadata.multiplier * score + query.offset + vector_offset
        }
    }

    fn find_alpha_offset_size_dim<'a>(
        orig_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    ) -> (f32, f32) {
        let (min, max) = find_min_max_from_iter(orig_data);
        Self::alpha_offset_from_min_max(min, max)
    }

    fn alpha_offset_from_min_max(min: f32, max: f32) -> (f32, f32) {
        let alpha = (max - min) / 127.0;
        let offset = min;
        (alpha, offset)
    }

    fn f32_to_u8(i: f32, alpha: f32, offset: f32) -> u8 {
        let i = (i - offset) / alpha;
        i.clamp(0.0, 127.0) as u8
    }

    #[inline]
    fn parse_vec_data(data: &[u8]) -> (f32, *const u8) {
        debug_assert!(data.len() >= std::mem::size_of::<f32>());
        unsafe {
            let offset = data.as_ptr().cast::<f32>().read_unaligned();
            let v_ptr = data.as_ptr().add(std::mem::size_of::<f32>());
            (offset, v_ptr)
        }
    }

    #[inline]
    fn get_vec_ptr(&self, i: u32) -> (f32, *const u8) {
        let vector_data_size = self.quantized_vector_size();
        let data = self
            .encoded_vectors
            .get_vector_data(i as usize, vector_data_size);
        Self::parse_vec_data(data)
    }

    pub fn get_quantized_vector(&self, i: u32) -> &[u8] {
        self.encoded_vectors
            .get_vector_data(i as usize, self.quantized_vector_size())
    }

    pub fn layout(&self) -> Layout {
        Layout::from_size_align(self.quantized_vector_size(), align_of::<u8>()).unwrap()
    }

    pub fn get_quantized_vector_offset_and_code(&self, i: u32) -> (f32, &[u8]) {
        let (offset, v_ptr) = self.get_vec_ptr(i);
        let vector_data_size = self.metadata.actual_dim;
        let code = unsafe { std::slice::from_raw_parts(v_ptr, vector_data_size) };
        (offset, code)
    }

    pub fn get_quantized_vector_size(vector_parameters: &VectorParameters) -> usize {
        let actual_dim = Self::get_actual_dim(vector_parameters);
        actual_dim + std::mem::size_of::<f32>()
    }

    pub fn get_multiplier(&self) -> f32 {
        self.metadata.multiplier
    }

    pub fn get_diff(&self) -> f32 {
        let diff = self.metadata.actual_dim as f32 * self.metadata.offset * self.metadata.offset;
        if self.metadata.vector_parameters.invert {
            -diff
        } else {
            diff
        }
    }

    pub fn get_actual_dim(vector_parameters: &VectorParameters) -> usize {
        vector_parameters.dim + (ALIGNMENT - vector_parameters.dim % ALIGNMENT) % ALIGNMENT
    }
}

impl<TStorage: EncodedStorage> EncodedVectors for EncodedVectorsU8<TStorage> {
    type EncodedQuery = EncodedQueryU8;

    fn save(&self, data_path: &Path, meta_path: &Path) -> std::io::Result<()> {
        meta_path.parent().map(std::fs::create_dir_all);
        atomic_save_json(meta_path, &self.metadata)?;

        data_path.parent().map(std::fs::create_dir_all);
        self.encoded_vectors.save_to_file(data_path)?;
        Ok(())
    }

    fn load(
        data_path: &Path,
        meta_path: &Path,
        vector_parameters: &VectorParameters,
    ) -> std::io::Result<Self> {
        let contents = std::fs::read_to_string(meta_path)?;
        let metadata: Metadata = serde_json::from_str(&contents)?;
        let quantized_vector_size = Self::get_quantized_vector_size(vector_parameters);
        let encoded_vectors = TStorage::from_file(data_path, quantized_vector_size)?;
        let result = Self {
            encoded_vectors,
            metadata,
        };
        Ok(result)
    }

    fn is_on_disk(&self) -> bool {
        self.encoded_vectors.is_on_disk()
    }

    fn encode_query(&self, query: &[f32]) -> EncodedQueryU8 {
        let dim = query.len();
        let mut query: Vec<_> = query
            .iter()
            .map(|&v| Self::f32_to_u8(v, self.metadata.alpha, self.metadata.offset))
            .collect();
        if dim % ALIGNMENT != 0 {
            for _ in 0..(ALIGNMENT - dim % ALIGNMENT) {
                let placeholder = match self.metadata.vector_parameters.distance_type {
                    DistanceType::Dot => 0.0,
                    DistanceType::L1 | DistanceType::L2 => self.metadata.offset,
                };
                let encoded =
                    Self::f32_to_u8(placeholder, self.metadata.alpha, self.metadata.offset);
                query.push(encoded);
            }
        }
        let offset = match self.metadata.vector_parameters.distance_type {
            DistanceType::Dot => {
                query.iter().map(|&x| f32::from(x)).sum::<f32>()
                    * self.metadata.alpha
                    * self.metadata.offset
            }
            DistanceType::L1 => 0.0,
            DistanceType::L2 => {
                query
                    .iter()
                    .map(|&x| f32::from(x) * f32::from(x))
                    .sum::<f32>()
                    * self.metadata.alpha
                    * self.metadata.alpha
            }
        };
        let offset = if self.metadata.vector_parameters.invert {
            -offset
        } else {
            offset
        };
        EncodedQueryU8 {
            offset,
            encoded_query: query,
        }
    }

    fn score_point(&self, query: &EncodedQueryU8, i: u32, hw_counter: &HardwareCounterCell) -> f32 {
        let vector_data_size = self.metadata.actual_dim + std::mem::size_of::<f32>();
        let bytes = self
            .encoded_vectors
            .get_vector_data(i as usize, vector_data_size);

        self.score_point_vs_bytes(query, bytes, hw_counter)
    }

    fn score_internal(&self, i: u32, j: u32, hw_counter: &HardwareCounterCell) -> f32 {
        hw_counter
            .cpu_counter()
            .incr_delta(self.metadata.vector_parameters.dim);

        hw_counter
            .vector_io_read()
            .incr_delta(self.metadata.vector_parameters.dim * 2);

        let (query_offset, q_ptr) = self.get_vec_ptr(i);
        let (vector_offset, v_ptr) = self.get_vec_ptr(j);
        let diff = self.metadata.actual_dim as f32 * self.metadata.offset * self.metadata.offset;
        let diff = if self.metadata.vector_parameters.invert {
            -diff
        } else {
            diff
        };
        let offset = query_offset + vector_offset - diff;

        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            unsafe {
                let score = match self.metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => {
                        impl_score_dot_avx(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                    DistanceType::L1 => {
                        impl_score_l1_avx(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                };

                return self.metadata.multiplier * score + offset;
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if is_x86_feature_detected!("sse4.1") {
            unsafe {
                let score = match self.metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => {
                        impl_score_dot_sse(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                    DistanceType::L1 => {
                        impl_score_l1_sse(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                };

                return self.metadata.multiplier * score + offset;
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        if std::arch::is_aarch64_feature_detected!("neon") {
            unsafe {
                let score = match self.metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => {
                        impl_score_dot_neon(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                    DistanceType::L1 => {
                        impl_score_l1_neon(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                };

                return self.metadata.multiplier * score + offset;
            }
        }

        let score = match self.metadata.vector_parameters.distance_type {
            DistanceType::Dot | DistanceType::L2 => {
                impl_score_dot(q_ptr, v_ptr, self.metadata.actual_dim)
            }
            DistanceType::L1 => impl_score_l1(q_ptr, v_ptr, self.metadata.actual_dim),
        };

        self.metadata.multiplier * score as f32 + offset
    }

    fn quantized_vector_size(&self) -> usize {
        // actual_dim rounds up vector_dimension to the next multiple of ALIGNMENT
        // also add scaling factor to the tally
        self.metadata.actual_dim + std::mem::size_of::<f32>()
    }

    fn encode_internal_vector(&self, id: u32) -> Option<EncodedQueryU8> {
        let offset_difference = match self.metadata.vector_parameters.distance_type {
            DistanceType::Dot => {
                self.metadata.actual_dim as f32 * self.metadata.offset * self.metadata.offset
            }
            DistanceType::L1 => 0.0,
            DistanceType::L2 => {
                self.metadata.actual_dim as f32 * self.metadata.offset * self.metadata.offset
            }
        };

        let (query_offset, q_ptr) = self.get_vec_ptr(id);
        let query_offset = if self.metadata.vector_parameters.invert {
            query_offset + offset_difference
        } else {
            query_offset - offset_difference
        };
        Some(EncodedQueryU8 {
            offset: query_offset,
            encoded_query: unsafe {
                std::slice::from_raw_parts(q_ptr, self.metadata.actual_dim).to_vec()
            },
        })
    }

    fn push_vector(
        &mut self,
        _vector: &[f32],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        debug_assert!(false, "SQ does not support push_vector",);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "SQ does not support push_vector",
        ))
    }

    fn vectors_count(&self) -> usize {
        self.encoded_vectors
            .vectors_count(Self::get_quantized_vector_size(
                &self.metadata.vector_parameters,
            ))
    }

    fn flusher(&self) -> MmapFlusher {
        self.encoded_vectors.flusher()
    }
}

impl<TStorage: EncodedStorage> EncodedVectorsBytes for EncodedVectorsU8<TStorage> {
    fn score_point_vs_bytes(
        &self,
        query: &Self::EncodedQuery,
        bytes: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        hw_counter
            .cpu_counter()
            .incr_delta(self.metadata.vector_parameters.dim);

        debug_assert!(bytes.len() >= std::mem::size_of::<f32>() + self.metadata.actual_dim);

        let (vector_offset, v_ptr) = Self::parse_vec_data(bytes);
        let q_ptr = query.encoded_query.as_ptr();

        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            unsafe {
                let score = match self.metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => {
                        impl_score_dot_avx(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                    DistanceType::L1 => {
                        impl_score_l1_avx(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                };

                return self.metadata.multiplier * score + query.offset + vector_offset;
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if is_x86_feature_detected!("sse4.1") {
            unsafe {
                let score = match self.metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => {
                        impl_score_dot_sse(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                    DistanceType::L1 => {
                        impl_score_l1_sse(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                };

                return self.metadata.multiplier * score + query.offset + vector_offset;
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        if std::arch::is_aarch64_feature_detected!("neon") {
            unsafe {
                let score = match self.metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => {
                        impl_score_dot_neon(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                    DistanceType::L1 => {
                        impl_score_l1_neon(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                };

                return self.metadata.multiplier * score + query.offset + vector_offset;
            }
        }

        let score = match self.metadata.vector_parameters.distance_type {
            DistanceType::Dot | DistanceType::L2 => {
                impl_score_dot(q_ptr, v_ptr, self.metadata.actual_dim)
            }
            DistanceType::L1 => impl_score_l1(q_ptr, v_ptr, self.metadata.actual_dim),
        };

        self.metadata.multiplier * score as f32 + query.offset + vector_offset
    }
}

fn impl_score_dot(q_ptr: *const u8, v_ptr: *const u8, actual_dim: usize) -> i32 {
    unsafe {
        let mut score = 0i32;
        for i in 0..actual_dim {
            score += i32::from(*q_ptr.add(i)) * i32::from(*v_ptr.add(i));
        }
        score
    }
}

fn impl_score_l1(q_ptr: *const u8, v_ptr: *const u8, actual_dim: usize) -> i32 {
    unsafe {
        let mut score = 0i32;
        for i in 0..actual_dim {
            score += i32::from(*q_ptr.add(i)).abs_diff(i32::from(*v_ptr.add(i))) as i32;
        }
        score
    }
}

#[cfg(target_arch = "x86_64")]
unsafe extern "C" {
    fn impl_score_dot_avx(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
    fn impl_score_l1_avx(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;

    fn impl_score_dot_sse(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
    fn impl_score_l1_sse(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
unsafe extern "C" {
    fn impl_score_dot_neon(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
    fn impl_score_l1_neon(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
}
