use std::alloc::Layout;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::True;
use common::types::PointOffsetType;
use fs_err as fs;
use io::file_operations::atomic_save_json;
use memory::mmap_type::MmapFlusher;
use serde::{Deserialize, Serialize};

use crate::EncodingError;
use crate::encoded_storage::{EncodedStorage, EncodedStorageBuilder};
use crate::encoded_vectors::{
    DistanceType, EncodedVectors, VectorParameters, validate_vector_parameters,
};
use crate::quantile::{find_min_max_from_iter, find_quantile_interval};

pub const ALIGNMENT: usize = 16;
// Each encoded vector stores an additional f32 at the beginning. Define it's size here.
const ADDITIONAL_CONSTANT_SIZE: usize = std::mem::size_of::<f32>();

#[derive(Clone, PartialEq, Debug)]
pub enum ScalarQuantizationMethod {
    Int8,
    // Future methods can be added here
}

pub struct EncodedVectorsU8<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    metadata_path: Option<PathBuf>,
}

pub struct EncodedQueryU8 {
    offset: f32,
    encoded_query: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum Metadata {
    Int8(MetadataInt8),
}

impl Metadata {
    pub fn vector_parameters(&self) -> &VectorParameters {
        match self {
            Metadata::Int8(meta) => &meta.vector_parameters,
        }
    }

    pub fn actual_dim(&self) -> usize {
        match self {
            Metadata::Int8(meta) => meta.actual_dim,
        }
    }

    pub fn postprocess_score(&self, score: f32, query_offset: f32, vector_offset: f32) -> f32 {
        match self {
            Metadata::Int8(metadata) => {
                metadata.postprocess_score(score, query_offset, vector_offset)
            }
        }
    }

    pub fn postprocess_internal_score(
        &self,
        score: f32,
        query_offset: f32,
        vector_offset: f32,
    ) -> f32 {
        match self {
            Metadata::Int8(metadata) => {
                metadata.postprocess_internal_score(score, query_offset, vector_offset)
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct MetadataInt8 {
    actual_dim: usize,
    alpha: f32,
    offset: f32,
    multiplier: f32,
    vector_parameters: VectorParameters,
}

impl MetadataInt8 {
    #[inline]
    pub fn encode_value(&self, value: f32) -> u8 {
        let i = (value - self.offset) / self.alpha;
        i.clamp(0.0, 127.0).round() as u8
    }

    #[inline]
    fn postprocess_score(&self, score: f32, query_offset: f32, vector_offset: f32) -> f32 {
        self.multiplier * score + query_offset + vector_offset
    }

    #[inline]
    fn postprocess_internal_score(
        &self,
        score: f32,
        vector_1_offset: f32,
        vector_2_offset: f32,
    ) -> f32 {
        let query_offset = vector_1_offset - self.get_shift();
        self.postprocess_score(score, query_offset, vector_2_offset)
    }

    fn get_shift(&self) -> f32 {
        // Dotprod after shifting produces a number which is not related to vector and query
        // (x - a)(y - a) = xy - ax - ay + a^2
        // this a^2 is returned here
        // L2 is handled the same way as Dot here
        let shift = match self.vector_parameters.distance_type {
            DistanceType::Dot | DistanceType::L2 => {
                self.actual_dim as f32 * self.offset * self.offset
            }
            DistanceType::L1 => 0.0,
        };
        if self.vector_parameters.invert {
            -shift
        } else {
            shift
        }
    }
}

impl<TStorage: EncodedStorage> EncodedVectorsU8<TStorage> {
    pub fn storage(&self) -> &TStorage {
        &self.encoded_vectors
    }

    #[allow(clippy::too_many_arguments)]
    pub fn encode<'a>(
        orig_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
        mut storage_builder: impl EncodedStorageBuilder<Storage = TStorage>,
        vector_parameters: &VectorParameters,
        count: usize,
        quantile: Option<f32>,
        method: ScalarQuantizationMethod,
        meta_path: Option<&Path>,
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        assert_eq!(method, ScalarQuantizationMethod::Int8);
        let actual_dim = Self::get_actual_dim(vector_parameters);

        if count == 0 {
            let metadata = Metadata::Int8(MetadataInt8 {
                actual_dim,
                alpha: 0.0,
                offset: 0.0,
                multiplier: 0.0,
                vector_parameters: vector_parameters.clone(),
            });
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
            return Ok(EncodedVectorsU8 {
                encoded_vectors: storage_builder.build().map_err(|e| {
                    EncodingError::EncodingError(format!("Failed to build storage: {e}",))
                })?,
                metadata,
                metadata_path: meta_path.map(PathBuf::from),
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

        let multiplier = match vector_parameters.distance_type {
            // (alpha*x - offset) * (alpha*y - offset) = alpha^2*x*y - alpha*offset*x - alpha*offset*y + offset^2
            // multiplier is applied to xy term only, so we need to multiply score by alpha^2
            DistanceType::Dot => alpha * alpha,
            // |(alpha*x - offset) - (alpha*y - offset)| = alpha*|x - y|
            // multiplier is applied to |x - y| term only, so we need to multiply score by alpha
            DistanceType::L1 => alpha,
            // ((alpha*x - offset) - (alpha*y - offset))^2 = alpha^2*(x - y)^2 = alpha^2*x^2 - 2*alpha^2*xy + alpha^2*y^2
            // multiplier is applied to (x - y)^2 term only, so we need to multiply score by -2*alpha^2
            DistanceType::L2 => -2.0 * alpha * alpha,
        };
        let multiplier = if vector_parameters.invert {
            -multiplier
        } else {
            multiplier
        };

        let metadata = MetadataInt8 {
            actual_dim,
            alpha,
            offset,
            multiplier,
            vector_parameters: vector_parameters.clone(),
        };

        for vector in orig_data {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let mut encoded_vector = Vec::with_capacity(actual_dim + ADDITIONAL_CONSTANT_SIZE);
            encoded_vector.extend_from_slice(&f32::default().to_ne_bytes());
            for &value in vector.as_ref() {
                let encoded = metadata.encode_value(value);
                encoded_vector.push(encoded);
            }
            if !vector_parameters.dim.is_multiple_of(ALIGNMENT) {
                for _ in 0..(ALIGNMENT - vector_parameters.dim % ALIGNMENT) {
                    let placeholder = match vector_parameters.distance_type {
                        DistanceType::Dot => 0.0,
                        DistanceType::L1 | DistanceType::L2 => offset,
                    };
                    let encoded = metadata.encode_value(placeholder);
                    encoded_vector.push(encoded);
                }
            }
            let vector_offset = match vector_parameters.distance_type {
                DistanceType::Dot => {
                    let elements_sum = encoded_vector.iter().map(|&x| f32::from(x)).sum::<f32>();
                    elements_sum * alpha * offset
                }
                DistanceType::L1 => 0.0,
                DistanceType::L2 => {
                    let elements_sqr_sum = encoded_vector
                        .iter()
                        .map(|&x| f32::from(x) * f32::from(x))
                        .sum::<f32>();
                    elements_sqr_sum * alpha * alpha
                }
            };
            let vector_offset = if vector_parameters.invert {
                -vector_offset
            } else {
                vector_offset
            };
            // apply `a^2` shift
            let vector_offset = metadata.get_shift() + vector_offset;
            encoded_vector[0..ADDITIONAL_CONSTANT_SIZE]
                .copy_from_slice(&vector_offset.to_ne_bytes());
            storage_builder
                .push_vector_data(&encoded_vector)
                .map_err(|e| {
                    EncodingError::EncodingError(format!("Failed to push encoded vector: {e}",))
                })?;
        }

        let encoded_vectors = storage_builder
            .build()
            .map_err(|e| EncodingError::EncodingError(format!("Failed to build storage: {e}",)))?;

        let metadata = Metadata::Int8(metadata);
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

        Ok(EncodedVectorsU8 {
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

    pub fn score_point_simple(&self, query: &EncodedQueryU8, bytes: &[u8]) -> f32 {
        match &self.metadata {
            Metadata::Int8(metadata) => {
                let (vector_offset, v_ptr) = Self::parse_vec_data(bytes);
                let q_ptr = query.encoded_query.as_ptr();

                let score = match metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => {
                        impl_score_dot(q_ptr, v_ptr, metadata.actual_dim)
                    }
                    DistanceType::L1 => impl_score_l1(q_ptr, v_ptr, metadata.actual_dim),
                };

                self.metadata
                    .postprocess_score(score as f32, query.offset, vector_offset)
            }
        }
    }

    pub fn score_point_simple_internal(&self, i: PointOffsetType, j: PointOffsetType) -> f32 {
        match &self.metadata {
            Metadata::Int8(metadata) => {
                let (query_offset, q_ptr) = self.get_vec_ptr(i);
                let (vector_offset, v_ptr) = self.get_vec_ptr(j);
                let score = match metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => {
                        impl_score_dot(q_ptr, v_ptr, metadata.actual_dim)
                    }
                    DistanceType::L1 => impl_score_l1(q_ptr, v_ptr, metadata.actual_dim),
                };
                self.metadata
                    .postprocess_internal_score(score as f32, query_offset, vector_offset)
            }
        }
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    pub fn score_point_neon(&self, query: &EncodedQueryU8, bytes: &[u8]) -> f32 {
        match &self.metadata {
            Metadata::Int8(metadata) => {
                let (vector_offset, v_ptr) = Self::parse_vec_data(bytes);
                let q_ptr = query.encoded_query.as_ptr();

                let score = match metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => unsafe {
                        impl_score_dot_neon(q_ptr, v_ptr, metadata.actual_dim as u32)
                    },
                    DistanceType::L1 => unsafe {
                        impl_score_l1_neon(q_ptr, v_ptr, metadata.actual_dim as u32)
                    },
                };
                self.metadata
                    .postprocess_score(score as f32, query.offset, vector_offset)
            }
        }
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    pub fn score_point_neon_internal(&self, i: PointOffsetType, j: PointOffsetType) -> f32 {
        match &self.metadata {
            Metadata::Int8(metadata) => {
                let (query_offset, q_ptr) = self.get_vec_ptr(i);
                let (vector_offset, v_ptr) = self.get_vec_ptr(j);

                let score = match metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => unsafe {
                        impl_score_dot_neon(q_ptr, v_ptr, metadata.actual_dim as u32)
                    },
                    DistanceType::L1 => unsafe {
                        impl_score_l1_neon(q_ptr, v_ptr, metadata.actual_dim as u32)
                    },
                };
                self.metadata
                    .postprocess_internal_score(score as f32, query_offset, vector_offset)
            }
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    pub fn score_point_sse(&self, query: &EncodedQueryU8, bytes: &[u8]) -> f32 {
        match &self.metadata {
            Metadata::Int8(metadata) => {
                let (vector_offset, v_ptr) = Self::parse_vec_data(bytes);
                let q_ptr = query.encoded_query.as_ptr();

                let score = match metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => unsafe {
                        impl_score_dot_sse(q_ptr, v_ptr, metadata.actual_dim as u32)
                    },
                    DistanceType::L1 => unsafe {
                        impl_score_l1_sse(q_ptr, v_ptr, metadata.actual_dim as u32)
                    },
                };
                self.metadata
                    .postprocess_score(score as f32, query.offset, vector_offset)
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    pub fn score_point_sse_internal(&self, i: PointOffsetType, j: PointOffsetType) -> f32 {
        match &self.metadata {
            Metadata::Int8(metadata) => {
                let (query_offset, q_ptr) = self.get_vec_ptr(i);
                let (vector_offset, v_ptr) = self.get_vec_ptr(j);

                let score = match metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => unsafe {
                        impl_score_dot_sse(q_ptr, v_ptr, metadata.actual_dim as u32)
                    },
                    DistanceType::L1 => unsafe {
                        impl_score_l1_sse(q_ptr, v_ptr, metadata.actual_dim as u32)
                    },
                };
                self.metadata
                    .postprocess_internal_score(score as f32, query_offset, vector_offset)
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    pub fn score_point_avx(&self, query: &EncodedQueryU8, bytes: &[u8]) -> f32 {
        match &self.metadata {
            Metadata::Int8(metadata) => {
                let (vector_offset, v_ptr) = Self::parse_vec_data(bytes);
                let q_ptr = query.encoded_query.as_ptr();

                let score = match metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => unsafe {
                        impl_score_dot_avx(q_ptr, v_ptr, metadata.actual_dim as u32)
                    },
                    DistanceType::L1 => unsafe {
                        impl_score_l1_avx(q_ptr, v_ptr, metadata.actual_dim as u32)
                    },
                };
                self.metadata
                    .postprocess_score(score as f32, query.offset, vector_offset)
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    pub fn score_point_avx_internal(&self, i: PointOffsetType, j: PointOffsetType) -> f32 {
        match &self.metadata {
            Metadata::Int8(metadata) => {
                let (query_offset, q_ptr) = self.get_vec_ptr(i);
                let (vector_offset, v_ptr) = self.get_vec_ptr(j);

                let score = match metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => unsafe {
                        impl_score_dot_avx(q_ptr, v_ptr, metadata.actual_dim as u32)
                    },
                    DistanceType::L1 => unsafe {
                        impl_score_l1_avx(q_ptr, v_ptr, metadata.actual_dim as u32)
                    },
                };
                self.metadata
                    .postprocess_internal_score(score as f32, query_offset, vector_offset)
            }
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

    #[inline]
    fn parse_vec_data(data: &[u8]) -> (f32, *const u8) {
        debug_assert!(data.len() >= ADDITIONAL_CONSTANT_SIZE);
        unsafe {
            let offset = data.as_ptr().cast::<f32>().read_unaligned();
            let v_ptr = data.as_ptr().add(ADDITIONAL_CONSTANT_SIZE);
            (offset, v_ptr)
        }
    }

    #[inline]
    fn get_vec_ptr(&self, i: PointOffsetType) -> (f32, *const u8) {
        let data = self.encoded_vectors.get_vector_data(i);
        Self::parse_vec_data(data)
    }

    pub fn get_quantized_vector(&self, i: PointOffsetType) -> &[u8] {
        self.encoded_vectors.get_vector_data(i)
    }

    pub fn layout(&self) -> Layout {
        Layout::from_size_align(self.quantized_vector_size(), align_of::<u8>()).unwrap()
    }

    pub fn get_quantized_vector_offset_and_code(&self, i: PointOffsetType) -> (f32, &[u8]) {
        let (offset, v_ptr) = self.get_vec_ptr(i);
        let vector_data_size = self.metadata.actual_dim();
        let code = unsafe { std::slice::from_raw_parts(v_ptr, vector_data_size) };
        (offset, code)
    }

    pub fn get_quantized_vector_size(vector_parameters: &VectorParameters) -> usize {
        let actual_dim = Self::get_actual_dim(vector_parameters);
        actual_dim + ADDITIONAL_CONSTANT_SIZE
    }

    pub fn get_multiplier(&self) -> f32 {
        match &self.metadata {
            Metadata::Int8(meta) => meta.multiplier,
        }
    }

    pub fn get_shift(&self) -> f32 {
        match &self.metadata {
            Metadata::Int8(metadata) => metadata.get_shift(),
        }
    }

    pub fn get_actual_dim(vector_parameters: &VectorParameters) -> usize {
        vector_parameters.dim + (ALIGNMENT - vector_parameters.dim % ALIGNMENT) % ALIGNMENT
    }

    fn encode_int8_query(metadata: &MetadataInt8, query: &[f32]) -> EncodedQueryU8 {
        let dim = query.len();
        let mut query: Vec<_> = query.iter().map(|&v| metadata.encode_value(v)).collect();
        if !dim.is_multiple_of(ALIGNMENT) {
            for _ in 0..(ALIGNMENT - dim % ALIGNMENT) {
                let placeholder = match metadata.vector_parameters.distance_type {
                    DistanceType::Dot => 0.0,
                    DistanceType::L1 | DistanceType::L2 => metadata.offset,
                };
                let encoded = metadata.encode_value(placeholder);
                query.push(encoded);
            }
        }
        let offset = match metadata.vector_parameters.distance_type {
            DistanceType::Dot => {
                let query_elements_sum = query.iter().map(|&x| f32::from(x)).sum::<f32>();
                query_elements_sum * metadata.alpha * metadata.offset
            }
            DistanceType::L1 => 0.0,
            DistanceType::L2 => {
                let query_elements_sqr_sum = query
                    .iter()
                    .map(|&x| f32::from(x) * f32::from(x))
                    .sum::<f32>();
                query_elements_sqr_sum * metadata.alpha * metadata.alpha
            }
        };
        let offset = if metadata.vector_parameters.invert {
            -offset
        } else {
            offset
        };
        EncodedQueryU8 {
            offset,
            encoded_query: query,
        }
    }
}

impl<TStorage: EncodedStorage> EncodedVectors for EncodedVectorsU8<TStorage> {
    type EncodedQuery = EncodedQueryU8;

    fn is_on_disk(&self) -> bool {
        self.encoded_vectors.is_on_disk()
    }

    fn encode_query(&self, query: &[f32]) -> EncodedQueryU8 {
        match &self.metadata {
            Metadata::Int8(meta) => Self::encode_int8_query(meta, query),
        }
    }

    fn score_point(
        &self,
        query: &EncodedQueryU8,
        i: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let bytes = self.encoded_vectors.get_vector_data(i);
        self.score_bytes(True, query, bytes, hw_counter)
    }

    fn score_internal(
        &self,
        i: PointOffsetType,
        j: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        hw_counter
            .cpu_counter()
            .incr_delta(self.metadata.vector_parameters().dim);

        hw_counter
            .vector_io_read()
            .incr_delta(self.metadata.vector_parameters().dim * 2);

        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return self.score_point_avx_internal(i, j);
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if is_x86_feature_detected!("sse4.1") {
            return self.score_point_sse_internal(i, j);
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        if std::arch::is_aarch64_feature_detected!("neon") {
            return self.score_point_neon_internal(i, j);
        }

        self.score_point_simple_internal(i, j)
    }

    fn quantized_vector_size(&self) -> usize {
        // Actual_dim rounds up vector_dimension to the next multiple of ALIGNMENT.
        // Also add scaling factor to the tally.
        match &self.metadata {
            Metadata::Int8(_) => self.metadata.actual_dim() + ADDITIONAL_CONSTANT_SIZE,
        }
    }

    fn encode_internal_vector(&self, id: PointOffsetType) -> Option<EncodedQueryU8> {
        match &self.metadata {
            Metadata::Int8(metadata) => {
                let (vector_offset, q_ptr) = self.get_vec_ptr(id);
                // Remove shift from offset because encoded query should not have it, it's contained in vector data only.
                let query_offset = vector_offset - metadata.get_shift();
                Some(EncodedQueryU8 {
                    offset: query_offset,
                    encoded_query: unsafe {
                        std::slice::from_raw_parts(q_ptr, metadata.actual_dim).to_vec()
                    },
                })
            }
        }
    }

    fn upsert_vector(
        &mut self,
        _id: PointOffsetType,
        _vector: &[f32],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        debug_assert!(false, "SQ does not support upsert_vector",);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "SQ does not support upsert_vector",
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
        hw_counter
            .cpu_counter()
            .incr_delta(self.metadata.vector_parameters().dim);

        debug_assert!(bytes.len() >= ADDITIONAL_CONSTANT_SIZE + self.metadata.actual_dim());

        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return self.score_point_avx(query, bytes);
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if is_x86_feature_detected!("sse4.1") {
            return self.score_point_sse(query, bytes);
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        if std::arch::is_aarch64_feature_detected!("neon") {
            return self.score_point_neon(query, bytes);
        }

        self.score_point_simple(query, bytes)
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
