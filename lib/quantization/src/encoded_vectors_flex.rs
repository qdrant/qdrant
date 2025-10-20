use std::alloc::Layout;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::True;
use common::types::PointOffsetType;
use fs_err as fs;
use io::file_operations::atomic_save_json;
use memory::mmap_type::MmapFlusher;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::EncodingError;
use crate::encoded_storage::{EncodedStorage, EncodedStorageBuilder};
use crate::encoded_vectors::{EncodedVectors, VectorParameters};
use crate::vector_stats::{VectorElementStats, VectorStats};

pub const ALIGNMENT: usize = 16;
pub const MAX_BITS_COUNT: usize = 10;
pub const DO_SQRT: bool = false;
pub const USE_FLOATS: bool = false;

pub struct EncodedVectorsFlex<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    metadata_path: Option<PathBuf>,
    queries: Vec<EncodedQueryFlex>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EncodedQueryFlex {
    offset: f32,
    original: Vec<f32>,
    transformed: Vec<f32>,
    transformed_internal: Vec<f32>,
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    actual_dim: usize,
    bits_count: usize,
    vector_parameters: VectorParameters,
    vector_stats: VectorStats,
    transform: Transform,
}

impl<TStorage: EncodedStorage> EncodedVectorsFlex<TStorage> {
    pub fn storage(&self) -> &TStorage {
        &self.encoded_vectors
    }

    pub fn encode<'a>(
        orig_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
        mut storage_builder: impl EncodedStorageBuilder<Storage = TStorage>,
        vector_parameters: &VectorParameters,
        count: usize,
        _quantile: Option<f32>,
        rotations: Option<usize>,
        sigmas: Option<f32>,
        bits_count: usize,
        meta_path: Option<&Path>,
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        let actual_dim = Self::get_actual_dim(vector_parameters);

        if count == 0 {
            unimplemented!("Encoding zero vectors is not supported yet");
        }

        let vector_stats = VectorStats::build(orig_data.clone(), vector_parameters);
        let transform = Transform::new(
            orig_data.clone(),
            &vector_stats,
            vector_parameters,
            sigmas.unwrap_or(3.0),
            rotations.unwrap_or(0),
            meta_path,
        );

        let metadata = Metadata {
            actual_dim,
            vector_parameters: vector_parameters.clone(),
            vector_stats,
            transform,
            bits_count,
        };

        let mut queries = Vec::with_capacity(count);
        for vector in orig_data {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let encoded_query = Self::encode_query(&metadata, vector.as_ref());
            queries.push(encoded_query);

            let mut encoded_vector =
                vec![0; Self::get_quantized_vector_size(vector_parameters, bits_count)];
            Self::encode_vector(&metadata, vector.as_ref(), &mut encoded_vector);

            storage_builder
                .push_vector_data(&encoded_vector)
                .map_err(|e| {
                    EncodingError::EncodingError(format!("Failed to push encoded vector: {e}",))
                })?;
        }

        let encoded_vectors = storage_builder
            .build()
            .map_err(|e| EncodingError::EncodingError(format!("Failed to build storage: {e}",)))?;

        if let Some(meta_path) = meta_path {
            let bin_path = meta_path.with_extension("qbin");
            let file = std::fs::File::create(bin_path).map_err(|e| {
                EncodingError::EncodingError(format!("Failed to create original data file: {e}",))
            })?;
            bincode::serialize_into(std::io::BufWriter::new(file), &queries).map_err(|e| {
                EncodingError::EncodingError(format!("Failed to save original data: {e}",))
            })?;

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

        Ok(EncodedVectorsFlex {
            encoded_vectors,
            metadata,
            metadata_path: meta_path.map(PathBuf::from),
            queries,
        })
    }

    pub fn load(encoded_vectors: TStorage, meta_path: &Path) -> std::io::Result<Self> {
        let contents = fs::read_to_string(meta_path)?;
        let metadata: Metadata = serde_json::from_str(&contents)?;

        let queries_data_path = meta_path.with_extension("qbin");
        let file = std::fs::File::open(queries_data_path)?;
        let queries: Vec<EncodedQueryFlex> =
            bincode::deserialize_from(std::io::BufReader::new(file)).unwrap();

        let result = Self {
            encoded_vectors,
            metadata,
            metadata_path: Some(meta_path.to_path_buf()),
            queries,
        };
        Ok(result)
    }

    pub fn get_quantized_vector(&self, i: PointOffsetType) -> &[u8] {
        self.encoded_vectors.get_vector_data(i)
    }

    pub fn layout(&self) -> Layout {
        Layout::from_size_align(self.quantized_vector_size(), align_of::<u8>()).unwrap()
    }

    pub fn get_quantized_vector_size(
        vector_parameters: &VectorParameters,
        bits_count: usize,
    ) -> usize {
        let actual_dim = Self::get_actual_dim(vector_parameters);
        std::mem::size_of::<f32>() + (actual_dim * bits_count / u8::BITS as usize)
    }

    pub fn get_actual_dim(vector_parameters: &VectorParameters) -> usize {
        vector_parameters.dim + (ALIGNMENT - vector_parameters.dim % ALIGNMENT) % ALIGNMENT
    }

    fn encode_query(metadata: &Metadata, query: &[f32]) -> EncodedQueryFlex {
        let (mut transformed_query, offset) = metadata.transform.transform_query(&query);
        while transformed_query.len() % ALIGNMENT != 0 {
            transformed_query.push(0.0);
        }

        let (mut transformed_internal, _offset) = metadata
            .transform
            .transform_vector(&query, metadata.bits_count);
        while transformed_internal.len() % ALIGNMENT != 0 {
            transformed_internal.push(0.0);
        }

        EncodedQueryFlex {
            offset,
            original: query.to_vec(),
            transformed: transformed_query,
            transformed_internal,
        }
    }

    fn encode_vector(metadata: &Metadata, vector: &[f32], result: &mut [u8]) {
        result.fill(0);
        let (mut transformed_vector, vector_offset) = metadata
            .transform
            .transform_vector(vector, metadata.bits_count);
        while transformed_vector.len() % ALIGNMENT != 0 {
            transformed_vector.push(0.0);
        }
        result[0..std::mem::size_of::<f32>()].copy_from_slice(&vector_offset.to_ne_bytes());

        let result = &mut result[std::mem::size_of::<f32>()..];
        for (chunk_index, chunk) in transformed_vector.chunks_exact(ALIGNMENT).enumerate() {
            let mut intbuf = [0i32; ALIGNMENT];
            for (i, &value) in chunk.iter().enumerate() {
                intbuf[i] = value as i32;
            }
            let bitpack_size_bytes = metadata.bits_count * ALIGNMENT / u8::BITS as usize;
            let bitpack = &mut result
                [chunk_index * bitpack_size_bytes..(chunk_index + 1) * bitpack_size_bytes];

            for bit_index in 0..metadata.bits_count - 1 {
                let bitpack = &mut bitpack[bit_index * ALIGNMENT / u8::BITS as usize
                    ..(bit_index + 1) * ALIGNMENT / u8::BITS as usize];
                for (i, v) in intbuf.iter().enumerate() {
                    let bit = ((*v).abs() >> (metadata.bits_count - 2 - bit_index)) & 1;
                    if bit != 0 {
                        bitpack[i / 8] |= 1 << (i % 8);
                    }
                }
            }

            // store sign bit
            let bitpack = &mut bitpack[(metadata.bits_count - 1) * ALIGNMENT / u8::BITS as usize..];
            for (i, v) in intbuf.iter().enumerate() {
                if *v >= 0 {
                    bitpack[i / 8] |= 1 << (i % 8);
                }
            }
        }
    }

    #[allow(dead_code)]
    fn score_bytes_impl(
        &self,
        query: &EncodedQueryFlex,
        bytes: &[u8],
        _vector_index: usize,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        hw_counter
            .cpu_counter()
            .incr_delta(self.metadata.vector_parameters.dim);

        let offset = f32::from_ne_bytes(
            bytes[0..std::mem::size_of::<f32>()]
                .try_into()
                .expect("Failed to read offset"),
        );
        let bytes = &bytes[std::mem::size_of::<f32>()..];

        let mut score = 0.0f32;
        for (chunk_index, chunk) in query.transformed.chunks_exact(ALIGNMENT).enumerate() {
            let bitpack_size_bytes = self.metadata.bits_count * ALIGNMENT / u8::BITS as usize;
            let bitpack =
                &bytes[chunk_index * bitpack_size_bytes..(chunk_index + 1) * bitpack_size_bytes];

            let mut intbuf = [0i32; ALIGNMENT];
            if self.metadata.bits_count > 1 {
                for bit_index in 0..self.metadata.bits_count - 1 {
                    let bitpack = &bitpack[bit_index * ALIGNMENT / u8::BITS as usize
                        ..(bit_index + 1) * ALIGNMENT / u8::BITS as usize];
                    for i in 0..ALIGNMENT {
                        let bit = (bitpack[i / 8] >> (i % 8)) & 1;
                        if bit != 0 {
                            intbuf[i] |= 1 << (self.metadata.bits_count - 2 - bit_index);
                        }
                    }
                }
            } else {
                for i in 0..ALIGNMENT {
                    intbuf[i] = 1;
                }
            }

            // read sign bit
            let bitpack =
                &bitpack[(self.metadata.bits_count - 1) * ALIGNMENT / u8::BITS as usize..];
            for i in 0..ALIGNMENT {
                let sign_bit = (bitpack[i / 8] >> (i % 8)) & 1;
                if sign_bit == 0 {
                    intbuf[i] = -intbuf[i];
                }
            }

            let multiplier = get_multiplier(self.metadata.bits_count);
            for (i, &q_value) in chunk.iter().enumerate() {
                // assert_eq!(decoded_vector_check[chunk_index * ALIGNMENT + i], intbuf[i] as f32, "Decoded vector does not match stored vector at index {}, chunk_index {}, i {}", vector_index, chunk_index, i);
                let v = intbuf[i] as f32 / multiplier;
                let v = if DO_SQRT {
                    if v < 0.0 { -v.abs().powi(2) } else { v.powi(2) }
                } else {
                    v
                };
                score += q_value * v;
            }
        }

        let score = score + query.offset + offset + self.metadata.transform.mean_sqr_sum;
        score
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    #[target_feature(enable = "avx")]
    #[target_feature(enable = "sse4.1")]
    #[target_feature(enable = "sse3")]
    #[target_feature(enable = "sse2")]
    #[target_feature(enable = "sse")]
    fn score_bytes_avx2_16(
        &self,
        query: &EncodedQueryFlex,
        bytes: &[u8],
        _vector_index: usize,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        use std::arch::x86_64::*;

        hw_counter
            .cpu_counter()
            .incr_delta(self.metadata.vector_parameters.dim);

        let offset = f32::from_ne_bytes(
            bytes[0..std::mem::size_of::<f32>()]
                .try_into()
                .expect("Failed to read offset"),
        );
        let bytes = &bytes[std::mem::size_of::<f32>()..];
        let mut bytes_ptr: *const u16 = bytes.as_ptr() as *const u16;

        let multiplier = get_multiplier(self.metadata.bits_count);
        let multiplier = _mm256_set1_ps(1.0 / multiplier);

        let mask = _mm256_set_epi32(
            1 << 7,
            1 << 6,
            1 << 5,
            1 << 4,
            1 << 3,
            1 << 2,
            1 << 1,
            1 << 0,
        );
        let mask_ones = _mm256_set1_epi32(1);
        let mask_zeros = _mm256_set1_epi32(0);

        let mut sum1 = _mm256_setzero_ps();
        let mut sum2 = _mm256_setzero_ps();

        let count = query.transformed.len() / ALIGNMENT;
        let mut chunk_ptr = query.transformed.as_ptr();
        for _chunk_index in 0..count {
            let chunk1 = unsafe { _mm256_loadu_ps(chunk_ptr) };
            let chunk2 = unsafe { _mm256_loadu_ps(chunk_ptr.add(8)) };
            chunk_ptr = unsafe { chunk_ptr.add(16) };

            let mut ints1: __m256i = _mm256_setzero_si256();
            let mut ints2: __m256i = _mm256_setzero_si256();
            let mut bits1: __m256i = _mm256_setzero_si256();
            let mut bits2: __m256i = _mm256_setzero_si256();
            for _bit_index in 0..self.metadata.bits_count {
                let bits: u16 = unsafe { *bytes_ptr };
                bytes_ptr = unsafe { bytes_ptr.add(1) };

                bits1 = _mm256_and_si256(_mm256_set1_epi32((bits & 0x00FF) as i32), mask);
                bits2 = _mm256_and_si256(_mm256_set1_epi32((bits >> 8) as i32), mask);

                bits1 = _mm256_and_si256(_mm256_cmpgt_epi32(bits1, mask_zeros), mask_ones);
                bits2 = _mm256_and_si256(_mm256_cmpgt_epi32(bits2, mask_zeros), mask_ones);

                ints1 = _mm256_or_si256(_mm256_slli_epi32(ints1, 1), bits1);
                ints2 = _mm256_or_si256(_mm256_slli_epi32(ints2, 1), bits2);
            }
            ints1 = _mm256_srli_epi32(ints1, 1);
            ints2 = _mm256_srli_epi32(ints2, 1);

            let signs1 =
                _mm256_cvtepi32_ps(_mm256_sub_epi32(_mm256_slli_epi32(bits1, 1), mask_ones));
            let signs2 =
                _mm256_cvtepi32_ps(_mm256_sub_epi32(_mm256_slli_epi32(bits2, 1), mask_ones));

            let mut floats1 = _mm256_mul_ps(_mm256_cvtepi32_ps(ints1), multiplier);
            let mut floats2 = _mm256_mul_ps(_mm256_cvtepi32_ps(ints2), multiplier);

            if DO_SQRT {
                floats1 = _mm256_mul_ps(floats1, floats1);
                floats2 = _mm256_mul_ps(floats2, floats2);
            }

            sum1 = unsafe { _mm256_fmadd_ps(chunk1, _mm256_mul_ps(floats1, signs1), sum1) };
            sum2 = unsafe { _mm256_fmadd_ps(chunk2, _mm256_mul_ps(floats2, signs2), sum2) };
        }
        let sum = Self::hsum256_ps_avx(sum1) + Self::hsum256_ps_avx(sum2);
        sum + query.offset + offset + self.metadata.transform.mean_sqr_sum
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    #[target_feature(enable = "avx")]
    #[target_feature(enable = "sse4.1")]
    #[target_feature(enable = "sse3")]
    #[target_feature(enable = "sse2")]
    #[target_feature(enable = "sse")]
    #[allow(clippy::missing_safety_doc)]
    pub fn hsum256_ps_avx(x: std::arch::x86_64::__m256) -> f32 {
        use std::arch::x86_64::*;

        let lr_sum: __m128 = _mm_add_ps(_mm256_extractf128_ps(x, 1), _mm256_castps256_ps128(x));
        let hsum = _mm_hadd_ps(lr_sum, lr_sum);
        let p1 = _mm_extract_ps(hsum, 0);
        let p2 = _mm_extract_ps(hsum, 1);
        f32::from_bits(p1 as u32) + f32::from_bits(p2 as u32)
    }
}

impl<TStorage: EncodedStorage> EncodedVectors for EncodedVectorsFlex<TStorage> {
    type EncodedQuery = EncodedQueryFlex;

    fn is_on_disk(&self) -> bool {
        self.encoded_vectors.is_on_disk()
    }

    fn encode_query(&self, query: &[f32]) -> EncodedQueryFlex {
        Self::encode_query(&self.metadata, query)
    }

    fn score_point(
        &self,
        query: &EncodedQueryFlex,
        i: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        if !USE_FLOATS {
            let bytes = self.encoded_vectors.get_vector_data(i);
            self.score_bytes(True, query, bytes, hw_counter)
        } else {
            let q = query.transformed.as_slice();
            let v = self.queries[i as usize].transformed_internal.as_slice();
            let multiplier = get_multiplier(self.metadata.bits_count);

            q.iter()
                .zip(v.iter())
                .map(|(&a, &b)| {
                    if DO_SQRT {
                        let b_f32 = b / multiplier;
                        let b_f32 = if b_f32 < 0.0 {
                            -b_f32.abs().powi(2)
                        } else {
                            b_f32.powi(2)
                        };
                        a * b_f32
                    } else {
                        a * b / multiplier
                    }
                })
                .sum::<f32>()
                + query.offset
                + self.queries[i as usize].offset
                + self.metadata.transform.mean_sqr_sum
        }
    }

    fn score_internal(
        &self,
        i: PointOffsetType,
        j: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        hw_counter
            .cpu_counter()
            .incr_delta(self.metadata.vector_parameters.dim);

        hw_counter
            .vector_io_read()
            .incr_delta(self.metadata.vector_parameters.dim * 2);

        self.score_bytes(
            True,
            &self.queries[i as usize],
            self.encoded_vectors.get_vector_data(j),
            hw_counter,
        )
    }

    fn quantized_vector_size(&self) -> usize {
        // actual_dim rounds up vector_dimension to the next multiple of ALIGNMENT
        // also add scaling factor to the tally
        self.metadata.actual_dim + std::mem::size_of::<f32>()
    }

    fn encode_internal_vector(&self, id: PointOffsetType) -> Option<EncodedQueryFlex> {
        Some(self.queries[id as usize].clone())
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
        self.queries.len()
    }

    fn flusher(&self) -> MmapFlusher {
        self.encoded_vectors.flusher()
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.encoded_vectors.files();
        if let Some(meta_path) = &self.metadata_path {
            files.push(meta_path.clone());
            files.push(meta_path.with_extension("qbin"));
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
        #[cfg(target_arch = "x86_64")]
        {
            unsafe { self.score_bytes_avx2_16(query, bytes, 0, hw_counter) }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            self.score_bytes_impl(query, bytes, 0, hw_counter)
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Transform {
    rotation: Option<Rotation>,
    shifter: Shifter,
    stddevs: Vec<f32>,
    mean_sqr_sum: f32,
    sigmas: f32,
}

impl Transform {
    pub fn new(
        data: impl Iterator<Item = impl AsRef<[f32]>> + Clone,
        vector_stats: &VectorStats,
        vector_params: &VectorParameters,
        sigmas: f32,
        rotation_steps: usize,
        debug_path: Option<&Path>,
    ) -> Self {
        let mean_sqr_sum = vector_stats
            .elements_stats
            .iter()
            .map(|m| (m.mean * m.mean) as f64)
            .sum::<f64>() as f32;
        let stddevs = vector_stats
            .elements_stats
            .iter()
            .map(|element_stats| {
                let VectorElementStats {
                    min: _,
                    max: _,
                    mean: _,
                    stddev,
                } = element_stats;
                *stddev
            })
            .collect::<Vec<f32>>();

        let shifter = Shifter::new(vector_stats);
        if rotation_steps > 0 {
            let rotation = Rotation::new(
                data.map(|v| {
                    let mut vector = v.as_ref().to_vec();
                    shifter.shift(&mut vector);
                    vector
                }),
                vector_params,
                rotation_steps,
                debug_path,
            );
            Self {
                rotation: Some(rotation),
                shifter,
                stddevs,
                mean_sqr_sum,
                sigmas,
            }
        } else {
            Self {
                rotation: None,
                shifter,
                stddevs,
                mean_sqr_sum,
                sigmas,
            }
        }
    }

    pub fn transform_query(&self, query: &[f32]) -> (Vec<f32>, f32) {
        let mut vector = query.to_owned();
        let sum = self.shifter.shift(&mut vector);
        for (v, stddev) in vector.iter_mut().zip(self.stddevs.iter()) {
            if *stddev > f32::EPSILON {
                *v *= self.sigmas * *stddev;
            } else {
                *v = 0.0;
            }
        }
        if let Some(rotation) = &self.rotation {
            rotation.rotate(&mut vector);
        }
        (vector, sum)
    }

    pub fn transform_vector(&self, vector: &[f32], bits_count: usize) -> (Vec<f32>, f32) {
        let multiplier = get_multiplier(bits_count);
        let mut vector = vector.to_owned();
        let sum = self.shifter.shift(&mut vector);
        for (v, stddev) in vector.iter_mut().zip(self.stddevs.iter()) {
            if *stddev > f32::EPSILON {
                if bits_count > 1 {
                    *v /= self.sigmas * *stddev;
                    *v = v.clamp(-1.0, 1.0);
                    if DO_SQRT {
                        if *v < 0.0 {
                            *v = -v.abs().sqrt();
                        } else {
                            *v = v.sqrt();
                        }
                    }
                    *v = (*v * multiplier).round();
                } else {
                    if *v >= 0.0 {
                        *v = multiplier;
                    } else {
                        *v = -multiplier;
                    }
                }
            } else {
                *v = 0.0;
            }
        }
        if let Some(rotation) = &self.rotation {
            rotation.rotate(&mut vector);
        }
        (vector, sum)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Shifter {
    vector_stats: Option<VectorStats>,
}

impl Shifter {
    pub fn new(vector_stats: &VectorStats) -> Self {
        let skip = std::env::var("SHIFTING")
            .unwrap_or_default()
            .trim()
            .parse()
            .unwrap_or(1)
            == 0;

        if skip {
            log::info!("Skipping shifting as per environment variable");
        }

        Self {
            vector_stats: Some(vector_stats.clone()),
        }
    }

    pub fn shift(&self, vector: &mut [f32]) -> f32 {
        if let Some(vector_stats) = &self.vector_stats {
            for (v, stats) in vector.iter_mut().zip(vector_stats.elements_stats.iter()) {
                *v -= stats.mean;
            }
            vector_stats
                .elements_stats
                .iter()
                .zip(vector.iter())
                .map(|(s, &v)| v * s.mean)
                .sum()
        } else {
            0.0
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Rotation {
    simple_rotations: Vec<SimpleRotation>,
}

#[derive(Serialize, Deserialize)]
pub struct SimpleRotation {
    permutation: Vec<usize>,
    pair_rotations: usize,
    vector_stats: VectorStats,
}

impl Rotation {
    pub fn new(
        data: impl Iterator<Item = impl AsRef<[f32]>> + Clone,
        vector_params: &VectorParameters,
        steps: usize,
        debug_path: Option<&Path>,
    ) -> Self {
        if data.clone().next().is_none() {
            return Self {
                simple_rotations: vec![],
            };
        }

        let dump = std::env::var("DUMP_IMAGES").unwrap_or_default();
        if dump == "1" {
            if let Some(debug_path) = debug_path {
                let debug_path = debug_path.parent().unwrap();
                std::fs::create_dir_all(&debug_path).ok();
                let debug_path = debug_path.join(format!("orig_histograms"));
                std::fs::create_dir_all(&debug_path).ok();
                for dim in 0..vector_params.dim {
                    let numbers = data.clone().map(|v| v.as_ref()[dim]).collect::<Vec<f32>>();
                    plot_histogram(
                        &numbers,
                        &debug_path.join(format!("orig_histogram_{dim}.png")),
                        None,
                    )
                    .unwrap();
                }
            }
        }

        log::info!("Rotation steps: {steps}");

        let mut simple_rotations: Vec<SimpleRotation> = vec![];
        for step in 0..steps {
            let data = data.clone().map(|v| {
                let mut vector = v.as_ref().to_vec();
                for simple_rotation in &simple_rotations {
                    simple_rotation.rotate(&mut vector);
                }
                vector
            });
            let debug_path = debug_path.map(|p| p.join(format!("rotation_{step}")));
            let rotation = SimpleRotation::new(
                data.clone(),
                vector_params,
                debug_path.as_ref().map(|p| p.as_ref()),
                step,
            );
            simple_rotations.push(rotation);
        }

        Self { simple_rotations }
    }

    pub fn rotate(&self, vector: &mut [f32]) {
        for simple_rotation in &self.simple_rotations {
            simple_rotation.rotate(vector);
        }
    }
}

impl SimpleRotation {
    fn new(
        data: impl Iterator<Item = impl AsRef<[f32]>> + Clone,
        vector_params: &VectorParameters,
        debug_path: Option<&Path>,
        index: usize,
    ) -> Self {
        let dump = std::env::var("DUMP_IMAGES").unwrap_or_default();
        if dump == "1" {
            if let Some(debug_path) = debug_path {
                let debug_path = debug_path.parent().unwrap();
                let debug_path = debug_path.join(format!("before_rotation_{index}"));
                std::fs::create_dir_all(&debug_path).ok();
                for dim in 0..vector_params.dim {
                    let numbers = data.clone().map(|v| v.as_ref()[dim]).collect::<Vec<f32>>();
                    plot_histogram(
                        &numbers,
                        &debug_path.join(format!("histogram_{dim}.png")),
                        None,
                    )
                    .unwrap();
                }
            }
        }

        let rotation_bound: f32 = std::env::var("ROTATION_BOUND")
            .unwrap_or_default()
            .trim()
            .parse()
            .unwrap_or(0.8);

        let vector_stats = VectorStats::build(data, vector_params);
        let mut indices: Vec<usize> = (0..vector_params.dim).collect();
        indices.sort_by_key(|&i| OrderedFloat(param(&vector_stats.elements_stats[i])));

        let mut permutation: Vec<usize> = Default::default();
        for i in 0..(vector_params.dim / 2) {
            permutation.push(indices[i]);
            permutation.push(indices[vector_params.dim - 1 - i]);
        }
        if vector_params.dim % 2 == 1 {
            permutation.push(indices[vector_params.dim / 2]);
        }

        let pair_rotations = permutation
            .chunks_exact(2)
            .map(|pair| (pair[0], pair[1]))
            .map(|(i, j)| {
                let param_i = param(&vector_stats.elements_stats[i]);
                let param_j = param(&vector_stats.elements_stats[j]);
                param_i.abs() / param_j.abs() < rotation_bound
            })
            .take_while(|&x| x)
            .count();

        println!(
            "Pair rotations: {pair_rotations} / {} with bound {rotation_bound}",
            vector_params.dim / 2
        );

        Self {
            permutation,
            pair_rotations,
            vector_stats,
        }
    }

    fn rotate(&self, vector: &mut [f32]) {
        // apply permutation
        let permuted = vector.to_owned();

        for (i, &p) in self.permutation.iter().enumerate() {
            vector[i] = permuted[p];
        }

        // apply 2x2 rotations
        for v in vector.chunks_exact_mut(2).take(self.pair_rotations) {
            let a = v[0] as f64;
            let b = v[1] as f64;
            let new_a = 0.707106 * a - 0.707106 * b;
            let new_b = 0.707106 * a + 0.707106 * b;
            v[0] = new_a as f32;
            v[1] = new_b as f32;
        }
    }
}

fn param(s: &VectorElementStats) -> f32 {
    s.stddev
}

pub fn plot_histogram(
    data: &[f32],
    path: &Path,
    maybe_bins: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    use plotters::prelude::*;

    let dump = std::env::var("DUMP_IMAGES").unwrap_or_default();
    if dump != "1" && dump.to_lowercase() != "true" {
        return Ok(());
    }

    let width_px: u32 = std::env::var("WIDTH_PX")
        .unwrap_or_default()
        .trim()
        .parse()
        .unwrap_or(512);

    let height_px: u32 = std::env::var("HEIGHT_PX")
        .unwrap_or_default()
        .trim()
        .parse()
        .unwrap_or(512);

    if data.is_empty() {
        return Err("Empty data array".into());
    }

    let (min_v_f32, max_v_f32) = data
        .iter()
        .fold((f32::INFINITY, f32::NEG_INFINITY), |(mn, mx), &v| {
            (mn.min(v), mx.max(v))
        });

    let (min_v, max_v) = if (max_v_f32 - min_v_f32).abs() < f32::EPSILON {
        let delta = 0.5_f32.max(max_v_f32.abs() * 0.1);
        ((min_v_f32 - delta) as f64, (max_v_f32 + delta) as f64)
    } else {
        (min_v_f32 as f64, max_v_f32 as f64)
    };

    let n = data.len();
    let bins = maybe_bins.unwrap_or_else(|| (n as f64).sqrt().round().max(5.0) as usize);

    let range = max_v - min_v;
    let bin_w = range / bins as f64;

    let safe_bin_w = if bin_w > 0.0 { bin_w } else { 1.0 };

    let mut counts = vec![0usize; bins];
    for &v in data {
        let mut idx = ((v as f64 - min_v) / safe_bin_w).floor() as isize;
        if idx < 0 {
            idx = 0;
        } else if idx as usize >= bins {
            idx = bins as isize - 1;
        }
        counts[idx as usize] += 1;
    }

    let max_count = counts.iter().copied().max().unwrap_or(1);

    let root = BitMapBackend::new(path, (width_px, height_px)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart = ChartBuilder::on(&root)
        .margin(20)
        .caption("Histogram", ("sans-serif", 24))
        .x_label_area_size(40)
        .y_label_area_size(50)
        .build_cartesian_2d(min_v..max_v, 0..(max_count + (max_count / 10).max(1)))?;

    chart
        .configure_mesh()
        .x_desc("Value")
        .y_desc("Count")
        .disable_mesh()
        .x_labels(10)
        .y_labels(10)
        .label_style(("sans-serif", 14))
        .draw()?;

    for (i, &c) in counts.iter().enumerate() {
        let x0 = min_v + i as f64 * safe_bin_w;
        let x1 = x0 + safe_bin_w;
        let y0 = 0;
        let y1 = c;

        chart
            .draw_series(std::iter::once(Rectangle::new(
                [(x0, y0), (x1, y1)],
                BLUE.mix(0.7).filled(),
            )))?
            .label("bin")
            .legend(|(x, y)| Rectangle::new([(x, y - 5), (x + 10, y + 5)], BLUE.filled()));
    }

    chart
        .configure_series_labels()
        .border_style(&BLACK)
        .background_style(&WHITE.mix(0.8))
        .draw()?;

    root.present()?;
    Ok(())
}

fn get_multiplier(bits_count: usize) -> f32 {
    if bits_count > 1 {
        ((1 << (bits_count - 1)) - 1) as f32
    } else {
        1.0
    }
}
