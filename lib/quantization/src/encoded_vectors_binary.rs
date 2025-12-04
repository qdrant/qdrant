use std::alloc::Layout;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::True;
use common::types::PointOffsetType;
use fs_err as fs;
use io::file_operations::atomic_save_json;
use memory::mmap_ops::{transmute_from_u8_to_slice, transmute_to_u8_slice};
use memory::mmap_type::MmapFlusher;
use serde::{Deserialize, Serialize};
use strum::EnumIter;

use crate::encoded_vectors::validate_vector_parameters;
use crate::vector_stats::{VectorElementStats, VectorStats};
use crate::{
    DistanceType, EncodedStorage, EncodedStorageBuilder, EncodedVectors, EncodingError,
    VectorParameters,
};

pub struct EncodedVectorsBin<TBitsStoreType: BitsStoreType, TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    metadata_path: Option<PathBuf>,
    bits_store_type: PhantomData<TBitsStoreType>,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize, Default, EnumIter)]
pub enum Encoding {
    #[default]
    OneBit,
    TwoBits,
    OneAndHalfBits,
}

impl Encoding {
    pub fn is_one(&self) -> bool {
        matches!(self, Encoding::OneBit)
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize, Default, EnumIter)]
pub enum QueryEncoding {
    #[default]
    SameAsStorage,
    Scalar4bits,
    Scalar8bits,
}

impl QueryEncoding {
    pub fn is_same_as_storage(&self) -> bool {
        matches!(self, QueryEncoding::SameAsStorage)
    }
}

pub enum EncodedQueryBQ<TBitsStoreType: BitsStoreType> {
    Binary(EncodedBinVector<TBitsStoreType>),
    Scalar4bits(EncodedScalarVector<TBitsStoreType>),
    Scalar8bits(EncodedScalarVector<TBitsStoreType>),
}

pub struct EncodedBinVector<TBitsStoreType: BitsStoreType> {
    encoded_vector: Vec<TBitsStoreType>,
}

/// Transposed Scalar Encoded Vector
///
/// This data structure represents a scalar-encoded vector optimized for efficient scoring
/// against Binary Quantized (BQ) vectors through bit-level transposition.
///
/// STANDARD ENCODING:
/// A regular scalar vector [float_1, float_2, ..., float_n] is quantized to
/// [scalar_1, scalar_2, ..., scalar_n], where each scalar_i is a u8 value.
///
/// PERFORMANCE ISSUE:
/// Standard encoding is inefficient for scoring because it requires extracting
/// individual bits from each BQ vector in the dataset to perform XOR operations
/// with the scalar vector.
///
/// TRANSPOSITION OPTIMIZATION:
/// To improve scoring efficiency, we reorganize the data using bit-level transposition:
///
/// 1. Take the encoded scalar vector [scalar_1, scalar_2, ..., scalar_n]
/// 2. Divide into batches of size sizeof::<TBitsStoreType>() = N:
///    [[scalar_1, scalar_2, ..., scalar_N], [scalar_N+1, ...], ...]
/// 3. Transpose bit positions within each batch:
///    - Store all first bits: [scalar_1[0], scalar_2[0], ..., scalar_N[0]]
///    - Store all second bits: [scalar_1[1], scalar_2[1], ..., scalar_N[1]]
///    - Continue for all bit positions...
///
/// SCORING ADVANTAGE:
/// This layout enables efficient batch operations:
/// - Extract a single TBitsStoreType value from the BQ vector
/// - Perform N parallel operations with corresponding scalar bits
/// - Use shift operations to compute the final score:
///   (scalar_1[0] ^ bq_vector[0] + ) << 0 +
///   (scalar_1[0] ^ bq_vector[0] + ) << 1 +
///   (scalar_1[0] ^ bq_vector[0] + ) << 2 ...
///
/// This eliminates the need to extract individual bits from BQ vectors during scoring.
/// This idea was taken from http://arxiv.org/pdf/2405.12497, see Figure 2.
pub struct EncodedScalarVector<TBitsStoreType: BitsStoreType> {
    pub encoded_vector: Vec<TBitsStoreType>,
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    vector_parameters: VectorParameters,
    #[serde(default)]
    #[serde(skip_serializing_if = "Encoding::is_one")]
    encoding: Encoding,
    #[serde(default)]
    #[serde(skip_serializing_if = "QueryEncoding::is_same_as_storage")]
    query_encoding: QueryEncoding,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    vector_stats: Option<VectorStats>,
}

pub trait BitsStoreType:
    Default
    + Copy
    + Clone
    + core::ops::BitOrAssign
    + std::ops::Shl<usize, Output = Self>
    + std::ops::Shr<usize, Output = Self>
    + std::ops::BitAnd<Output = Self>
    + num_traits::identities::One
    + num_traits::cast::FromPrimitive
    + num_traits::cast::ToPrimitive
    + bytemuck::Pod
    + std::fmt::Debug
{
    /// Xor vectors and return the number of bits set to 1
    ///
    /// Assume that `v1` and `v2` are aligned to `BITS_STORE_TYPE_SIZE` with both with zeros
    /// So it does not affect the resulting number of bits set to 1
    fn xor_popcnt(v1: &[Self], v2: &[Self]) -> usize;

    /// Calculate score between BQ encoded vector and `EncodedScalarVector<Self>` query.
    ///
    /// It calculates sum of XOR popcount between each bit of the `vector` and the corresponding scalar value in the `query`.
    /// XOR between scalar and bit is a XOR for each bit of the scalar value.
    /// See `EncodedScalarVector` docs for more details about the transposition optimization to avoid extracting bits from BQ vectors.
    fn xor_popcnt_scalar(vector: &[Self], query: &[Self], query_bits_count: usize) -> usize;

    /// Estimates how many `StorageType` elements are needed to store `size` bits
    fn get_storage_size(size: usize) -> usize;
}

impl BitsStoreType for u8 {
    fn xor_popcnt(v1: &[Self], v2: &[Self]) -> usize {
        debug_assert!(v1.len() == v2.len());

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if is_x86_feature_detected!("sse4.2") {
            unsafe {
                if v1.len() > 16 {
                    return impl_xor_popcnt_sse_uint128(
                        v1.as_ptr(),
                        v2.as_ptr(),
                        (v1.len() as u32) / 16,
                    ) as usize;
                } else if v1.len() > 8 {
                    return impl_xor_popcnt_sse_uint64(
                        v1.as_ptr(),
                        v2.as_ptr(),
                        (v1.len() as u32) / 8,
                    ) as usize;
                } else if v1.len() > 4 {
                    return impl_xor_popcnt_sse_uint32(
                        v1.as_ptr(),
                        v2.as_ptr(),
                        (v1.len() as u32) / 4,
                    ) as usize;
                }
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        if std::arch::is_aarch64_feature_detected!("neon") {
            unsafe {
                if v1.len() > 16 {
                    return impl_xor_popcnt_neon_uint128(
                        v1.as_ptr(),
                        v2.as_ptr(),
                        (v1.len() as u32) / 16,
                    ) as usize;
                } else if v1.len() > 8 {
                    return impl_xor_popcnt_neon_uint64(
                        v1.as_ptr(),
                        v2.as_ptr(),
                        (v1.len() as u32) / 8,
                    ) as usize;
                }
            }
        }

        let mut result = 0;
        for (&b1, &b2) in v1.iter().zip(v2.iter()) {
            result += (b1 ^ b2).count_ones() as usize;
        }
        result
    }

    fn xor_popcnt_scalar(vector: &[Self], query: &[Self], query_bits_count: usize) -> usize {
        debug_assert!(query.len() >= vector.len() * query_bits_count);

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        if std::arch::is_aarch64_feature_detected!("neon") {
            if query_bits_count == 8 {
                unsafe {
                    return impl_xor_popcnt_scalar8_neon_u8(
                        query.as_ptr().cast::<u8>(),
                        vector.as_ptr().cast::<u8>(),
                        vector.len() as u32,
                    ) as usize;
                }
            } else if query_bits_count == 4 {
                unsafe {
                    return impl_xor_popcnt_scalar4_neon_u8(
                        query.as_ptr().cast::<u8>(),
                        vector.as_ptr().cast::<u8>(),
                        vector.len() as u32,
                    ) as usize;
                }
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if is_x86_feature_detected!("sse4.2") {
            if query_bits_count == 8 {
                unsafe {
                    return impl_xor_popcnt_scalar8_sse_u8(
                        query.as_ptr().cast::<u8>(),
                        vector.as_ptr().cast::<u8>(),
                        vector.len() as u32,
                    ) as usize;
                }
            } else if query_bits_count == 4 {
                unsafe {
                    return impl_xor_popcnt_scalar4_sse_u8(
                        query.as_ptr().cast::<u8>(),
                        vector.as_ptr().cast::<u8>(),
                        vector.len() as u32,
                    ) as usize;
                }
            }
        }

        let mut result = 0;
        for (&b1, b2_chunk) in vector.iter().zip(query.chunks_exact(query_bits_count)) {
            for (i, &b2) in b2_chunk.iter().enumerate() {
                result += (b1 ^ b2).count_ones() << i;
            }
        }
        result as usize
    }

    fn get_storage_size(size: usize) -> usize {
        let bytes_count = if size > 128 {
            std::mem::size_of::<u128>()
        } else if size > 64 {
            std::mem::size_of::<u64>()
        } else if size > 32 {
            std::mem::size_of::<u32>()
        } else {
            std::mem::size_of::<u8>()
        };

        let bits_count = u8::BITS as usize * bytes_count;
        let mut result = size / bits_count;
        if !size.is_multiple_of(bits_count) {
            result += 1;
        }
        result * bytes_count
    }
}

impl BitsStoreType for u128 {
    fn xor_popcnt(v1: &[Self], v2: &[Self]) -> usize {
        debug_assert!(v1.len() == v2.len());

        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx512vl")
            && is_x86_feature_detected!("avx512vpopcntdq")
            && is_x86_feature_detected!("avx2")
            && is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("sse4.1")
            && is_x86_feature_detected!("sse2")
        {
            unsafe {
                return impl_xor_popcnt_avx512_uint128(
                    v1.as_ptr().cast::<u8>(),
                    v2.as_ptr().cast::<u8>(),
                    v1.len() as u32,
                ) as usize;
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if is_x86_feature_detected!("sse4.2") {
            unsafe {
                return impl_xor_popcnt_sse_uint128(
                    v1.as_ptr().cast::<u8>(),
                    v2.as_ptr().cast::<u8>(),
                    v1.len() as u32,
                ) as usize;
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        if std::arch::is_aarch64_feature_detected!("neon") {
            unsafe {
                return impl_xor_popcnt_neon_uint128(
                    v1.as_ptr().cast::<u8>(),
                    v2.as_ptr().cast::<u8>(),
                    v1.len() as u32,
                ) as usize;
            }
        }

        let mut result = 0;
        for (&b1, &b2) in v1.iter().zip(v2.iter()) {
            result += (b1 ^ b2).count_ones() as usize;
        }
        result
    }

    fn xor_popcnt_scalar(vector: &[Self], query: &[Self], query_bits_count: usize) -> usize {
        debug_assert!(query.len() >= vector.len() * query_bits_count);

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        if std::arch::is_aarch64_feature_detected!("neon") {
            if query_bits_count == 8 {
                unsafe {
                    return impl_xor_popcnt_scalar8_neon_uint128(
                        query.as_ptr().cast::<u8>(),
                        vector.as_ptr().cast::<u8>(),
                        vector.len() as u32,
                    ) as usize;
                }
            } else if query_bits_count == 4 {
                unsafe {
                    return impl_xor_popcnt_scalar4_neon_uint128(
                        query.as_ptr().cast::<u8>(),
                        vector.as_ptr().cast::<u8>(),
                        vector.len() as u32,
                    ) as usize;
                }
            }
        }

        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("sse4.2") {
            if query_bits_count == 8 {
                unsafe {
                    return impl_xor_popcnt_scalar8_avx_uint128(
                        query.as_ptr().cast::<u8>(),
                        vector.as_ptr().cast::<u8>(),
                        vector.len() as u32,
                    ) as usize;
                }
            } else if query_bits_count == 4 {
                unsafe {
                    return impl_xor_popcnt_scalar4_avx_uint128(
                        query.as_ptr().cast::<u8>(),
                        vector.as_ptr().cast::<u8>(),
                        vector.len() as u32,
                    ) as usize;
                }
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if is_x86_feature_detected!("sse4.2") {
            if query_bits_count == 8 {
                unsafe {
                    return impl_xor_popcnt_scalar8_sse_uint128(
                        query.as_ptr().cast::<u8>(),
                        vector.as_ptr().cast::<u8>(),
                        vector.len() as u32,
                    ) as usize;
                }
            } else if query_bits_count == 4 {
                unsafe {
                    return impl_xor_popcnt_scalar4_sse_uint128(
                        query.as_ptr().cast::<u8>(),
                        vector.as_ptr().cast::<u8>(),
                        vector.len() as u32,
                    ) as usize;
                }
            }
        }

        let mut result = 0;
        for (&b1, b2_chunk) in vector.iter().zip(query.chunks_exact(query_bits_count)) {
            for (i, &b2) in b2_chunk.iter().enumerate() {
                result += (b1 ^ b2).count_ones() << i;
            }
        }
        result as usize
    }

    fn get_storage_size(size: usize) -> usize {
        let bits_count = u8::BITS as usize * std::mem::size_of::<Self>();
        let mut result = size / bits_count;
        if !size.is_multiple_of(bits_count) {
            result += 1;
        }
        result
    }
}

impl<TBitsStoreType: BitsStoreType, TStorage: EncodedStorage>
    EncodedVectorsBin<TBitsStoreType, TStorage>
{
    pub fn storage(&self) -> &TStorage {
        &self.encoded_vectors
    }

    pub fn encode<'a>(
        orig_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
        mut storage_builder: impl EncodedStorageBuilder<Storage = TStorage>,
        vector_parameters: &VectorParameters,
        encoding: Encoding,
        query_encoding: QueryEncoding,
        meta_path: Option<&Path>,
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        debug_assert!(validate_vector_parameters(orig_data.clone(), vector_parameters).is_ok());

        let storage_encoding_needs_states = match encoding {
            Encoding::OneBit => false,
            // Requires stats for bit boundaries
            Encoding::TwoBits | Encoding::OneAndHalfBits => true,
        };

        let query_encoding_needs_stats = match query_encoding {
            QueryEncoding::SameAsStorage => storage_encoding_needs_states,
            QueryEncoding::Scalar4bits | QueryEncoding::Scalar8bits => true,
        };

        let vector_stats = if storage_encoding_needs_states || query_encoding_needs_stats {
            Some(VectorStats::build(orig_data.clone(), vector_parameters))
        } else {
            None
        };

        for vector in orig_data {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let encoded_vector = Self::encode_vector(vector.as_ref(), &vector_stats, encoding);
            let encoded_vector_slice = encoded_vector.encoded_vector.as_slice();
            let bytes = transmute_to_u8_slice(encoded_vector_slice);
            storage_builder.push_vector_data(bytes).map_err(|e| {
                EncodingError::EncodingError(format!("Failed to push encoded vector: {e}",))
            })?;
        }

        let encoded_vectors = storage_builder
            .build()
            .map_err(|e| EncodingError::EncodingError(format!("Failed to build storage: {e}",)))?;

        let metadata = Metadata {
            vector_parameters: vector_parameters.clone(),
            encoding,
            query_encoding,
            vector_stats,
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
            bits_store_type: PhantomData,
        })
    }

    pub fn load(encoded_vectors: TStorage, meta_path: &Path) -> std::io::Result<Self> {
        let contents = fs::read_to_string(meta_path)?;
        let metadata: Metadata = serde_json::from_str(&contents)?;
        let result = Self {
            metadata,
            metadata_path: Some(meta_path.to_path_buf()),
            encoded_vectors,
            bits_store_type: PhantomData,
        };
        Ok(result)
    }

    fn encode_vector(
        vector: &[f32],
        vector_stats: &Option<VectorStats>,
        encoding: Encoding,
    ) -> EncodedBinVector<TBitsStoreType> {
        let encoded_vector_size =
            Self::get_quantized_vector_size_from_params(vector.len(), encoding)
                / std::mem::size_of::<TBitsStoreType>();
        let mut encoded_vector = vec![Default::default(); encoded_vector_size];

        match encoding {
            Encoding::OneBit => Self::encode_one_bit_vector(vector, &mut encoded_vector),
            Encoding::TwoBits => {
                Self::encode_two_bits_vector(vector, &mut encoded_vector, vector_stats)
            }
            Encoding::OneAndHalfBits => {
                Self::encode_one_and_half_bits_vector(vector, &mut encoded_vector, vector_stats)
            }
        }

        EncodedBinVector { encoded_vector }
    }

    fn encode_one_bit_vector(vector: &[f32], encoded_vector: &mut [TBitsStoreType]) {
        let bits_count = u8::BITS as usize * std::mem::size_of::<TBitsStoreType>();
        let one = TBitsStoreType::one();
        for (i, &v) in vector.iter().enumerate() {
            // flag is true if the value is positive
            // It's expected that the vector value is in range [-1; 1]
            if v > 0.0 {
                encoded_vector[i / bits_count] |= one << (i % bits_count);
            }
        }
    }

    fn encode_two_bits_vector(
        vector: &[f32],
        encoded_vector: &mut [TBitsStoreType],
        vector_stats: &Option<VectorStats>,
    ) {
        let bits_count = u8::BITS as usize * std::mem::size_of::<TBitsStoreType>();
        let one = TBitsStoreType::one();
        for i in 0..vector.len() {
            let value = vector[i];
            let stats = vector_stats.as_ref().map(|stats| &stats.elements_stats[i]);
            let (b1, b2) = Self::encode_two_bits_value(value, stats);
            if b1 {
                encoded_vector[i / bits_count] |= one << (i % bits_count);
            }
            if b2 {
                let j = vector.len() + i;
                encoded_vector[j / bits_count] |= one << (j % bits_count);
            }
        }
    }

    fn encode_one_and_half_bits_vector(
        vector: &[f32],
        encoded_vector: &mut [TBitsStoreType],
        vector_stats: &Option<VectorStats>,
    ) {
        // One and half bit encoding is a 2bit quantization but first bit,
        // which describes that value is less that sigma,
        // is united with the bit from the next value using OR operand.
        // Scoring for 1.5bit quantization is the same as for 2bit and 1bit quantization.
        //
        // Example 1:
        // `Value1` has `[1,0]` 2bits encoding, value `Value2` has `[1,1]` 2bits encoding.
        // The resulting 1.5bit encoding will be `[value1[0], value2[0], value1[1] | value2[1]] = [1, 1, 1]`.
        //
        // Example 2:
        // `Value1` has `[0,0]` 2bits encoding, value `Value2` has `[1,0]` 2bits encoding.
        // The resulting 1.5bit encoding will be `[value1[0], value2[0], value1[1] | value2[1]] = [0, 1, 0]`.
        let bits_count = u8::BITS as usize * std::mem::size_of::<TBitsStoreType>();
        let one = TBitsStoreType::one();
        for i in 0..vector.len() {
            let value = vector[i];
            let stats = vector_stats.as_ref().map(|stats| &stats.elements_stats[i]);
            let (b1, b2) = Self::encode_two_bits_value(value, stats);
            if b1 {
                encoded_vector[i / bits_count] |= one << (i % bits_count);
            }
            if b2 {
                let j = vector.len() + i / 2;
                encoded_vector[j / bits_count] |= one << (j % bits_count);
            }
        }
    }

    fn encode_two_bits_value(
        value: f32,
        element_stats: Option<&VectorElementStats>,
    ) -> (bool, bool) {
        // Two bit encoding is a regular BQ with "zero".
        // It uses 2 bits per value and encodes values in the following way:
        // 00 - if the value is in the range (-inf; -SIGMAS];
        // 10 - if the value is in the range (-SIGMAS; SIGMAS);
        // 11 - if the value is in the range [SIGMAS; +inf);
        // where sigma is the standard deviation of the value.
        //
        // Scoring for 2bit quantization is the same as for 1bit quantization.
        let Some(element_stats) = element_stats else {
            return if value > 0.0 {
                (true, true)
            } else {
                (false, false)
            };
        };
        let VectorElementStats {
            min: _,
            max: _,
            mean,
            stddev,
        } = element_stats;

        let sd = *stddev;

        if sd < f32::EPSILON {
            // If standard deviation is zero,
            // we cannot calculate z-score count so use regular BQ with zero-comparison.
            return (value > 0.0, false);
        }

        // Calculate z-score for the value
        let v_z = (value - mean) / sd;

        // Define sigmas count which describes a zero range for 2bit encoding.
        const SIGMAS: f32 = 2.0 / 3.0;

        if v_z <= -SIGMAS {
            (false, false) // (-inf; -SIGMAS]
        } else if v_z < SIGMAS {
            (true, false) // (-SIGMAS; SIGMAS)
        } else {
            (true, true) // [SIGMAS; +inf)
        }
    }

    fn encode_query_vector(
        query: &[f32],
        vector_stats: &Option<VectorStats>,
        encoding: Encoding,
        query_encoding: QueryEncoding,
    ) -> EncodedQueryBQ<TBitsStoreType> {
        match query_encoding {
            QueryEncoding::SameAsStorage => {
                EncodedQueryBQ::Binary(Self::encode_vector(query, vector_stats, encoding))
            }
            QueryEncoding::Scalar8bits => EncodedQueryBQ::Scalar8bits(
                Self::encode_scalar_query_vector(query, encoding, u8::BITS as usize),
            ),
            QueryEncoding::Scalar4bits => EncodedQueryBQ::Scalar4bits(
                Self::encode_scalar_query_vector(query, encoding, (u8::BITS / 2) as usize),
            ),
        }
    }

    fn encode_scalar_query_vector(
        query: &[f32],
        encoding: Encoding,
        bits_count: usize,
    ) -> EncodedScalarVector<TBitsStoreType> {
        match encoding {
            Encoding::OneBit => Self::_encode_scalar_query_vector(query, bits_count),
            Encoding::TwoBits => {
                // For two bits encoding we need to extend the query vector
                let mut extended_query = Vec::with_capacity(query.len() * 2);
                // Copy the original query vector twice: for first and second bits in 2bit BQ encoding
                extended_query.extend_from_slice(query);
                extended_query.extend_from_slice(query);
                Self::_encode_scalar_query_vector(&extended_query, bits_count)
            }
            Encoding::OneAndHalfBits => {
                // For one and half bits encoding we need to extend the query vector
                let mut extended_query = Vec::with_capacity(query.len() + query.len().div_ceil(2));
                extended_query.extend_from_slice(query);
                // For 1.5bit BQ use max of two consecutive values
                extended_query.extend(
                    query
                        .chunks(2)
                        .map(|v| if v.len() == 2 { v[0].max(v[1]) } else { v[0] }),
                );
                Self::_encode_scalar_query_vector(&extended_query, bits_count)
            }
        }
    }

    fn _encode_scalar_query_vector(
        query: &[f32],
        bits_count: usize,
    ) -> EncodedScalarVector<TBitsStoreType> {
        let encoded_query_size = TBitsStoreType::get_storage_size(query.len().max(1)) * bits_count;
        let mut encoded_query: Vec<TBitsStoreType> = vec![Default::default(); encoded_query_size];

        let max_abs_value = query.iter().map(|x| x.abs()).fold(0.0, f32::max);
        let (min, max) = (-max_abs_value, max_abs_value);

        let ranges = (1usize << bits_count) - 1;
        let delta = (max - min) / ranges as f32;

        let storage_bits_count = std::mem::size_of::<TBitsStoreType>() * u8::BITS as usize;
        for (chunk_index, chunk) in query.chunks(storage_bits_count).enumerate() {
            for (shift, value) in chunk.iter().enumerate() {
                let shifted_value = value - min;
                let delted_value = if delta > f32::EPSILON {
                    shifted_value / delta
                } else {
                    0.0
                };
                let rounded_value = delted_value.round() as usize;
                let quantized = rounded_value % (ranges + 1);
                let quantized = TBitsStoreType::from_usize(quantized).unwrap_or_default();
                for b in 0..bits_count {
                    let bit_value = ((quantized >> b) & TBitsStoreType::one()) << shift;
                    encoded_query[bits_count * chunk_index + b] |= bit_value;
                }
            }
        }

        EncodedScalarVector {
            encoded_vector: encoded_query,
        }
    }

    pub fn get_quantized_vector_size_from_params(dim: usize, encoding: Encoding) -> usize {
        let extended_dim = match encoding {
            Encoding::OneBit => dim,
            Encoding::TwoBits => dim * 2,
            Encoding::OneAndHalfBits => (dim * 3).div_ceil(2), // ceil(dim * 1.5)
        };
        TBitsStoreType::get_storage_size(extended_dim.max(1))
            * std::mem::size_of::<TBitsStoreType>()
    }

    fn get_quantized_vector_size(&self) -> usize {
        Self::get_quantized_vector_size_from_params(
            self.metadata.vector_parameters.dim,
            self.metadata.encoding,
        )
    }

    fn calculate_metric(
        &self,
        vector: &[TBitsStoreType],
        query: &[TBitsStoreType],
        query_bits_count: usize,
    ) -> f32 {
        // Dot product in a range [-1; 1] is approximated by NXOR in a range [0; 1]
        // L1 distance in range [-1; 1] (alpha=2) is approximated by alpha*XOR in a range [0; 1]
        // L2 distance in range [-1; 1] (alpha=2) is approximated by alpha*sqrt(XOR) in a range [0; 1]
        // For example:

        // |  A   |  B   | Dot product | L1 | L2 |
        // | -0.5 | -0.5 |  0.25       | 0  | 0  |
        // | -0.5 |  0.5 | -0.25       | 1  | 1  |
        // |  0.5 | -0.5 | -0.25       | 1  | 1  |
        // |  0.5 |  0.5 |  0.25       | 0  | 0  |

        // | A | B | NXOR | XOR
        // | 0 | 0 | 1    | 0
        // | 0 | 1 | 0    | 1
        // | 1 | 0 | 0    | 1
        // | 1 | 1 | 1    | 0

        let xor_product = if query_bits_count == 1 {
            TBitsStoreType::xor_popcnt(vector, query) as f32
        } else {
            let xor_product = TBitsStoreType::xor_popcnt_scalar(vector, query, query_bits_count);
            (xor_product as f32) / (((1 << query_bits_count) - 1) as f32)
        };

        let dim = self.metadata.vector_parameters.dim as f32;
        let zeros_count = dim - xor_product;

        match (
            self.metadata.vector_parameters.distance_type,
            self.metadata.vector_parameters.invert,
        ) {
            // So if `invert` is true we return XOR, otherwise we return (dim - XOR)
            (DistanceType::Dot, true) => xor_product - zeros_count,
            (DistanceType::Dot, false) => zeros_count - xor_product,
            // This also results in exact ordering as L1 and L2 but reversed.
            (DistanceType::L1 | DistanceType::L2, true) => zeros_count - xor_product,
            (DistanceType::L1 | DistanceType::L2, false) => xor_product - zeros_count,
        }
    }

    pub fn get_quantized_vector(&self, i: PointOffsetType) -> &[u8] {
        self.encoded_vectors.get_vector_data(i as _)
    }

    pub fn layout(&self) -> Layout {
        Layout::from_size_align(
            self.get_quantized_vector_size(),
            align_of::<TBitsStoreType>(),
        )
        .unwrap()
    }

    pub fn get_vector_parameters(&self) -> &VectorParameters {
        &self.metadata.vector_parameters
    }

    pub fn encode_internal_query(&self, point_id: u32) -> EncodedQueryBQ<TBitsStoreType> {
        // For internal queries we use the same encoding as for storage
        EncodedQueryBQ::Binary(EncodedBinVector {
            encoded_vector: bytemuck::cast_slice::<u8, TBitsStoreType>(
                self.get_quantized_vector(point_id),
            )
            .to_vec(),
        })
    }
}

impl<TBitsStoreType: BitsStoreType, TStorage: EncodedStorage> EncodedVectors
    for EncodedVectorsBin<TBitsStoreType, TStorage>
{
    type EncodedQuery = EncodedQueryBQ<TBitsStoreType>;

    fn is_on_disk(&self) -> bool {
        self.encoded_vectors.is_on_disk()
    }

    fn encode_query(&self, query: &[f32]) -> EncodedQueryBQ<TBitsStoreType> {
        debug_assert!(query.len() == self.metadata.vector_parameters.dim);
        Self::encode_query_vector(
            query,
            &self.metadata.vector_stats,
            self.metadata.encoding,
            self.metadata.query_encoding,
        )
    }

    fn score_point(
        &self,
        query: &EncodedQueryBQ<TBitsStoreType>,
        i: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let vector_data = self.encoded_vectors.get_vector_data(i);

        self.score_bytes(True, query, vector_data, hw_counter)
    }

    fn score_internal(
        &self,
        i: PointOffsetType,
        j: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let vector_data_1 = self.encoded_vectors.get_vector_data(i);
        let vector_data_2 = self.encoded_vectors.get_vector_data(j);

        hw_counter
            .vector_io_read()
            .incr_delta(vector_data_1.len() + vector_data_2.len());

        let vector_data_usize_1 = transmute_from_u8_to_slice(vector_data_1);
        let vector_data_usize_2 = transmute_from_u8_to_slice(vector_data_2);

        hw_counter
            .cpu_counter()
            .incr_delta(vector_data_usize_2.len());

        self.calculate_metric(vector_data_usize_1, vector_data_usize_2, 1)
    }

    fn quantized_vector_size(&self) -> usize {
        self.get_quantized_vector_size()
    }

    fn encode_internal_vector(
        &self,
        id: PointOffsetType,
    ) -> Option<EncodedQueryBQ<TBitsStoreType>> {
        Some(EncodedQueryBQ::Binary(EncodedBinVector {
            encoded_vector: transmute_from_u8_to_slice(self.encoded_vectors.get_vector_data(id))
                .to_vec(),
        }))
    }

    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[f32],
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        let encoded_vector =
            Self::encode_vector(vector, &self.metadata.vector_stats, self.metadata.encoding);
        self.encoded_vectors.upsert_vector(
            id,
            bytemuck::cast_slice(encoded_vector.encoded_vector.as_slice()),
            hw_counter,
        )
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
        let vector_data_usize = transmute_from_u8_to_slice(bytes);

        hw_counter.cpu_counter().incr_delta(bytes.len());

        match query {
            EncodedQueryBQ::Binary(encoded_vector) => {
                self.calculate_metric(vector_data_usize, &encoded_vector.encoded_vector, 1)
            }
            EncodedQueryBQ::Scalar8bits(encoded_vector) => self.calculate_metric(
                vector_data_usize,
                &encoded_vector.encoded_vector,
                u8::BITS as usize,
            ),
            EncodedQueryBQ::Scalar4bits(encoded_vector) => self.calculate_metric(
                vector_data_usize,
                &encoded_vector.encoded_vector,
                u8::BITS as usize / 2,
            ),
        }
    }
}

#[cfg(target_arch = "x86_64")]
unsafe extern "C" {
    fn impl_xor_popcnt_sse_uint128(query_ptr: *const u8, vector_ptr: *const u8, count: u32) -> u32;

    fn impl_xor_popcnt_sse_uint64(query_ptr: *const u8, vector_ptr: *const u8, count: u32) -> u32;

    fn impl_xor_popcnt_sse_uint32(query_ptr: *const u8, vector_ptr: *const u8, count: u32) -> u32;

    fn impl_xor_popcnt_scalar8_sse_uint128(
        query_ptr: *const u8,
        vector_ptr: *const u8,
        count: u32,
    ) -> u32;

    fn impl_xor_popcnt_scalar4_sse_uint128(
        query_ptr: *const u8,
        vector_ptr: *const u8,
        count: u32,
    ) -> u32;

    fn impl_xor_popcnt_scalar8_avx_uint128(
        query_ptr: *const u8,
        vector_ptr: *const u8,
        count: u32,
    ) -> u32;

    fn impl_xor_popcnt_scalar4_avx_uint128(
        query_ptr: *const u8,
        vector_ptr: *const u8,
        count: u32,
    ) -> u32;

    fn impl_xor_popcnt_scalar8_sse_u8(
        query_ptr: *const u8,
        vector_ptr: *const u8,
        count: u32,
    ) -> u32;

    fn impl_xor_popcnt_scalar4_sse_u8(
        query_ptr: *const u8,
        vector_ptr: *const u8,
        count: u32,
    ) -> u32;
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
unsafe extern "C" {
    fn impl_xor_popcnt_neon_uint128(query_ptr: *const u8, vector_ptr: *const u8, count: u32)
    -> u32;

    fn impl_xor_popcnt_neon_uint64(query_ptr: *const u8, vector_ptr: *const u8, count: u32) -> u32;

    fn impl_xor_popcnt_scalar8_neon_uint128(
        query_ptr: *const u8,
        vector_ptr: *const u8,
        count: u32,
    ) -> u32;

    fn impl_xor_popcnt_scalar4_neon_uint128(
        query_ptr: *const u8,
        vector_ptr: *const u8,
        count: u32,
    ) -> u32;

    fn impl_xor_popcnt_scalar8_neon_u8(
        query_ptr: *const u8,
        vector_ptr: *const u8,
        count: u32,
    ) -> u32;

    fn impl_xor_popcnt_scalar4_neon_u8(
        query_ptr: *const u8,
        vector_ptr: *const u8,
        count: u32,
    ) -> u32;
}

#[allow(missing_docs)]
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl")]
#[target_feature(enable = "avx512vpopcntdq")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "avx")]
#[target_feature(enable = "sse4.1")]
#[target_feature(enable = "sse2")]
unsafe fn impl_xor_popcnt_avx512_uint128(
    query_ptr: *const u8,
    vector_ptr: *const u8,
    u128_count: u32,
) -> u32 {
    use std::arch::x86_64::*;

    let mut query_ptr = query_ptr.cast::<__m256i>();
    let mut vector_ptr = vector_ptr.cast::<__m256i>();
    let m256_count = u128_count / 2;
    let mut sum = _mm256_setzero_si256();
    for _ in 0..m256_count {
        let query_chunk = unsafe { _mm256_loadu_si256(query_ptr) };
        let vector_chunk = unsafe { _mm256_loadu_si256(vector_ptr) };
        sum = _mm256_add_epi64(
            _mm256_popcnt_epi64(_mm256_xor_si256(query_chunk, vector_chunk)),
            sum,
        );

        query_ptr = unsafe { query_ptr.add(1) };
        vector_ptr = unsafe { vector_ptr.add(1) };
    }

    if m256_count * 2 != u128_count {
        let vector_chunk = unsafe { _mm_loadu_si128(vector_ptr.cast::<__m128i>()) };
        let query_chunk = unsafe { _mm_loadu_si128(query_ptr.cast::<__m128i>()) };

        let popcnt = _mm_popcnt_epi64(_mm_xor_si128(query_chunk, vector_chunk));

        sum = _mm256_add_epi64(_mm256_set_m128i(popcnt, _mm_setzero_si128()), sum);
    }

    // Extract high and low 128-bit lanes
    let low = _mm256_castsi256_si128(sum); // Elements 0,1
    let high = _mm256_extracti128_si256(sum, 1); // Elements 2,3
    let sum128 = _mm_add_epi64(low, high);

    // Extract and add the two 64-bit elements
    let low64 = _mm_extract_epi64(sum128, 0);
    let high64 = _mm_extract_epi64(sum128, 1);

    (low64 + high64) as u32
}
