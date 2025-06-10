use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use io::file_operations::atomic_save_json;
use memory::mmap_ops::{transmute_from_u8_to_slice, transmute_to_u8_slice};
use serde::{Deserialize, Serialize};

use crate::encoded_vectors::validate_vector_parameters;
use crate::vector_stats::VectorStats;
use crate::{
    DistanceType, EncodedStorage, EncodedStorageBuilder, EncodedVectors, EncodingError,
    VectorParameters,
};

pub struct EncodedVectorsBin<TBitsStoreType: BitsStoreType, TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    vector_stats: Option<VectorStats>,
    bits_store_type: PhantomData<TBitsStoreType>,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize, Default)]
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

#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize, Default)]
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

pub struct EncodedScalarVector<TBitsStoreType: BitsStoreType> {
    encoded_vector: Vec<TBitsStoreType>,
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
    + std::fmt::Debug
{
    /// Xor vectors and return the number of bits set to 1
    ///
    /// Assume that `v1` and `v2` are aligned to `BITS_STORE_TYPE_SIZE` with both with zeros
    /// So it does not affect the resulting number of bits set to 1
    fn xor_popcnt(v1: &[Self], v2: &[Self]) -> usize;

    fn xor_popcnt_scalar(v1: &[Self], v2: &[Self], bits_count: usize) -> usize;

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

    fn xor_popcnt_scalar(v1: &[Self], v2: &[Self], bits_count: usize) -> usize {
        debug_assert!(v2.len() >= v1.len() * bits_count);

        let mut result = 0;
        for (&b1, b2_chunk) in v1.iter().zip(v2.chunks_exact(bits_count)) {
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
        if size % bits_count != 0 {
            result += 1;
        }
        result * bytes_count
    }
}

impl BitsStoreType for u128 {
    fn xor_popcnt(v1: &[Self], v2: &[Self]) -> usize {
        debug_assert!(v1.len() == v2.len());

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

    fn xor_popcnt_scalar(v1: &[Self], v2: &[Self], bits_count: usize) -> usize {
        debug_assert!(v2.len() >= v1.len() * bits_count);

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        if std::arch::is_aarch64_feature_detected!("neon") {
            if bits_count == 8 {
                unsafe {
                    return impl_xor_popcnt_scalar8_neon_uint128(
                        v2.as_ptr().cast::<u8>(),
                        v1.as_ptr().cast::<u8>(),
                        v1.len() as u32,
                    ) as usize;
                }
            } else if bits_count == 4 {
                unsafe {
                    return impl_xor_popcnt_scalar4_neon_uint128(
                        v2.as_ptr().cast::<u8>(),
                        v1.as_ptr().cast::<u8>(),
                        v1.len() as u32,
                    ) as usize;
                }
            }
        }

        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("sse4.2") {
            if bits_count == 8 {
                unsafe {
                    return impl_xor_popcnt_scalar8_avx_uint128(
                        v2.as_ptr().cast::<u8>(),
                        v1.as_ptr().cast::<u8>(),
                        v1.len() as u32,
                    ) as usize;
                }
            } else if bits_count == 4 {
                unsafe {
                    return impl_xor_popcnt_scalar4_avx_uint128(
                        v2.as_ptr().cast::<u8>(),
                        v1.as_ptr().cast::<u8>(),
                        v1.len() as u32,
                    ) as usize;
                }
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if is_x86_feature_detected!("sse4.2") {
            if bits_count == 8 {
                unsafe {
                    return impl_xor_popcnt_scalar8_sse_uint128(
                        v2.as_ptr().cast::<u8>(),
                        v1.as_ptr().cast::<u8>(),
                        v1.len() as u32,
                    ) as usize;
                }
            } else if bits_count == 4 {
                unsafe {
                    return impl_xor_popcnt_scalar4_sse_uint128(
                        v2.as_ptr().cast::<u8>(),
                        v1.as_ptr().cast::<u8>(),
                        v1.len() as u32,
                    ) as usize;
                }
            }
        }

        let mut result = 0;
        for (&b1, b2_chunk) in v1.iter().zip(v2.chunks_exact(bits_count)) {
            for (i, &b2) in b2_chunk.iter().enumerate() {
                result += (b1 ^ b2).count_ones() << i;
            }
        }
        result as usize
    }

    fn get_storage_size(size: usize) -> usize {
        let bits_count = u8::BITS as usize * std::mem::size_of::<Self>();
        let mut result = size / bits_count;
        if size % bits_count != 0 {
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
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        debug_assert!(validate_vector_parameters(orig_data.clone(), vector_parameters).is_ok());

        let vector_stats = match encoding {
            Encoding::OneBit => None,
            Encoding::TwoBits | Encoding::OneAndHalfBits => {
                Some(VectorStats::build(orig_data.clone(), vector_parameters))
            }
        };

        for vector in orig_data {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let encoded_vector = Self::encode_vector(vector.as_ref(), &vector_stats, encoding);
            let encoded_vector_slice = encoded_vector.encoded_vector.as_slice();
            let bytes = transmute_to_u8_slice(encoded_vector_slice);
            storage_builder.push_vector_data(bytes);
        }

        Ok(Self {
            encoded_vectors: storage_builder.build(),
            metadata: Metadata {
                vector_parameters: vector_parameters.clone(),
                encoding,
                query_encoding,
            },
            vector_stats,
            bits_store_type: PhantomData,
        })
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
        let Some(vector_stats) = vector_stats else {
            debug_assert!(false, "Vector stats must be provided for two bits encoding");
            // If vector stats are not provided, we cannot encode two bits
            // So we fall back to one bit encoding
            return Self::encode_one_bit_vector(vector, encoded_vector);
        };

        let bits_count = u8::BITS as usize * std::mem::size_of::<TBitsStoreType>();
        let one = TBitsStoreType::one();
        for (i, &v) in vector.iter().enumerate() {
            let mean = vector_stats.elements_stats[i].mean;
            let sd = vector_stats.elements_stats[i].stddev;

            let (b1, b2) = Self::encode_two_bits_value(v, mean, sd);

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
        let Some(vector_stats) = vector_stats else {
            debug_assert!(false, "Vector stats must be provided for two bits encoding");
            // If vector stats are not provided, we cannot encode one and half bits
            // So we fall back to one bit encoding
            return Self::encode_one_bit_vector(vector, encoded_vector);
        };

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
        for (i, &v) in vector.iter().enumerate() {
            let mean = vector_stats.elements_stats[i].mean;
            let sd = vector_stats.elements_stats[i].stddev;
            let (b1, b2) = Self::encode_two_bits_value(v, mean, sd);

            if b1 {
                encoded_vector[i / bits_count] |= one << (i % bits_count);
            }
            if b2 {
                let j = vector.len() + i / 2;
                encoded_vector[j / bits_count] |= one << (j % bits_count);
            }
        }
    }

    fn encode_two_bits_value(value: f32, mean: f32, sd: f32) -> (bool, bool) {
        // Two bit encoding is a regular BQ with "zero".
        // It uses 2 bits per value and encodes values in the following way:
        // 00 - if the value is in the range [-2*sigma; -sigma);
        // 10 - if the value is in the range [-sigma; sigma);
        // 11 - if the value is in the range [sigma; 2*sigma];
        // where sigma is the standard deviation of the value.
        //
        // Scoring for 2bit quantization is the same as for 1bit quantization.

        if sd < f32::EPSILON {
            // If standard deviation is zero,
            // we cannot calculate z-score count so use regular BQ with zero-comparison.
            return (value > 0.0, false);
        }

        // How many ranges we want to divide the values into, it's 3:
        // [-2*sigma; -sigma), [-sigma; sigma), [sigma; 2*sigma)
        let ranges = 3;

        // Calculate z-score for the value
        let v_z = (value - mean) / sd;

        let min_border = -2.0; // -2*sigma
        let max_border = 2.0; // 2*sigma

        // Normalize z-score to the range [-2*sigma; 2*sigma]
        let normalized_z = (v_z - min_border) / (max_border - min_border);

        // Calculate index in the ranges list: [-2*sigma; -sigma), [-sigma; sigma), [sigma; 2*sigma)
        let index = normalized_z * (ranges as f32);

        // Index 0 and less is [0, 0] encoding
        // Index 1 is [1, 0] encoding
        // Index 2 and more is [1, 1] encoding
        if index >= 1.0 {
            let count_ones = (index.floor() as usize).min(2);
            (count_ones > 0, count_ones > 1)
        } else {
            (false, false)
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
            Encoding::OneBit => Self::encode_scalar_extended_query_vector(query, bits_count),
            Encoding::TwoBits => {
                let mut extended_query = query.to_vec();
                extended_query.extend_from_slice(query);
                Self::encode_scalar_extended_query_vector(&extended_query, bits_count)
            }
            Encoding::OneAndHalfBits => {
                let mut extended_query = query.to_vec();
                extended_query.extend(query.chunks(2).map(|v| {
                    if v.len() == 2 {
                        if v[0] > v[1] { v[0] } else { v[1] }
                    } else {
                        v[0]
                    }
                }));
                Self::encode_scalar_extended_query_vector(&extended_query, bits_count)
            }
        }
    }

    fn encode_scalar_extended_query_vector(
        query: &[f32],
        bits_count: usize,
    ) -> EncodedScalarVector<TBitsStoreType> {
        let encoded_query_size = TBitsStoreType::get_storage_size(query.len().max(1)) * bits_count;
        let mut encoded_query: Vec<TBitsStoreType> = vec![Default::default(); encoded_query_size];

        let max_abs_value = query.iter().map(|x| x.abs()).fold(0.0, f32::max);
        let max = max_abs_value;
        let min = -max_abs_value;

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
        v1: &[TBitsStoreType],
        v2: &[TBitsStoreType],
        v2_bits_count: usize,
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

        let xor_product = if v2_bits_count == 1 {
            TBitsStoreType::xor_popcnt(v1, v2) as f32
        } else {
            let xor_product = TBitsStoreType::xor_popcnt_scalar(v1, v2, v2_bits_count);
            (xor_product as f32) / (((1 << v2_bits_count) - 1) as f32)
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

    pub fn get_quantized_vector(&self, i: u32) -> &[u8] {
        self.encoded_vectors
            .get_vector_data(i as _, self.get_quantized_vector_size())
    }

    pub fn get_vector_parameters(&self) -> &VectorParameters {
        &self.metadata.vector_parameters
    }

    pub fn vectors_count(&self) -> usize {
        self.metadata.vector_parameters.count
    }

    fn get_vector_stats_path_from_meta_path(meta_path: &Path) -> Option<PathBuf> {
        let mut vector_stats_path = meta_path.parent()?.to_owned();
        vector_stats_path.push("vector_stats.json");
        Some(vector_stats_path)
    }

    pub fn encode_internal_query(&self, point_id: u32) -> EncodedQueryBQ<TBitsStoreType> {
        let encoded_data = self.get_quantized_vector(point_id).to_vec();
        EncodedQueryBQ::Binary(EncodedBinVector {
            encoded_vector: transmute_from_u8_to_slice(&encoded_data).to_vec(),
        })
    }
}

impl<TBitsStoreType: BitsStoreType, TStorage: EncodedStorage> EncodedVectors
    for EncodedVectorsBin<TBitsStoreType, TStorage>
{
    type EncodedQuery = EncodedQueryBQ<TBitsStoreType>;

    fn save(&self, data_path: &Path, meta_path: &Path) -> std::io::Result<()> {
        meta_path.parent().map(std::fs::create_dir_all);
        atomic_save_json(meta_path, &self.metadata)?;

        data_path.parent().map(std::fs::create_dir_all);
        self.encoded_vectors.save_to_file(data_path)?;

        let vector_stats_path =
            Self::get_vector_stats_path_from_meta_path(meta_path).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Failed to get vector stats path",
                )
            })?;
        if let Some(vector_stats) = &self.vector_stats {
            vector_stats_path.parent().map(std::fs::create_dir_all);
            atomic_save_json(&vector_stats_path, &vector_stats)?;
        } else if let Ok(true) = std::fs::exists(&vector_stats_path) {
            std::fs::remove_file(&vector_stats_path)?;
        }

        Ok(())
    }

    fn load(
        data_path: &Path,
        meta_path: &Path,
        vector_parameters: &VectorParameters,
    ) -> std::io::Result<Self> {
        let contents = std::fs::read_to_string(meta_path)?;
        let metadata: Metadata = serde_json::from_str(&contents)?;
        let quantized_vector_size =
            Self::get_quantized_vector_size_from_params(vector_parameters.dim, metadata.encoding);
        let encoded_vectors =
            TStorage::from_file(data_path, quantized_vector_size, vector_parameters.count)?;

        let vector_stats_path =
            Self::get_vector_stats_path_from_meta_path(meta_path).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Failed to get vector stats path",
                )
            })?;
        let vector_stats = match metadata.encoding {
            Encoding::OneBit => None,
            Encoding::TwoBits | Encoding::OneAndHalfBits => {
                let vector_stats_contents = std::fs::read_to_string(&vector_stats_path)?;
                let vector_stats: VectorStats = serde_json::from_str(&vector_stats_contents)?;
                Some(vector_stats)
            }
        };

        let result = Self {
            metadata,
            encoded_vectors,
            vector_stats,
            bits_store_type: PhantomData,
        };
        Ok(result)
    }

    fn is_on_disk(&self) -> bool {
        self.encoded_vectors.is_on_disk()
    }

    fn encode_query(&self, query: &[f32]) -> EncodedQueryBQ<TBitsStoreType> {
        debug_assert!(query.len() == self.metadata.vector_parameters.dim);
        Self::encode_query_vector(
            query,
            &self.vector_stats,
            self.metadata.encoding,
            self.metadata.query_encoding,
        )
    }

    fn score_point(
        &self,
        query: &EncodedQueryBQ<TBitsStoreType>,
        i: u32,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let vector_data_1 = self
            .encoded_vectors
            .get_vector_data(i as _, self.get_quantized_vector_size());

        let vector_data_usize_1 = transmute_from_u8_to_slice(vector_data_1);

        hw_counter.cpu_counter().incr_delta(vector_data_1.len());

        match query {
            EncodedQueryBQ::Binary(encoded_vector) => {
                self.calculate_metric(vector_data_usize_1, &encoded_vector.encoded_vector, 1)
            }
            EncodedQueryBQ::Scalar8bits(encoded_vector) => self.calculate_metric(
                vector_data_usize_1,
                &encoded_vector.encoded_vector,
                u8::BITS as usize,
            ),
            EncodedQueryBQ::Scalar4bits(encoded_vector) => self.calculate_metric(
                vector_data_usize_1,
                &encoded_vector.encoded_vector,
                u8::BITS as usize / 2,
            ),
        }
    }

    fn score_internal(&self, i: u32, j: u32, hw_counter: &HardwareCounterCell) -> f32 {
        let vector_data_1 = self
            .encoded_vectors
            .get_vector_data(i as _, self.get_quantized_vector_size());
        let vector_data_2 = self
            .encoded_vectors
            .get_vector_data(j as _, self.get_quantized_vector_size());

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

    fn encode_internal_vector(&self, id: u32) -> Option<EncodedQueryBQ<TBitsStoreType>> {
        Some(EncodedQueryBQ::Binary(EncodedBinVector {
            encoded_vector: transmute_from_u8_to_slice(
                self.encoded_vectors
                    .get_vector_data(id as _, self.get_quantized_vector_size()),
            )
            .to_vec(),
        }))
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
}
