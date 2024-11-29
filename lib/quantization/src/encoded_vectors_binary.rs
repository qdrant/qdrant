use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use serde::{Deserialize, Serialize};

use crate::encoded_vectors::validate_vector_parameters;
use crate::utils::{transmute_from_u8_to_slice, transmute_to_u8_slice};
use crate::{
    DistanceType, EncodedStorage, EncodedStorageBuilder, EncodedVectors, EncodingError,
    VectorParameters,
};

pub struct EncodedVectorsBin<TBitsStoreType: BitsStoreType, TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    bits_store_type: PhantomData<TBitsStoreType>,
}

pub struct EncodedBinVector<TBitsStoreType: BitsStoreType> {
    encoded_vector: Vec<TBitsStoreType>,
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    vector_parameters: VectorParameters,
}

pub trait BitsStoreType:
    Default
    + Copy
    + Clone
    + core::ops::BitOrAssign
    + std::ops::Shl<usize, Output = Self>
    + num_traits::identities::One
{
    /// Xor vectors and return the number of bits set to 1
    ///
    /// Assume that `v1` and `v2` are aligned to `BITS_STORE_TYPE_SIZE` with both with zeros
    /// So it does not affect the resulting number of bits set to 1
    fn xor_popcnt(v1: &[Self], v2: &[Self]) -> usize;

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

    fn get_storage_size(size: usize) -> usize {
        let bits_count = 8 * std::mem::size_of::<Self>();
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
    pub fn encode<'a>(
        orig_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
        mut storage_builder: impl EncodedStorageBuilder<TStorage>,
        vector_parameters: &VectorParameters,
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        debug_assert!(validate_vector_parameters(orig_data.clone(), vector_parameters).is_ok());

        for vector in orig_data {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let encoded_vector = Self::encode_vector(vector.as_ref());
            let encoded_vector_slice = encoded_vector.encoded_vector.as_slice();
            let bytes = transmute_to_u8_slice(encoded_vector_slice);
            storage_builder.push_vector_data(bytes);
        }

        Ok(Self {
            encoded_vectors: storage_builder.build(),
            metadata: Metadata {
                vector_parameters: vector_parameters.clone(),
            },
            bits_store_type: PhantomData,
        })
    }

    fn encode_vector(vector: &[f32]) -> EncodedBinVector<TBitsStoreType> {
        let mut encoded_vector =
            vec![Default::default(); TBitsStoreType::get_storage_size(vector.len())];

        let bits_count = u8::BITS as usize * std::mem::size_of::<TBitsStoreType>();
        let one = TBitsStoreType::one();
        for (i, &v) in vector.iter().enumerate() {
            // flag is true if the value is positive
            // It's expected that the vector value is in range [-1; 1]
            if v > 0.0 {
                encoded_vector[i / bits_count] |= one << (i % bits_count);
            }
        }

        EncodedBinVector { encoded_vector }
    }

    pub fn get_quantized_vector_size_from_params(vector_parameters: &VectorParameters) -> usize {
        TBitsStoreType::get_storage_size(vector_parameters.dim)
            * std::mem::size_of::<TBitsStoreType>()
    }

    fn get_quantized_vector_size(&self) -> usize {
        Self::get_quantized_vector_size_from_params(&self.metadata.vector_parameters)
    }

    fn calculate_metric(&self, v1: &[TBitsStoreType], v2: &[TBitsStoreType]) -> f32 {
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

        let xor_product = TBitsStoreType::xor_popcnt(v1, v2) as f32;

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
}

impl<TBitsStoreType: BitsStoreType, TStorage: EncodedStorage>
    EncodedVectors<EncodedBinVector<TBitsStoreType>>
    for EncodedVectorsBin<TBitsStoreType, TStorage>
{
    fn save(&self, data_path: &Path, meta_path: &Path) -> std::io::Result<()> {
        let metadata_bytes = serde_json::to_vec(&self.metadata)?;
        meta_path.parent().map(std::fs::create_dir_all);
        std::fs::write(meta_path, metadata_bytes)?;

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
        let quantized_vector_size = Self::get_quantized_vector_size_from_params(vector_parameters);
        let encoded_vectors =
            TStorage::from_file(data_path, quantized_vector_size, vector_parameters.count)?;
        let result = Self {
            metadata,
            encoded_vectors,
            bits_store_type: PhantomData,
        };
        Ok(result)
    }

    fn encode_query(&self, query: &[f32]) -> EncodedBinVector<TBitsStoreType> {
        debug_assert!(query.len() == self.metadata.vector_parameters.dim);
        Self::encode_vector(query)
    }

    fn score_point(
        &self,
        query: &EncodedBinVector<TBitsStoreType>,
        i: u32,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let vector_data_1 = self
            .encoded_vectors
            .get_vector_data(i as _, self.get_quantized_vector_size());
        let vector_data_usize_1 = transmute_from_u8_to_slice(vector_data_1);

        hw_counter
            .cpu_counter()
            .incr_delta(query.encoded_vector.len());

        self.calculate_metric(vector_data_usize_1, &query.encoded_vector)
    }

    fn score_internal(&self, i: u32, j: u32, hw_counter: &HardwareCounterCell) -> f32 {
        let vector_data_1 = self
            .encoded_vectors
            .get_vector_data(i as _, self.get_quantized_vector_size());
        let vector_data_2 = self
            .encoded_vectors
            .get_vector_data(j as _, self.get_quantized_vector_size());

        let vector_data_usize_1 = transmute_from_u8_to_slice(vector_data_1);
        let vector_data_usize_2 = transmute_from_u8_to_slice(vector_data_2);

        hw_counter
            .cpu_counter()
            .incr_delta(vector_data_usize_2.len());

        self.calculate_metric(vector_data_usize_1, vector_data_usize_2)
    }
}

#[cfg(target_arch = "x86_64")]
extern "C" {
    fn impl_xor_popcnt_sse_uint128(query_ptr: *const u8, vector_ptr: *const u8, count: u32) -> u32;

    fn impl_xor_popcnt_sse_uint64(query_ptr: *const u8, vector_ptr: *const u8, count: u32) -> u32;

    fn impl_xor_popcnt_sse_uint32(query_ptr: *const u8, vector_ptr: *const u8, count: u32) -> u32;
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
extern "C" {
    fn impl_xor_popcnt_neon_uint128(query_ptr: *const u8, vector_ptr: *const u8, count: u32)
        -> u32;

    fn impl_xor_popcnt_neon_uint64(query_ptr: *const u8, vector_ptr: *const u8, count: u32) -> u32;
}
