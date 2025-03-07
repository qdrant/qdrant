use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use io::file_operations::atomic_save_json;
use serde::{Deserialize, Serialize};

use crate::{
    DistanceType, EncodedStorage, EncodedStorageBuilder, EncodedVectors, EncodingError,
    VectorParameters,
};

const DIM_ALIGNMENT: usize = 32;

pub struct EncodedVectorsRQ<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
}

pub struct EncodedQueryRQ {
    /// `- delta * sum(encoded_u4) / 2 - dim * min / 2`
    pub q_precomputed: f32,
    pub min: f32,
    pub delta: f32,
    pub encoded: Vec<u32>,
}

struct EncodedVector {
    /// `2 / (dot(original_vector, quantized_vector) * sqrt(dim))`
    v_precomputed: f32,
    bits_count: f32,
    encoded: *const u32,
}

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    vector_parameters: VectorParameters,
    aligned_dim: usize,
    inv_sqrt_dim: f32,
    transform: Option<Vec<Vec<f32>>>,
}

impl<TStorage: EncodedStorage> EncodedVectorsRQ<TStorage> {
    pub fn encode<'a>(
        data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone + Send,
        mut storage_builder: impl EncodedStorageBuilder<TStorage> + Send,
        vector_parameters: &VectorParameters,
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        assert_eq!(vector_parameters.distance_type, DistanceType::Dot);
        let aligned_dim = vector_parameters.dim.next_multiple_of(DIM_ALIGNMENT);
        let inv_sqrt_dim = 1.0 / (vector_parameters.dim as f32).sqrt();

        let mut encoded_vector = vec![0u8; 8 + aligned_dim / u8::BITS as usize];
        for vector in data {
            if stopped.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            encoded_vector.iter_mut().for_each(|x| *x = 0);
            let mut bits_count = 0.0f32;
            let mut dot_q_o = 0.0f32;

            for (chunk_index, chunk) in vector.as_ref().chunks(u8::BITS as usize).enumerate() {
                let mut encoded_chunk = 0u8;
                for (shift, &value) in chunk.iter().enumerate() {
                    dot_q_o += value.abs() * inv_sqrt_dim;
                    let quantized = if value >= 0.0 {
                        bits_count += 1.0;
                        1
                    } else {
                        0
                    };
                    encoded_chunk |= quantized << shift;
                }
                encoded_vector[8 + chunk_index] = encoded_chunk;
            }

            let v_precomputed: f32 = 2.0 / (dot_q_o * (vector_parameters.dim as f32).sqrt());

            let v_precomputed = v_precomputed.to_ne_bytes();
            encoded_vector[0] = v_precomputed[0];
            encoded_vector[1] = v_precomputed[1];
            encoded_vector[2] = v_precomputed[2];
            encoded_vector[3] = v_precomputed[3];

            let bits_count = bits_count.to_ne_bytes();
            encoded_vector[4] = bits_count[0];
            encoded_vector[5] = bits_count[1];
            encoded_vector[6] = bits_count[2];
            encoded_vector[7] = bits_count[3];

            storage_builder.push_vector_data(&encoded_vector);
        }

        Ok(Self {
            encoded_vectors: storage_builder.build(),
            metadata: Metadata {
                vector_parameters: vector_parameters.clone(),
                aligned_dim,
                inv_sqrt_dim,
                transform: None,
            },
        })
    }

    pub fn get_quantized_vector_size(vector_parameters: &VectorParameters) -> usize {
        2 * std::mem::size_of::<f32>() + vector_parameters.dim.next_multiple_of(DIM_ALIGNMENT) / 8
    }

    /// Decompose the encoded vector into
    fn decompose_vector(&self, i: usize) -> EncodedVector {
        let vector_data_size = self.metadata.aligned_dim / 8 + 2 * std::mem::size_of::<f32>();
        let vector = self
            .encoded_vectors
            .get_vector_data(i as usize, vector_data_size);
        let v_precomputed =
            f32::from_ne_bytes(vector[0..4].try_into().expect("Invalid vector data"));
        let bits_count = f32::from_ne_bytes(vector[4..8].try_into().expect("Invalid vector data"));
        EncodedVector {
            v_precomputed,
            bits_count,
            encoded: vector[8..].as_ptr().cast(),
        }
    }

    fn quantized_dot(mut vector: *const u32, mut query: *const u32, dim: usize) -> f32 {
        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx512vpopcntdq") && is_x86_feature_detected!("avx512vl") {
            return unsafe {
                impl_and_popcnt_m128i_avx512(query.cast(), vector.cast(), dim as u32)
            };
        }

        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("sse4.2") {
            return unsafe { impl_and_popcnt_m128i_sse(query.cast(), vector.cast(), dim as u32) };
        }

        let mut result = 0usize;
        for _ in 0..dim / u32::BITS as usize {
            let v = unsafe { *vector };
            vector = unsafe { vector.add(1) };

            let q1 = unsafe { *query };
            result += (v & q1).count_ones() as usize;

            let q2 = unsafe { *query.add(1) };
            result += ((v & q2).count_ones() as usize) << 1;

            let q3 = unsafe { *query.add(2) };
            result += ((v & q3).count_ones() as usize) << 2;

            let q4 = unsafe { *query.add(3) };
            result += ((v & q4).count_ones() as usize) << 3;

            query = unsafe { query.add(4) };
        }
        result as f32
    }
}

impl<TStorage: EncodedStorage> EncodedVectors<EncodedQueryRQ> for EncodedVectorsRQ<TStorage> {
    fn save(
        &self,
        data_path: &std::path::Path,
        meta_path: &std::path::Path,
    ) -> std::io::Result<()> {
        meta_path.parent().map(std::fs::create_dir_all);
        atomic_save_json(meta_path, &self.metadata)?;

        data_path.parent().map(std::fs::create_dir_all);
        self.encoded_vectors.save_to_file(data_path)?;
        Ok(())
    }

    fn load(
        data_path: &std::path::Path,
        meta_path: &std::path::Path,
        vector_parameters: &VectorParameters,
    ) -> std::io::Result<Self> {
        let contents = std::fs::read_to_string(meta_path)?;
        let metadata: Metadata = serde_json::from_str(&contents)?;
        let quantized_vector_size = Self::get_quantized_vector_size(vector_parameters);
        let encoded_vectors =
            TStorage::from_file(data_path, quantized_vector_size, vector_parameters.count)?;
        let result = Self {
            encoded_vectors,
            metadata,
        };
        Ok(result)
    }

    fn encode_query(&self, query: &[f32]) -> EncodedQueryRQ {
        let min = query.iter().copied().fold(f32::INFINITY, f32::min);
        let max = query.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let delta = (max - min) / 15.0;
        // Each element is encoded as `u4`. The number of encoded elements is `dim / 2` bytes.
        let encoded_vector_bytes = self.metadata.aligned_dim / 2;
        let mut encoded: Vec<u32> = vec![0; encoded_vector_bytes / std::mem::size_of::<u32>()];
        let mut encoded_sum = 0usize;
        for (chunk_index, chunk) in query.chunks(u32::BITS as usize).enumerate() {
            for (shift, value) in chunk.iter().enumerate() {
                let shifted_value = value - min;
                let delted_value = shifted_value / delta;
                let rounded_value = delted_value.round();
                let quantized = rounded_value as u32 % 16;
                encoded_sum += quantized as usize;
                for b in 0..4 {
                    encoded[4 * chunk_index + b] |= ((quantized >> b) & 0b1) << shift;
                }
            }
        }

        let q_precomputed = -delta * encoded_sum as f32 / 2.0
            - self.metadata.vector_parameters.dim as f32 * min / 2.0;
        EncodedQueryRQ {
            q_precomputed,
            min,
            delta,
            encoded,
        }
    }

    fn score_point(&self, query: &EncodedQueryRQ, i: u32, hw_counter: &HardwareCounterCell) -> f32 {
        hw_counter
            .cpu_counter()
            .incr_delta(self.metadata.aligned_dim);

        let EncodedVector {
            v_precomputed,
            bits_count,
            encoded,
        } = self.decompose_vector(i as usize);
        let d = Self::quantized_dot(encoded, query.encoded.as_ptr(), self.metadata.aligned_dim);
        v_precomputed * (query.delta * d + query.min * bits_count + query.q_precomputed)
    }

    fn score_internal(&self, i: u32, j: u32, hw_counter: &HardwareCounterCell) -> f32 {
        hw_counter
            .cpu_counter()
            .incr_delta(self.metadata.aligned_dim);

        let EncodedVector {
            encoded: mut encoded_i,
            ..
        } = self.decompose_vector(i as usize);
        let EncodedVector {
            encoded: mut encoded_j,
            ..
        } = self.decompose_vector(j as usize);

        let mut xor_product = 0usize;
        for _ in 0..self.metadata.aligned_dim / u32::BITS as usize {
            let decoded_i = unsafe { *encoded_i };
            let decoded_j = unsafe { *encoded_j };
            let xor = decoded_i ^ decoded_j;
            xor_product += xor.count_ones() as usize;
            encoded_i = unsafe { encoded_i.add(1) };
            encoded_j = unsafe { encoded_j.add(1) };
        }
        let zeros_count = self.metadata.vector_parameters.dim as f32 - xor_product as f32;
        (zeros_count as f32 - xor_product as f32) / self.metadata.vector_parameters.dim as f32
    }
}

#[cfg(target_arch = "x86_64")]
extern "C" {
    fn impl_and_popcnt_m128i_avx512(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
    fn impl_and_popcnt_m128i_sse(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
}
