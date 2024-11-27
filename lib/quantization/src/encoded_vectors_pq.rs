#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use std::arch::aarch64::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
use std::iter::repeat_with;
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};

use common::counter::hardware_counter::HardwareCounterCell;
use serde::{Deserialize, Serialize};

use crate::encoded_storage::{EncodedStorage, EncodedStorageBuilder};
use crate::encoded_vectors::{validate_vector_parameters, EncodedVectors, VectorParameters};
use crate::kmeans::kmeans;
use crate::{ConditionalVariable, EncodingError};

pub const KMEANS_SAMPLE_SIZE: usize = 10_000;
pub const KMEANS_MAX_ITERATIONS: usize = 100;
pub const KMEANS_ACCURACY: f32 = 1e-5;
pub const CENTROIDS_COUNT: usize = 256;

pub struct EncodedVectorsPQ<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
}

/// PQ lookup table
/// Lookup table is a distance from each query chunk to
/// each centroid related to this chunk
pub struct EncodedQueryPQ {
    lut: Vec<f32>,
}

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    pub centroids: Vec<Vec<f32>>,
    pub vector_division: Vec<Range<usize>>,
    pub vector_parameters: VectorParameters,
}

impl<TStorage: EncodedStorage> EncodedVectorsPQ<TStorage> {
    /// Encode vector data using product quantization.
    ///
    /// # Arguments
    /// * `data` - iterator over original vector data
    /// * `storage_builder` - encoding result storage builder
    /// * `vector_parameters` - parameters of original vector data (dimension, distance, etc)
    /// * `chunk_size` - Max size of f32 chunk that replaced by centroid index (in original vector dimension)
    /// * `max_threads` - Max allowed threads for kmeans and encodind process
    /// * `stop_condition` - Function that returns `true` if encoding should be stopped
    pub fn encode<'a>(
        data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone + Send,
        mut storage_builder: impl EncodedStorageBuilder<TStorage> + Send,
        vector_parameters: &VectorParameters,
        chunk_size: usize,
        max_kmeans_threads: usize,
        stop_condition: impl Fn() -> bool + Sync,
    ) -> Result<Self, EncodingError> {
        debug_assert!(validate_vector_parameters(data.clone(), vector_parameters).is_ok());

        // first, divide vector into chunks
        let vector_division = Self::get_vector_division(vector_parameters.dim, chunk_size);

        // then, find flattened centroid positions
        let centroids = Self::find_centroids(
            data.clone(),
            &vector_division,
            vector_parameters,
            CENTROIDS_COUNT,
            max_kmeans_threads,
            &stop_condition,
        )?;

        // finally, encode data
        Self::encode_storage(
            data,
            &mut storage_builder,
            &vector_division,
            &centroids,
            max_kmeans_threads,
            &stop_condition,
        )?;

        let storage = storage_builder.build();

        if !stop_condition() {
            Ok(Self {
                encoded_vectors: storage,
                metadata: Metadata {
                    centroids,
                    vector_division,
                    vector_parameters: vector_parameters.clone(),
                },
            })
        } else {
            Err(EncodingError::Stopped)
        }
    }

    pub fn get_quantized_vector_size(
        vector_parameters: &VectorParameters,
        chunk_size: usize,
    ) -> usize {
        (0..vector_parameters.dim).step_by(chunk_size).count()
    }

    fn get_vector_division(dim: usize, chunk_size: usize) -> Vec<Range<usize>> {
        (0..dim)
            .step_by(chunk_size)
            .map(|i| i..std::cmp::min(i + chunk_size, dim))
            .collect()
    }

    /// Encode whole storage
    ///
    /// # Arguments
    /// * `data` - Original vector data iterator
    /// * `storage_builder` - Builder of encoded data container
    /// * `vector_division` - Division of original vector into chunks
    /// * `centroids` - Centroid positions (flattened by chunks; for similarity to vector data format)
    /// * `max_threads` - Max allowed threads for encoding process
    /// * `stop_condition` - Function that returns `true` if encoding should be stopped
    ///
    /// # Lifetimes
    /// 'a is lifetime of vector in vector storage
    /// 'b is lifetime of parent scope
    fn encode_storage<'a: 'b, 'b>(
        data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone + Send + 'b,
        storage_builder: &'b mut (impl EncodedStorageBuilder<TStorage> + Send),
        vector_division: &'b [Range<usize>],
        centroids: &'b [Vec<f32>],
        max_threads: usize,
        stop_condition: &(impl Fn() -> bool + Sync),
    ) -> Result<(), EncodingError> {
        rayon::ThreadPoolBuilder::new()
            .thread_name(|idx| format!("pq-encoding-{idx}"))
            .num_threads(std::cmp::max(1, max_threads))
            .build()
            .map_err(|e| {
                EncodingError::EncodingError(format!(
                    "Failed PQ encoding while thread pool init: {e}"
                ))
            })?
            .scope(|s| {
                Self::encode_storage_rayon(
                    s,
                    data,
                    storage_builder,
                    vector_division,
                    centroids,
                    max_threads,
                    &stop_condition,
                )
            });
        Ok(())
    }

    /// Encode whole storage inside rayon context
    /// This function should be called inside `rayon::scope`
    fn encode_storage_rayon<'a: 'b, 'b>(
        scope: &rayon::Scope<'b>,
        data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone + Send + 'b,
        storage_builder: &'b mut (impl EncodedStorageBuilder<TStorage> + Send),
        vector_division: &'b [Range<usize>],
        centroids: &'b [Vec<f32>],
        max_threads: usize,
        stop_condition: &'b (impl Fn() -> bool + Sync),
    ) {
        let storage_builder = Arc::new(Mutex::new(storage_builder));

        // Synchronization between threads. Use conditional variable for
        // each thread. Each condvar is blocked instead of first.
        // While encoding, thread `N` after `storage_builder` usage blocks themself and
        // unblock thread `N+1`.
        // In summary, access to `storage_builder` is ordered by `thread_index` below.
        let mut condvars: Vec<ConditionalVariable> =
            repeat_with(Default::default).take(max_threads).collect();
        condvars[0].notify(); // Allow first thread to use storage

        for thread_index in 0..max_threads {
            // Thread process vectors `N` that `(N + thread_index) % max_threads == 0`.
            let data = data.clone().skip(thread_index);
            let storage_builder = storage_builder.clone();
            let condvar = condvars[thread_index].clone();
            let next_condvar = condvars[(thread_index + 1) % max_threads].clone();

            scope.spawn(move |_| {
                let mut encoded_vector = Vec::with_capacity(vector_division.len());
                for vector in data.step_by(max_threads) {
                    if stop_condition() {
                        return;
                    }

                    Self::encode_vector(
                        vector.as_ref(),
                        vector_division,
                        centroids,
                        &mut encoded_vector,
                    );
                    // wait for permission from prev thread to use storage
                    let is_disconnected = condvar.wait();
                    // push encoded vector to storage
                    storage_builder
                        .lock()
                        .unwrap()
                        .push_vector_data(&encoded_vector);
                    // Notify next thread to use storage
                    next_condvar.notify();
                    if is_disconnected {
                        return;
                    }
                }
            });
        }
        // free condvars to allow threads to exit when panicking
        condvars.clear();
    }

    /// Encode single vector from `&[f32]` into `&[u8]`.
    /// This method divides `vector_data` into chunks, for each chunk
    /// finds nearest centroid and replace whole chunk by nearest centroid index.
    ///
    /// # Arguments
    /// * `vector_data` - Original vector data
    /// * `vector_division` - Division of original vector into chunks
    /// * `centroids` - Centroid positions (flattened by chunks; for similarity to vector data format)
    /// * `encoded_vector` - Encoded result as a preallocated vector
    fn encode_vector(
        vector_data: &[f32],
        vector_division: &[Range<usize>],
        centroids: &[Vec<f32>],
        encoded_vector: &mut Vec<u8>,
    ) {
        encoded_vector.clear();
        for range in vector_division {
            let subvector_data = &vector_data[range.clone()];
            let mut min_distance = f32::MAX;
            let mut min_centroid_index = 0;
            for (centroid_index, centroid) in centroids.iter().enumerate() {
                // because centroids are flattened by chunks, take centroid position using `range`
                let centroid_data = &centroid[range.clone()];
                // by product quantization algorithm use euclid metric for any similarity function
                let distance = subvector_data
                    .iter()
                    .zip(centroid_data)
                    .map(|(a, b)| (a - b).powi(2))
                    .sum();
                if distance < min_distance {
                    min_distance = distance;
                    min_centroid_index = centroid_index;
                }
            }
            // encoding, replace whole chunk `range` by one `u8` index of nearest centroid
            encoded_vector.push(min_centroid_index as u8);
        }
    }

    /// Encode single vector from `&[f32]` into `&[u8]`.
    /// This method divides `vector_data` into chunks, for each chunk
    /// finds nearest centroid and replace whole chunk by nearest centroid index.
    ///
    /// # Arguments
    /// * `data` - Original vector data
    /// * `vector_division` - Division of original vector into chunks
    /// * `vector_parameters` - parameters of original vector data (dimension, distance, etc)
    /// * `centroids_count` - Count of centroids for each chunk
    /// * `max_kmeans_threads` - Max allowed threads for kmeans process
    /// * `stop_condition` - Function that returns `true` if encoding should be stopped
    fn find_centroids<'a>(
        data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
        vector_division: &[Range<usize>],
        vector_parameters: &VectorParameters,
        centroids_count: usize,
        max_kmeans_threads: usize,
        stop_condition: &(impl Fn() -> bool + Sync),
    ) -> Result<Vec<Vec<f32>>, EncodingError> {
        let sample_size = KMEANS_SAMPLE_SIZE.min(vector_parameters.count);
        let mut result = vec![vec![]; centroids_count];

        // if there are not enough vectors, set centroids as point positions
        if vector_parameters.count <= centroids_count {
            for (i, vector_data) in data.into_iter().enumerate() {
                result[i] = vector_data.as_ref().to_vec();
            }
            // fill empty centroids just with zeros
            result[vector_parameters.count..centroids_count].fill(vec![0.0; vector_parameters.dim]);
            return Ok(result);
        }

        // find random subset of data as random non-intersected indexes
        let permutor = permutation_iterator::Permutor::new(vector_parameters.count as u64);
        let mut selected_vectors: Vec<usize> =
            permutor.map(|i| i as usize).take(sample_size).collect();
        if stop_condition() {
            return Err(EncodingError::Stopped);
        }

        selected_vectors.sort_unstable();

        // find centroids for each chunk
        for range in vector_division.iter() {
            // take data subset using indexes from
            let mut data_subset = Vec::with_capacity(sample_size * range.len());
            let mut selected_index: usize = 0;
            for (vector_index, vector_data) in data.clone().enumerate() {
                let vector_data = vector_data.as_ref();
                if vector_index == selected_vectors[selected_index] {
                    data_subset.extend_from_slice(&vector_data[range.clone()]);
                    selected_index += 1;
                    if selected_index == sample_size {
                        break;
                    }
                }
            }

            let centroids = kmeans(
                &data_subset,
                centroids_count,
                range.len(),
                KMEANS_MAX_ITERATIONS,
                max_kmeans_threads,
                KMEANS_ACCURACY,
                stop_condition,
            )?;

            // push found chunk centroids into result
            for (centroid_index, centroid_data) in centroids.chunks_exact(range.len()).enumerate() {
                result[centroid_index].extend_from_slice(centroid_data);
            }
        }

        Ok(result)
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.1")]
    unsafe fn score_point_sse(&self, query: &EncodedQueryPQ, i: u32) -> f32 {
        let centroids = self
            .encoded_vectors
            .get_vector_data(i as usize, self.metadata.vector_division.len());
        let len = centroids.len();
        let centroids_count = self.metadata.centroids.len();

        let mut centroids = centroids.as_ptr();
        let mut lut = query.lut.as_ptr();
        let mut sum128: __m128 = _mm_setzero_ps();
        for _ in 0..len / 4 {
            let buffer = [
                *lut.add(*centroids as usize),
                *lut.add(centroids_count + *centroids.add(1) as usize),
                *lut.add(2 * centroids_count + *centroids.add(2) as usize),
                *lut.add(3 * centroids_count + *centroids.add(3) as usize),
            ];
            let c = _mm_loadu_ps(buffer.as_ptr());
            sum128 = _mm_add_ps(sum128, c);

            centroids = centroids.add(4);
            lut = lut.add(4 * centroids_count);
        }
        let sum64: __m128 = _mm_add_ps(sum128, _mm_movehl_ps(sum128, sum128));
        let sum32: __m128 = _mm_add_ss(sum64, _mm_shuffle_ps(sum64, sum64, 0x55));
        let mut sum = _mm_cvtss_f32(sum32);

        for _ in 0..len % 4 {
            sum += *lut.add(*centroids as usize);
            centroids = centroids.add(1);
            lut = lut.add(centroids_count);
        }
        sum
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    unsafe fn score_point_neon(&self, query: &EncodedQueryPQ, i: u32) -> f32 {
        let centroids = self
            .encoded_vectors
            .get_vector_data(i as usize, self.metadata.vector_division.len());
        let len = centroids.len();
        let centroids_count = self.metadata.centroids.len();

        let mut centroids = centroids.as_ptr();
        let mut lut = query.lut.as_ptr();
        let mut sum128 = vdupq_n_f32(0.);
        for _ in 0..len / 4 {
            let buffer = [
                *lut.add(*centroids as usize),
                *lut.add(centroids_count + *centroids.add(1) as usize),
                *lut.add(2 * centroids_count + *centroids.add(2) as usize),
                *lut.add(3 * centroids_count + *centroids.add(3) as usize),
            ];
            let c = vld1q_f32(buffer.as_ptr());
            sum128 = vaddq_f32(sum128, c);

            centroids = centroids.add(4);
            lut = lut.add(4 * centroids_count);
        }
        let mut sum = vaddvq_f32(sum128);

        for _ in 0..len % 4 {
            sum += *lut.add(*centroids as usize);
            centroids = centroids.add(1);
            lut = lut.add(centroids_count);
        }
        sum
    }

    fn score_point_simple(&self, query: &EncodedQueryPQ, i: u32) -> f32 {
        let centroids = self
            .encoded_vectors
            .get_vector_data(i as usize, self.metadata.vector_division.len());
        let len = centroids.len();
        let centroids_count = self.metadata.centroids.len();

        let mut centroids = centroids.as_ptr();
        let mut lut = query.lut.as_ptr();

        (0..len)
            .map(|_| unsafe {
                let value = *lut.add(*centroids as usize);
                centroids = centroids.add(1);
                lut = lut.add(centroids_count);
                value
            })
            .sum()
    }

    pub fn get_quantized_vector(&self, i: u32) -> &[u8] {
        self.encoded_vectors
            .get_vector_data(i as _, self.metadata.vector_division.len())
    }

    pub fn get_metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn vectors_count(&self) -> usize {
        self.metadata.vector_parameters.count
    }
}

impl<TStorage: EncodedStorage> EncodedVectors<EncodedQueryPQ> for EncodedVectorsPQ<TStorage> {
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
        let quantized_vector_size = metadata.vector_division.len();
        let encoded_vectors =
            TStorage::from_file(data_path, quantized_vector_size, vector_parameters.count)?;
        let result = Self {
            encoded_vectors,
            metadata,
        };
        Ok(result)
    }

    fn encode_query(&self, query: &[f32]) -> EncodedQueryPQ {
        let lut_capacity = self.metadata.vector_division.len() * self.metadata.centroids.len();
        let mut lut = Vec::with_capacity(lut_capacity);
        for range in &self.metadata.vector_division {
            let subquery = &query[range.clone()];
            for i in 0..self.metadata.centroids.len() {
                let centroid = &self.metadata.centroids[i];
                let subcentroid = &centroid[range.clone()];
                let distance = self
                    .metadata
                    .vector_parameters
                    .distance_type
                    .distance(subquery, subcentroid);
                let distance = if self.metadata.vector_parameters.invert {
                    -distance
                } else {
                    distance
                };
                lut.push(distance);
            }
        }
        EncodedQueryPQ { lut }
    }

    fn score_point(&self, query: &EncodedQueryPQ, i: u32, hw_counter: &HardwareCounterCell) -> f32 {
        hw_counter
            .cpu_counter()
            .incr_delta(self.metadata.vector_division.len());

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if is_x86_feature_detected!("sse4.1") {
            return unsafe { self.score_point_sse(query, i) };
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        if std::arch::is_aarch64_feature_detected!("neon") {
            return unsafe { self.score_point_neon(query, i) };
        }

        self.score_point_simple(query, i)
    }

    /// Score two points inside endoded data by their indexes
    /// To find score, this method decode both encoded vectors.
    /// Decocing in PQ is a replacing centroid index by centroid position
    fn score_internal(&self, i: u32, j: u32, hw_counter: &HardwareCounterCell) -> f32 {
        let centroids_i = self
            .encoded_vectors
            .get_vector_data(i as usize, self.metadata.vector_division.len());
        let centroids_j = self
            .encoded_vectors
            .get_vector_data(j as usize, self.metadata.vector_division.len());

        hw_counter.cpu_counter().incr_delta(
            centroids_i.len()
            // Chunk size
                * self
                    .metadata
                    .vector_division
                    .first()
                    .map(|i| i.len())
                    .unwrap_or(1),
        );

        let distance: f32 = centroids_i
            .iter()
            .zip(centroids_j)
            .enumerate()
            .map(|(range_index, (&c_i, &c_j))| {
                let range = &self.metadata.vector_division[range_index];
                // get centroid positions and calculate distance as distance between centroids
                let data_i = &self.metadata.centroids[c_i as usize][range.clone()];
                let data_j = &self.metadata.centroids[c_j as usize][range.clone()];
                self.metadata
                    .vector_parameters
                    .distance_type
                    .distance(data_i, data_j)
            })
            .sum();
        if self.metadata.vector_parameters.invert {
            -distance
        } else {
            distance
        }
    }
}
