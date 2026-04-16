pub mod lloyd_max;
mod permutation;
pub mod quantization;
pub mod rotation;

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

use crate::EncodingError;
use crate::encoded_storage::{EncodedStorage, EncodedStorageBuilder};
use crate::encoded_vectors::{EncodedVectors, VectorParameters, validate_vector_parameters};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TQBits {
    Bits4,
    Bits2,
    Bits1_5,
    Bits1,
}

impl TQBits {
    #[inline]
    pub fn get_centroids(&self) -> &'static [f32] {
        let bit_size = self.bit_size();
        lloyd_max::get_centroids(bit_size)
    }

    #[inline]
    pub fn get_centroid_boundaries(&self) -> &'static [f32] {
        let bit_size = self.bit_size();
        lloyd_max::get_centroid_boundaries(bit_size)
    }

    #[inline]
    pub fn bit_size(&self) -> u8 {
        match self {
            TQBits::Bits4 => 4,
            TQBits::Bits2 => 2,
            TQBits::Bits1_5 => {
                unimplemented!()
            }
            TQBits::Bits1 => 1,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TQMode {
    Normal,
    Plus,
}

pub struct EncodedVectorsTQ<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    metadata_path: Option<PathBuf>,
}

/// Encoded query type for Turbo Quant.
pub struct EncodedQueryTQ {}

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    pub vector_parameters: VectorParameters,
    pub bits: TQBits,
    pub mode: TQMode,
}

impl<TStorage: EncodedStorage> EncodedVectorsTQ<TStorage> {
    pub fn storage(&self) -> &TStorage {
        &self.encoded_vectors
    }

    /// Encode vector data
    ///
    /// # Arguments
    /// * `data` - iterator over original vector data
    /// * `storage_builder` - encoding result storage builder
    /// * `vector_parameters` - parameters of original vector data (dimension, distance, etc)
    /// * `count` - number of vectors in `data` iterator
    /// * `bits` - bits for quantization
    /// * `mode` - quantization mode
    /// * `meta_path` - optional path to save metadata, if `None`, metadata will not be saved
    /// * `stopped` - Atomic bool that indicates if encoding should be stopped
    #[allow(clippy::too_many_arguments)]
    pub fn encode<'a>(
        data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
        mut storage_builder: impl EncodedStorageBuilder<Storage = TStorage>,
        vector_parameters: &VectorParameters,
        _count: usize,
        bits: TQBits,
        mode: TQMode,
        meta_path: Option<&Path>,
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        debug_assert!(validate_vector_parameters(data.clone(), vector_parameters).is_ok());

        for vector in data {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let encoded_vector: Vec<u8> = Self::encode_vector(vector.as_ref(), bits, mode);

            storage_builder
                .push_vector_data(&encoded_vector)
                .map_err(|e| {
                    EncodingError::EncodingError(format!("Failed to push encoded vector: {e}",))
                })?;
        }

        let encoded_vectors = storage_builder
            .build()
            .map_err(|e| EncodingError::EncodingError(format!("Failed to build storage: {e}",)))?;

        let metadata = Metadata {
            vector_parameters: vector_parameters.clone(),
            bits,
            mode,
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

    // Get quantized vector size in bytes
    pub fn get_quantized_vector_size(
        _vector_parameters: &VectorParameters,
        _bits: TQBits,
        _mode: TQMode,
    ) -> usize {
        todo!()
    }

    fn encode_vector(_vector_data: &[f32], _bits: TQBits, _mode: TQMode) -> Vec<u8> {
        todo!()
    }

    pub fn get_quantized_vector(&self, i: PointOffsetType) -> Cow<'_, [u8]> {
        self.encoded_vectors.get_vector_data(i)
    }

    pub fn layout(&self) -> Layout {
        Layout::from_size_align(self.quantized_vector_size(), align_of::<f32>()).unwrap()
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

    fn encode_query(&self, _query: &[f32]) -> EncodedQueryTQ {
        EncodedQueryTQ {}
    }

    fn score_point(
        &self,
        query: &EncodedQueryTQ,
        i: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let encoded_vector = self.encoded_vectors.get_vector_data(i);
        self.score_bytes(True, query, &encoded_vector, hw_counter)
    }

    /// Score two points inside endoded data by their indexes
    fn score_internal(
        &self,
        i: PointOffsetType,
        j: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let v1 = self.encoded_vectors.get_vector_data(i);
        let v2 = self.encoded_vectors.get_vector_data(j);

        hw_counter.vector_io_read().incr_delta(v1.len() + v2.len());

        todo!()
    }

    fn quantized_vector_size(&self) -> usize {
        Self::get_quantized_vector_size(
            &self.metadata.vector_parameters,
            self.metadata.bits,
            self.metadata.mode,
        )
    }

    fn encode_internal_vector(&self, _id: PointOffsetType) -> Option<EncodedQueryTQ> {
        // Turbo quant is asymmetric, so we cannot encode internal vectors, only queries.
        // This method is used for symmetric quantization,
        // where we can encode internal vectors without access to original vector data,
        // which may require disk access.
        None
    }

    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[f32],
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        let encoded_vector = Self::encode_vector(vector, self.metadata.bits, self.metadata.mode);
        self.encoded_vectors.upsert_vector(
            id,
            bytemuck::cast_slice(encoded_vector.as_slice()),
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
        _query: &Self::EncodedQuery,
        bytes: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        hw_counter.cpu_counter().incr_delta(bytes.len());

        todo!()
    }
}
