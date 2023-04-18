use std::cmp::Ordering;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use bitvec::prelude::BitSlice;
use ordered_float::OrderedFloat;

use super::memmap_vector_storage::MemmapVectorStorage;
use super::quantized::quantized_vectors_base::QuantizedVectorsStorage;
use super::simple_vector_storage::SimpleVectorStorage;
use crate::common::Flusher;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::types::{Distance, PointOffsetType, QuantizationConfig, ScoreType};

#[derive(Copy, Clone, PartialEq, Debug, Default)]
pub struct ScoredPointOffset {
    pub idx: PointOffsetType,
    pub score: ScoreType,
}

impl Eq for ScoredPointOffset {}

impl Ord for ScoredPointOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        OrderedFloat(self.score).cmp(&OrderedFloat(other.score))
    }
}

impl PartialOrd for ScoredPointOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Trait for vector storage
/// El - type of vector element, expected numerical type
/// Storage operates with internal IDs (`PointOffsetType`), which always starts with zero and have no skips
pub trait VectorStorage {
    fn vector_dim(&self) -> usize;

    fn distance(&self) -> Distance;

    /// Number of vectors
    ///
    /// - includes soft deleted vectors, as they are still stored
    fn total_vector_count(&self) -> usize;

    /// Get the number of available vectors, considering deleted points and vectors
    ///
    /// This uses [`total_vector_count`] and [`deleted_vec_count`] internally.
    ///
    /// # Warning
    ///
    /// This number may not always be accurate. See warning in [`deleted_vec_count`] documentation.
    fn available_vec_count(&self) -> usize {
        self.total_vector_count()
            .saturating_sub(self.deleted_vec_count())
    }

    /// Number of all stored vectors including deleted
    fn get_vector(&self, key: PointOffsetType) -> &[VectorElementType];

    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: &[VectorElementType],
    ) -> OperationResult<()>;

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut dyn Iterator<Item = PointOffsetType>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>>;

    fn flusher(&self) -> Flusher;

    // Generate quantized vectors and store them on disk
    fn quantize(
        &mut self,
        data_path: &Path,
        quantization_config: &QuantizationConfig,
    ) -> OperationResult<()>;

    // Load quantized vectors from disk
    fn load_quantization(&mut self, data_path: &Path) -> OperationResult<()>;

    fn quantized_storage(&self) -> Option<&QuantizedVectorsStorage>;

    fn files(&self) -> Vec<PathBuf>;

    /// Flag the vector by the given key as deleted
    /// Returns true if the vector was not deleted before and is now deleted
    fn delete_vec(&mut self, key: PointOffsetType) -> OperationResult<bool>;

    /// Check whether the vector at the given key is flagged as deleted
    fn is_deleted_vec(&self, key: PointOffsetType) -> bool;

    /// Get the number of deleted vectors, considering deleted points and vectors
    ///
    /// Vectors may be deleted at two levels, as point or as vector. Deleted points should
    /// propagate to deleting the vectors. That means that the deleted vector count includes the
    /// number of deleted points as well.
    ///
    /// # Warning
    ///
    /// In some very exceptional cases it is possible for this count not to include some deleted
    /// points. That may happen when flushing a segment to disk fails. This should be recovered
    /// when loading/recovering the segment, but that isn't guaranteed. You should therefore use
    /// the deleted count with care.
    fn deleted_vec_count(&self) -> usize;

    /// Get [`BitSlice`] representation for deleted vectors with deletion flags
    ///
    /// The size of this slice is not guaranteed. It may be smaller/larger than the number of
    /// vectors in this segment.
    fn deleted_vec_bitslice(&self) -> &BitSlice;
}

pub enum VectorStorageEnum {
    Simple(SimpleVectorStorage),
    Memmap(Box<MemmapVectorStorage>),
}

impl VectorStorage for VectorStorageEnum {
    fn vector_dim(&self) -> usize {
        match self {
            VectorStorageEnum::Simple(v) => v.vector_dim(),
            VectorStorageEnum::Memmap(v) => v.vector_dim(),
        }
    }

    fn distance(&self) -> Distance {
        match self {
            VectorStorageEnum::Simple(v) => v.distance(),
            VectorStorageEnum::Memmap(v) => v.distance(),
        }
    }

    fn total_vector_count(&self) -> usize {
        match self {
            VectorStorageEnum::Simple(v) => v.total_vector_count(),
            VectorStorageEnum::Memmap(v) => v.total_vector_count(),
        }
    }

    fn get_vector(&self, key: PointOffsetType) -> &[VectorElementType] {
        match self {
            VectorStorageEnum::Simple(v) => v.get_vector(key),
            VectorStorageEnum::Memmap(v) => v.get_vector(key),
        }
    }

    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: &[VectorElementType],
    ) -> OperationResult<()> {
        match self {
            VectorStorageEnum::Simple(v) => v.insert_vector(key, vector),
            VectorStorageEnum::Memmap(v) => v.insert_vector(key, vector),
        }
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut dyn Iterator<Item = PointOffsetType>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        match self {
            VectorStorageEnum::Simple(v) => v.update_from(other, other_ids, stopped),
            VectorStorageEnum::Memmap(v) => v.update_from(other, other_ids, stopped),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            VectorStorageEnum::Simple(v) => v.flusher(),
            VectorStorageEnum::Memmap(v) => v.flusher(),
        }
    }

    fn quantize(
        &mut self,
        data_path: &Path,
        quantization_config: &QuantizationConfig,
    ) -> OperationResult<()> {
        match self {
            VectorStorageEnum::Simple(v) => v.quantize(data_path, quantization_config),
            VectorStorageEnum::Memmap(v) => v.quantize(data_path, quantization_config),
        }
    }

    fn load_quantization(&mut self, data_path: &Path) -> OperationResult<()> {
        match self {
            VectorStorageEnum::Simple(v) => v.load_quantization(data_path),
            VectorStorageEnum::Memmap(v) => v.load_quantization(data_path),
        }
    }

    fn quantized_storage(&self) -> Option<&QuantizedVectorsStorage> {
        match self {
            VectorStorageEnum::Simple(v) => v.quantized_storage(),
            VectorStorageEnum::Memmap(v) => v.quantized_storage(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            VectorStorageEnum::Simple(v) => v.files(),
            VectorStorageEnum::Memmap(v) => v.files(),
        }
    }

    fn delete_vec(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        match self {
            VectorStorageEnum::Simple(v) => v.delete_vec(key),
            VectorStorageEnum::Memmap(v) => v.delete_vec(key),
        }
    }

    fn is_deleted_vec(&self, key: PointOffsetType) -> bool {
        match self {
            VectorStorageEnum::Simple(v) => v.is_deleted_vec(key),
            VectorStorageEnum::Memmap(v) => v.is_deleted_vec(key),
        }
    }

    fn deleted_vec_count(&self) -> usize {
        match self {
            VectorStorageEnum::Simple(v) => v.deleted_vec_count(),
            VectorStorageEnum::Memmap(v) => v.deleted_vec_count(),
        }
    }

    fn deleted_vec_bitslice(&self) -> &BitSlice {
        match self {
            VectorStorageEnum::Simple(v) => v.deleted_vec_bitslice(),
            VectorStorageEnum::Memmap(v) => v.deleted_vec_bitslice(),
        }
    }
}
