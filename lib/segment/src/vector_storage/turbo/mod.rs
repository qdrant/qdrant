//! TurboQuant-backed dense vector storage.
//!
//! [`TurboVectorStorage`] is a *primary* dense storage that keeps only the
//! TurboQuant (TQ) encoded vectors plus its own deletion state — as opposed to
//! the secondary `QuantizedVectorStorage` layer that sits beside a
//! full-precision storage.
//!
//! It intentionally implements only [`VectorStorageRead`] + [`VectorStorage`],
//! **not** `DenseVectorStorage<T>`.

// Scaffold: nothing constructs `TurboVectorStorage` yet. Remove once it is wired
// into `VectorStorageEnum::DenseTurbo`.
#![allow(dead_code)]

mod turbo_encoded_vectors;

use std::alloc::Layout;
use std::borrow::Cow;
use std::path::PathBuf;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;
use quantization::turboquant::quantization::TurboQuantizer;
use quantization::turboquant::{TQBits, TQMode};

use self::turbo_encoded_vectors::TurboEncodedVectorStorage;
use crate::common::Flusher;
use crate::common::flags::bitvec_flags::BitvecFlags;
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::VectorRef;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::{VectorStorage, VectorStorageRead};

// TurboQuant DataType (TQDT) always uses 4 bits without shift+scale error correction.
const TQDT_BITS: TQBits = TQBits::Bits4;
const TQDT_MODE: TQMode = TQMode::Normal;

/// Vector storage for TurboQuant encoded vectors.
pub struct TurboVectorStorage {
    /// Raw quantized storage over one of the three backends.
    storage: TurboEncodedVectorStorage,

    /// Quantizer used to de/quantize.
    quantizer: TurboQuantizer,

    /// Persisted flags marking which vectors are soft-deleted.
    deleted: BitvecFlags<MmapFile>,
    /// Number of vectors currently flagged as deleted.
    deleted_count: usize,

    /// Distance used for scoring / query preprocessing.
    distance: Distance,
    /// Original (un-padded) vector dimensionality.
    dim: usize,
}

impl TurboVectorStorage {
    /// Bytes used by all available (non-deleted) vectors in their encoded form.
    pub fn size_of_available_vectors_in_bytes(&self) -> usize {
        // TODO: available_vector_count() * quantizer.quantized_size().
        unimplemented!("TODO: encoded size of available vectors")
    }

    /// Memory layout of a single encoded vector.
    pub fn quantized_vector_layout(&self) -> OperationResult<Layout> {
        // TODO: build from quantized_vector_size() with the encoding alignment.
        unimplemented!("TODO: layout of one encoded vector")
    }

    /// Raw encoded vector blob for one vector (no dequantization/lloyd lookup).
    pub fn get_quantized_vector(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        self.storage.get_quantized_vector(key)
    }

    /// Add the given vectors to the storage.
    ///
    /// Inherent method (mirrors [`VectorStorageEnum::update_from`]) rather than a
    /// `VectorStorage` trait method: `update_from` now lives on the per-kind
    /// sub-traits, and `TurboVectorStorage` deliberately implements neither
    /// `DenseVectorStorage<T>` nor the others.
    ///
    /// # Returns
    /// The range of point offsets that were added to the storage.
    ///
    /// If stopped, the operation returns a cancellation error.
    pub fn update_from<'a>(
        &mut self,
        _other_vectors: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        _stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        // TODO: encode each incoming f32 vector and propagate deleted flags.
        unimplemented!("TODO: encode vectors from another storage (optimize)")
    }
}

impl VectorStorageRead for TurboVectorStorage {
    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        unimplemented!("TODO: datatype of TurboVectorStorage not yet decided")
    }

    fn is_on_disk(&self) -> bool {
        self.storage.is_on_disk()
    }

    fn total_vector_count(&self) -> usize {
        self.storage.vectors_count()
    }

    fn get_vector<P: AccessPattern>(&self, _key: PointOffsetType) -> CowVector<'_> {
        // TODO: dequantize the encoded blob into `dim` f32 -> CowVector::Dense.
        unimplemented!("TODO: dequantize encoded vector to f32")
    }

    fn get_vector_opt<P: AccessPattern>(&self, _key: PointOffsetType) -> Option<CowVector<'_>> {
        // TODO: as get_vector, returning None when the key is out of range.
        unimplemented!("TODO: dequantize encoded vector to f32 (optional)")
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted.get(key)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.get_bitslice()
    }
}

impl VectorStorage for TurboVectorStorage {
    fn insert_vector(
        &mut self,
        _key: PointOffsetType,
        _vector: VectorRef,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // TODO: encode the f32 vector via `self.encoded.upsert_vector` (live insert).
        unimplemented!("TODO: encode and insert a single vector")
    }

    fn flusher(&self) -> Flusher {
        // TODO: combine `self.encoded.flusher()` and `self.deleted.flusher()`.
        unimplemented!("TODO: flush encoded storage + deleted flags")
    }

    fn files(&self) -> Vec<PathBuf> {
        // Encoded blob + quantized.meta.json + the mutable deleted flags.
        let mut files = self.storage.files();
        files.extend(self.deleted.files());
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        // Encoded blob + meta are immutable; deleted.dat is not.
        self.storage.immutable_files()
    }

    fn delete_vector(&mut self, _key: PointOffsetType) -> OperationResult<bool> {
        // TODO: set the bit in `self.deleted` and bump `self.deleted_count`.
        unimplemented!("TODO: flag a vector as deleted")
    }
}
