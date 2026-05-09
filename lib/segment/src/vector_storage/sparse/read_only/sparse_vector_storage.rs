use common::bitvec::{BitSlice, BitVec};
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use gridstore::GridstoreReader;
use sparse::common::sparse_vector::SparseVector;

use crate::data_types::named_vectors::CowVector;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::VectorStorageRead;
use crate::vector_storage::sparse::SPARSE_VECTOR_DISTANCE;
use crate::vector_storage::sparse::stored_sparse_vectors::StoredSparseVector;

#[derive(Debug)]
pub struct ReadOnlySparseVectorStorage<S: UniversalRead> {
    storage: GridstoreReader<StoredSparseVector, S>,
    /// Flags marking deleted vectors
    ///
    /// Structure grows dynamically, but may be smaller than actual number of vectors. Must not
    /// depend on its length.
    deleted: BitVec,
    deleted_count: usize,
    next_point_offset: usize,
}

impl<S: UniversalRead> VectorStorageRead for ReadOnlySparseVectorStorage<S> {
    fn distance(&self) -> Distance {
        SPARSE_VECTOR_DISTANCE
    }

    fn datatype(&self) -> VectorStorageDatatype {
        VectorStorageDatatype::Float32
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn total_vector_count(&self) -> usize {
        self.next_point_offset
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.get_vector_opt::<P>(key)
            .unwrap_or_else(CowVector::default_sparse)
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        match self
            .storage
            .get_value::<P>(key, &HardwareCounterCell::disposable())
        {
            Ok(Some(stored)) => SparseVector::try_from(stored).ok().map(CowVector::from),
            _ => None,
        }
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted.get(key as usize).is_some_and(|bit| *bit)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.as_bitslice()
    }
}
