use common::bitvec::{BitSlice, BitVec};
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;
use crate::vector_storage::{VectorOffsetType, VectorStorageRead};

#[derive(Debug)]
pub struct ReadOnlyChunkedDenseVectorStorage<T: PrimitiveVectorElement, S: UniversalRead<T>> {
    vectors: ChunkedVectorsRead<T, S>,
    /// Flags marking deleted vectors
    ///
    /// Structure grows dynamically, but may be smaller than actual number of vectors. Must not
    /// depend on its length.
    deleted: BitVec,
    distance: Distance,
    deleted_count: usize,
}

impl<T: PrimitiveVectorElement, S: UniversalRead<T>> VectorStorageRead
    for ReadOnlyChunkedDenseVectorStorage<T, S>
{
    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        T::datatype()
    }

    fn is_on_disk(&self) -> bool {
        self.vectors.is_on_disk()
    }

    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.vectors
            .get::<P>(key as VectorOffsetType)
            .map(|slice| CowVector::from(T::slice_to_float_cow(slice)))
            .expect("Vector not found")
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        self.vectors
            .get::<P>(key as VectorOffsetType)
            .map(|slice| CowVector::from(T::slice_to_float_cow(slice)))
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
