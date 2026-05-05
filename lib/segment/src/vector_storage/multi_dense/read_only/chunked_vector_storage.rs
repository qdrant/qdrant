use std::borrow::Cow;

use common::bitvec::{BitSlice, BitVec};
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, UniversalRead};

use crate::data_types::named_vectors::{CowMultiVector, CowVector};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{TypedMultiDenseVector, TypedMultiDenseVectorRef};
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::MultivectorMmapOffset;
use crate::vector_storage::{VectorOffset, VectorOffsetType, VectorStorageRead};

#[derive(Debug)]
pub struct ReadOnlyChunkedMultiDenseVectorStorage<T: PrimitiveVectorElement, S: UniversalRead<T>> {
    vectors: ChunkedVectorsRead<T, S>,
    offsets: ChunkedVectorsRead<MultivectorMmapOffset, MmapFile>,
    /// Flags marking deleted vectors
    ///
    /// Structure grows dynamically, but may be smaller than actual number of vectors. Must not
    /// depend on its length.
    deleted: BitVec,
    distance: Distance,
    deleted_count: usize,
}

impl<T: PrimitiveVectorElement, S: UniversalRead<T>> VectorStorageRead
    for ReadOnlyChunkedMultiDenseVectorStorage<T, S>
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
        self.offsets.len()
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.get_vector_opt::<P>(key).expect("vector not found")
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        self.offsets
            .get::<P>(key as VectorOffsetType)
            .and_then(|mmap_offset| {
                let mmap_offset = mmap_offset.first().expect("mmap_offset must not be empty");
                self.vectors
                    .get_many::<P>(mmap_offset.offset(), mmap_offset.multi_vector_count())
            })
            .map(|flattened_vectors| {
                let multi = match flattened_vectors {
                    Cow::Borrowed(slice) => CowMultiVector::Borrowed(TypedMultiDenseVectorRef {
                        flattened_vectors: slice,
                        dim: self.vectors.dim(),
                    }),
                    Cow::Owned(vec) => CowMultiVector::Owned(TypedMultiDenseVector {
                        flattened_vectors: vec,
                        dim: self.vectors.dim(),
                    }),
                };
                CowVector::MultiDense(T::into_float_multivector(multi))
            })
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
