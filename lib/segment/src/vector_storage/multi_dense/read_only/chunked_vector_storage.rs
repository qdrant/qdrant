use std::path::Path;

use common::bitvec::BitSlice;
use common::generic_consts::AccessPattern;
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::VectorStorageRead;
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::{
    DELETED_DIR_PATH, MultivectorMmapOffset, OFFSETS_DIR_PATH, VECTORS_DIR_PATH,
    flattened_to_multi_vector, read_multi_vector,
};

#[derive(Debug)]
pub struct ReadOnlyChunkedMultiDenseVectorStorage<T: PrimitiveVectorElement, S: UniversalRead> {
    vectors: ChunkedVectorsRead<T, S>,
    offsets: ChunkedVectorsRead<MultivectorMmapOffset, S>,
    /// Flags marking deleted vectors.
    deleted: InMemoryBitvecFlags,
    distance: Distance,
}

impl<T: PrimitiveVectorElement, S: UniversalRead> ReadOnlyChunkedMultiDenseVectorStorage<T, S> {
    /// Open the read-only counterpart of the appendable multi-dense storage at
    /// `path`, threading every file open through `fs`; reads the existing layout
    /// but creates and writes nothing. `populate` warms the vector and offset
    /// chunks.
    #[allow(dead_code)] // pending: read-only vector storage enum will use this
    pub fn open(
        fs: &S::Fs,
        path: &Path,
        dim: usize,
        distance: Distance,
        advice: AdviceSetting,
        populate: bool,
    ) -> OperationResult<Self> {
        let vectors = ChunkedVectorsRead::open(
            fs,
            &path.join(VECTORS_DIR_PATH),
            dim,
            advice,
            Some(populate),
        )?;

        // Offsets store one `MultivectorMmapOffset` element per point, so the
        // chunked storage dimensionality is 1.
        let offsets =
            ChunkedVectorsRead::open(fs, &path.join(OFFSETS_DIR_PATH), 1, advice, Some(populate))?;

        let deleted = InMemoryBitvecFlags::open::<S>(fs, &path.join(DELETED_DIR_PATH))?;

        Ok(Self {
            vectors,
            offsets,
            deleted,
            distance,
        })
    }
}

impl<T: PrimitiveVectorElement, S: UniversalRead> VectorStorageRead
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

    fn read_vectors<P: AccessPattern, U: Copy>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) {
        let point_offsets = keys
            .into_iter()
            .map(|(user_data, point_offset)| ((user_data, point_offset), point_offset));

        let vectors =
            super::iter_vectors::<P, _, _, _>(&self.offsets, &self.vectors, point_offsets);

        for ((user_data, point_offset), flattened) in vectors {
            let vector = CowVector::MultiDense(T::into_float_multivector(
                flattened_to_multi_vector(flattened, self.vectors.dim()),
            ));

            callback(user_data, point_offset, vector);
        }
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        read_multi_vector::<T, P, _>(&self.offsets, &self.vectors, key)
            .map(|multi| CowVector::MultiDense(T::into_float_multivector(multi)))
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted.get(key)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted.count()
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.as_bitslice()
    }
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::generic_consts::Random;
    use common::universal_io::{MmapFile, MmapFs};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::{
        MultiDenseVectorInternal, TypedMultiDenseVectorRef, VectorElementType, VectorRef,
    };
    use crate::types::MultiVectorConfig;
    use crate::vector_storage::VectorStorage;
    use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::open_appendable_memmap_multi_vector_storage_impl;

    /// Write multivectors (deleting ~10%) through the writable appendable
    /// storage, then reopen the same directory read-only and assert it mirrors
    /// the state — including per-point multivector contents, which exercises the
    /// offsets storage.
    #[test]
    fn read_only_chunked_multi_dense_round_trip() {
        const POINT_COUNT: PointOffsetType = 1000;
        const DIM: usize = 128;

        let dir = Builder::new().prefix("ro_multi_dense").tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(42);
        let hw = HardwareCounterCell::disposable();

        let multivectors: Vec<MultiDenseVectorInternal> = (0..POINT_COUNT)
            .map(|_| {
                let inner = rng.random_range(1..=4);
                let vectors = std::iter::repeat_with(|| {
                    std::iter::repeat_with(|| rng.random_range(-1.0..1.0))
                        .take(DIM)
                        .collect()
                })
                .take(inner)
                .collect::<Vec<Vec<VectorElementType>>>();
                MultiDenseVectorInternal::try_from(vectors).unwrap()
            })
            .collect();

        let mut deleted_ids = Vec::new();
        {
            let mut storage =
                open_appendable_memmap_multi_vector_storage_impl::<VectorElementType>(
                    dir.path(),
                    DIM,
                    Distance::Dot,
                    MultiVectorConfig::default(),
                    AdviceSetting::Global,
                    false,
                )
                .unwrap();
            for (id, multivec) in multivectors.iter().enumerate() {
                storage
                    .insert_vector(id as PointOffsetType, VectorRef::from(multivec), &hw)
                    .unwrap();
            }
            for id in 0..POINT_COUNT {
                if rng.random_bool(0.1) {
                    storage.delete_vector(id).unwrap();
                    deleted_ids.push(id);
                }
            }
            storage.flusher()().unwrap();
        }

        let storage = ReadOnlyChunkedMultiDenseVectorStorage::<VectorElementType, MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            Distance::Dot,
            AdviceSetting::Global,
            false,
        )
        .unwrap();

        assert_eq!(storage.total_vector_count(), POINT_COUNT as usize);
        assert_eq!(storage.distance(), Distance::Dot);
        assert_eq!(storage.deleted_vector_count(), deleted_ids.len());

        for id in 0..POINT_COUNT {
            assert_eq!(storage.is_deleted_vector(id), deleted_ids.contains(&id));

            let stored = storage.get_vector::<Random>(id);
            let multi: TypedMultiDenseVectorRef<VectorElementType> =
                stored.as_vec_ref().try_into().unwrap();
            assert_eq!(
                multi.to_owned(),
                multivectors[id as usize],
                "vector {id} mismatch",
            );
        }
    }
}
