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
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;
use crate::vector_storage::dense::appendable_dense_vector_storage::{
    DELETED_DIR_PATH, VECTORS_DIR_PATH,
};
use crate::vector_storage::{VectorOffsetType, VectorStorageRead};

#[derive(Debug)]
pub struct ReadOnlyChunkedDenseVectorStorage<T: PrimitiveVectorElement, S: UniversalRead> {
    vectors: ChunkedVectorsRead<T, S>,
    /// Flags marking deleted vectors.
    deleted: InMemoryBitvecFlags,
    distance: Distance,
}

impl<T: PrimitiveVectorElement, S: UniversalRead> ReadOnlyChunkedDenseVectorStorage<T, S> {
    /// Open the read-only counterpart of the appendable dense storage at `path`,
    /// threading every file open through `fs`; reads the existing layout but
    /// creates and writes nothing. `populate` warms the vector chunks.
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

        let deleted = InMemoryBitvecFlags::open::<S>(fs, &path.join(DELETED_DIR_PATH))?;

        Ok(Self {
            vectors,
            deleted,
            distance,
        })
    }
}

impl<T: PrimitiveVectorElement, S: UniversalRead> VectorStorageRead
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

    fn read_vectors<P: AccessPattern, U: Copy>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) {
        let keys = keys
            .into_iter()
            .map(|(user_data, point_offset)| ((user_data, point_offset), point_offset, 1));

        for ((user_data, point_offset), vector) in self.vectors.iter_vectors::<P, _>(keys) {
            let vector = CowVector::from(T::slice_to_float_cow(vector));
            callback(user_data, point_offset, vector);
        }
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        self.vectors
            .get::<P>(key as VectorOffsetType)
            .map(|slice| CowVector::from(T::slice_to_float_cow(slice)))
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
    use crate::data_types::vectors::{DenseVector, VectorElementType, VectorRef};
    use crate::vector_storage::VectorStorage;
    use crate::vector_storage::dense::appendable_dense_vector_storage::open_appendable_memmap_vector_storage_impl;

    /// Write vectors (deleting ~10%) through the writable appendable storage,
    /// then reopen the same directory read-only and assert it mirrors the state.
    #[test]
    fn read_only_chunked_dense_round_trip() {
        const POINT_COUNT: PointOffsetType = 2500; // spans more than one chunk
        const DIM: usize = 128;

        let dir = Builder::new().prefix("ro_dense").tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(42);
        let hw = HardwareCounterCell::disposable();

        let vectors: Vec<DenseVector> = (0..POINT_COUNT)
            .map(|_| {
                std::iter::repeat_with(|| rng.random_range(-1.0..1.0))
                    .take(DIM)
                    .collect()
            })
            .collect();

        let mut deleted_ids = Vec::new();
        {
            let mut storage = open_appendable_memmap_vector_storage_impl::<VectorElementType>(
                dir.path(),
                DIM,
                Distance::Dot,
                AdviceSetting::Global,
                false,
            )
            .unwrap();
            for (id, vector) in vectors.iter().enumerate() {
                storage
                    .insert_vector(id as PointOffsetType, VectorRef::from(vector), &hw)
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

        let storage = ReadOnlyChunkedDenseVectorStorage::<VectorElementType, MmapFile>::open(
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

            let got: DenseVector = storage
                .get_vector::<Random>(id)
                .to_owned()
                .try_into()
                .unwrap();
            assert_eq!(got, vectors[id as usize], "vector {id} mismatch");
        }
    }
}
