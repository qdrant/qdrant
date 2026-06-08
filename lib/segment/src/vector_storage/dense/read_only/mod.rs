use common::universal_io::UniversalRead;

use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::types::Distance;
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;

mod lifecycle;
mod read_ops;

#[derive(Debug)]
pub struct ReadOnlyChunkedDenseVectorStorage<T: PrimitiveVectorElement, S: UniversalRead> {
    vectors: ChunkedVectorsRead<T, S>,
    /// Flags marking deleted vectors.
    deleted: InMemoryBitvecFlags,
    distance: Distance,
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::generic_consts::Random;
    use common::mmap::AdviceSetting;
    use common::types::PointOffsetType;
    use common::universal_io::{MmapFile, MmapFs};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::{DenseVector, VectorElementType, VectorRef};
    use crate::vector_storage::dense::appendable_dense_vector_storage::open_appendable_memmap_vector_storage_impl;
    use crate::vector_storage::{VectorStorage, VectorStorageRead};

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
