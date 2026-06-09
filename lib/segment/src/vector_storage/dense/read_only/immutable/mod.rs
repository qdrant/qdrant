use common::universal_io::UniversalRead;

use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::types::Distance;
use crate::vector_storage::dense::immutable_dense_vectors::ImmutableDenseVectorData;

mod lifecycle;
mod live_reload;
mod read_ops;

/// Read-only counterpart of the immutable (mmap) dense vector storage.
///
/// Vector data is immutable, so it is read straight from the shared
/// [`ImmutableDenseVectorData`] blob; deletions are tracked in memory and folded
/// from the live-reload delta.
#[derive(Debug)]
pub struct ReadOnlyImmutableDenseVectorStorage<T: PrimitiveVectorElement, S: UniversalRead> {
    vectors: ImmutableDenseVectorData<T, S>,
    /// Flags marking deleted vectors.
    deleted: InMemoryBitvecFlags,
    distance: Distance,
    /// Whether vector data is populated into RAM (drives `is_on_disk`).
    populate: bool,
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::generic_consts::Random;
    use common::sorted_slice::SortedSlice;
    use common::types::PointOffsetType;
    use common::universal_io::{MmapFile, MmapFs};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    use super::*;
    use crate::common::live_reload::LiveReload;
    use crate::data_types::vectors::{DenseVector, VectorElementType, VectorRef};
    use crate::segment_constructor::batched_reader::merge_from_single_source;
    use crate::vector_storage::dense::dense_vector_storage::open_dense_vector_storage;
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::{VectorStorage, VectorStorageRead};

    fn rand_vec(rng: &mut StdRng, dim: usize) -> DenseVector {
        std::iter::repeat_with(|| rng.random_range(-1.0..1.0))
            .take(dim)
            .collect()
    }

    /// Build the immutable storage (deleting some) from a staging storage, then
    /// reopen the same directory read-only and assert it mirrors the state.
    #[test]
    fn read_only_immutable_dense_round_trip() {
        const DIM: usize = 32;
        const POINT_COUNT: PointOffsetType = 300;

        let dir = Builder::new()
            .prefix("ro_immutable_dense")
            .tempdir()
            .unwrap();
        let mut rng = StdRng::seed_from_u64(42);
        let hw = HardwareCounterCell::disposable();

        let vectors: Vec<DenseVector> = (0..POINT_COUNT).map(|_| rand_vec(&mut rng, DIM)).collect();

        let mut deleted_ids = Vec::new();
        {
            let mut storage =
                open_dense_vector_storage(dir.path(), DIM, Distance::Dot, false).unwrap();
            let mut staging = new_volatile_dense_vector_storage(DIM, Distance::Dot);
            for (id, vector) in vectors.iter().enumerate() {
                staging
                    .insert_vector(id as PointOffsetType, VectorRef::from(vector), &hw)
                    .unwrap();
            }
            for id in (0..POINT_COUNT).step_by(11) {
                staging.delete_vector(id).unwrap();
                deleted_ids.push(id);
            }
            merge_from_single_source(&mut storage, &staging, POINT_COUNT).unwrap();
            storage.flusher()().unwrap();
        }

        let storage = ReadOnlyImmutableDenseVectorStorage::<VectorElementType, MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            Distance::Dot,
            false,
        )
        .unwrap();

        assert_eq!(storage.total_vector_count(), POINT_COUNT as usize);
        assert_eq!(storage.distance(), Distance::Dot);
        assert_eq!(storage.deleted_vector_count(), deleted_ids.len());

        // The immutable storage keeps deleted vectors' data, so every vector reads back.
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

    /// After `live_reload`, deletions made through the writer are reflected;
    /// vector data is immutable so nothing else changes.
    #[test]
    fn live_reload_picks_up_deletions() {
        const DIM: usize = 24;
        const POINT_COUNT: PointOffsetType = 200;

        let dir = Builder::new()
            .prefix("ro_immutable_dense_reload")
            .tempdir()
            .unwrap();
        let mut rng = StdRng::seed_from_u64(7);
        let hw = HardwareCounterCell::disposable();

        let vectors: Vec<DenseVector> = (0..POINT_COUNT).map(|_| rand_vec(&mut rng, DIM)).collect();

        let mut writer = open_dense_vector_storage(dir.path(), DIM, Distance::Dot, false).unwrap();
        {
            let mut staging = new_volatile_dense_vector_storage(DIM, Distance::Dot);
            for (id, vector) in vectors.iter().enumerate() {
                staging
                    .insert_vector(id as PointOffsetType, VectorRef::from(vector), &hw)
                    .unwrap();
            }
            merge_from_single_source(&mut writer, &staging, POINT_COUNT).unwrap();
            writer.flusher()().unwrap();
        }

        let mut reader = ReadOnlyImmutableDenseVectorStorage::<VectorElementType, MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            Distance::Dot,
            false,
        )
        .unwrap();
        assert_eq!(reader.deleted_vector_count(), 0);

        // Delete points through the writer, then feed the delta to the reader.
        let deleted_ids: Vec<PointOffsetType> = vec![1, 7, 42, 199];
        for &id in &deleted_ids {
            writer.delete_vector(id).unwrap();
        }
        writer.flusher()().unwrap();

        reader
            .live_reload(
                &MmapFs,
                &SortedSlice::new(&deleted_ids).unwrap(),
                &SortedSlice::new(&[]).unwrap(),
                &hw,
            )
            .unwrap();

        assert_eq!(reader.total_vector_count(), POINT_COUNT as usize);
        assert_eq!(reader.deleted_vector_count(), deleted_ids.len());
        for &id in &deleted_ids {
            assert!(reader.is_deleted_vector(id));
        }
        assert!(!reader.is_deleted_vector(0));
        assert!(!reader.is_deleted_vector(2));

        // Vector data is immutable: an untouched point still reads back correctly.
        let got: DenseVector = reader
            .get_vector::<Random>(2)
            .to_owned()
            .try_into()
            .unwrap();
        assert_eq!(got, vectors[2]);
    }
}
