use common::universal_io::UniversalRead;

use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::types::Distance;
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;

mod immutable;
mod lifecycle;
mod live_reload;
mod read_ops;

pub use immutable::ReadOnlyImmutableDenseVectorStorage;

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
    use common::sorted_slice::SortedSlice;
    use common::types::PointOffsetType;
    use common::universal_io::{MmapFile, MmapFs, Populate};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    use super::*;
    use crate::common::live_reload::LiveReload;
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
            Populate::No,
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

    /// After `live_reload`, the read-only view reflects appends and deletions.
    #[test]
    fn live_reload_picks_up_appends_and_deletions() {
        const DIM: usize = 64;
        let dir = Builder::new().prefix("ro_dense_reload").tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(7);
        let hw = HardwareCounterCell::disposable();

        let rand_vec = |rng: &mut StdRng| -> DenseVector {
            std::iter::repeat_with(|| rng.random_range(-1.0..1.0))
                .take(DIM)
                .collect()
        };
        let first: Vec<DenseVector> = (0..200).map(|_| rand_vec(&mut rng)).collect();
        let second: Vec<DenseVector> = (0..150).map(|_| rand_vec(&mut rng)).collect();

        let mut writer = open_appendable_memmap_vector_storage_impl::<VectorElementType>(
            dir.path(),
            DIM,
            Distance::Dot,
            AdviceSetting::Global,
            false,
        )
        .unwrap();
        for (id, vector) in first.iter().enumerate() {
            writer
                .insert_vector(id as PointOffsetType, VectorRef::from(vector), &hw)
                .unwrap();
        }
        writer.flusher()().unwrap();

        let mut reader = ReadOnlyChunkedDenseVectorStorage::<VectorElementType, MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            Distance::Dot,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();
        assert_eq!(reader.total_vector_count(), first.len());

        // Append new vectors and delete a few existing ones through the writer.
        for (offset, vector) in second.iter().enumerate() {
            writer
                .insert_vector(
                    (first.len() + offset) as PointOffsetType,
                    VectorRef::from(vector),
                    &hw,
                )
                .unwrap();
        }
        let deleted_ids: Vec<PointOffsetType> = vec![3, 50, 199];
        for &id in &deleted_ids {
            writer.delete_vector(id).unwrap();
        }
        writer.flusher()().unwrap();

        let new_ids: Vec<PointOffsetType> = (first.len()..first.len() + second.len())
            .map(|offset| offset as PointOffsetType)
            .collect();
        reader
            .live_reload(
                &MmapFs,
                &SortedSlice::new(&deleted_ids).unwrap(),
                &SortedSlice::new(&new_ids).unwrap(),
                &hw,
            )
            .unwrap();

        assert_eq!(reader.total_vector_count(), first.len() + second.len());
        assert_eq!(reader.deleted_vector_count(), deleted_ids.len());

        // The appended vector is now visible and correct.
        let got: DenseVector = reader
            .get_vector::<Random>(first.len() as PointOffsetType)
            .to_owned()
            .try_into()
            .unwrap();
        assert_eq!(got, second[0]);

        // Deletions are reflected; untouched points stay live.
        for &id in &deleted_ids {
            assert!(reader.is_deleted_vector(id));
        }
        assert!(!reader.is_deleted_vector(0));
    }

    /// A point can be appended live while one of its named vectors is missing:
    /// the writer stores a placeholder and deletes that vector slot. The
    /// deletion is recorded only in the on-disk flags file, never in the
    /// id-tracker delta, so `live_reload` must read it back for the appended
    /// offset instead of assuming every appended point is live.
    #[test]
    fn live_reload_picks_up_appended_vector_deletion() {
        const DIM: usize = 4;
        let dir = Builder::new()
            .prefix("ro_dense_appended_deleted")
            .tempdir()
            .unwrap();
        let hw = HardwareCounterCell::disposable();

        let mut writer = open_appendable_memmap_vector_storage_impl::<VectorElementType>(
            dir.path(),
            DIM,
            Distance::Dot,
            AdviceSetting::Global,
            false,
        )
        .unwrap();
        writer
            .insert_vector(0, VectorRef::from(&vec![1.0; DIM]), &hw)
            .unwrap();
        writer.flusher()().unwrap();

        let mut reader = ReadOnlyChunkedDenseVectorStorage::<VectorElementType, MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            Distance::Dot,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();

        // Append offset 1 as a placeholder, then delete its vector slot.
        writer
            .insert_vector(1, VectorRef::from(&vec![0.0; DIM]), &hw)
            .unwrap();
        writer.delete_vector(1).unwrap();
        writer.flusher()().unwrap();

        let deleted_ids: Vec<PointOffsetType> = vec![];
        let new_ids: Vec<PointOffsetType> = vec![1];
        reader
            .live_reload(
                &MmapFs,
                &SortedSlice::new(&deleted_ids).unwrap(),
                &SortedSlice::new(&new_ids).unwrap(),
                &hw,
            )
            .unwrap();

        assert_eq!(reader.total_vector_count(), 2);
        assert!(reader.is_deleted_vector(1));
    }
}
