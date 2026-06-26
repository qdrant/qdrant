use std::borrow::Cow;

use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::appendable_mmap_multi_dense_vector_storage::MultivectorMmapOffset;
use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::types::{Distance, MultiVectorConfig};
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;

mod lifecycle;
mod live_reload;
mod read_ops;

#[derive(Debug)]
pub struct ReadOnlyChunkedMultiDenseVectorStorage<T: PrimitiveVectorElement, S: UniversalRead> {
    vectors: ChunkedVectorsRead<T, S>,
    offsets: ChunkedVectorsRead<MultivectorMmapOffset, S>,
    /// Flags marking deleted vectors.
    deleted: InMemoryBitvecFlags,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
}

pub fn iter_vectors<'a, P, T, U, S>(
    offsets: &'a ChunkedVectorsRead<MultivectorMmapOffset, S>,
    vectors: &'a ChunkedVectorsRead<T, S>,
    keys: impl IntoIterator<Item = (U, PointOffsetType)>,
) -> impl Iterator<Item = (U, Cow<'a, [T]>)>
where
    P: AccessPattern,
    T: PrimitiveVectorElement,
    S: UniversalRead,
{
    let point_offsets = keys
        .into_iter()
        .map(|(user_data, point_offset)| (user_data, point_offset as _, 1));

    let vector_offsets =
        offsets
            .iter_vectors::<P, _>(point_offsets)
            .map(|(user_data, multi_offset)| {
                let &[multi_offset] = multi_offset.as_ref() else {
                    unreachable!("multi-vector offsets are stored as vectors of length 1");
                };

                let MultivectorMmapOffset {
                    offset,
                    count,
                    capacity: _,
                } = multi_offset;

                (user_data, offset, count)
            });

    vectors.iter_vectors::<P, _>(vector_offsets)
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::generic_consts::Random;
    use common::mmap::AdviceSetting;
    use common::sorted_slice::SortedSlice;
    use common::universal_io::{MmapFile, MmapFs, Populate};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    use super::*;
    use crate::common::live_reload::LiveReload;
    use crate::data_types::vectors::{
        MultiDenseVectorInternal, TypedMultiDenseVectorRef, VectorElementType, VectorRef,
    };
    use crate::types::MultiVectorConfig;
    use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::open_appendable_memmap_multi_vector_storage_impl;
    use crate::vector_storage::{VectorStorage, VectorStorageRead};

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
            MultiVectorConfig::default(),
            AdviceSetting::Global,
            Populate::No,
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

    /// After `live_reload`, the read-only view reflects appends and deletions.
    #[test]
    fn live_reload_picks_up_appends_and_deletions() {
        const DIM: usize = 48;
        let dir = Builder::new().prefix("ro_multi_reload").tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(11);
        let hw = HardwareCounterCell::disposable();

        let rand_multi = |rng: &mut StdRng| -> MultiDenseVectorInternal {
            let inner = rng.random_range(1..=3);
            let vectors = std::iter::repeat_with(|| {
                std::iter::repeat_with(|| rng.random_range(-1.0..1.0))
                    .take(DIM)
                    .collect()
            })
            .take(inner)
            .collect::<Vec<Vec<VectorElementType>>>();
            MultiDenseVectorInternal::try_from(vectors).unwrap()
        };
        let first: Vec<MultiDenseVectorInternal> = (0..150).map(|_| rand_multi(&mut rng)).collect();
        let second: Vec<MultiDenseVectorInternal> =
            (0..100).map(|_| rand_multi(&mut rng)).collect();

        let mut writer = open_appendable_memmap_multi_vector_storage_impl::<VectorElementType>(
            dir.path(),
            DIM,
            Distance::Dot,
            MultiVectorConfig::default(),
            AdviceSetting::Global,
            false,
        )
        .unwrap();
        for (id, multivec) in first.iter().enumerate() {
            writer
                .insert_vector(id as PointOffsetType, VectorRef::from(multivec), &hw)
                .unwrap();
        }
        writer.flusher()().unwrap();

        let mut reader =
            ReadOnlyChunkedMultiDenseVectorStorage::<VectorElementType, MmapFile>::open(
                &MmapFs,
                dir.path(),
                DIM,
                Distance::Dot,
                MultiVectorConfig::default(),
                AdviceSetting::Global,
                Populate::No,
            )
            .unwrap();
        assert_eq!(reader.total_vector_count(), first.len());

        for (offset, multivec) in second.iter().enumerate() {
            writer
                .insert_vector(
                    (first.len() + offset) as PointOffsetType,
                    VectorRef::from(multivec),
                    &hw,
                )
                .unwrap();
        }
        let deleted_ids: Vec<PointOffsetType> = vec![1, 75, 149];
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

        // The appended multivector is visible and correct.
        let stored = reader.get_vector::<Random>(first.len() as PointOffsetType);
        let multi: TypedMultiDenseVectorRef<VectorElementType> =
            stored.as_vec_ref().try_into().unwrap();
        assert_eq!(multi.to_owned(), second[0]);

        for &id in &deleted_ids {
            assert!(reader.is_deleted_vector(id));
        }
        assert!(!reader.is_deleted_vector(0));
    }
}
