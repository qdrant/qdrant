use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::{AccessPattern, Random, Sequential};
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs, Populate, UniversalRead, UserData};
use fs_err as fs;

use super::buffered_offsets::BufferedOffsets;
use crate::common::Flusher;
use crate::common::flags::bitvec_flags::BitvecFlags;
use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::named_vectors::{CowMultiVector, CowVector};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    TypedMultiDenseVector, TypedMultiDenseVectorRef, VectorElementType, VectorRef,
};
use crate::types::{Distance, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::chunked_vectors::{ChunkedVectors, ChunkedVectorsRead};
use crate::vector_storage::dense::appendable_dense_vector_storage::{
    open_appendable_memmap_vector_storage_byte, open_appendable_memmap_vector_storage_full,
    open_appendable_memmap_vector_storage_half,
};
use crate::vector_storage::turbo::multi::open_appendable_turbo_multi_vector_storage;
use crate::vector_storage::turbo::open_appendable_turbo_vector_storage;
use crate::vector_storage::{
    MultiVectorStorage, MultiVectorStorageRead, VectorOffsetType, VectorStorage, VectorStorageEnum,
    VectorStorageRead,
};

pub(crate) const VECTORS_DIR_PATH: &str = "vectors";
pub(crate) const OFFSETS_DIR_PATH: &str = "offsets";
pub(crate) const DELETED_DIR_PATH: &str = "deleted";

#[derive(Copy, Clone, Debug, Default, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
pub struct MultivectorMmapOffset {
    pub offset: u32,
    pub count: u32,
    pub capacity: u32,
}

pub(crate) fn flattened_to_multi_vector<T: PrimitiveVectorElement>(
    flattened: Cow<'_, [T]>,
    dim: usize,
) -> CowMultiVector<'_, T> {
    match flattened {
        Cow::Borrowed(flattened_vectors) => CowMultiVector::Borrowed(TypedMultiDenseVectorRef {
            flattened_vectors,
            dim,
        }),
        Cow::Owned(flattened_vectors) => CowMultiVector::Owned(TypedMultiDenseVector {
            flattened_vectors,
            dim,
        }),
    }
}

/// Resolve the multi-vector at `key` from a pair of chunked stores: the
/// `offsets` store maps `key` to a [`MultivectorMmapOffset`], which is then
/// used to fetch the flattened element slice from `vectors`.
///
/// Shared by the appendable and read-only storage variants — both back the
/// same on-disk layout, so they only differ in the writability of the chunked
/// stores.
pub(crate) fn read_multi_vector<'a, T, P, S>(
    offsets: &'a ChunkedVectorsRead<MultivectorMmapOffset, S>,
    vectors: &'a ChunkedVectorsRead<T, S>,
    key: PointOffsetType,
) -> Option<CowMultiVector<'a, T>>
where
    T: PrimitiveVectorElement,
    P: AccessPattern,
    S: UniversalRead,
{
    let &[multi_offset] = offsets.get::<P>(key as VectorOffsetType)?.as_ref() else {
        unreachable!("multi-vector offsets are stored as vectors of length 1");
    };

    let MultivectorMmapOffset {
        offset,
        count,
        capacity: _,
    } = multi_offset;

    let flattened = vectors.get_many::<P>(offset as _, count as _)?;

    Some(flattened_to_multi_vector(flattened, vectors.dim()))
}

#[derive(Debug)]
pub struct AppendableMmapMultiDenseVectorStorage<T: PrimitiveVectorElement> {
    vectors: ChunkedVectors<T, MmapFile>,
    offsets: BufferedOffsets,
    /// Flags marking deleted vectors
    ///
    /// Structure grows dynamically, but may be smaller than actual number of vectors. Must not
    /// depend on its length.
    deleted: BitvecFlags<MmapFile>,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    deleted_count: usize,
}

impl<T: PrimitiveVectorElement> AppendableMmapMultiDenseVectorStorage<T> {
    /// Set deleted flag for given key. Returns previous deleted state.
    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        if !deleted && self.vectors.len() <= key as usize {
            return false;
        }

        // set value
        let previous = self.deleted.set(key, deleted);

        // update counter
        if !previous && deleted {
            self.deleted_count += 1;
        } else if previous && !deleted {
            self.deleted_count -= 1;
        }

        previous
    }

    /// Insert a multi-vector already in the storage's element type `T`.
    ///
    /// Leaves the deleted flag cleared; callers that need it set should call
    /// [`Self::set_deleted`] afterwards.
    fn insert_multi_native(
        &mut self,
        key: PointOffsetType,
        multi_vector: TypedMultiDenseVectorRef<T>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        assert_eq!(multi_vector.dim, self.vectors.dim());
        let multivector_size_in_bytes = std::mem::size_of_val(multi_vector.flattened_vectors);
        let max_vector_size_bytes = self.vectors.max_vector_size_bytes();
        if multivector_size_in_bytes >= max_vector_size_bytes {
            return Err(OperationError::service_error(format!(
                "Cannot insert multi vector of size {multivector_size_in_bytes} to the mmap vector storage.\
                 It's too large, maximum size is {max_vector_size_bytes}."
            )));
        }

        let mut offset = self
            .offsets
            .get::<Random>(key as VectorOffsetType)
            .unwrap_or_default();

        if multi_vector.vectors_count() > offset.capacity as usize {
            // append vector to the end
            let mut new_key = self.vectors.len();
            let chunk_left_keys = self.vectors.get_remaining_chunk_keys(new_key);
            if multi_vector.vectors_count() > chunk_left_keys {
                new_key += chunk_left_keys;
            }

            offset = MultivectorMmapOffset {
                offset: new_key as PointOffsetType,
                count: multi_vector.vectors_count() as PointOffsetType,
                capacity: multi_vector.vectors_count() as PointOffsetType,
            };
        } else {
            // use existing place to insert vector
            offset.count = multi_vector.vectors_count() as PointOffsetType;
        }

        self.vectors.insert_many(
            offset.offset as VectorOffsetType,
            multi_vector.flattened_vectors,
            multi_vector.vectors_count(),
            hw_counter,
        )?;
        self.offsets.set(key as VectorOffsetType, offset);
        self.set_deleted(key, false);

        Ok(())
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        // deleted bitvec is already loaded
        self.vectors.populate()?;
        self.offsets.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            vectors,
            offsets,
            deleted,
            distance: _,
            multi_vector_config: _,
            deleted_count: _,
        } = self;

        vectors.clear_cache()?;
        offsets.clear_cache()?;
        deleted.clear_cache()?;
        Ok(())
    }
}

impl<T: PrimitiveVectorElement> MultiVectorStorageRead<T>
    for AppendableMmapMultiDenseVectorStorage<T>
{
    fn vector_dim(&self) -> usize {
        self.vectors.dim()
    }

    /// Panics if key is not found
    fn get_multi<P: AccessPattern>(&self, key: PointOffsetType) -> CowMultiVector<'_, T> {
        self.get_multi_opt::<P>(key).expect("vector not found")
    }

    /// Returns None if key is not found
    fn get_multi_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
    ) -> Option<CowMultiVector<'_, T>> {
        // Resolve the offset through the buffer (pending overlay then durable),
        // then fetch the row slice from `vectors`.
        let offset = self.offsets.get::<P>(key as VectorOffsetType)?;
        let flattened = self
            .vectors
            .get_many::<P>(offset.offset as _, offset.count as _)?;
        Some(flattened_to_multi_vector(flattened, self.vectors.dim()))
    }

    fn for_each_in_batch_multi<F>(&self, keys: &[PointOffsetType], mut callback: F)
    where
        F: FnMut(usize, TypedMultiDenseVectorRef<'_, T>),
    {
        // Resolve offsets through the buffer first (so unflushed writes are seen),
        // then reuse the row-side prefetch reader over `vectors`.
        let row_offsets = self
            .offsets
            .resolve_rows::<Sequential, _, _>(keys.iter().copied().enumerate());

        self.vectors
            .for_each_vector::<Sequential, _>(row_offsets.into_iter(), |index, flattened| {
                let vector = TypedMultiDenseVectorRef::new(flattened.as_ref(), self.vector_dim());
                callback(index, vector);
                Ok(())
            })
            .expect("read vectors");
    }

    fn iterate_inner_vectors(&self) -> impl Iterator<Item = Cow<'_, [T]>> + Clone + Send {
        // TODO: Implement based on `iter_vectors`!?

        (0..self.total_vector_count()).flat_map(move |key| {
            let mmap_offset = self
                .offsets
                .get::<Sequential>(key as VectorOffsetType)
                .unwrap();
            (0..mmap_offset.count).map(move |i| {
                self.vectors
                    .get::<Sequential>((mmap_offset.offset + i) as VectorOffsetType)
                    .unwrap()
            })
        })
    }

    fn multi_vector_config(&self) -> &MultiVectorConfig {
        &self.multi_vector_config
    }
}

impl<T: PrimitiveVectorElement> MultiVectorStorage<T> for AppendableMmapMultiDenseVectorStorage<T> {
    fn update_from<'a>(
        &mut self,
        other_vectors: &mut impl Iterator<Item = (CowMultiVector<'a, T>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.offsets.len() as PointOffsetType;
        let disposed_hw_counter = HardwareCounterCell::disposable(); // Internal operation
        for (other_vector, other_deleted) in other_vectors {
            check_process_stopped(stopped)?;
            let new_id = self.offsets.len() as PointOffsetType;
            self.insert_multi_native(new_id, other_vector.as_ref(), &disposed_hw_counter)?;
            self.set_deleted(new_id, other_deleted);
        }
        let end_index = self.offsets.len() as PointOffsetType;
        Ok(start_index..end_index)
    }
}

impl<T: PrimitiveVectorElement> VectorStorageRead for AppendableMmapMultiDenseVectorStorage<T> {
    fn size_of_available_vectors_in_bytes(&self) -> usize {
        if self.total_vector_count() > 0 {
            let total_size = self.vectors.len() * self.vector_dim() * std::mem::size_of::<T>();
            (total_size as u128 * self.available_vector_count() as u128
                / self.total_vector_count() as u128) as usize
        } else {
            0
        }
    }

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

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) {
        // Resolve offsets through the buffer first (so unflushed writes are seen),
        // then reuse the row-side prefetch reader over `vectors`.
        let row_offsets = self.offsets.resolve_rows::<P, _, _>(
            keys.into_iter()
                .map(|(user_data, point_offset)| ((user_data, point_offset), point_offset)),
        );

        self.vectors
            .for_each_vector::<P, _>(
                row_offsets.into_iter(),
                |(user_data, point_offset), flattened| {
                    let vector = CowVector::MultiDense(T::into_float_multivector(
                        flattened_to_multi_vector(flattened, self.vectors.dim()),
                    ));

                    callback(user_data, point_offset, vector);
                    Ok(())
                },
            )
            .expect("read vectors");
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        self.get_multi_opt::<P>(key).map(|multi_dense_vector| {
            CowVector::MultiDense(T::into_float_multivector(multi_dense_vector))
        })
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

impl<T: PrimitiveVectorElement> VectorStorage for AppendableMmapMultiDenseVectorStorage<T> {
    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let multi_vector: TypedMultiDenseVectorRef<VectorElementType> = vector.try_into()?;
        let multi_vector = T::from_float_multivector(CowMultiVector::Borrowed(multi_vector));
        self.insert_multi_native(key, multi_vector.as_ref(), hw_counter)
    }

    fn flusher(&self) -> Flusher {
        Box::new({
            let vectors_flusher = self.vectors.flusher();
            let offsets_flusher = self.offsets.flusher();
            let deleted_flusher = self.deleted.flusher();
            move || {
                vectors_flusher()?;
                offsets_flusher()?;
                deleted_flusher()?;
                Ok(())
            }
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.vectors.files();
        files.extend(self.offsets.files());
        files.extend(self.deleted.files());
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = self.vectors.immutable_files();
        files.extend(self.offsets.immutable_files());
        files
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        Ok(!self.set_deleted(key, true))
    }
}

pub fn open_appendable_memmap_vector_storage(
    storage_element_type: VectorStorageDatatype,
    vector_storage_path: &Path,
    size: usize,
    distance: Distance,
    madvise: AdviceSetting,
    populate: bool,
) -> OperationResult<VectorStorageEnum> {
    match storage_element_type {
        VectorStorageDatatype::Float32 => open_appendable_memmap_vector_storage_full(
            vector_storage_path,
            size,
            distance,
            madvise,
            populate,
        ),
        VectorStorageDatatype::Uint8 => open_appendable_memmap_vector_storage_byte(
            vector_storage_path,
            size,
            distance,
            madvise,
            populate,
        ),
        VectorStorageDatatype::Float16 => open_appendable_memmap_vector_storage_half(
            vector_storage_path,
            size,
            distance,
            madvise,
            populate,
        ),
        VectorStorageDatatype::Turbo4 => {
            open_appendable_turbo_vector_storage(vector_storage_path, size, distance, populate)
                .map(|s| VectorStorageEnum::DenseTurbo(Box::new(s)))
        }
    }
}

pub fn open_appendable_memmap_multi_vector_storage(
    storage_element_type: VectorStorageDatatype,
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    madvise: AdviceSetting,
    populate: bool,
) -> OperationResult<VectorStorageEnum> {
    match storage_element_type {
        VectorStorageDatatype::Float32 => open_appendable_memmap_multi_vector_storage_full(
            path,
            dim,
            distance,
            multi_vector_config,
            madvise,
            populate,
        ),
        VectorStorageDatatype::Uint8 => open_appendable_memmap_multi_vector_storage_byte(
            path,
            dim,
            distance,
            multi_vector_config,
            madvise,
            populate,
        ),
        VectorStorageDatatype::Float16 => open_appendable_memmap_multi_vector_storage_half(
            path,
            dim,
            distance,
            multi_vector_config,
            madvise,
            populate,
        ),
        VectorStorageDatatype::Turbo4 => open_appendable_turbo_multi_vector_storage(
            path,
            dim,
            distance,
            multi_vector_config,
            populate,
        )
        .map(|s| VectorStorageEnum::MultiDenseTurbo(Box::new(s))),
    }
}

pub fn open_appendable_memmap_multi_vector_storage_full(
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    madvise: AdviceSetting,
    populate: bool,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_appendable_memmap_multi_vector_storage_impl::<VectorElementType>(
        path,
        dim,
        distance,
        multi_vector_config,
        madvise,
        populate,
    )?;

    Ok(VectorStorageEnum::MultiDenseAppendableMemmap(Box::new(
        storage,
    )))
}

pub fn open_appendable_memmap_multi_vector_storage_byte(
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    madvise: AdviceSetting,
    populate: bool,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_appendable_memmap_multi_vector_storage_impl(
        path,
        dim,
        distance,
        multi_vector_config,
        madvise,
        populate,
    )?;

    Ok(VectorStorageEnum::MultiDenseAppendableMemmapByte(Box::new(
        storage,
    )))
}

pub fn open_appendable_memmap_multi_vector_storage_half(
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    madvise: AdviceSetting,
    populate: bool,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_appendable_memmap_multi_vector_storage_impl(
        path,
        dim,
        distance,
        multi_vector_config,
        madvise,
        populate,
    )?;

    Ok(VectorStorageEnum::MultiDenseAppendableMemmapHalf(Box::new(
        storage,
    )))
}

pub fn open_appendable_memmap_multi_vector_storage_impl<T: PrimitiveVectorElement>(
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    madvise: AdviceSetting,
    populate: bool,
) -> OperationResult<AppendableMmapMultiDenseVectorStorage<T>> {
    fs::create_dir_all(path)?;

    let vectors_path = path.join(VECTORS_DIR_PATH);
    let offsets_path = path.join(OFFSETS_DIR_PATH);
    let deleted_path = path.join(DELETED_DIR_PATH);

    let vectors = ChunkedVectors::open(
        MmapFs,
        &vectors_path,
        dim,
        madvise,
        Populate::from(populate),
    )?;
    let offsets =
        ChunkedVectors::open(MmapFs, &offsets_path, 1, madvise, Populate::from(populate))?;
    // The offsets store is buffered so its durable state can never race ahead of
    // the durable `vectors` length, which would corrupt points on reload.
    let offsets = BufferedOffsets::new(offsets);

    let deleted = BitvecFlags::new(
        MmapFs,
        DynamicStoredFlags::open(&MmapFs, &deleted_path, Populate::from(populate))?,
    )?;
    let deleted_count = deleted.count_trues();

    Ok(AppendableMmapMultiDenseVectorStorage {
        vectors,
        offsets,
        deleted,
        distance,
        multi_vector_config,
        deleted_count,
    })
}

/// Find files related to this dense vector storage
#[cfg(test)]
pub(crate) fn find_storage_files(vector_storage_path: &Path) -> OperationResult<Vec<PathBuf>> {
    let vectors_path = vector_storage_path.join(VECTORS_DIR_PATH);
    let offsets_path = vector_storage_path.join(OFFSETS_DIR_PATH);
    let deleted_path = vector_storage_path.join(DELETED_DIR_PATH);

    let mut files = vec![];
    files.extend(common::disk::list_files(&vectors_path)?);
    files.extend(common::disk::list_files(&offsets_path)?);
    files.extend(common::disk::list_files(&deleted_path)?);
    Ok(files)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::MultiDenseVectorInternal;

    const RAND_SEED: u64 = 42;

    /// Test that `find_storage_files` finds all files that are reported by the storage.
    #[test]
    fn test_find_storage_files() {
        // Numbers chosen so we get 3 data chunks, not just 1
        const POINT_COUNT: PointOffsetType = 1000;
        const DIM: usize = 128;

        let mutli_vector_config = MultiVectorConfig::default();
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let mut storage = open_appendable_memmap_multi_vector_storage_full(
            dir.path(),
            DIM,
            Distance::Dot,
            mutli_vector_config,
            AdviceSetting::Global,
            false,
        )
        .unwrap();

        let mut rng = StdRng::seed_from_u64(RAND_SEED);
        let hw_counter = HardwareCounterCell::disposable();

        // Insert points, delete 10% of it, and flush
        for internal_id in 0..POINT_COUNT {
            let size = rng.random_range(1..=4);
            let vectors = std::iter::repeat_with(|| {
                std::iter::repeat_with(|| rng.random_range(-1.0..1.0))
                    .take(DIM)
                    .collect()
            })
            .take(size)
            .collect::<Vec<Vec<_>>>();
            let multivec = MultiDenseVectorInternal::try_from(vectors).unwrap();
            storage
                .insert_vector(internal_id, VectorRef::from(&multivec), &hw_counter)
                .unwrap();
        }
        for internal_id in 0..POINT_COUNT {
            if !rng.random_bool(0.1) {
                continue;
            }
            storage.delete_vector(internal_id).unwrap();
        }
        storage.flusher()().unwrap();

        let storage_files = storage.files().into_iter().collect::<HashSet<_>>();
        let found_files = find_storage_files(dir.path())
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();

        assert_eq!(
            storage_files, found_files,
            "find_storage_files must find same files that storage reports",
        );
    }

    fn multivec(rows: usize, fill: VectorElementType, dim: usize) -> MultiDenseVectorInternal {
        MultiDenseVectorInternal::try_from(vec![vec![fill; dim]; rows]).unwrap()
    }

    /// A re-upsert that relocates a point's rows *after* a flush has already been
    /// started must be deferred to the next flush, not become durable ahead of the
    /// `vectors` length. Reopening then sees a consistent pre-relocation state —
    /// never the head-clobbered corruption the unbuffered skew produced.
    ///
    /// This is the buffered-store alternative to the reload-time repair: the skew
    /// is prevented rather than patched.
    #[test]
    fn relocation_after_flush_start_is_deferred_not_corrupt() {
        const DIM: usize = 4;

        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::disposable();

        let open = || {
            open_appendable_memmap_multi_vector_storage_impl::<VectorElementType>(
                dir.path(),
                DIM,
                Distance::Dot,
                MultiVectorConfig::default(),
                AdviceSetting::Global,
                false,
            )
            .unwrap()
        };

        // Baseline: point 0 (2 rows) then point 1 (2 rows), durably flushed.
        let p0_small = multivec(2, 1.0, DIM);
        let p1 = multivec(2, 7.0, DIM);
        {
            let mut storage = open();
            storage
                .insert_vector(0, VectorRef::from(&p0_small), &hw_counter)
                .unwrap();
            storage
                .insert_vector(1, VectorRef::from(&p1), &hw_counter)
                .unwrap();
            storage.flusher()().unwrap();

            // Start a flush: it snapshots vectors.len and the (now empty) offsets
            // pending set at this instant.
            let flush = storage.flusher();

            // Grow point 0 past its capacity -> append path relocates its rows to
            // the end. This is the dangerous in-window write.
            let p0_grown = multivec(4, 2.0, DIM);
            storage
                .insert_vector(0, VectorRef::from(&p0_grown), &hw_counter)
                .unwrap();

            // Execute the in-window flush. The relocation must NOT be persisted.
            flush().unwrap();
        }

        // Reopen and verify a consistent, uncorrupted state.
        let storage = open();

        let read0 = storage
            .get_multi_opt::<Random>(0)
            .expect("point 0 readable after reload");
        assert_eq!(
            read0.as_vec_ref().flattened_vectors,
            p0_small.flattened_vectors.as_slice(),
            "point 0 must read its durable pre-relocation value, not a rejected/garbled one",
        );

        let read1 = storage
            .get_multi_opt::<Random>(1)
            .expect("point 1 readable after reload");
        assert_eq!(
            read1.as_vec_ref().flattened_vectors,
            p1.flattened_vectors.as_slice(),
            "point 1 must be intact — the unbuffered skew would clobber its head rows",
        );
    }
}
