use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs, Populate, UniversalRead, UserData};

use crate::common::Flusher;
use crate::common::flags::FlagsMode;
use crate::common::flags::bitvec_flags::BitvecFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::VectorRef;
use crate::index::hnsw_index::HnswGraph;
use crate::types::{Distance, IoBackend, VectorStorageDatatype};
use crate::vector_storage::common::error_immutable_insert;
use crate::vector_storage::dense::appendable_dense_vector_storage::DELETED_DIR_PATH;
use crate::vector_storage::dense::dense_vectors::DenseVectorBlob;
use crate::vector_storage::graph_vectors::GraphVectors;
use crate::vector_storage::{
    DenseVectorStorage, DenseVectorStorageRead, VectorStorage, VectorStorageRead,
};

#[derive(Debug)]
pub struct GraphInlineDenseVectorStorage<T: PrimitiveVectorElement, S: UniversalRead> {
    vectors: GraphVectors<T, S>,
    deleted: BitvecFlags<MmapFile>,
    deleted_count: usize,
    distance: Distance,
}

impl<T: PrimitiveVectorElement, S: UniversalRead> GraphInlineDenseVectorStorage<T, S> {
    pub fn open(
        graph: HnswGraph<S>,
        path: &Path,
        dim: usize,
        distance: Distance,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        fs_err::create_dir_all(path)?;
        let deleted = BitvecFlags::open_or_create(
            MmapFs,
            &path.join(DELETED_DIR_PATH),
            FlagsMode::from_feature_flags(),
            Populate::from(!graph.is_on_disk()),
            hw_counter,
        )?;
        let deleted_count = deleted.count_trues();
        Ok(Self {
            vectors: GraphVectors::new(graph, dim)?,
            deleted,
            deleted_count,
            distance,
        })
    }

    pub fn hnsw_graph(&self) -> &HnswGraph<S> {
        self.vectors.graph()
    }

    pub fn populate(&self, hw_counter: &HardwareCounterCell) {
        self.vectors.populate(hw_counter);
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.vectors.clear_cache()?;
        self.deleted.clear_cache()
    }
}

impl<T: PrimitiveVectorElement, S: UniversalRead> DenseVectorStorageRead<T>
    for GraphInlineDenseVectorStorage<T, S>
{
    fn vector_dim(&self) -> usize {
        self.vectors.dim()
    }

    fn get_dense<P: AccessPattern>(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Cow<'_, [T]> {
        self.vectors
            .get_vector_opt::<P>(key, hw_counter)
            .unwrap_or_else(|| panic!("vector not found: {key}"))
    }

    fn for_each_in_dense_batch<F: FnMut(usize, &[T])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.vectors.for_each_in_batch(keys, f, hw_counter)
    }

    fn read_dense_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let (user_data, keys): (Vec<_>, Vec<_>) = keys.into_iter().unzip();
        self.vectors.for_each_in_batch(
            &keys,
            |idx, vector| {
                callback(
                    user_data[idx],
                    keys[idx],
                    bytemuck::cast_slice(vector).to_vec(),
                );
            },
            hw_counter,
        )
    }
}

impl<T: PrimitiveVectorElement, S: UniversalRead> DenseVectorStorage<T>
    for GraphInlineDenseVectorStorage<T, S>
{
    fn update_from<'a>(
        &mut self,
        _other_vectors: &mut impl Iterator<Item = (Cow<'a, [T]>, bool)>,
        _stopped: &AtomicBool,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Range<PointOffsetType>> {
        Err(OperationError::service_error(
            "Cannot merge into a graph-backed vector storage",
        ))
    }
}

impl<T: PrimitiveVectorElement, S: UniversalRead> VectorStorageRead
    for GraphInlineDenseVectorStorage<T, S>
{
    fn size_of_available_vectors_in_bytes(&self, hw_counter: &HardwareCounterCell) -> usize {
        self.available_vector_count(hw_counter) * self.vectors.dim() * std::mem::size_of::<T>()
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        T::datatype()
    }

    fn is_on_disk(&self) -> bool {
        self.vectors.graph().is_on_disk()
    }

    fn io_backend(&self) -> Option<IoBackend> {
        self.vectors.graph().io_backend()
    }

    fn total_vector_count(&self, _hw_counter: &HardwareCounterCell) -> usize {
        self.vectors.num_vectors()
    }

    fn get_vector<P: AccessPattern>(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> CowVector<'_> {
        self.get_vector_opt::<P>(key, hw_counter)
            .expect("Vector not found")
    }

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
        hw_counter: &HardwareCounterCell,
    ) {
        let (user_data, keys): (Vec<_>, Vec<_>) = keys.into_iter().unzip();
        self.vectors
            .for_each_in_batch(
                &keys,
                |idx, vector| {
                    let vector = CowVector::from(T::slice_to_float_cow(Cow::Borrowed(vector)));
                    callback(user_data[idx], keys[idx], vector);
                },
                hw_counter,
            )
            .expect("read vectors");
    }

    fn get_vector_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<CowVector<'_>> {
        self.vectors
            .get_vector_opt::<P>(key, hw_counter)
            .map(|vector| CowVector::from(T::slice_to_float_cow(vector)))
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

    fn read_vector_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, Vec<u8>),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.read_dense_bytes::<P, U>(keys, callback, hw_counter)
    }
}

impl<T: PrimitiveVectorElement, S: UniversalRead> VectorStorage
    for GraphInlineDenseVectorStorage<T, S>
{
    fn insert_vector(
        &mut self,
        _key: PointOffsetType,
        _vector: VectorRef,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(error_immutable_insert())
    }

    fn flusher(&self, hw_counter: &HardwareCounterCell) -> Flusher {
        self.deleted.flusher(hw_counter)
    }

    fn files(&self) -> Vec<PathBuf> {
        self.deleted.files()
    }

    fn delete_vector(
        &mut self,
        key: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        let was_deleted = self.deleted.set(key, true);
        if !was_deleted {
            self.deleted_count += 1;
        }
        Ok(!was_deleted)
    }
}
