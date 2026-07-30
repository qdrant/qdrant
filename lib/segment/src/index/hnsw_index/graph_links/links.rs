//! The [`GraphLinks`] handle: a self-referential pairing of the owned serialized
//! bytes ([`GraphLinksEnum`]) with a zero-copy parsed view ([`GraphLinksView`])
//! that borrows them.
//!
//! All read accessors (`links`, `links_with_vectors`, `point_level`, ...) go
//! through the parsed view and never touch the owning storage, so the search
//! hot path involves no dynamic dispatch even for the `Universal` backend.

use std::borrow::Cow;
use std::fmt::Debug;
use std::io::Cursor;
use std::path::Path;

use common::mmap::{Advice, AdviceSetting};
use common::types::PointOffsetType;
use common::universal_io::{OpenOptions, Populate, UniversalRead, UniversalReadFs};

use super::format::{GraphLinksFormat, GraphLinksFormatParam};
use super::serializer::serialize_graph_links;
use super::view::{CompressionInfo, GraphLinksView, LinksIterator, LinksWithVectorsIterator};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::hnsw_index::HnswM;

self_cell::self_cell! {
    pub struct GraphLinks {
        owner: GraphLinksEnum,
        #[covariant]
        dependent: GraphLinksView,
    }

    impl {Debug}
}

/// How the serialized links should reside in memory after loading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphLinksResidency {
    /// Mmap without populating: pages are faulted in on demand and remain
    /// evictable by the OS. For graphs expected to stay on disk.
    Cold,
    /// Mmap with a blocking populate: the OS page cache is primed on load,
    /// but the pages remain evictable under memory pressure.
    Cached,
    /// Materialize the links into an anonymous heap allocation, evicting
    /// whatever the read left in the OS page cache. Not evictable by the OS.
    Pinned,
}

impl GraphLinks {
    /// Open options [`Self::load_universal`] uses for `residency`.
    pub(in crate::index::hnsw_index) fn open_options(
        residency: GraphLinksResidency,
    ) -> OpenOptions {
        let populate = match residency {
            // Pin does not populate because we load into heap later
            GraphLinksResidency::Cold | GraphLinksResidency::Pinned => Populate::No,
            GraphLinksResidency::Cached => Populate::Blocking,
        };
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate,
            advice: AdviceSetting::Advice(Advice::Random),
        }
    }

    /// Load the links through universal IO with the requested [`GraphLinksResidency`].
    ///
    /// `Cold`/`Cached` require a borrowable (mmap-backed) backend to keep the
    /// handle alive; non-borrowable backends (io_uring, remote object stores, …)
    /// fall back to `Pinned`-like materialization into RAM; see
    /// [`GraphLinksEnum::from_storage`].
    pub fn load_universal<Fs>(
        fs: &Fs,
        path: &Path,
        format: GraphLinksFormat,
        residency: GraphLinksResidency,
    ) -> OperationResult<Self>
    where
        Fs: UniversalReadFs,
        Fs::File: 'static,
    {
        let storage = fs.open(path, Self::open_options(residency), Default::default())?;
        let owner = match residency {
            GraphLinksResidency::Cold | GraphLinksResidency::Cached => {
                GraphLinksEnum::from_storage(storage)?
            }
            GraphLinksResidency::Pinned => GraphLinksEnum::pinned_from_storage(storage)?,
        };
        Self::try_new(owner, |x| GraphLinksView::load(x.as_bytes()?, format))
    }

    pub fn new_from_edges(
        edges: Vec<Vec<Vec<PointOffsetType>>>,
        format_param: GraphLinksFormatParam<'_>,
        hnsw_m: HnswM,
    ) -> OperationResult<Self> {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        serialize_graph_links(edges, format_param, hnsw_m, &mut cursor)?;
        let mut bytes = cursor.into_inner();
        bytes.shrink_to_fit();
        Self::try_new(GraphLinksEnum::Ram(bytes), |x| {
            GraphLinksView::load(x.as_bytes()?, format_param.as_format())
        })
    }

    pub(super) fn view(&self) -> &GraphLinksView<'_> {
        self.borrow_dependent()
    }

    pub fn as_bytes(&self) -> OperationResult<&[u8]> {
        self.borrow_owner().as_bytes()
    }

    /// Heap RAM held by the serialized links, in bytes.
    /// Zero when the links are backed by a live (mmap-backed) file handle;
    /// see [`GraphLinksEnum::heap_size_bytes`].
    pub fn heap_size_bytes(&self) -> usize {
        self.borrow_owner().heap_size_bytes()
    }

    pub fn format(&self) -> GraphLinksFormat {
        match self.view().compression {
            CompressionInfo::Uncompressed { .. } => GraphLinksFormat::Plain,
            CompressionInfo::Compressed { .. } => GraphLinksFormat::Compressed,
            CompressionInfo::CompressedWithVectors { .. } => {
                GraphLinksFormat::CompressedWithVectors
            }
        }
    }

    pub fn num_points(&self) -> usize {
        self.view().reindex.len()
    }

    pub fn for_each_link(
        &self,
        point_id: PointOffsetType,
        level: usize,
        f: impl FnMut(PointOffsetType),
    ) {
        self.links(point_id, level).for_each(f);
    }

    #[inline]
    pub fn links(&self, point_id: PointOffsetType, level: usize) -> LinksIterator<'_> {
        self.view().links(point_id, level)
    }

    #[inline]
    pub fn links_empty(&self, point_id: PointOffsetType, level: usize) -> bool {
        self.view().links_empty(point_id, level)
    }

    #[inline]
    pub fn links_with_vectors(
        &self,
        point_id: PointOffsetType,
        level: usize,
    ) -> (&[u8], LinksWithVectorsIterator<'_>) {
        let (base_vector, links, vectors) = self.view().links_with_vectors(point_id, level);
        (base_vector, links.zip(vectors))
    }

    pub fn point_level(&self, point_id: PointOffsetType) -> usize {
        self.view().point_level(point_id)
    }

    /// Convert the graph links to a vector of edges, suitable for passing into
    /// [`serialize_graph_links`] or using in tests.
    pub fn to_edges(&self) -> Vec<Vec<Vec<PointOffsetType>>> {
        self.to_edges_impl(|point_id, level| self.links(point_id, level).collect())
    }

    /// Convert the graph links to a vector of edges, generic over the container type.
    pub fn to_edges_impl<Container>(
        &self,
        mut f: impl FnMut(PointOffsetType, usize) -> Container,
    ) -> Vec<Vec<Container>> {
        let mut edges = Vec::with_capacity(self.num_points());
        for point_id in 0..self.num_points() {
            let num_levels = self.point_level(point_id as PointOffsetType) + 1;
            let mut levels = Vec::with_capacity(num_levels);
            levels.extend((0..num_levels).map(|level| f(point_id as PointOffsetType, level)));
            edges.push(levels);
        }
        edges
    }

    /// Populate the disk cache with data, if applicable.
    /// This is a blocking operation.
    pub fn populate(&self) -> OperationResult<()> {
        self.borrow_owner().populate()
    }

    /// Hint to the OS that pages backing this storage can be reclaimed.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.borrow_owner().clear_cache()
    }
}

/// Type-erased universal-IO storage backing a [`GraphLinksEnum::Universal`].
///
/// [`UniversalRead`] is not object-safe (it is `Sized` and has generic
/// methods), so the storage handle is kept behind this minimal object-safe
/// trait. It is blanket-implemented for every [`UniversalRead`], which lets
/// the links keep an arbitrary universal-IO file handle alive (mirroring the
/// former mmap-backed variant) without making [`GraphLinks`](super::GraphLinks)
/// generic.
pub(super) trait GraphLinksStorage: Debug + Send + Sync {
    /// Borrow the whole serialized links blob.
    ///
    /// The backing storage must be borrowable (i.e. mmap-backed): backends
    /// that materialize the whole file into an owned buffer on read are not
    /// supported here.
    fn bytes(&self) -> OperationResult<&[u8]>;

    /// Populate the OS page cache for the backing file, if applicable.
    fn populate(&self) -> OperationResult<()>;

    /// Hint to the OS that the backing pages can be reclaimed, if applicable.
    fn clear_cache(&self) -> OperationResult<()>;
}

impl<S: UniversalRead> GraphLinksStorage for S {
    fn bytes(&self) -> OperationResult<&[u8]> {
        match self.read_whole::<u8>()? {
            Cow::Borrowed(bytes) => Ok(bytes),
            Cow::Owned(_) => Err(OperationError::service_error(
                "Universal graph links storage must be borrowable (mmap-backed)",
            )),
        }
    }

    fn populate(&self) -> OperationResult<()> {
        UniversalRead::populate(self)?;
        Ok(())
    }

    fn clear_cache(&self) -> OperationResult<()> {
        self.clear_ram_cache()?;
        Ok(())
    }
}

#[derive(Debug)]
pub(super) enum GraphLinksEnum {
    /// Links built in memory (e.g. freshly serialized from edges).
    Ram(Vec<u8>),
    /// Links backed by a (type-erased) universal-IO storage handle.
    Universal(Box<dyn GraphLinksStorage>),
}

impl GraphLinksEnum {
    /// Build the backing for serialized links from a universal-IO file handle.
    ///
    /// Backends whose data is resident in RAM or mapped into the address space
    /// (`UniversalKind::is_in_ram_or_mmap`) yield borrowable reads, so their
    /// handle is kept live as [`GraphLinksEnum::Universal`]. Any other backend
    /// (io_uring, remote object stores, …) is not borrowable, so its contents
    /// are materialized into RAM as [`GraphLinksEnum::Ram`]. This is what
    /// upholds the borrowability invariant relied on by [`GraphLinksStorage::bytes`],
    /// so that error path is unreachable in practice.
    pub(super) fn from_storage<S: UniversalRead + 'static>(storage: S) -> OperationResult<Self> {
        if S::kind().is_in_ram_or_mmap() {
            Ok(GraphLinksEnum::Universal(Box::new(storage)))
        } else {
            Self::pinned_from_storage(storage)
        }
    }

    /// Materialize the whole links blob into an anonymous heap allocation
    /// ([`GraphLinksEnum::Ram`]), regardless of the backend.
    pub(super) fn pinned_from_storage<S: UniversalRead>(storage: S) -> OperationResult<Self> {
        let bytes = storage.read_whole::<u8>()?.into_owned();
        // The heap copy is authoritative from here on: evict whatever the read
        // left in the OS page cache or backend caches, so the links are not
        // resident twice.
        storage.clear_ram_cache()?;
        Ok(GraphLinksEnum::Ram(bytes))
    }

    pub(super) fn as_bytes(&self) -> OperationResult<&[u8]> {
        match self {
            GraphLinksEnum::Ram(data) => Ok(data.as_slice()),
            GraphLinksEnum::Universal(storage) => storage.bytes(),
        }
    }

    /// Heap RAM held by the links themselves, in bytes.
    ///
    /// Non-zero only for [`GraphLinksEnum::Ram`], i.e. freshly built links or
    /// links materialized from a non-borrowable universal-IO backend. Storage
    /// kept behind a live handle ([`GraphLinksEnum::Universal`]) is backed by
    /// the OS page cache and reported via file residency instead.
    pub(super) fn heap_size_bytes(&self) -> usize {
        match self {
            GraphLinksEnum::Ram(data) => data.len(),
            GraphLinksEnum::Universal(_) => 0,
        }
    }

    /// Populate the OS page cache for the backing storage, if applicable.
    pub(super) fn populate(&self) -> OperationResult<()> {
        match self {
            GraphLinksEnum::Universal(storage) => storage.populate(),
            GraphLinksEnum::Ram(_) => Ok(()),
        }
    }

    /// Hint to the OS that the backing pages can be reclaimed, if applicable.
    pub(super) fn clear_cache(&self) -> OperationResult<()> {
        match self {
            GraphLinksEnum::Universal(storage) => storage.clear_cache(),
            GraphLinksEnum::Ram(_) => Ok(()),
        }
    }
}
