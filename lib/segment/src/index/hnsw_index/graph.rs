use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::ext::aligned_vec::ACow;
use common::types::{PointOffsetType, ScoredPointOffset};
#[cfg(not(target_os = "linux"))]
use common::universal_io::MmapFile;
use common::universal_io::{CachedReadFs, UniversalKind, UniversalRead, UniversalReadFs};
#[cfg(target_os = "linux")]
use common::universal_io::{IoUringFile, IoUringFs};
use itertools::Itertools as _;

use super::GraphWithVectorsScorers;
use super::entry_points::{EntryPoint, EntryPoints};
use super::graph_layers::{GraphLayers, SearchAlgorithm};
use super::graph_layers_batched::GraphLayersBatched;
use super::graph_links::{GraphLinks, GraphLinksFile, GraphLinksFormat, GraphLinksResidency};
use super::point_scorer::{FilteredScorer, ScorerFilters};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::IoBackend;

#[derive(Debug)]
pub enum HnswGraph<S: UniversalRead> {
    Direct(Arc<GraphLayers>),
    Batched(Arc<GraphLayersBatched<S>>),
}

impl<S: UniversalRead> Clone for HnswGraph<S> {
    fn clone(&self) -> Self {
        match self {
            Self::Direct(graph) => Self::Direct(Arc::clone(graph)),
            Self::Batched(graph) => Self::Batched(Arc::clone(graph)),
        }
    }
}

/// Backend the writable [`HNSWIndex`][1] reads its graph links through.
///
/// [1]: super::hnsw::HNSWIndex
pub type HnswLinksStorage = cfg_select! {
    target_os = "linux" => IoUringFile,
    _ => MmapFile,
};

/// Args for [`HnswGraph::search`].
pub struct GraphSearchArgs<'a> {
    pub top: usize,
    pub ef: usize,
    pub algorithm: SearchAlgorithm,
    pub scorers: SearchScorers<'a>,
    pub custom_entry_points: Option<&'a [PointOffsetType]>,
    pub is_stopped: &'a AtomicBool,
}

pub enum SearchScorers<'a> {
    Regular(FilteredScorer<'a>),
    WithVectors(GraphWithVectorsScorers<'a>),
}

impl HnswGraph<HnswLinksStorage> {
    pub fn open(
        dir: &Path,
        residency: GraphLinksResidency,
        do_convert: bool,
        with_uring: bool,
    ) -> OperationResult<Self> {
        if with_uring {
            #[cfg(target_os = "linux")]
            if Self::is_batched(&IoUringFs, dir, residency)? {
                let graph = GraphLayersBatched::open(&IoUringFs, dir, residency)?;
                return Ok(HnswGraph::Batched(Arc::new(graph)));
            }
        }

        let graph = GraphLayers::load(dir, residency, do_convert)?;
        Ok(HnswGraph::Direct(Arc::new(graph)))
    }
}

impl<S: UniversalRead> HnswGraph<S> {
    /// Whether [`Self::open_universal`] will open a batched-IO graph.
    pub(super) fn is_batched(
        fs: &impl UniversalReadFs<File = S>,
        dir: &Path,
        residency: GraphLinksResidency,
    ) -> OperationResult<bool> {
        let format = GraphLayers::probe_links_format(fs, dir)?;
        Ok(format.is_some_and(|format| Self::format_is_batched(format, residency)))
    }

    fn format_is_batched(format: GraphLinksFormat, residency: GraphLinksResidency) -> bool {
        if residency == GraphLinksResidency::Pinned {
            return false;
        }
        match format {
            GraphLinksFormat::CompressedWithVectors | GraphLinksFormat::Compressed => {
                S::kind().can_be_async()
            }
            GraphLinksFormat::Plain => false,
        }
    }

    pub fn open_universal(
        fs: &impl UniversalReadFs<File = S>,
        dir: &Path,
        residency: GraphLinksResidency,
    ) -> OperationResult<Self>
    where
        S: 'static,
    {
        Ok(if Self::is_batched(fs, dir, residency)? {
            HnswGraph::Batched(Arc::new(GraphLayersBatched::open(fs, dir, residency)?))
        } else {
            // Note that on non-borrowable backends this materializes the
            // links into heap RAM whatever the residency: `Plain` is not
            // supported by the batched view, and `Pinned` asks for
            // materialization explicitly.
            HnswGraph::Direct(Arc::new(GraphLayers::load_universal(fs, dir, residency)?))
        })
    }

    /// Schedule background prefetch of the files [`Self::open_universal`]
    /// will read: the graph data plus whichever links format is present,
    /// probed in the same order as the open.
    pub fn preopen_universal(
        fs: &impl CachedReadFs<File = S>,
        dir: &Path,
        residency: GraphLinksResidency,
    ) -> OperationResult<()> {
        fs.schedule_open(&GraphLayers::get_path(dir), None, None);
        let Some(format) = GraphLayers::probe_links_format(fs, dir)? else {
            return Ok(());
        };
        let options = if Self::format_is_batched(format, residency) {
            GraphLinksFile::<S>::preopen_options(format, residency)
        } else {
            GraphLinks::preopen_options(residency)
        };
        fs.schedule_open(
            &GraphLayers::get_links_path(dir, format),
            Some(options),
            None,
        );
        Ok(())
    }

    pub fn search(&self, args: GraphSearchArgs<'_>) -> OperationResult<Vec<ScoredPointOffset>> {
        let GraphSearchArgs {
            top,
            ef,
            algorithm,
            mut scorers,
            custom_entry_points,
            is_stopped,
        } = args;

        let filters = match &scorers {
            SearchScorers::Regular(scorer) => scorer.filters(),
            SearchScorers::WithVectors(scorers) => scorers.links.filters(),
        };
        let Some(entry) = self.get_entry_point(filters, custom_entry_points)? else {
            return Ok(Vec::new());
        };

        let batch_size = match S::kind() {
            UniversalKind::IoUring => 4,
            // Not tested, but let's assume it's same as for IoUring
            UniversalKind::Mmap => 4,
            // Network-based backends.
            UniversalKind::DiskCache
            | UniversalKind::SimpleDiskCache
            | UniversalKind::CachedBlob
            | UniversalKind::S3
            | UniversalKind::Gcs
            | UniversalKind::Azure
            | UniversalKind::UioGrpc => 16,
        };

        match (self, &mut scorers) {
            (HnswGraph::Direct(graph), SearchScorers::Regular(scorer)) => {
                Ok(graph.search(top, ef, algorithm, scorer, entry, is_stopped)?)
            }
            (HnswGraph::Direct(graph), SearchScorers::WithVectors(scorers)) => {
                Ok(graph.search_with_vectors(top, ef, *scorers, entry, is_stopped)?)
            }
            (HnswGraph::Batched(graph), SearchScorers::Regular(scorer)) => {
                graph.search(top, ef, algorithm, scorer, entry, batch_size, is_stopped)
            }
            (HnswGraph::Batched(graph), SearchScorers::WithVectors(scorers)) => {
                graph.search_with_vectors(top, ef, *scorers, entry, batch_size, is_stopped)
            }
        }
    }

    fn get_entry_point(
        &self,
        filters: &ScorerFilters<'_>,
        custom_entry_points: Option<&[PointOffsetType]>,
    ) -> OperationResult<Option<EntryPoint>> {
        let custom_best = custom_entry_points
            .unwrap_or_default()
            .iter()
            .filter(|&&point_id| filters.check_vector(point_id))
            .map(|&point_id| {
                let level = self.point_level(point_id)?;
                OperationResult::Ok(EntryPoint { point_id, level })
            })
            .process_results(|it| it.max_by_key(|ep| ep.level))?;
        Ok(custom_best.or_else(|| {
            self.entry_points()
                .get_entry_point(|point_id| filters.check_vector(point_id))
        }))
    }

    fn point_level(&self, point_id: PointOffsetType) -> OperationResult<usize> {
        match self {
            HnswGraph::Direct(graph) => Ok(graph.point_level(point_id)),
            HnswGraph::Batched(graph) => graph.links.point_level(point_id),
        }
    }

    fn entry_points(&self) -> &EntryPoints {
        match self {
            HnswGraph::Direct(graph) => &graph.entry_points,
            HnswGraph::Batched(graph) => &graph.entry_points,
        }
    }

    /// Return an error if this graph doesn't provide base vectors of said layout.
    pub fn check_base_vector_layout_compatibility(
        &self,
        expected_size: usize,
        expected_align: usize,
    ) -> OperationResult<()> {
        let layout = match self {
            HnswGraph::Direct(graph) => graph.links.base_vector_layout(),
            HnswGraph::Batched(graph) => graph.links.base_vector_layout(),
        };
        if let Some(layout) = layout
            && layout.size() == expected_size
            && layout.align() >= expected_align
        {
            return Ok(());
        };
        Err(OperationError::service_error(format!(
            "Inline graph vector layout mismatch: expected \
             size={expected_size} align={expected_align}, found={layout:?}",
        )))
    }

    pub fn for_each_base_vector_in_batch<T>(
        &self,
        keys: &[PointOffsetType],
        mut f: impl FnMut(usize, &[T]),
    ) -> OperationResult<()>
    where
        T: bytemuck::Pod,
    {
        match self {
            HnswGraph::Direct(graph) => {
                for (position, &point_id) in keys.iter().enumerate() {
                    let (bytes, _links) = graph.links.links_with_vectors(point_id, 0);
                    f(position, &cast_vector::<T>(ACow::Borrowed(bytes))?);
                }
                Ok(())
            }
            HnswGraph::Batched(graph) => graph.links.read_base_vectors(
                &stumpalo::Arena::new(),
                keys,
                align_of::<T>(),
                |position, bytes| {
                    f(position, &cast_vector::<T>(bytes)?);
                    Ok(())
                },
            ),
        }
    }

    pub fn base_vector<T>(&self, key: PointOffsetType) -> OperationResult<Cow<'_, [T]>>
    where
        T: bytemuck::Pod,
    {
        cast_vector(match self {
            HnswGraph::Direct(graph) => ACow::Borrowed(graph.links.links_with_vectors(key, 0).0),
            HnswGraph::Batched(graph) => graph.links.read_base_vector(key, align_of::<T>())?,
        })
    }

    pub fn has_inline_vectors(&self) -> bool {
        self.format().is_with_vectors()
    }

    pub(super) fn as_direct(&self) -> Option<&GraphLayers> {
        match self {
            HnswGraph::Direct(graph) => Some(graph),
            HnswGraph::Batched(_) => None,
        }
    }

    pub fn num_points(&self) -> usize {
        match self {
            HnswGraph::Direct(graph) => graph.links.num_points(),
            HnswGraph::Batched(graph) => graph.num_points(),
        }
    }

    pub fn format(&self) -> GraphLinksFormat {
        match self {
            HnswGraph::Direct(graph) => graph.links.format(),
            HnswGraph::Batched(graph) => graph.links.format(),
        }
    }

    pub fn io_backend(&self) -> Option<IoBackend> {
        match self {
            HnswGraph::Direct(_) => {
                // Ambiguity: the graph probably *was loaded* from an mmap,
                // but *right now* it can be either mmap or `Vec<>`.
                // Arbitrary choice: report the former.
                Some(IoBackend::Mmap)
            }
            HnswGraph::Batched(_) => IoBackend::from_universal_kind(S::kind()),
        }
    }

    pub fn files(&self, path: &Path) -> Vec<PathBuf> {
        match self {
            HnswGraph::Direct(graph) => graph.files(path),
            HnswGraph::Batched(graph) => vec![
                GraphLayers::get_path(path),
                GraphLayers::get_links_path(path, graph.links.format()),
            ],
        }
    }

    pub fn links_heap_size_bytes(&self) -> usize {
        match self {
            HnswGraph::Direct(graph) => graph.links_heap_size_bytes(),
            HnswGraph::Batched(_) => 0,
        }
    }

    pub fn populate(&self) -> OperationResult<()> {
        match self {
            HnswGraph::Direct(graph) => graph.links.populate(),
            HnswGraph::Batched(graph) => graph.links.populate(),
        }
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            HnswGraph::Direct(graph) => graph.links.clear_cache(),
            HnswGraph::Batched(graph) => graph.links.clear_cache(),
        }
    }
}

fn cast_vector<T: bytemuck::Pod>(bytes: ACow<'_>) -> OperationResult<Cow<'_, [T]>> {
    bytes.try_cast_bytemuck().map_err(|err| {
        OperationError::service_error(format!("Misplaced inline-graph vector: {err}"))
    })
}
