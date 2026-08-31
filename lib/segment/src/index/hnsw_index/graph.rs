use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

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
use crate::common::operation_error::OperationResult;

#[derive(Debug)]
pub enum HnswGraph<S: UniversalRead> {
    Direct(GraphLayers),
    Batched(Box<GraphLayersBatched<S>>),
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
                return Ok(HnswGraph::Batched(Box::new(graph)));
            }
        }

        let graph = GraphLayers::load(dir, residency, do_convert)?;
        Ok(HnswGraph::Direct(graph))
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
            HnswGraph::Batched(Box::new(GraphLayersBatched::open(fs, dir, residency)?))
        } else {
            // Note that on non-borrowable backends this materializes the
            // links into heap RAM whatever the residency: `Plain` is not
            // supported by the batched view, and `Pinned` asks for
            // materialization explicitly.
            HnswGraph::Direct(GraphLayers::load_universal(fs, dir, residency)?)
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

    pub fn has_inline_vectors(&self) -> bool {
        match self {
            HnswGraph::Direct(graph) => graph.links.format().is_with_vectors(),
            HnswGraph::Batched(graph) => graph.links.is_with_vectors(),
        }
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
