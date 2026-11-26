//! What core and the indexer exchange: the task parameters, the payload block plan, and the
//! uploaded graph. Transport-independent: the gRPC layer converts these to and from protobuf.

use std::time::Duration;

use bitvec::slice::BitSlice;
use common::bitvec::BitSliceExt as _;
use common::types::PointOffsetType;
use quantization::encoded_vectors_binary::BinaryParameters;
use quantization::encoded_vectors_tq::TurboParameters;
use quantization::encoded_vectors_u8::ScalarParameters;
use serde::{Deserialize, Serialize};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::entry_points::{EntryPoint, EntryPoints};
use crate::index::hnsw_index::graph_algorithm::MainGraphAlgorithm;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::HNSW_USE_HEURISTIC;
use crate::types::{Distance, MultiVectorConfig, QuantizationConfig, VectorStorageDatatype};

/// Version of [`WireGraph`]. Bump on any change to its meaning, and keep reading the old one
/// for as long as indexers may send it (D9).
pub const GRAPH_FORMAT_VERSION_V1: u32 = 1;

/// Graph formats this core reads, sent with every task. The indexer must answer in one of them.
pub const SUPPORTED_GRAPH_FORMAT_VERSIONS: &[u32] = &[GRAPH_FORMAT_VERSION_V1];

/// How vectors are encoded in `ReadVectors`: the storage-native bytes of the vector storage
/// (`VectorStorageEnum::vector_bytes_opt`), for the datatype and multivector config in
/// [`RemoteGraphParams`]. Lossless: the indexer scores exactly what a local build would score
/// without quantization.
pub const VECTOR_REPRESENTATION_STORAGE_NATIVE: &str = "storage_native_v1";

/// What the indexer scores with, and so what core sends it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorSource {
    /// Vectors through `ReadVectors`, in the storage-native encoding: raw for float32,
    /// float16 and uint8, already encoded for TurboQuant storages.
    Storage,
    /// The quantized codes through `ReadVectors`, as the quantizer stores them, read with
    /// [`RemoteGraphParams::quantization`]. No original vectors. Used when the build scores
    /// from the codes alone (scalar, binary and TurboQuant quantization), so the remote graph
    /// is the one a local build makes, from a fraction of the bytes.
    Quantized,
}

/// How the codes of [`VectorSource::Quantized`] were encoded: everything the indexer needs,
/// besides the codes, to score them as core does. Independent of how core stores them.
#[derive(Debug, Clone, PartialEq)]
pub struct RemoteQuantization {
    /// The quantization config of the vector, as in the collection config.
    pub config: QuantizationConfig,
    pub parameters: QuantizerParameters,
}

/// Encoder parameters, per quantization method.
#[derive(Debug, Clone, PartialEq)]
pub enum QuantizerParameters {
    Scalar(ScalarParameters),
    Binary(BinaryParameters),
    Turbo(TurboParameters),
}

/// Everything the indexer needs to build the graph, besides the vectors.
#[derive(Debug, Clone, PartialEq)]
pub struct RemoteGraphParams {
    pub vector_name: String,
    pub dim: usize,
    pub datatype: VectorStorageDatatype,
    pub distance: Distance,
    /// Set for multivectors: then each vector is a flattened list of `dim`-sized vectors.
    pub multivector_config: Option<MultiVectorConfig>,
    /// The index to build, and its parameters.
    pub index: RemoteIndexParams,
    /// Number of point offsets in the segment, deleted included: `0..point_count`. Points
    /// not to be indexed have an empty entry in `ReadVectors`.
    pub point_count: usize,
    pub vector_representation: String,
    pub vector_source: VectorSource,
    /// Set for [`VectorSource::Quantized`] only.
    pub quantization: Option<RemoteQuantization>,
    /// Whether payload blocks may come, through `ReadPayloadBlocks`.
    pub has_payload_blocks: bool,
    /// How long the indexer waits for the next payload block before failing the task.
    pub max_block_wait: Duration,
}

/// The index a task builds, with the parameters of its kind. Sent with `SubmitTask`, so the
/// entry point can refuse what it does not build (or not that large) before any data moves,
/// and with `GetTask`.
///
/// A new kind of index, or a version of one with other parameters, is a new variant here and
/// a new case of the `IndexingIndexParams` oneof. Indexers refuse cases they do not know, and
/// every `match` on this enum shows what the new variant needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteIndexParams {
    Hnsw(HnswBuildParams),
}

/// Parameters of an HNSW graph: the main graph and the payload block graphs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswBuildParams {
    /// Main graph `m` and `m0`. `m == 0` means no main graph: only payload blocks link points.
    pub m: usize,
    pub m0: usize,
    pub ef_construct: usize,
    /// Number of entry points the main graph keeps.
    pub entry_points_num: usize,
    /// `m` and `m0` of payload block graphs.
    pub payload_m: usize,
    pub payload_m0: usize,
    /// How the main graph is built, as the local build would. Payload block graphs, and main
    /// graphs the algorithm does not apply to, are built by HNSW insertion with `ef_construct`.
    pub main_graph: MainGraphAlgorithm,
}

impl HnswBuildParams {
    pub fn main_m(&self) -> HnswM {
        HnswM::new(self.m, self.m0)
    }

    pub fn block_m(&self) -> HnswM {
        HnswM::new(self.payload_m, self.payload_m0)
    }
}

/// Payload blocks chosen by core: per indexed field, in build order, the point sets to link
/// among themselves. Core leaves out blocks that are too large (by an estimate that does not
/// need the graph) or mostly deleted. The indexer decides which of the rest are worth linking
/// given its graph, and how (O1). Streamed to the indexer as core prepares them, see
/// [`super::blocks::BlockStream`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BlockPlan {
    pub fields: Vec<FieldBlockPlan>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldBlockPlan {
    /// Name of the field, for progress reporting.
    pub field: String,
    /// Skip blocks the graph built so far already connects well. False for the first field
    /// and for tenant fields, whose blocks are always built.
    pub skip_if_connected: bool,
    pub blocks: Vec<PlannedBlock>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlannedBlock {
    /// The payload index's estimate of the block size. The size limit is checked against this.
    pub cardinality: usize,
    /// Non-deleted point offsets of the block, in the segment under construction.
    pub points: Vec<PointOffsetType>,
}

impl BlockPlan {
    pub fn num_blocks(&self) -> usize {
        self.fields.iter().map(|f| f.blocks.len()).sum()
    }

    pub fn num_points(&self) -> usize {
        self.fields
            .iter()
            .flat_map(|f| &f.blocks)
            .map(|b| b.points.len())
            .sum()
    }

    /// For each point, the number of planned blocks it is in: each can add up to
    /// `payload_m0` links on level 0.
    fn block_memberships(&self, point_count: usize) -> Vec<u16> {
        let mut memberships = vec![0u16; point_count];
        for block in self.fields.iter().flat_map(|f| &f.blocks) {
            for &point in &block.points {
                if let Some(count) = memberships.get_mut(point as usize) {
                    *count = count.saturating_add(1);
                }
            }
        }
        memberships
    }

    /// Append `block` to `field`, which is either the last field or a new one. Returns the
    /// index of the field.
    pub fn push(&mut self, field: &str, skip_if_connected: bool, block: PlannedBlock) -> usize {
        match self.fields.last_mut() {
            Some(last) if last.field == field => last.blocks.push(block),
            Some(_) | None => self.fields.push(FieldBlockPlan {
                field: field.to_string(),
                skip_if_connected,
                blocks: vec![block],
            }),
        }
        self.fields.len() - 1
    }

    /// The blocks of `field`.
    pub fn field(&self, field: &str) -> Option<&FieldBlockPlan> {
        self.fields.iter().find(|planned| planned.field == field)
    }
}

/// The graph as uploaded by the indexer (format version 1). Point-major CSR:
/// for each point `p` in `0..point_count`, for each level `l` in `0..=levels[p]`,
/// `link_counts` has the number of links, and `links` has them, in that order.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WireGraph {
    pub format_version: u32,
    pub levels: Vec<u8>,
    pub link_counts: Vec<u32>,
    pub links: Vec<PointOffsetType>,
    /// Main entry points, best first.
    pub entry_points: Vec<EntryPoint>,
    /// Fallback entry points, used when no main one passes a filter.
    pub extra_entry_points: Vec<EntryPoint>,
}

/// Bounds the uploaded graph must respect. See [`WireGraph::validate`].
pub struct GraphLimits<'a> {
    pub point_count: usize,
    pub main_m: HnswM,
    pub block_m: HnswM,
    /// For each point, the number of planned blocks it is in.
    pub block_memberships: Vec<u16>,
    /// One flag per point, set if the point must not be linked.
    pub deleted: &'a BitSlice,
}

impl<'a> GraphLimits<'a> {
    pub fn new(params: &RemoteGraphParams, plan: &BlockPlan, deleted: &'a BitSlice) -> Self {
        let RemoteIndexParams::Hnsw(hnsw) = &params.index;
        Self {
            point_count: params.point_count,
            main_m: hnsw.main_m(),
            block_m: hnsw.block_m(),
            block_memberships: plan.block_memberships(params.point_count),
            deleted,
        }
    }

    fn max_degree(&self, point: PointOffsetType, level: usize) -> usize {
        let main = self.main_m.level_m(level);
        if level == 0 {
            // Block links are merged on top of the main links, without pruning
            let blocks = self
                .block_memberships
                .get(point as usize)
                .copied()
                .unwrap_or(0);
            main + self.block_m.m0 * usize::from(blocks)
        } else {
            main
        }
    }
}

/// Upper levels are rare: level `l` holds about `m^-l` of the points. This bounds memory on a
/// corrupted upload.
const MAX_LEVEL: usize = 64;

/// The most levels above 0 a graph may have, summed over its points.
///
/// A point has `1 / (m - 1)` of them on average, which is 1 at `m = 2`, the smallest `m` the
/// levels are drawn for. `MAX_LEVEL` alone would let an upload claim 64 for every point, and
/// each level costs a link container in the builder, linked or not.
pub fn max_upper_levels(point_count: usize) -> usize {
    2 * point_count + MAX_LEVEL
}

impl WireGraph {
    /// Check the shape of the graph (§4.7). The graph replaces a locally built one, so it must
    /// keep the invariants the local build keeps.
    pub fn validate(&self, limits: &GraphLimits) -> OperationResult<()> {
        let err = |msg: String| Err(OperationError::validation_error(msg));

        if !SUPPORTED_GRAPH_FORMAT_VERSIONS.contains(&self.format_version) {
            return err(format!(
                "unsupported graph format version {}, supported: {SUPPORTED_GRAPH_FORMAT_VERSIONS:?}",
                self.format_version,
            ));
        }
        if self.levels.len() != limits.point_count {
            return err(format!(
                "graph has {} points, expected {}",
                self.levels.len(),
                limits.point_count,
            ));
        }
        let upper_levels: usize = self.levels.iter().map(|&l| l as usize).sum();
        if upper_levels > max_upper_levels(limits.point_count) {
            return err(format!(
                "graph has {upper_levels} levels above 0, at most {} allowed",
                max_upper_levels(limits.point_count),
            ));
        }
        let expected_counts = self.levels.len() + upper_levels;
        if self.link_counts.len() != expected_counts {
            return err(format!(
                "graph has {} link lists, levels require {expected_counts}",
                self.link_counts.len(),
            ));
        }
        let expected_links: u64 = self.link_counts.iter().map(|&c| u64::from(c)).sum();
        if self.links.len() as u64 != expected_links {
            return err(format!(
                "graph has {} links, link counts require {expected_links}",
                self.links.len(),
            ));
        }

        let is_deleted =
            |point: PointOffsetType| limits.deleted.get_bit(point as usize).unwrap_or(false);

        let mut lists = self.link_lists();
        for point in 0..limits.point_count as PointOffsetType {
            let level = self.levels[point as usize] as usize;
            if level > MAX_LEVEL {
                return err(format!("point {point} has level {level} > {MAX_LEVEL}"));
            }
            let deleted = is_deleted(point);
            for l in 0..=level {
                let links = lists.next().expect("counts checked above");
                if links.is_empty() {
                    continue;
                }
                if deleted {
                    return err(format!("deleted point {point} has links on level {l}"));
                }
                if links.len() > limits.max_degree(point, l) {
                    return err(format!(
                        "point {point} has {} links on level {l}, at most {} allowed",
                        links.len(),
                        limits.max_degree(point, l),
                    ));
                }
                for &link in links {
                    if link as usize >= limits.point_count {
                        return err(format!("point {point} links to {link} out of range"));
                    }
                    if link == point {
                        return err(format!("point {point} links to itself on level {l}"));
                    }
                    if is_deleted(link) {
                        return err(format!("point {point} links to deleted point {link}"));
                    }
                    if (self.levels[link as usize] as usize) < l {
                        return err(format!(
                            "point {point} links to {link} on level {l}, above the level of {link}",
                        ));
                    }
                }
            }
        }

        for entry in self.entry_points.iter().chain(&self.extra_entry_points) {
            let EntryPoint { point_id, level } = *entry;
            if point_id as usize >= limits.point_count {
                return err(format!("entry point {point_id} out of range"));
            }
            if is_deleted(point_id) {
                return err(format!("entry point {point_id} is deleted"));
            }
            // Block entry points are on level 0 of their block, whatever the point's level
            if level > self.levels[point_id as usize] as usize {
                return err(format!(
                    "entry point {point_id} on level {level}, the point is on level {}",
                    self.levels[point_id as usize],
                ));
            }
        }
        let any_linked = (0..limits.point_count).any(|p| !is_deleted(p as PointOffsetType));
        if any_linked && self.entry_points.is_empty() && limits.main_m.m > 0 {
            return err("graph has no entry points".to_string());
        }
        Ok(())
    }

    /// The link lists, in CSR order.
    fn link_lists(&self) -> impl Iterator<Item = &[PointOffsetType]> {
        let mut offset = 0usize;
        self.link_counts.iter().map(move |&count| {
            let list = &self.links[offset..offset + count as usize];
            offset += count as usize;
            list
        })
    }

    /// Turn a validated graph into a builder with the main graph parameters, ready to save.
    pub fn into_graph_layers_builder(
        self,
        main_m: HnswM,
        ef_construct: usize,
        entry_points_num: usize,
    ) -> GraphLayersBuilder {
        let point_count = self.levels.len();
        let mut graph = GraphLayersBuilder::new_with_params(
            point_count,
            main_m,
            ef_construct,
            entry_points_num,
            HNSW_USE_HEURISTIC,
            false,
        );
        let mut lists = self.link_lists();
        for (point, &level) in self.levels.iter().enumerate() {
            let point = point as PointOffsetType;
            graph.set_levels(point, level as usize);
            for l in 0..=level as usize {
                let links = lists.next().expect("graph is validated");
                if !links.is_empty() {
                    graph.links_mut(point, l).fill_from(links.iter().copied());
                }
            }
        }
        drop(lists);
        let WireGraph {
            format_version: _,
            levels: _,
            link_counts: _,
            links: _,
            entry_points,
            extra_entry_points,
        } = self;
        graph.set_entry_points(EntryPoints::from_parts(
            entry_points,
            extra_entry_points,
            entry_points_num,
        ));
        graph
    }

    /// Serialize a finished builder, as the indexer does before uploading.
    pub fn from_graph_layers_builder(graph: &GraphLayersBuilder) -> Self {
        let point_count = graph.links_layers_len();
        let mut levels = Vec::with_capacity(point_count);
        let mut link_counts = Vec::with_capacity(point_count);
        let mut links = Vec::new();
        for point in 0..point_count as PointOffsetType {
            let level = graph.get_point_level(point);
            levels.push(level as u8);
            for l in 0..=level {
                let list = graph.links(point, l);
                link_counts.push(list.links().len() as u32);
                links.extend_from_slice(list.links());
            }
        }
        let entry_points = graph.get_entry_points();
        WireGraph {
            format_version: GRAPH_FORMAT_VERSION_V1,
            levels,
            link_counts,
            links,
            entry_points: entry_points.main_points().to_vec(),
            extra_entry_points: entry_points.extra_points(),
        }
    }

    pub fn num_points(&self) -> usize {
        self.levels.len()
    }

    /// Approximate size in bytes, for metrics.
    pub fn size_bytes(&self) -> usize {
        self.levels.len()
            + (self.link_counts.len() + self.links.len()) * size_of::<u32>()
            + (self.entry_points.len() + self.extra_entry_points.len()) * size_of::<EntryPoint>()
    }
}

#[cfg(test)]
mod tests {
    use bitvec::vec::BitVec;

    use super::*;

    fn params(point_count: usize) -> RemoteGraphParams {
        RemoteGraphParams {
            vector_name: String::new(),
            dim: 4,
            datatype: VectorStorageDatatype::Float32,
            distance: Distance::Dot,
            multivector_config: None,
            index: RemoteIndexParams::Hnsw(HnswBuildParams {
                m: 2,
                m0: 4,
                ef_construct: 10,
                entry_points_num: 1,
                payload_m: 2,
                payload_m0: 4,
                main_graph: MainGraphAlgorithm::Hnsw,
            }),
            point_count,
            vector_representation: VECTOR_REPRESENTATION_STORAGE_NATIVE.to_string(),
            vector_source: VectorSource::Storage,
            quantization: None,
            has_payload_blocks: false,
            max_block_wait: Duration::from_secs(1),
        }
    }

    /// 3 points: 0 and 1 on level 0, 2 on level 1. Point 1 deleted when `deleted_1`.
    fn graph() -> WireGraph {
        WireGraph {
            format_version: GRAPH_FORMAT_VERSION_V1,
            levels: vec![0, 0, 1],
            // p0: l0 [2]; p1: l0 []; p2: l0 [0], l1 []
            link_counts: vec![1, 0, 1, 0],
            links: vec![2, 0],
            entry_points: vec![EntryPoint {
                point_id: 2,
                level: 1,
            }],
            extra_entry_points: vec![],
        }
    }

    fn check(graph: &WireGraph, deleted: &[bool]) -> OperationResult<()> {
        let deleted: BitVec = deleted.iter().copied().collect();
        let params = params(deleted.len());
        let plan = BlockPlan::default();
        graph.validate(&GraphLimits::new(&params, &plan, &deleted))
    }

    #[test]
    fn valid_graph_roundtrips() {
        let g = graph();
        check(&g, &[false, true, false]).unwrap();
        let builder = g.clone().into_graph_layers_builder(HnswM::new(2, 4), 10, 1);
        let back = WireGraph::from_graph_layers_builder(&builder);
        assert_eq!(back, g);
    }

    #[test]
    fn rejects_corruptions() {
        let deleted = [false, true, false];
        type Corruption = Box<dyn Fn(&mut WireGraph)>;
        let cases: Vec<(&str, Corruption)> = vec![
            ("version", Box::new(|g| g.format_version = 99)),
            ("point count", Box::new(|g| g.levels.push(0))),
            (
                "counts",
                Box::new(|g| g.link_counts.pop().map(|_| ()).unwrap()),
            ),
            ("links", Box::new(|g| g.links.push(1))),
            ("out of range", Box::new(|g| g.links[0] = 7)),
            ("self link", Box::new(|g| g.links[0] = 0)),
            ("to deleted", Box::new(|g| g.links[0] = 1)),
            (
                "from deleted",
                Box::new(|g| {
                    g.link_counts = vec![1, 1, 0, 0];
                    g.links = vec![2, 0];
                }),
            ),
            (
                "degree",
                Box::new(|g| {
                    g.link_counts = vec![1, 0, 6, 0];
                    g.links = vec![2, 0, 0, 0, 0, 0, 0];
                }),
            ),
            (
                "level",
                Box::new(|g| {
                    g.link_counts = vec![1, 0, 1, 1];
                    g.links = vec![2, 0, 0];
                }),
            ),
            (
                // Every level is in range, and there is nothing else to check in a graph
                // with no links
                "too many levels",
                Box::new(|g| {
                    g.levels = vec![50, 0, 50];
                    g.link_counts = vec![0; 103];
                    g.links = vec![];
                }),
            ),
            ("entry level", Box::new(|g| g.entry_points[0].level = 5)),
            (
                "entry deleted",
                Box::new(|g| g.entry_points[0].point_id = 1),
            ),
            ("no entry", Box::new(|g| g.entry_points.clear())),
        ];
        for (name, corrupt) in cases {
            let mut g = graph();
            corrupt(&mut g);
            assert!(check(&g, &deleted).is_err(), "{name} not rejected");
        }
    }
}
