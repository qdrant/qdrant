//! Conversions between the remote indexing protocol and its segment-side types. Used by core
//! (`IndexingTask` service) and by the indexing service (client).

mod blocks;
mod quantization;

use std::time::Duration;

use segment::index::hnsw_index::graph_algorithm::{MainGraphAlgorithm, PipnnSettings};
use segment::index::hnsw_index::remote::EntryPoint;
use segment::index::hnsw_index::remote::executor::FetchedTask;
use segment::index::hnsw_index::remote::registry::{
    HeartbeatAction, TaskDescription, TaskProgress,
};
use segment::index::hnsw_index::remote::wire::{
    HnswBuildParams, RemoteGraphParams, RemoteIndexParams, VectorSource, WireGraph,
    max_upper_levels,
};
use segment::types::{Distance, MultiVectorComparator, MultiVectorConfig, VectorStorageDatatype};
use uuid::Uuid;

pub use self::blocks::{blocks_batch_from_grpc, blocks_batch_to_grpc};
use self::quantization::{quantization_from_grpc, quantization_to_grpc};
use super::qdrant::{
    IndexingDistance, IndexingEntryPoint, IndexingGraphAlgorithm, IndexingGraphHeader,
    IndexingGraphLinks, IndexingHeartbeatAction, IndexingHeartbeatRequest,
    IndexingHeartbeatResponse, IndexingHnswInsertion, IndexingHnswParams, IndexingIndexParams,
    IndexingMultiVectorComparator, IndexingPipnnParams, IndexingTaskDescription,
    IndexingVectorDatatype, IndexingVectorSource, UploadIndexingGraphRequest,
    indexing_graph_algorithm, indexing_index_params, upload_indexing_graph_request,
};

/// Metadata header carrying the one-time task token.
pub const INDEXING_TOKEN_HEADER: &str = "x-qdrant-indexing-token";

/// Highest level accepted in an upload, as in `WireGraph::validate`: level `l` holds about
/// `m^-l` of the points.
const MAX_UPLOAD_LEVEL: u8 = 64;

/// Points per `IndexingGraphLinks` chunk of an upload.
pub const UPLOAD_CHUNK_POINTS: usize = 16 * 1024;

fn datatype_to_grpc(datatype: VectorStorageDatatype) -> IndexingVectorDatatype {
    match datatype {
        VectorStorageDatatype::Float32 => IndexingVectorDatatype::IndexingFloat32,
        VectorStorageDatatype::Float16 => IndexingVectorDatatype::IndexingFloat16,
        VectorStorageDatatype::Uint8 => IndexingVectorDatatype::IndexingUint8,
        VectorStorageDatatype::Turbo4 => IndexingVectorDatatype::IndexingTurbo4,
        VectorStorageDatatype::Turbo8 => IndexingVectorDatatype::IndexingTurbo8,
        VectorStorageDatatype::Turbo16 => IndexingVectorDatatype::IndexingTurbo16,
    }
}

fn source_to_grpc(source: VectorSource) -> IndexingVectorSource {
    match source {
        VectorSource::Storage => IndexingVectorSource::IndexingSourceStorage,
        VectorSource::Quantized => IndexingVectorSource::IndexingSourceQuantized,
    }
}

fn source_from_grpc(source: IndexingVectorSource) -> VectorSource {
    match source {
        IndexingVectorSource::IndexingSourceStorage => VectorSource::Storage,
        IndexingVectorSource::IndexingSourceQuantized => VectorSource::Quantized,
    }
}

fn datatype_from_grpc(datatype: IndexingVectorDatatype) -> VectorStorageDatatype {
    match datatype {
        IndexingVectorDatatype::IndexingFloat32 => VectorStorageDatatype::Float32,
        IndexingVectorDatatype::IndexingFloat16 => VectorStorageDatatype::Float16,
        IndexingVectorDatatype::IndexingUint8 => VectorStorageDatatype::Uint8,
        IndexingVectorDatatype::IndexingTurbo4 => VectorStorageDatatype::Turbo4,
        IndexingVectorDatatype::IndexingTurbo8 => VectorStorageDatatype::Turbo8,
        IndexingVectorDatatype::IndexingTurbo16 => VectorStorageDatatype::Turbo16,
    }
}

fn distance_to_grpc(distance: Distance) -> IndexingDistance {
    match distance {
        Distance::Cosine => IndexingDistance::IndexingCosine,
        Distance::Euclid => IndexingDistance::IndexingEuclid,
        Distance::Dot => IndexingDistance::IndexingDot,
        Distance::Manhattan => IndexingDistance::IndexingManhattan,
    }
}

fn distance_from_grpc(distance: IndexingDistance) -> Result<Distance, String> {
    match distance {
        IndexingDistance::IndexingCosine => Ok(Distance::Cosine),
        IndexingDistance::IndexingEuclid => Ok(Distance::Euclid),
        IndexingDistance::IndexingDot => Ok(Distance::Dot),
        IndexingDistance::IndexingManhattan => Ok(Distance::Manhattan),
        IndexingDistance::Unknown => Err("unknown distance".to_string()),
    }
}

fn size(value: u64) -> Result<usize, String> {
    usize::try_from(value).map_err(|_| format!("index parameter too large: {value}"))
}

fn main_graph_to_grpc(main_graph: MainGraphAlgorithm) -> IndexingGraphAlgorithm {
    let algorithm = match main_graph {
        MainGraphAlgorithm::Hnsw => {
            indexing_graph_algorithm::Algorithm::Hnsw(IndexingHnswInsertion {})
        }
        MainGraphAlgorithm::Pipnn(settings) => {
            let PipnnSettings {
                leaf_k,
                reservoir_factor,
            } = settings;
            indexing_graph_algorithm::Algorithm::Pipnn(IndexingPipnnParams {
                leaf_k: leaf_k as u64,
                reservoir_factor: reservoir_factor as u64,
            })
        }
    };
    IndexingGraphAlgorithm {
        algorithm: Some(algorithm),
    }
}

/// Fails if unset, or of an algorithm this build does not know (a newer sender).
fn main_graph_from_grpc(
    main_graph: Option<IndexingGraphAlgorithm>,
) -> Result<MainGraphAlgorithm, String> {
    let Some(IndexingGraphAlgorithm { algorithm }) = main_graph else {
        return Err("no main graph algorithm".to_string());
    };
    match algorithm {
        Some(indexing_graph_algorithm::Algorithm::Hnsw(IndexingHnswInsertion {})) => {
            Ok(MainGraphAlgorithm::Hnsw)
        }
        Some(indexing_graph_algorithm::Algorithm::Pipnn(pipnn)) => {
            let IndexingPipnnParams {
                leaf_k,
                reservoir_factor,
            } = pipnn;
            Ok(MainGraphAlgorithm::Pipnn(PipnnSettings {
                leaf_k: size(leaf_k)?,
                reservoir_factor: size(reservoir_factor)?,
            }))
        }
        None => Err("unknown main graph algorithm".to_string()),
    }
}

/// The index of a task, as sent with `SubmitTask` and `GetTask`.
pub fn index_params_to_grpc(index: &RemoteIndexParams) -> IndexingIndexParams {
    let index = match index {
        RemoteIndexParams::Hnsw(hnsw) => {
            let HnswBuildParams {
                m,
                m0,
                ef_construct,
                entry_points_num,
                payload_m,
                payload_m0,
                main_graph,
            } = *hnsw;
            indexing_index_params::Index::Hnsw(IndexingHnswParams {
                m: m as u64,
                m0: m0 as u64,
                ef_construct: ef_construct as u64,
                entry_points_num: entry_points_num as u64,
                payload_m: payload_m as u64,
                payload_m0: payload_m0 as u64,
                main_graph: Some(main_graph_to_grpc(main_graph)),
            })
        }
    };
    IndexingIndexParams { index: Some(index) }
}

/// The index of a task. Fails if it is unset, or of a kind this build does not know (a newer
/// sender): the task cannot be built here.
pub fn index_params_from_grpc(
    index: Option<IndexingIndexParams>,
) -> Result<RemoteIndexParams, String> {
    let Some(IndexingIndexParams { index }) = index else {
        return Err("no index parameters".to_string());
    };
    match index {
        Some(indexing_index_params::Index::Hnsw(hnsw)) => {
            let IndexingHnswParams {
                m,
                m0,
                ef_construct,
                entry_points_num,
                payload_m,
                payload_m0,
                main_graph,
            } = hnsw;
            Ok(RemoteIndexParams::Hnsw(HnswBuildParams {
                m: size(m)?,
                m0: size(m0)?,
                ef_construct: size(ef_construct)?,
                entry_points_num: size(entry_points_num)?,
                payload_m: size(payload_m)?,
                payload_m0: size(payload_m0)?,
                main_graph: main_graph_from_grpc(main_graph)?,
            }))
        }
        None => Err("unknown kind of index".to_string()),
    }
}

/// `GetTask` response for `description`.
pub fn task_description_to_grpc(
    description: &TaskDescription,
    graph_format_versions: &[u32],
) -> Result<IndexingTaskDescription, String> {
    let TaskDescription { task_id, params } = description;
    let RemoteGraphParams {
        vector_name,
        dim,
        datatype,
        distance,
        multivector_config,
        index,
        point_count,
        vector_representation,
        vector_source,
        quantization,
        has_payload_blocks,
        max_block_wait,
    } = params;
    Ok(IndexingTaskDescription {
        task_id: task_id.to_string(),
        vector_name: vector_name.clone(),
        dim: *dim as u64,
        datatype: i32::from(datatype_to_grpc(*datatype)),
        distance: i32::from(distance_to_grpc(*distance)),
        multivector_comparator: multivector_config.map(|config| {
            i32::from(match config.comparator {
                MultiVectorComparator::MaxSim => IndexingMultiVectorComparator::IndexingMaxSim,
            })
        }),
        point_count: *point_count as u64,
        vector_representation: vector_representation.clone(),
        vector_source: i32::from(source_to_grpc(*vector_source)),
        quantization: quantization.as_ref().map(quantization_to_grpc),
        graph_format_versions: graph_format_versions.to_vec(),
        has_payload_blocks: *has_payload_blocks,
        max_block_wait_sec: max_block_wait.as_secs().max(1),
        index: Some(index_params_to_grpc(index)),
    })
}

/// The task as the indexer sees it.
pub fn task_description_from_grpc(
    description: IndexingTaskDescription,
) -> Result<FetchedTask, String> {
    let IndexingTaskDescription {
        task_id,
        vector_name,
        dim,
        datatype,
        distance,
        multivector_comparator,
        point_count,
        vector_representation,
        graph_format_versions,
        vector_source,
        quantization,
        has_payload_blocks,
        max_block_wait_sec,
        index,
    } = description;
    let vector_source = IndexingVectorSource::try_from(vector_source)
        .map(source_from_grpc)
        .map_err(|_| format!("unknown vector source {vector_source}"))?;
    let datatype = IndexingVectorDatatype::try_from(datatype)
        .map_err(|_| format!("unknown datatype {datatype}"))?;
    let distance = IndexingDistance::try_from(distance)
        .map_err(|_| format!("unknown distance {distance}"))
        .and_then(distance_from_grpc)?;
    let multivector_config = multivector_comparator
        .map(|comparator| {
            IndexingMultiVectorComparator::try_from(comparator)
                .map(|comparator| match comparator {
                    IndexingMultiVectorComparator::IndexingMaxSim => MultiVectorConfig {
                        comparator: MultiVectorComparator::MaxSim,
                    },
                })
                .map_err(|_| format!("unknown comparator {comparator}"))
        })
        .transpose()?;
    Ok(FetchedTask {
        task_id: Uuid::parse_str(&task_id).map_err(|err| format!("task id: {err}"))?,
        params: RemoteGraphParams {
            vector_name,
            dim: dim as usize,
            datatype: datatype_from_grpc(datatype),
            distance,
            multivector_config,
            index: index_params_from_grpc(index)?,
            point_count: point_count as usize,
            vector_representation,
            vector_source,
            quantization: quantization.map(quantization_from_grpc).transpose()?,
            has_payload_blocks,
            max_block_wait: Duration::from_secs(max_block_wait_sec),
        },
        graph_format_versions,
    })
}

pub fn heartbeat_to_grpc(progress: TaskProgress) -> IndexingHeartbeatRequest {
    let TaskProgress { phase, done, total } = progress;
    IndexingHeartbeatRequest { phase, done, total }
}

pub fn heartbeat_from_grpc(request: IndexingHeartbeatRequest) -> TaskProgress {
    let IndexingHeartbeatRequest { phase, done, total } = request;
    TaskProgress { phase, done, total }
}

pub fn heartbeat_action_to_grpc(action: HeartbeatAction) -> IndexingHeartbeatResponse {
    IndexingHeartbeatResponse {
        action: i32::from(match action {
            HeartbeatAction::Continue => IndexingHeartbeatAction::IndexingContinue,
            HeartbeatAction::Cancel => IndexingHeartbeatAction::IndexingCancel,
        }),
    }
}

pub fn heartbeat_action_from_grpc(response: IndexingHeartbeatResponse) -> HeartbeatAction {
    match IndexingHeartbeatAction::try_from(response.action) {
        Ok(IndexingHeartbeatAction::IndexingContinue) => HeartbeatAction::Continue,
        // Unknown actions are treated as cancel: stop rather than waste work
        Ok(IndexingHeartbeatAction::IndexingCancel) | Err(_) => HeartbeatAction::Cancel,
    }
}

fn entry_point_to_grpc(entry: &EntryPoint) -> IndexingEntryPoint {
    IndexingEntryPoint {
        point_id: entry.point_id,
        level: entry.level as u32,
    }
}

fn entry_point_from_grpc(entry: IndexingEntryPoint) -> EntryPoint {
    EntryPoint {
        point_id: entry.point_id,
        level: entry.level as usize,
    }
}

/// The upload stream of `graph`: the header, then chunks of [`UPLOAD_CHUNK_POINTS`] points.
pub fn graph_to_grpc(graph: &WireGraph) -> Vec<UploadIndexingGraphRequest> {
    let WireGraph {
        format_version,
        levels,
        link_counts,
        links,
        entry_points,
        extra_entry_points,
    } = graph;
    let mut messages = vec![UploadIndexingGraphRequest {
        part: Some(upload_indexing_graph_request::Part::Header(
            IndexingGraphHeader {
                format_version: *format_version,
                point_count: levels.len() as u64,
                entry_points: entry_points.iter().map(entry_point_to_grpc).collect(),
                extra_entry_points: extra_entry_points.iter().map(entry_point_to_grpc).collect(),
            },
        )),
    }];
    let mut counts_offset = 0;
    let mut links_offset = 0;
    for (chunk_idx, chunk_levels) in levels.chunks(UPLOAD_CHUNK_POINTS).enumerate() {
        let num_counts: usize = chunk_levels.iter().map(|&l| l as usize + 1).sum();
        let chunk_counts = &link_counts[counts_offset..counts_offset + num_counts];
        let num_links: usize = chunk_counts.iter().map(|&c| c as usize).sum();
        messages.push(UploadIndexingGraphRequest {
            part: Some(upload_indexing_graph_request::Part::Links(
                IndexingGraphLinks {
                    start: (chunk_idx * UPLOAD_CHUNK_POINTS) as u32,
                    levels: chunk_levels.to_vec(),
                    link_counts: chunk_counts.to_vec(),
                    links: links[links_offset..links_offset + num_links].to_vec(),
                },
            )),
        });
        counts_offset += num_counts;
        links_offset += num_links;
    }
    messages
}

/// Assembles an uploaded graph from its stream. Rejects malformed streams early, and bounds
/// memory: at most `max_points` points, `max_links` links and [`max_upper_levels`] levels
/// above 0 in total, with every chunk self-consistent (counts match levels, links match
/// counts) before it is kept. The shape is validated afterwards, by the registry.
pub struct GraphAssembler {
    graph: Option<WireGraph>,
    max_points: usize,
    max_links: usize,
}

impl GraphAssembler {
    pub fn new(max_points: usize, max_links: usize) -> Self {
        Self {
            graph: None,
            max_points,
            max_links,
        }
    }

    pub fn push(&mut self, message: UploadIndexingGraphRequest) -> Result<(), String> {
        match message.part {
            Some(upload_indexing_graph_request::Part::Header(header)) => {
                if self.graph.is_some() {
                    return Err("header sent twice".to_string());
                }
                let IndexingGraphHeader {
                    format_version,
                    point_count,
                    entry_points,
                    extra_entry_points,
                } = header;
                if point_count as usize > self.max_points {
                    return Err(format!(
                        "graph of {point_count} points, task has {}",
                        self.max_points
                    ));
                }
                self.graph = Some(WireGraph {
                    format_version,
                    levels: Vec::with_capacity(point_count as usize),
                    link_counts: Vec::with_capacity(point_count as usize),
                    links: Vec::new(),
                    entry_points: entry_points
                        .into_iter()
                        .map(entry_point_from_grpc)
                        .collect(),
                    extra_entry_points: extra_entry_points
                        .into_iter()
                        .map(entry_point_from_grpc)
                        .collect(),
                });
                Ok(())
            }
            Some(upload_indexing_graph_request::Part::Links(chunk)) => {
                let Some(graph) = &mut self.graph else {
                    return Err("links before header".to_string());
                };
                let IndexingGraphLinks {
                    start,
                    levels,
                    link_counts,
                    links,
                } = chunk;
                if start as usize != graph.levels.len() {
                    return Err(format!(
                        "chunk starts at {start}, expected {}",
                        graph.levels.len()
                    ));
                }
                if let Some(&level) = levels.iter().find(|&&l| l > MAX_UPLOAD_LEVEL) {
                    return Err(format!("level {level} above {MAX_UPLOAD_LEVEL}"));
                }
                // Each chunk must be self-consistent, so that `link_counts` is bounded by
                // `levels`, and `links` by `link_counts`, before anything is kept
                let expected_counts: usize = levels.iter().map(|&l| l as usize + 1).sum();
                if link_counts.len() != expected_counts {
                    return Err(format!(
                        "chunk has {} link counts, its levels require {expected_counts}",
                        link_counts.len()
                    ));
                }
                let expected_links: u64 = link_counts.iter().map(|&c| u64::from(c)).sum();
                if links.len() as u64 != expected_links {
                    return Err(format!(
                        "chunk has {} links, its counts require {expected_links}",
                        links.len()
                    ));
                }
                // A link list per point and per level above 0
                let max_link_counts = self.max_points + max_upper_levels(self.max_points);
                if graph.levels.len() + levels.len() > self.max_points
                    || graph.link_counts.len() + link_counts.len() > max_link_counts
                    || graph.links.len() + links.len() > self.max_links
                {
                    return Err("graph larger than the task allows".to_string());
                }
                graph.levels.extend_from_slice(&levels);
                graph.link_counts.extend_from_slice(&link_counts);
                graph.links.extend_from_slice(&links);
                Ok(())
            }
            None => Err("empty upload message".to_string()),
        }
    }

    pub fn finish(self) -> Result<WireGraph, String> {
        self.graph.ok_or_else(|| "no graph uploaded".to_string())
    }
}

#[cfg(test)]
mod tests {
    use segment::index::hnsw_index::remote::wire::GRAPH_FORMAT_VERSION_V1;

    use super::*;

    fn hnsw(main_graph: MainGraphAlgorithm) -> RemoteIndexParams {
        RemoteIndexParams::Hnsw(HnswBuildParams {
            m: 16,
            m0: 32,
            ef_construct: 100,
            entry_points_num: 10,
            payload_m: 8,
            payload_m0: 16,
            main_graph,
        })
    }

    #[test]
    fn unknown_main_graph_algorithm_is_refused() {
        let with_main_graph = |main_graph| {
            let mut grpc = index_params_to_grpc(&hnsw(MainGraphAlgorithm::Hnsw));
            let Some(indexing_index_params::Index::Hnsw(params)) = &mut grpc.index else {
                unreachable!("built above");
            };
            params.main_graph = main_graph;
            index_params_from_grpc(Some(grpc))
        };
        assert_eq!(
            with_main_graph(None),
            Err("no main graph algorithm".to_string())
        );
        // A case added by a newer sender decodes as no case at all
        assert_eq!(
            with_main_graph(Some(IndexingGraphAlgorithm { algorithm: None })),
            Err("unknown main graph algorithm".to_string())
        );
    }

    #[test]
    fn index_params_of_unknown_kind_are_refused() {
        assert_eq!(
            index_params_from_grpc(None),
            Err("no index parameters".to_string())
        );
        // A case added by a newer sender decodes as no case at all
        assert_eq!(
            index_params_from_grpc(Some(IndexingIndexParams { index: None })),
            Err("unknown kind of index".to_string())
        );
    }

    #[test]
    fn graph_roundtrip_through_chunks() {
        let point_count = UPLOAD_CHUNK_POINTS * 2 + 7;
        let levels: Vec<u8> = (0..point_count).map(|i| u8::from(i % 3 == 0)).collect();
        let mut link_counts = Vec::new();
        let mut links = Vec::new();
        for (i, &level) in levels.iter().enumerate() {
            for l in 0..=level {
                let n = (i + l as usize) % 4;
                link_counts.push(n as u32);
                links.extend((0..n).map(|k| ((i + k + 1) % point_count) as u32));
            }
        }
        let graph = WireGraph {
            format_version: GRAPH_FORMAT_VERSION_V1,
            levels,
            link_counts,
            links,
            entry_points: vec![EntryPoint {
                point_id: 0,
                level: 1,
            }],
            extra_entry_points: vec![EntryPoint {
                point_id: 3,
                level: 1,
            }],
        };
        let mut assembler = GraphAssembler::new(point_count, point_count * 8);
        for message in graph_to_grpc(&graph) {
            assembler.push(message).unwrap();
        }
        assert_eq!(assembler.finish().unwrap(), graph);
    }

    #[test]
    fn assembler_bounds_memory() {
        let graph = WireGraph {
            format_version: GRAPH_FORMAT_VERSION_V1,
            levels: vec![0; 10],
            link_counts: vec![0; 10],
            links: vec![],
            entry_points: vec![],
            extra_entry_points: vec![],
        };
        let mut assembler = GraphAssembler::new(5, 40);
        let messages = graph_to_grpc(&graph);
        assert!(assembler.push(messages[0].clone()).is_err());
    }

    #[test]
    fn assembler_rejects_inconsistent_chunks() {
        let header = UploadIndexingGraphRequest {
            part: Some(upload_indexing_graph_request::Part::Header(
                IndexingGraphHeader {
                    format_version: GRAPH_FORMAT_VERSION_V1,
                    point_count: 4,
                    entry_points: vec![],
                    extra_entry_points: vec![],
                },
            )),
        };
        let chunk =
            |levels: Vec<u8>, link_counts: Vec<u32>, links: Vec<u32>| UploadIndexingGraphRequest {
                part: Some(upload_indexing_graph_request::Part::Links(
                    IndexingGraphLinks {
                        start: 0,
                        levels,
                        link_counts,
                        links,
                    },
                )),
            };
        let cases = [
            // Counts without levels: would grow `link_counts` without bound
            chunk(vec![], vec![0; 1000], vec![]),
            // Counts do not match levels
            chunk(vec![0, 1], vec![1, 0], vec![1]),
            // Links do not match counts
            chunk(vec![0], vec![2], vec![1]),
            // Level too high
            chunk(vec![200], vec![0; 201], vec![]),
            // More levels than the task allows, with no links at all
            chunk(vec![64; 4], vec![0; 4 * 65], vec![]),
            // More links than the task allows
            chunk(vec![0, 0], vec![20, 20], vec![1; 40]),
        ];
        for (i, case) in cases.into_iter().enumerate() {
            let mut assembler = GraphAssembler::new(4, 16);
            assembler.push(header.clone()).unwrap();
            assert!(assembler.push(case).is_err(), "case {i} accepted");
        }
    }
}
