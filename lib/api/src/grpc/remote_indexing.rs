//! Conversions between the remote indexing protocol and its segment-side types. Used by core
//! (`IndexingTask` service) and by the indexing service (client).

use bitvec::vec::BitVec;
use segment::index::hnsw_index::remote::EntryPoint;
use segment::index::hnsw_index::remote::executor::FetchedTask;
use segment::index::hnsw_index::remote::registry::{
    HeartbeatAction, TaskDescription, TaskProgress,
};
use segment::index::hnsw_index::remote::wire::{
    BlockPlan, FieldBlockPlan, PlannedBlock, RemoteFile, RemoteGraphParams, VectorSource, WireGraph,
};
use segment::types::{Distance, MultiVectorComparator, MultiVectorConfig, VectorStorageDatatype};
use uuid::Uuid;

use super::qdrant::{
    IndexingDeletedMask, IndexingDistance, IndexingEntryPoint, IndexingFieldBlocks, IndexingFile,
    IndexingGraphHeader, IndexingGraphLinks, IndexingHeartbeatAction, IndexingHeartbeatRequest,
    IndexingHeartbeatResponse, IndexingMultiVectorComparator, IndexingPayloadBlock,
    IndexingTaskDescription, IndexingVectorDatatype, IndexingVectorSource,
    UploadIndexingGraphRequest, upload_indexing_graph_request,
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

/// `GetTask` response for `description`.
pub fn task_description_to_grpc(
    description: &TaskDescription,
    graph_format_versions: &[u32],
) -> Result<IndexingTaskDescription, String> {
    let TaskDescription {
        task_id,
        params,
        block_plan,
    } = description;
    let RemoteGraphParams {
        vector_name,
        dim,
        datatype,
        distance,
        multivector_config,
        m,
        m0,
        ef_construct,
        entry_points_num,
        payload_m,
        payload_m0,
        point_count,
        indexed_point_count,
        vector_representation,
        vector_source,
        files,
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
        m: *m as u64,
        m0: *m0 as u64,
        ef_construct: *ef_construct as u64,
        entry_points_num: *entry_points_num as u64,
        payload_m: *payload_m as u64,
        payload_m0: *payload_m0 as u64,
        point_count: *point_count as u64,
        indexed_point_count: *indexed_point_count as u64,
        vector_representation: vector_representation.clone(),
        vector_source: i32::from(source_to_grpc(*vector_source)),
        files: files
            .iter()
            .map(|file| IndexingFile {
                name: file.name.clone(),
                size: file.size,
            })
            .collect(),
        graph_format_versions: graph_format_versions.to_vec(),
        block_plan: block_plan
            .fields
            .iter()
            .map(|field| IndexingFieldBlocks {
                field: field.field.clone(),
                skip_if_connected: field.skip_if_connected,
                blocks: field
                    .blocks
                    .iter()
                    .map(|block| IndexingPayloadBlock {
                        cardinality: block.cardinality as u64,
                        points: block.points.clone(),
                    })
                    .collect(),
            })
            .collect(),
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
        m,
        m0,
        ef_construct,
        entry_points_num,
        payload_m,
        payload_m0,
        point_count,
        indexed_point_count,
        vector_representation,
        graph_format_versions,
        block_plan,
        vector_source,
        files,
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
            m: m as usize,
            m0: m0 as usize,
            ef_construct: ef_construct as usize,
            entry_points_num: entry_points_num as usize,
            payload_m: payload_m as usize,
            payload_m0: payload_m0 as usize,
            point_count: point_count as usize,
            indexed_point_count: indexed_point_count as usize,
            vector_representation,
            vector_source,
            files: files
                .into_iter()
                .map(|file| RemoteFile {
                    name: file.name,
                    size: file.size,
                })
                .collect(),
        },
        block_plan: BlockPlan {
            fields: block_plan
                .into_iter()
                .map(|field| FieldBlockPlan {
                    field: field.field,
                    skip_if_connected: field.skip_if_connected,
                    blocks: field
                        .blocks
                        .into_iter()
                        .map(|block| PlannedBlock {
                            cardinality: block.cardinality as usize,
                            points: block.points,
                        })
                        .collect(),
                })
                .collect(),
        },
        graph_format_versions,
    })
}

/// Deleted mask as bytes, LSB first.
pub fn deleted_to_grpc(deleted: &BitVec) -> IndexingDeletedMask {
    let mut bytes = vec![0u8; deleted.len().div_ceil(8)];
    for idx in deleted.iter_ones() {
        bytes[idx / 8] |= 1 << (idx % 8);
    }
    IndexingDeletedMask {
        point_count: deleted.len() as u64,
        deleted: bytes,
    }
}

pub fn deleted_from_grpc(mask: IndexingDeletedMask) -> Result<BitVec, String> {
    let IndexingDeletedMask {
        point_count,
        deleted,
    } = mask;
    let point_count = point_count as usize;
    if deleted.len() != point_count.div_ceil(8) {
        return Err(format!(
            "deleted mask has {} bytes for {point_count} points",
            deleted.len()
        ));
    }
    Ok((0..point_count)
        .map(|idx| deleted[idx / 8] & (1 << (idx % 8)) != 0)
        .collect())
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
/// memory: at most `max_points` points and `max_links` links in total, with every chunk
/// self-consistent (counts match levels, links match counts) before it is kept. The shape is
/// validated afterwards, by the registry.
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
                if graph.levels.len() + levels.len() > self.max_points
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
            // More links than the task allows
            chunk(vec![0, 0], vec![20, 20], vec![1; 40]),
        ];
        for (i, case) in cases.into_iter().enumerate() {
            let mut assembler = GraphAssembler::new(4, 16);
            assembler.push(header.clone()).unwrap();
            assert!(assembler.push(case).is_err(), "case {i} accepted");
        }
    }

    #[test]
    fn deleted_mask_roundtrip() {
        let deleted: BitVec = (0..21).map(|i| i % 3 == 0).collect();
        assert_eq!(
            deleted_from_grpc(deleted_to_grpc(&deleted)).unwrap(),
            deleted
        );
    }
}
