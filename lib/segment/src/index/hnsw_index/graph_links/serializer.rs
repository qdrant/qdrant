use std::alloc::Layout;
use std::cmp::Reverse;
use std::fs::File;
use std::io::Write;
use std::mem::{size_of, take};
use std::path::Path;

use common::bitpacking::packed_bits;
use common::bitpacking_links::{MIN_BITS_PER_VALUE, pack_links};
use common::bitpacking_ordered;
use common::types::PointOffsetType;
use common::zeros::WriteZerosExt;
use integer_encoding::VarIntWriter as _;
use itertools::Either;
use zerocopy::IntoBytes as AsBytes;
use zerocopy::little_endian::U64 as LittleU64;

use super::header::{HEADER_VERSION_COMPRESSED, HeaderCompressed, HeaderPlain};
use super::{GraphLinks, GraphLinksEnum, GraphLinksFormat, GraphLinksFormatParam};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::graph_links::header::{
    HEADER_VERSION_COMPRESSED_WITH_VECTORS, HeaderCompressedWithVectors,
};

pub struct GraphLinksSerializer {
    hnsw_m: HnswM,
    neighbors: Vec<u8>,
    kind: Kind,
    reindex: Vec<PointOffsetType>,
    level_offsets: Vec<u64>,
}

enum Kind {
    Uncompressed {
        offsets_padding: usize,
        offsets: Vec<u64>,
    },
    Compressed {
        compressed_offsets: Vec<u8>,
        offsets_parameters: bitpacking_ordered::Parameters,
    },
    CompressedWithVectors {
        neighbors_padding: usize,
        compressed_offsets: Vec<u8>,
        offsets_parameters: bitpacking_ordered::Parameters,
        vector_layout: Layout,
    },
}

impl GraphLinksSerializer {
    pub fn new(
        mut edges: Vec<Vec<Vec<PointOffsetType>>>,
        format_param: GraphLinksFormatParam,
        hnsw_m: HnswM,
    ) -> OperationResult<Self> {
        // create map from index in `offsets` to point_id
        let mut back_index: Vec<usize> = (0..edges.len()).collect();
        // sort by max layer and use this map to build `Self.reindex`
        back_index.sort_unstable_by_key(|&i| Reverse(edges[i].len()));

        // `reindex` is map from point id to index in `Self.offsets`
        let mut reindex = vec![0; back_index.len()];
        for i in 0..back_index.len() {
            reindex[back_index[i]] = i as PointOffsetType;
        }

        let levels_count = back_index
            .first()
            .map_or(0, |&point_id| edges[point_id].len());
        let mut point_count_by_level = vec![0; levels_count];
        for point in &edges {
            point_count_by_level[point.len() - 1] += 1;
        }

        let mut total_offsets_len = 0;
        let mut level_offsets = Vec::with_capacity(levels_count);
        let mut suffix_sum = point_count_by_level.iter().sum::<u64>();
        for &value in point_count_by_level.iter() {
            level_offsets.push(total_offsets_len);
            total_offsets_len += suffix_sum;
            suffix_sum -= value;
        }
        total_offsets_len += 1;

        let mut neighbors = Vec::new();
        let mut offsets = Vec::with_capacity(total_offsets_len as usize);
        offsets.push(0);
        let bits_per_unsorted = packed_bits(u32::try_from(edges.len().saturating_sub(1)).unwrap())
            .max(MIN_BITS_PER_VALUE);

        let vector_layout = match format_param {
            GraphLinksFormatParam::Plain => None,
            GraphLinksFormatParam::Compressed => None,
            GraphLinksFormatParam::CompressedWithVectors(v) => {
                let vector_layout = v.vector_layout()?;
                if vector_layout.size() % vector_layout.align() != 0 {
                    return Err(OperationError::service_error(
                        "Vector size must be a multiple of its alignment",
                    ));
                }
                Some(vector_layout)
            }
        };

        for level in 0..levels_count {
            let count = point_count_by_level.iter().skip(level).sum::<u64>() as usize;
            let (sorted_count, mut iter) = match level {
                0 => (hnsw_m.m0, Either::Left(0..count)),
                _ => (hnsw_m.m, Either::Right(back_index[..count].iter().copied())),
            };

            let mut compressed_links_buf = Vec::new();

            iter.try_for_each(|id| {
                let mut raw_links = take(&mut edges[id][level]);
                match format_param {
                    GraphLinksFormatParam::Compressed => {
                        pack_links(
                            &mut neighbors,
                            &mut raw_links,
                            bits_per_unsorted,
                            sorted_count,
                        );
                        offsets.push(neighbors.len() as u64);
                    }
                    GraphLinksFormatParam::Plain => {
                        neighbors.extend_from_slice(raw_links.as_bytes());
                        offsets
                            .push((neighbors.len() as u64) / size_of::<PointOffsetType>() as u64);
                    }
                    GraphLinksFormatParam::CompressedWithVectors(vectors) => {
                        // Unwrap safety: `vector_layout` is `Some` for `CompressedWithVectors`.
                        let vector_layout = vector_layout.unwrap();

                        // 1. The varint-encoded length (`N` in the doc).
                        // Unwrap safety: `impl Write for Vec<u8>` never fails.
                        neighbors.write_varint(raw_links.len() as u64).unwrap();

                        // 2. Padding to align vectors (`_` in the doc).
                        let padding = neighbors.len().next_multiple_of(vector_layout.align())
                            - neighbors.len();
                        neighbors.write_zeros(padding).unwrap();

                        // Prepare compressed links.
                        // NOTE: we can avoid using intermediate buffer if we
                        // split `pack_links` into two steps: prepare and write.
                        // But it's not a bottleneck for now.
                        pack_links(
                            &mut compressed_links_buf,
                            &mut raw_links,
                            bits_per_unsorted,
                            sorted_count,
                        );

                        // 3. Vectors (`V` in the doc).
                        // Write them in the same order as `raw_links`.
                        for i in raw_links {
                            let vector = vectors.get_vector(i)?;
                            if vector.len() != vector_layout.size() {
                                return Err(OperationError::service_error("Vector size mismatch"));
                            }
                            neighbors.extend(vector);
                        }

                        // 4. Compressed links (`c` in the doc)
                        neighbors.extend_from_slice(&compressed_links_buf);
                        compressed_links_buf.clear();

                        offsets.push(neighbors.len() as u64);
                    }
                }
                Ok(())
            })?;
        }

        let kind = match format_param {
            GraphLinksFormatParam::Compressed => {
                let (compressed_offsets, offsets_parameters) =
                    bitpacking_ordered::compress(&offsets);
                Kind::Compressed {
                    compressed_offsets,
                    offsets_parameters,
                }
            }
            GraphLinksFormatParam::CompressedWithVectors(_) => {
                let (compressed_offsets, offsets_parameters) =
                    bitpacking_ordered::compress(&offsets);

                let len = size_of::<HeaderCompressedWithVectors>()
                    + level_offsets.as_bytes().len()
                    + reindex.as_bytes().len();

                // Unwrap safety: `vector_layout` is `Some` for `CompressedWithVectors`.
                let vector_layout = vector_layout.unwrap();
                Kind::CompressedWithVectors {
                    neighbors_padding: len.next_multiple_of(vector_layout.align()) - len,
                    compressed_offsets,
                    offsets_parameters,
                    vector_layout,
                }
            }
            GraphLinksFormatParam::Plain => {
                let len = neighbors.len() + reindex.as_bytes().len();
                Kind::Uncompressed {
                    offsets_padding: len.next_multiple_of(size_of::<u64>()) - len,
                    offsets,
                }
            }
        };

        Ok(Self {
            hnsw_m,
            neighbors,
            kind,
            reindex,
            level_offsets,
        })
    }

    pub fn to_graph_links_ram(&self) -> GraphLinks {
        let format = match &self.kind {
            Kind::Uncompressed { .. } => GraphLinksFormat::Plain,
            Kind::Compressed { .. } => GraphLinksFormat::Compressed,
            Kind::CompressedWithVectors { .. } => GraphLinksFormat::CompressedWithVectors,
        };

        let size = self.level_offsets.as_bytes().len()
            + self.reindex.as_bytes().len()
            + self.neighbors.len()
            + (match &self.kind {
                Kind::Uncompressed {
                    offsets_padding,
                    offsets,
                } => size_of::<HeaderPlain>() + offsets_padding + offsets.as_bytes().len(),
                Kind::Compressed {
                    compressed_offsets,
                    offsets_parameters: _,
                } => size_of::<HeaderCompressed>() + compressed_offsets.len(),
                Kind::CompressedWithVectors {
                    neighbors_padding,
                    compressed_offsets,
                    offsets_parameters: _,
                    vector_layout: _,
                } => {
                    size_of::<HeaderCompressedWithVectors>()
                        + neighbors_padding
                        + compressed_offsets.len()
                }
            });

        let mut data = Vec::with_capacity(size);
        // Unwrap should be the safe as `impl Write` for `Vec` never fails.
        self.serialize_to_writer(&mut data).unwrap();
        debug_assert_eq!(data.len(), size);
        // Unwrap should be safe as we just created the data.
        GraphLinks::try_new(GraphLinksEnum::Ram(data), |x| x.load_view(format)).unwrap()
    }

    fn serialize_to_writer(&self, writer: &mut impl Write) -> std::io::Result<()> {
        match &self.kind {
            Kind::Uncompressed {
                offsets_padding,
                offsets,
            } => {
                let header = HeaderPlain {
                    point_count: self.reindex.len() as u64,
                    levels_count: self.level_offsets.len() as u64,
                    total_neighbors_count: self.neighbors.len() as u64
                        / size_of::<PointOffsetType>() as u64,
                    total_offset_count: offsets.len() as u64,
                    offsets_padding_bytes: *offsets_padding as u64,
                    zero_padding: [0; 24],
                };
                writer.write_all(header.as_bytes())?;
            }
            Kind::Compressed {
                compressed_offsets: _,
                offsets_parameters,
            } => {
                let header = HeaderCompressed {
                    version: HEADER_VERSION_COMPRESSED.into(),
                    point_count: LittleU64::new(self.reindex.len() as u64),
                    total_neighbors_bytes: LittleU64::new(self.neighbors.len() as u64),
                    offsets_parameters: *offsets_parameters,
                    levels_count: LittleU64::new(self.level_offsets.len() as u64),
                    m: LittleU64::new(self.hnsw_m.m as u64),
                    m0: LittleU64::new(self.hnsw_m.m0 as u64),
                    zero_padding: [0; 5],
                };
                writer.write_all(header.as_bytes())?;
            }
            Kind::CompressedWithVectors {
                neighbors_padding: _,
                compressed_offsets: _,
                offsets_parameters,
                vector_layout,
            } => {
                let header = HeaderCompressedWithVectors {
                    version: HEADER_VERSION_COMPRESSED_WITH_VECTORS.into(),
                    point_count: LittleU64::new(self.reindex.len() as u64),
                    total_neighbors_bytes: LittleU64::new(self.neighbors.len() as u64),
                    offsets_parameters: *offsets_parameters,
                    levels_count: LittleU64::new(self.level_offsets.len() as u64),
                    m: LittleU64::new(self.hnsw_m.m as u64),
                    m0: LittleU64::new(self.hnsw_m.m0 as u64),
                    vector_size_bytes: LittleU64::new(vector_layout.size() as u64),
                    vector_alignment: vector_layout
                        .align()
                        .try_into()
                        .expect("Alignment must fit in u8"),
                    zero_padding: [0; 4],
                };
                writer.write_all(header.as_bytes())?;
            }
        }

        writer.write_all(self.level_offsets.as_bytes())?;
        writer.write_all(self.reindex.as_bytes())?;

        match &self.kind {
            Kind::Uncompressed {
                offsets_padding,
                offsets,
            } => {
                writer.write_all(&self.neighbors)?;
                writer.write_zeros(*offsets_padding)?;
                writer.write_all(offsets.as_bytes())?;
            }
            Kind::Compressed {
                compressed_offsets,
                offsets_parameters: _,
            } => {
                writer.write_all(&self.neighbors)?;
                writer.write_all(compressed_offsets)?;
            }
            Kind::CompressedWithVectors {
                neighbors_padding,
                compressed_offsets,
                offsets_parameters: _,
                vector_layout: _,
            } => {
                writer.write_zeros(*neighbors_padding)?;
                writer.write_all(&self.neighbors)?;
                writer.write_all(compressed_offsets)?;
            }
        }

        Ok(())
    }

    pub fn save_as(&self, path: &Path) -> OperationResult<()> {
        let temp_path = path.with_extension("tmp");
        let file = File::create(temp_path.as_path())?;
        let mut buf = std::io::BufWriter::new(&file);
        self.serialize_to_writer(&mut buf)?;

        // Explicitly flush write buffer so we can catch IO errors
        buf.flush()?;
        file.sync_all()?;

        std::fs::rename(temp_path, path)?;
        Ok(())
    }
}
