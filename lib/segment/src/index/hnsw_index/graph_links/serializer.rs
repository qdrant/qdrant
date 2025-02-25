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
use itertools::Either;
use zerocopy::IntoBytes as AsBytes;
use zerocopy::little_endian::U64 as LittleU64;

use super::header::{HEADER_VERSION_COMPRESSED, HeaderCompressed, HeaderPlain};
use super::{GraphLinks, GraphLinksEnum, GraphLinksFormat};
use crate::common::operation_error::OperationResult;

pub struct GraphLinksSerializer {
    m: usize,
    m0: usize,
    links: Vec<u8>,
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
}

impl GraphLinksSerializer {
    pub fn new(
        mut edges: Vec<Vec<Vec<PointOffsetType>>>,
        format: GraphLinksFormat,
        m: usize,
        m0: usize,
    ) -> Self {
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

        let mut links = Vec::new();
        let mut offsets = Vec::with_capacity(total_offsets_len as usize);
        offsets.push(0);
        let bits_per_unsorted = packed_bits(u32::try_from(edges.len().saturating_sub(1)).unwrap())
            .max(MIN_BITS_PER_VALUE);

        for level in 0..levels_count {
            let count = point_count_by_level.iter().skip(level).sum::<u64>() as usize;
            let (sorted_count, iter) = match level {
                0 => (m0, Either::Left(0..count)),
                _ => (m, Either::Right(back_index[..count].iter().copied())),
            };
            iter.for_each(|id| {
                let raw_links = take(&mut edges[id][level]);
                match format {
                    GraphLinksFormat::Compressed => {
                        pack_links(&mut links, raw_links, bits_per_unsorted, sorted_count);
                        offsets.push(links.len() as u64);
                    }
                    GraphLinksFormat::Plain => {
                        links.extend_from_slice(raw_links.as_bytes());
                        offsets.push((links.len() as u64) / size_of::<PointOffsetType>() as u64);
                    }
                }
            });
        }

        let kind = match format {
            GraphLinksFormat::Compressed => {
                let (compressed_offsets, offsets_parameters) =
                    bitpacking_ordered::compress(&offsets);
                Kind::Compressed {
                    compressed_offsets,
                    offsets_parameters,
                }
            }
            GraphLinksFormat::Plain => {
                let len = links.len() + reindex.as_bytes().len();
                Kind::Uncompressed {
                    offsets_padding: len.next_multiple_of(size_of::<u64>()) - len,
                    offsets,
                }
            }
        };

        Self {
            m,
            m0,
            links,
            kind,
            reindex,
            level_offsets,
        }
    }

    pub fn to_graph_links_ram(&self) -> GraphLinks {
        let format = match &self.kind {
            Kind::Uncompressed { .. } => GraphLinksFormat::Plain,
            Kind::Compressed { .. } => GraphLinksFormat::Compressed,
        };

        let size = self.level_offsets.as_bytes().len()
            + self.reindex.as_bytes().len()
            + self.links.len()
            + (match &self.kind {
                Kind::Uncompressed {
                    offsets_padding: padding,
                    offsets,
                } => size_of::<HeaderPlain>() + padding + offsets.as_bytes().len(),
                Kind::Compressed {
                    compressed_offsets,
                    offsets_parameters: _,
                } => size_of::<HeaderCompressed>() + compressed_offsets.len(),
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
                    total_links_count: self.links.len() as u64
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
                    total_links_bytes: LittleU64::new(self.links.len() as u64),
                    offsets_parameters: *offsets_parameters,
                    levels_count: LittleU64::new(self.level_offsets.len() as u64),
                    m: LittleU64::new(self.m as u64),
                    m0: LittleU64::new(self.m0 as u64),
                    zero_padding: [0; 5],
                };
                writer.write_all(header.as_bytes())?;
            }
        }

        writer.write_all(self.level_offsets.as_bytes())?;
        writer.write_all(self.reindex.as_bytes())?;
        writer.write_all(&self.links)?;
        match &self.kind {
            Kind::Uncompressed {
                offsets_padding: padding,
                offsets,
            } => {
                writer.write_zeros(*padding)?;
                writer.write_all(offsets.as_bytes())?;
            }
            Kind::Compressed {
                compressed_offsets,
                offsets_parameters: _,
            } => {
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
        file.sync_all()?;
        std::fs::rename(temp_path, path)?;
        Ok(())
    }
}
