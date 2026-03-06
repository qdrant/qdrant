use std::alloc::Layout;
use std::cmp::Reverse;
use std::io::{Seek, Write};

use common::bitpacking::packed_bits;
use common::bitpacking_links::{MIN_BITS_PER_VALUE, pack_links};
use common::bitpacking_ordered;
use common::types::PointOffsetType;
use common::zeros::WriteZerosExt;
use integer_encoding::{VarInt, VarIntWriter};
use itertools::Either;
use zerocopy::IntoBytes as AsBytes;
use zerocopy::little_endian::U64 as LittleU64;

use super::GraphLinksFormatParam;
use super::header::{HEADER_VERSION_COMPRESSED, HeaderCompressed, HeaderPlain};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::graph_links::header::{
    HEADER_VERSION_COMPRESSED_WITH_VECTORS, HeaderCompressedWithVectors, PackedVectorLayout,
};

pub fn serialize_graph_links<W: Write + Seek>(
    mut edges: Vec<Vec<Vec<PointOffsetType>>>,
    format_param: GraphLinksFormatParam,
    hnsw_m: HnswM,
    writer: &mut W,
) -> OperationResult<()> {
    let bits_per_unsorted =
        packed_bits(u32::try_from(edges.len().saturating_sub(1)).unwrap()).max(MIN_BITS_PER_VALUE);

    let vectors_layout = match format_param {
        GraphLinksFormatParam::Plain => None,
        GraphLinksFormatParam::Compressed => None,
        GraphLinksFormatParam::CompressedWithVectors(v) => {
            let vectors_layout = v.vectors_layout();
            if vectors_layout.base.size() % vectors_layout.base.align() != 0 {
                return Err(OperationError::service_error(
                    "Base vector size must be a multiple of its alignment",
                ));
            }
            if vectors_layout.link.size() % vectors_layout.link.align() != 0 {
                return Err(OperationError::service_error(
                    "Link vector size must be a multiple of its alignment",
                ));
            }
            Some(vectors_layout)
        }
    };

    // create map from index in `offsets` to point_id
    let mut back_index: Vec<PointOffsetType> = (0..edges.len()).map(|i| i as _).collect();
    // sort by max layer and use this map to build `reindex`
    back_index.sort_unstable_by_key(|&i| Reverse(edges[i as usize].len()));

    let levels_count = back_index.first().map_or(0, |&id| edges[id as usize].len());
    let mut point_count_by_level = vec![0; levels_count];
    for point in &edges {
        point_count_by_level[point.len() - 1] += 1;
    }

    // 1. Write header (placeholder, will be rewritten later)
    writer.write_zeros(match &format_param {
        GraphLinksFormatParam::Plain => size_of::<HeaderPlain>(),
        GraphLinksFormatParam::Compressed => size_of::<HeaderCompressed>(),
        GraphLinksFormatParam::CompressedWithVectors(_) => size_of::<HeaderCompressedWithVectors>(),
    })?;

    // 2. Write level offsets
    let mut total_offsets_len = 0;
    {
        let mut suffix_sum = point_count_by_level.iter().sum::<u64>();
        for &value in point_count_by_level.iter() {
            writer.write_all(total_offsets_len.as_bytes())?;
            total_offsets_len += suffix_sum;
            suffix_sum -= value;
        }
        total_offsets_len += 1;
    }

    // 3. Write reindex (aka map from point id to index in `offsets`)
    {
        let mut reindex = vec![0; back_index.len()];
        for i in 0..back_index.len() {
            reindex[back_index[i] as usize] = i as PointOffsetType;
        }
        writer.write_all(reindex.as_bytes())?;
    }

    // 4. Write neighbors padding (if applicable)
    if let Some(vectors_layout) = vectors_layout.as_ref() {
        let pos = writer.stream_position()? as usize;
        let alignment = std::cmp::max(vectors_layout.base.align(), vectors_layout.link.align());
        writer.write_zeros(pos.next_multiple_of(alignment) - pos)?;
    }

    // 5. Write neighbors (and calculate `offsets`)
    let mut links_buf = Vec::new();
    let mut offset = 0; // elements for Plain, bytes for Compressed/CompressedWithVectors
    let mut offsets = Vec::with_capacity(total_offsets_len as usize);
    offsets.push(0);

    #[expect(clippy::needless_range_loop)]
    // this clippy lint is positively demented, can't wait till they remove it 🙄
    for level in 0..levels_count {
        let count = point_count_by_level.iter().skip(level).sum::<u64>() as usize;
        let (level_m, mut iter) = match level {
            0 => (hnsw_m.m0, Either::Left((0..count).map(|x| x as u32))),
            _ => (hnsw_m.m, Either::Right(back_index[..count].iter().copied())),
        };

        iter.try_for_each(|id| {
            let mut raw_links = std::mem::take(&mut edges[id as usize][level]);
            match format_param {
                GraphLinksFormatParam::Plain => {
                    writer.write_all(raw_links.as_bytes())?;
                    offset += raw_links.len();
                }
                GraphLinksFormatParam::Compressed => {
                    pack_links(&mut links_buf, &mut raw_links, bits_per_unsorted, level_m);
                    writer.write_all(&links_buf)?;
                    offset += links_buf.len();
                }
                GraphLinksFormatParam::CompressedWithVectors(vectors) => {
                    // Unwrap safety: `vectors_layout` is `Some` for `CompressedWithVectors`.
                    let vectors_layout = vectors_layout.as_ref().unwrap();

                    // 1. Base vector (`B` in the doc, only on level 0).
                    if level == 0 {
                        let vector = vectors.get_base_vector(id)?;
                        if vector.len() != vectors_layout.base.size() {
                            return Err(OperationError::service_error("Vector size mismatch"));
                        }
                        writer.write_all(vector)?;
                        offset += vector.len();
                    }

                    // 2. The varint-encoded length (`#` in the doc).
                    writer.write_varint(raw_links.len() as u64)?;
                    offset += VarInt::required_space(raw_links.len() as u64);

                    // 3. Compressed links (`c` in the doc)
                    pack_links(&mut links_buf, &mut raw_links, bits_per_unsorted, level_m);
                    writer.write_all(&links_buf)?;
                    offset += links_buf.len();

                    // 4. Padding to align link vectors (`_` in the doc).
                    let padding = offset.next_multiple_of(vectors_layout.link.align()) - offset;
                    writer.write_zeros(padding)?;
                    offset += padding;

                    // 5. Link vectors (`L` in the doc).
                    // Write them in the same order as `raw_links`.
                    for i in raw_links {
                        let vector = vectors.get_link_vector(i)?;
                        if vector.len() != vectors_layout.link.size() {
                            return Err(OperationError::service_error("Vector size mismatch"));
                        }
                        writer.write_all(vector)?;
                        offset += vector.len();
                    }

                    // 6. Padding to align the next base vector (`_` in the doc).
                    if level == 0 {
                        let padding = offset.next_multiple_of(vectors_layout.base.align()) - offset;
                        writer.write_zeros(padding)?;
                        offset += padding;
                    }
                }
            }
            offsets.push(offset as u64);
            links_buf.clear();
            Ok(())
        })?;
    }
    drop(back_index);

    // 7. Write offsets (and get some info for the header)
    let (offsets_padding, offsets_parameters) = match &format_param {
        GraphLinksFormatParam::Plain => {
            let len = writer.stream_position()? as usize;
            let offsets_padding = len.next_multiple_of(size_of::<u64>()) - len;
            writer.write_zeros(offsets_padding)?;
            writer.write_all(offsets.as_bytes())?;
            (Some(offsets_padding), None)
        }
        GraphLinksFormatParam::Compressed | GraphLinksFormatParam::CompressedWithVectors(_) => {
            let (compressed_offsets, offsets_parameters) = bitpacking_ordered::compress(&offsets);
            writer.write_all(&compressed_offsets)?;
            (None, Some(offsets_parameters))
        }
    };

    // 8. Write header (not a placeholder anymore)
    writer.seek(std::io::SeekFrom::Start(0))?;
    match format_param {
        GraphLinksFormatParam::Plain => {
            let header = HeaderPlain {
                point_count: edges.len() as u64,
                levels_count: levels_count as u64,
                total_neighbors_count: offset as u64,
                total_offset_count: offsets.len() as u64,
                offsets_padding_bytes: offsets_padding.unwrap() as u64,
                zero_padding: [0; 24],
            };
            writer.write_all(header.as_bytes())?;
        }
        GraphLinksFormatParam::Compressed => {
            let header = HeaderCompressed {
                version: LittleU64::from(HEADER_VERSION_COMPRESSED),
                point_count: LittleU64::new(edges.len() as u64),
                total_neighbors_bytes: LittleU64::new(offset as u64),
                offsets_parameters: offsets_parameters.unwrap(),
                levels_count: LittleU64::new(levels_count as u64),
                m: LittleU64::new(hnsw_m.m as u64),
                m0: LittleU64::new(hnsw_m.m0 as u64),
                zero_padding: [0; 5],
            };
            writer.write_all(header.as_bytes())?;
        }
        GraphLinksFormatParam::CompressedWithVectors(_) => {
            let vectors_layout = vectors_layout.as_ref().unwrap();
            let header = HeaderCompressedWithVectors {
                version: LittleU64::from(HEADER_VERSION_COMPRESSED_WITH_VECTORS),
                point_count: LittleU64::new(edges.len() as u64),
                total_neighbors_bytes: LittleU64::new(offset as u64),
                offsets_parameters: offsets_parameters.unwrap(),
                levels_count: LittleU64::new(levels_count as u64),
                m: LittleU64::new(hnsw_m.m as u64),
                m0: LittleU64::new(hnsw_m.m0 as u64),
                base_vector_layout: pack_layout(&vectors_layout.base),
                link_vector_layout: pack_layout(&vectors_layout.link),
                zero_padding: [0; 3],
            };
            writer.write_all(header.as_bytes())?;
        }
    };

    Ok(())
}

fn pack_layout(layout: &Layout) -> PackedVectorLayout {
    PackedVectorLayout {
        size: LittleU64::new(layout.size() as u64),
        alignment: u8::try_from(layout.align()).expect("Alignment must fit in u8"),
    }
}
