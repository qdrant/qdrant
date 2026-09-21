use std::alloc::Layout;
use std::num::NonZero;

use common::bitpacking_links::{PackedLinksIterator, iterate_packed_links};
use common::bitpacking_ordered;
use common::ext::aligned_vec::ACow;
use common::generic_consts::{Random, Sequential};
use common::mmap::{Advice, AdviceSetting};
use common::types::PointOffsetType;
use common::universal_io::{
    OpenOptions, Populate, ReadBytesItem, ReadRange, UioResult, UniversalRead, UniversalReadAsync,
};
use itertools::Itertools;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::graph_links::header::{
    HEADER_MAX_SIZE, Header, HeaderCompressed, HeaderCompressedWithVectors,
};
use crate::index::hnsw_index::graph_links::view_utils::{
    bits_per_unsorted, find_level, last_offset_idx, link_vector_size, parse_links_with_vectors,
};
use crate::index::hnsw_index::graph_links::{GraphLinksFormat, GraphLinksResidency};

/// UIO-backed counterpart of [super::view::GraphLinksView].
#[derive(Debug)]
pub struct GraphLinksFile<S: UniversalRead> {
    file: S,
    point_count: u64,
    /// See [`super::view::GraphLinksView::level_offsets`].
    level_offsets: Vec<u64>,
    /// Byte offset of the reindex table ([`PointOffsetType`] per point).
    reindex_offset: u64,
    /// Byte offset of the neighbors data.
    neighbors_offset: u64,
    /// Decoder for [`Self::offsets_data`].
    offsets_reader: bitpacking_ordered::Reader,
    offsets_offset: u64,
    offsets_data: Vec<u8>,
    hnsw_m: HnswM,
    bits_per_unsorted: u8,
    compression: CompressionInfo,
}

#[derive(Debug)]
enum CompressionInfo {
    Compressed,
    CompressedWithVectors {
        base_vector_layout: Layout,
        /// `NonZero` to avoid handling unlikely corner cases.
        link_vector_size: NonZero<usize>,
        link_vector_alignment: u8,
    },
}

impl<S: UniversalRead> GraphLinksFile<S> {
    /// Preloads header + level_offsets.
    pub fn preopen_options(residency: GraphLinksResidency) -> OpenOptions {
        let eager_read_size = Self::eager_read_size();

        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: match residency {
                GraphLinksResidency::Cold => Populate::Partial(ReadRange::new(0, eager_read_size)),
                GraphLinksResidency::Cached => Populate::PreferBackground,
                GraphLinksResidency::Pinned => Populate::PreferBackground,
            },
            advice: AdviceSetting::Advice(Advice::Random),
        }
    }

    /// Header + level_offsets, rounded up.
    fn eager_read_size() -> u64 {
        // Upper bound of the number of levels in a HNSW graph.
        //
        // Most graphs have no more than 8 levels, but to be safe, let's assume
        // the worst case:
        // ```
        // N_POINTS = 2**32; HNSW_M = 2; CONFIDENCE = 0.999
        // math.log(N_POINTS / -math.log(CONFIDENCE), HNSW_M) - 0.5
        // ```
        let max_levels_guess = 42;

        (HEADER_MAX_SIZE + max_levels_guess * size_of::<u64>()) as u64
    }

    pub fn open(file: S, format: GraphLinksFormat) -> OperationResult<Self> {
        let header_len = (HEADER_MAX_SIZE as u64).min(file.len::<u8>()?);
        let bytes = file.read_bytes(0..header_len, Random, align_of::<Header>())?;
        let header = Header::parse(&bytes, format)?;

        let (
            header_size,
            point_count,
            levels_count,
            total_neighbors_bytes,
            offsets_parameters,
            hnsw_m,
            compression,
        ) = match &header {
            Header::Plain(_) => {
                let err = "Plain graph links are not supported by the batched reader";
                debug_assert!(false, "{err}");
                return Err(OperationError::service_error(err));
            }
            Header::Compressed(header) => (
                size_of::<HeaderCompressed>() as u64,
                header.point_count.get(),
                header.levels_count.get(),
                header.total_neighbors_bytes.get(),
                header.offsets_parameters,
                HnswM::new(header.m.get() as usize, header.m0.get() as usize),
                CompressionInfo::Compressed,
            ),
            Header::CompressedWithVectors(header) => {
                let link_vector_layout = header.link_vector_layout.try_into_layout()?;
                (
                    size_of::<HeaderCompressedWithVectors>() as u64,
                    header.point_count.get(),
                    header.levels_count.get(),
                    header.total_neighbors_bytes.get(),
                    header.offsets_parameters,
                    HnswM::new(header.m.get() as usize, header.m0.get() as usize),
                    CompressionInfo::CompressedWithVectors {
                        base_vector_layout: header.base_vector_layout.try_into_layout()?,
                        link_vector_size: link_vector_size(link_vector_layout)?,
                        link_vector_alignment: link_vector_layout.align() as u8,
                    },
                )
            }
        };

        let offsets = header.offsets_range()?;
        let offsets_offset = offsets.start;
        let reindex_offset = header_size + levels_count * size_of::<u64>() as u64;
        let neighbors_offset = offsets.start - total_neighbors_bytes;

        let range = ReadRange {
            byte_offset: header_size,
            length: levels_count,
        };
        let mut level_offsets = Vec::with_capacity(levels_count as usize + 1);
        level_offsets.extend_from_slice(&file.read::<_, u64>(range, Random)?);
        level_offsets.push(last_offset_idx(offsets_parameters.length.get())?);

        // Preload offsets once.
        let offsets_data = file.read_bytes(offsets, Sequential, 1)?.to_vec();

        Ok(Self {
            file,
            point_count,
            level_offsets,
            reindex_offset,
            neighbors_offset,
            offsets_reader: offsets_parameters.validate()?,
            offsets_offset,
            offsets_data,
            hnsw_m,
            bits_per_unsorted: bits_per_unsorted(point_count)?,
            compression,
        })
    }

    pub fn uio_trace_sections(&self) -> Vec<(&'static str, u64)> {
        let header_end = Self::eager_read_size();
        vec![
            ("header", 0),
            ("reindex", self.reindex_offset.max(header_end)),
            ("neighbors", self.neighbors_offset.max(header_end)),
            ("offsets", self.offsets_offset.max(header_end)),
        ]
    }

    pub fn num_points(&self) -> usize {
        self.point_count as usize
    }

    pub fn format(&self) -> GraphLinksFormat {
        match self.compression {
            CompressionInfo::Compressed => GraphLinksFormat::Compressed,
            CompressionInfo::CompressedWithVectors { .. } => {
                GraphLinksFormat::CompressedWithVectors
            }
        }
    }

    /// Returns `true` if the links format contains inline vectors.
    pub fn is_with_vectors(&self) -> bool {
        self.format().is_with_vectors()
    }

    /// See [`super::view::GraphLinksView::point_level`].
    pub fn point_level(&self, point_id: PointOffsetType) -> OperationResult<usize> {
        let range = self.reindex_range(point_id);
        let reindexed = self.file.read::<_, PointOffsetType>(range, Random)?[0];
        Ok(find_level(u64::from(reindexed), &self.level_offsets))
    }

    /// See [`super::GraphLinks::base_vector_layout`].
    pub fn base_vector_layout(&self) -> Option<Layout> {
        match self.compression {
            CompressionInfo::Compressed => None,
            CompressionInfo::CompressedWithVectors {
                base_vector_layout,
                link_vector_size: _,
                link_vector_alignment: _,
            } => Some(base_vector_layout),
        }
    }

    pub fn populate(&self) -> OperationResult<()> {
        Ok(self.file.populate()?)
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        Ok(self.file.clear_ram_cache()?)
    }

    /// Read links for a batch of points at the given level.
    pub fn links(
        &self,
        arena: &stumpalo::Arena,
        point_ids: &[PointOffsetType],
        level: usize,
        mut callback: impl FnMut(usize, PackedLinksIterator<'_>),
    ) -> OperationResult<()> {
        match self.compression {
            CompressionInfo::Compressed => {
                let sorted_count = self.hnsw_m.level_m(level);
                let cb = |position, _start, data: ACow<'_>| {
                    let iter = iterate_packed_links(&data, self.bits_per_unsorted, sorted_count);
                    callback(position, iter);
                    OperationResult::Ok(())
                };
                self.read_entries(arena, point_ids, level, 1, None, cb)
            }
            CompressionInfo::CompressedWithVectors { .. } => self.links_with_vectors(
                arena,
                point_ids,
                level,
                |position, _base_vector, links, _link_vectors| {
                    callback(position, links);
                    Ok(())
                },
            ),
        }
    }

    /// See [`super::view::GraphLinksView::links_with_vectors`].
    pub fn links_with_vectors(
        &self,
        arena: &stumpalo::Arena,
        point_ids: &[PointOffsetType],
        level: usize,
        mut callback: impl FnMut(
            usize,
            &[u8],
            PackedLinksIterator<'_>,
            std::slice::ChunksExact<'_, u8>,
        ) -> OperationResult<()>,
    ) -> OperationResult<()> {
        let (base_vector_layout, link_vector_size, link_vector_alignment) = match self.compression {
            CompressionInfo::Compressed => unimplemented!(),
            CompressionInfo::CompressedWithVectors {
                base_vector_layout,
                link_vector_size,
                link_vector_alignment,
            } => (base_vector_layout, link_vector_size, link_vector_alignment),
        };

        let sorted_count = self.hnsw_m.level_m(level);
        let align = std::cmp::max(base_vector_layout.align(), link_vector_alignment as usize);
        let cb = |position, start, data: ACow<'_>| {
            let (base_vector, links, link_vectors) = parse_links_with_vectors(
                &data,
                start as usize,
                (level == 0).then_some(base_vector_layout),
                self.bits_per_unsorted,
                sorted_count,
                link_vector_size,
                link_vector_alignment,
            );
            callback(position, base_vector, links, link_vectors)
        };
        self.read_entries(arena, point_ids, level, align, None, cb)
    }

    /// Read a single base vector.
    pub fn read_base_vector(
        &self,
        point_id: PointOffsetType,
        align: usize,
    ) -> OperationResult<ACow<'_>> {
        let (start, _end) = self
            .offsets()
            .read_pair(point_id as usize)
            .ok_or_else(|| OperationError::service_error("Expect the point"))?;
        let start = self.neighbors_offset + start;
        let size = self.base_vector_layout().unwrap().size() as u64;
        Ok(self.file.read_bytes(start..start + size, Random, align)?)
    }

    /// Batched read of base vectors.
    pub fn read_base_vectors(
        &self,
        arena: &stumpalo::Arena,
        point_ids: &[PointOffsetType],
        align: usize,
        mut callback: impl FnMut(usize, ACow<'_>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.read_entries(
            arena,
            point_ids,
            0,
            align,
            Some(self.base_vector_layout().unwrap().size() as u64),
            |position, _start, data| callback(position, data),
        )
    }

    fn read_entries(
        &self,
        arena: &stumpalo::Arena,
        point_ids: &[PointOffsetType],
        level: usize,
        align: usize,
        vec_size: Option<u64>, // If `Some(x)`, read only the first bytes.
        mut callback: impl FnMut(usize, u64, ACow<'_>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        // Compute an offset index for each point.
        let offset_indices = if level == 0 {
            arena.alloc_slice_fill_iter(point_ids.iter().map(|&id| id as usize))
        } else {
            // Read the reindex
            let level_offset = self.level_offsets[level] as usize;
            let indices = arena.alloc_slice_fill_default(point_ids.len());
            let ranges = std::iter::zip(point_ids, &mut *indices)
                .map(|(&id, out)| (out, self.reindex_range(id)));
            self.file
                .read_batch(ranges, Random, |out, reindexed: &[PointOffsetType]| {
                    *out = level_offset + reindexed[0] as usize;
                    OperationResult::Ok(())
                })?;
            indices
        };

        let offsets = self.offsets();
        let pairs = offset_indices.iter().enumerate().map(|(position, &index)| {
            (offsets.read_pair(index))
                .map(|pair| (position, pair))
                .ok_or_else(|| OperationError::service_error("Offset out of bounds"))
        });
        pairs.process_results(|pairs| {
            let items = pairs.map(|(position, (start, end))| ReadBytesItem {
                user_data: (position, start),
                range: self.neighbors_offset + start
                    ..self.neighbors_offset + vec_size.map_or(end, |len| start + len),
                align,
            });
            for result in self.file.read_bytes_iter(items, Random)? {
                let ((position, start), data) = result?;
                callback(position, start, data)?;
            }
            Ok(())
        })?
    }

    fn offsets(&self) -> bitpacking_ordered::SliceReader<'_> {
        self.offsets_reader
            .slice_reader(&self.offsets_data)
            .unwrap()
    }

    /// Byte range of the reindex table entry for the given point.
    fn reindex_range(&self, point_id: PointOffsetType) -> ReadRange {
        let offset =
            self.reindex_offset + u64::from(point_id) * size_of::<PointOffsetType>() as u64;
        ReadRange::one(offset)
    }
}

impl<S: UniversalReadAsync> GraphLinksFile<S> {
    pub async fn preload_offsets(file: S, format: GraphLinksFormat) -> UioResult<S> {
        let header_len = (HEADER_MAX_SIZE as u64).min(file.len::<u8>()?);
        let bytes = file
            .read_bytes_async(0..header_len, Random, align_of::<Header>())
            .await?;
        let offsets = Header::parse(&bytes, format)
            .and_then(|header| header.offsets_range())
            .ok();

        if let Some(offsets) = offsets {
            file.populate_range_async(offsets).await?;
        }
        Ok(file)
    }
}
