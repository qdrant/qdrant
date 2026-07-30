//! Helpers shared between [`super::view`] and [`super::links_file`].

use std::alloc::Layout;
use std::num::NonZero;
use std::slice::ChunksExact;

use common::bitpacking::packed_bits;
use common::bitpacking_links::{
    MIN_BITS_PER_VALUE, PackedLinksIterator, iterate_packed_links, packed_links_size,
};
use integer_encoding::VarInt as _;
use itertools::Itertools as _;

use crate::common::operation_error::{OperationError, OperationResult};

/// Parse a single neighbors entry for [`super::view::CompressionInfo::CompressedWithVectors`].
pub(super) fn parse_links_with_vectors<'a>(
    data: &'a [u8],
    data_start: usize, // The absolute offset of `data` within the neighbors data.
    base_vector_layout: Option<Layout>, // `None` on levels > 0.
    bits_per_unsorted: u8,
    sorted_count: usize,
    link_vector_size: NonZero<usize>,
    link_vector_alignment: u8,
) -> (&'a [u8], PackedLinksIterator<'a>, ChunksExact<'a, u8>) {
    let mut pos = 0;

    // 1. Base vector (`B` in the doc, only on level 0).
    let mut base_vector: &[u8] = &[];
    if let Some(layout) = base_vector_layout {
        base_vector = &data[..layout.size()];
        let addr = base_vector.as_ptr().addr();
        debug_assert!(addr.is_multiple_of(layout.align()));
        pos += layout.size();
    }

    // 2. The varint-encoded length (`#` in the doc).
    let (count, count_size) = u64::decode_var(&data[pos..]).unwrap();
    pos += count_size;

    // 3. Compressed links (`c` in the doc).
    let links_size = packed_links_size(
        &data[pos..],
        bits_per_unsorted,
        sorted_count,
        count as usize,
    );
    let links = iterate_packed_links(
        &data[pos..pos + links_size],
        bits_per_unsorted,
        sorted_count,
    );
    pos += links_size;

    // 4. Padding to align link vectors (`_` in the doc).
    pos = (data_start + pos).next_multiple_of(link_vector_alignment as usize) - data_start;

    // 5. Link vectors (`L` in the doc).
    let link_vectors = &data[pos..pos + count as usize * link_vector_size.get()];
    let addr = link_vectors.as_ptr().addr();
    debug_assert!(addr.is_multiple_of(link_vector_alignment as usize));
    let link_vectors = link_vectors.chunks_exact(link_vector_size.get());

    (base_vector, links, link_vectors)
}

/// Find the level of a point given its reindexed id.
pub(super) fn find_level(reindexed_point_id: u64, level_offsets: &[u64]) -> usize {
    for (level, (&a, &b)) in level_offsets.iter().skip(1).tuple_windows().enumerate() {
        if reindexed_point_id >= b - a {
            return level;
        }
    }
    // See the doc comment on `GraphLinksView::level_offsets`.
    level_offsets.len() - 2
}

pub(super) fn bits_per_unsorted(point_count: u64) -> OperationResult<u8> {
    let max_id = u32::try_from(point_count.saturating_sub(1))
        .map_err(|_| OperationError::service_error("Too many points in GraphLinks file"))?;
    Ok(MIN_BITS_PER_VALUE.max(packed_bits(max_id)))
}

pub(super) fn link_vector_size(layout: Layout) -> OperationResult<NonZero<usize>> {
    NonZero::try_from(layout.size())
        .map_err(|_| OperationError::service_error("Zero link vector size in GraphLinks file"))
}

pub(super) fn last_offset_idx(total_offset_count: u64) -> OperationResult<u64> {
    total_offset_count.checked_sub(1).ok_or_else(|| {
        OperationError::service_error("Total offset count should be at least 1 in GraphLinks file")
    })
}

pub(super) fn error_insufficient_size() -> OperationError {
    OperationError::service_error("Insufficient file size for GraphLinks file")
}
