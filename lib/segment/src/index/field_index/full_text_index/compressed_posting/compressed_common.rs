use bitpacking::BitPacker;
use common::types::PointOffsetType;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

pub type BitPackerImpl = bitpacking::BitPacker4x;

#[derive(Clone, Debug, Default, AsBytes, FromBytes, FromZeroes)]
#[repr(C)]
pub struct CompressedPostingChunksIndex {
    /// First document ID value in the compressed chunk
    pub initial: PointOffsetType,
    /// Position of chunks in the compressed posting list
    pub offset: u32,
}

/// This function takes posting list and estimates chunks layout for it.
///
/// It returns a list of chunks, a list of remaining ids that are not going to be compressed, and the compressed chunks data size
fn estimate_chunks(
    posting_list: &[PointOffsetType],
) -> (
    Vec<CompressedPostingChunksIndex>,
    Vec<PointOffsetType>,
    usize,
) {
    let bitpacker = BitPackerImpl::new();

    let mut chunks = Vec::with_capacity(posting_list.len() / BitPackerImpl::BLOCK_LEN);
    let mut compressed_data_size = 0;

    let mut chunks_iter = posting_list.chunks_exact(BitPackerImpl::BLOCK_LEN);

    for chunk_data in &mut chunks_iter {
        let initial = chunk_data[0];
        let chunk_bits: u8 = bitpacker.num_bits_sorted(initial, chunk_data);
        let chunk_size = BitPackerImpl::compressed_block_size(chunk_bits);
        chunks.push(CompressedPostingChunksIndex {
            initial,
            offset: compressed_data_size as u32,
        });
        compressed_data_size += chunk_size;
    }

    let remainder = chunks_iter.remainder().to_vec();

    (chunks, remainder, compressed_data_size)
}

pub fn get_chunk_size(
    chunks: &[CompressedPostingChunksIndex],
    data_len_bytes: usize,
    chunk_index: usize,
) -> usize {
    assert!(chunk_index < chunks.len());
    if chunk_index + 1 < chunks.len() {
        chunks[chunk_index + 1].offset as usize - chunks[chunk_index].offset as usize
    } else {
        data_len_bytes - chunks[chunk_index].offset as usize
    }
}

/// This function takes a sorted list of ids (posting list of a term) and compresses it into a byte array.
///
/// Function requires a pre-allocated byte array to store compressed data.
/// Remainder is ignored.
fn compress_chunks(
    posting_list: &[PointOffsetType],
    chunks: &[CompressedPostingChunksIndex],
    data: &mut [u8],
) {
    let bitpacker = BitPackerImpl::new();

    for (chunk_index, chunk_data) in posting_list
        .chunks_exact(BitPackerImpl::BLOCK_LEN)
        .enumerate()
    {
        let chunk = &chunks[chunk_index];
        let chunk_size = get_chunk_size(chunks, data.len(), chunk_index);
        let chunk_bits = (chunk_size * 8) / BitPackerImpl::BLOCK_LEN;
        bitpacker.compress_sorted(
            chunk.initial,
            chunk_data,
            &mut data[chunk.offset as usize..chunk.offset as usize + chunk_size],
            chunk_bits as u8,
        );
    }
}

pub fn compress_posting(
    posting_list: &[PointOffsetType],
) -> (
    Vec<CompressedPostingChunksIndex>,
    Vec<PointOffsetType>,
    Vec<u8>,
) {
    let (chunks, remainder_postings, data_size) = estimate_chunks(posting_list);

    // compressed data storage
    let mut data = vec![0u8; data_size];

    compress_chunks(posting_list, &chunks, &mut data);

    (chunks, remainder_postings, data)
}
