use bitpacking::BitPacker;
use common::types::PointOffsetType;

use crate::index::field_index::full_text_index::compressed_posting::compressed_chunks_reader::ChunkReader;

pub type BitPackerImpl = bitpacking::BitPacker4x;

#[derive(Clone, Debug, Default)]
pub struct CompressedPostingChunk {
    pub initial: PointOffsetType,
    pub offset: u32,
}

/// This function takes posting list and estimates chunks layout for it.
///
/// It returns a list of chunks and a list of remaining ids that are not going to be compressed.
pub fn estimate_chunks(
    posting_list: &[PointOffsetType],
) -> (Vec<CompressedPostingChunk>, Vec<PointOffsetType>, usize) {
    let bitpacker = BitPackerImpl::new();

    let mut chunks = Vec::with_capacity(posting_list.len() / BitPackerImpl::BLOCK_LEN);
    let mut compressed_data_size = 0;

    for chunk_data in posting_list.chunks(BitPackerImpl::BLOCK_LEN) {
        if chunk_data.len() == BitPackerImpl::BLOCK_LEN {
            let initial = chunk_data[0];
            let chunk_bits: u8 = bitpacker.num_bits_sorted(initial, chunk_data);
            let chunk_size = BitPackerImpl::compressed_block_size(chunk_bits);
            chunks.push(CompressedPostingChunk {
                initial,
                offset: compressed_data_size as u32,
            });
            compressed_data_size += chunk_size;
        } else {
            // last chunk that is not aligned with the block size
            // Return it as a remainder
            return (chunks, chunk_data.to_vec(), compressed_data_size);
        }
    }

    (chunks, Vec::new(), compressed_data_size)
}

/// This function takes a sorted list of ids (posting list of a term) and compresses it into a byte array.
///
/// Function requires a pre-allocated byte array to store compressed data.
/// Remainder is ignored.
pub fn compress_posting(
    posting_list: &[PointOffsetType],
    chunks: &[CompressedPostingChunk],
    data: &mut [u8],
) {
    let bitpacker = BitPackerImpl::new();

    for (chunk_index, chunk_data) in posting_list
        .chunks_exact(BitPackerImpl::BLOCK_LEN)
        .enumerate()
    {
        let chunk = &chunks[chunk_index];
        let chunk_size = ChunkReader::get_chunk_size(chunks, data, chunk_index);
        let chunk_bits = (chunk_size * 8) / BitPackerImpl::BLOCK_LEN;
        bitpacker.compress_sorted(
            chunk.initial,
            chunk_data,
            &mut data[chunk.offset as usize..chunk.offset as usize + chunk_size],
            chunk_bits as u8,
        );
    }
}
