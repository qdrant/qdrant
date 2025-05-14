mod builder;
mod iterator;
mod posting_list;
mod value_handler;
mod visitor;

use bitpacking::BitPacker;

type BitPackerImpl = bitpacking::BitPacker4x;

/// How many elements are packed in a single chunk.
const CHUNK_SIZE: usize = BitPackerImpl::BLOCK_LEN;

pub trait FixedSizedValue: Sized + Copy + std::fmt::Debug {}

pub trait VarSizedValue {
    fn to_bytes(&self) -> Vec<u8>;

    fn from_bytes(data: &[u8]) -> Self;
}

pub trait CompressedPostingList<V> {
    fn from_builder(builder: PostingBuilder<V>) -> Self;
}

pub use builder::PostingBuilder;
pub use posting_list::{PostingChunk, PostingElement, PostingList};
pub use visitor::PostingListView;
