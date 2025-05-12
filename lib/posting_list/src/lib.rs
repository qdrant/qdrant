mod posting_builder;
mod posting_list;
mod value_handler;

use bitpacking::BitPacker;
use posting_builder::PostingBuilder;

type BitPackerImpl = bitpacking::BitPacker4x;

/// How many elements are packed in a single chunk.
const CHUNK_SIZE: usize = BitPackerImpl::BLOCK_LEN;

pub trait FixedSizedValue: Sized + std::fmt::Debug {}

pub trait VarSizedValue {
    fn to_bytes(&self) -> Vec<u8>;

    fn from_bytes(data: &[u8]) -> Self;
}

trait CompressedPostingList<V> {
    type Fixed;
    type Var;

    fn from_builder(builder: PostingBuilder<V>) -> Self;
}
