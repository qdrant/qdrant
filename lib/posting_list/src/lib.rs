mod builder;
mod iterator;
mod posting_list;
#[cfg(test)]
mod tests;
mod value_handler;
mod view;
mod visitor;

use bitpacking::BitPacker;

type BitPackerImpl = bitpacking::BitPacker4x;

/// How many elements are packed in a single chunk.
const CHUNK_LEN: usize = 128;
const _: () = assert!(128 == BitPackerImpl::BLOCK_LEN);

pub trait SizedValue: Sized + Copy + std::fmt::Debug {}

impl SizedValue for () {}
impl SizedValue for u32 {}
impl SizedValue for u64 {}

pub trait UnsizedValue: std::fmt::Debug {
    fn write_len(&self) -> usize;

    fn write_to(&self, dst: &mut [u8]);

    fn from_bytes(data: &[u8]) -> Self;
}

/// Posting list of ids, where ids are compressed.
pub type IdsPostingList = PostingList<SizedHandler<()>>;

/// Posting list of ids + small fixed-sized values, where ids are compressed.
pub type WeightsPostingList<W> = PostingList<SizedHandler<W>>;

/// Posting list of ids + variably-sized values, where ids are compressed.
pub type VarPostingList<V> = PostingList<UnsizedHandler<V>>;

/// Non-owning posting list of ids, where ids are compressed.
pub type IdsPostingListView<'a> = PostingListView<'a, SizedHandler<()>>;

/// Non-owning posting list of ids + small fixed-sized values, where ids are compressed.
pub type WeightsPostingListView<'a, W> = PostingListView<'a, SizedHandler<W>>;

/// Non-owning posting list of ids + variably-sized values, where ids are compressed.
pub type VarPostingListView<'a, V> = PostingListView<'a, UnsizedHandler<V>>;

pub use builder::PostingBuilder;
pub use posting_list::{PostingChunk, PostingElement, PostingList};
use value_handler::{SizedHandler, UnsizedHandler};
pub use view::PostingListView;
