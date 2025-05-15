mod builder;
mod iterator;
mod posting_list;
#[cfg(test)]
mod tests;
mod value_handler;
mod visitor;
use std::borrow::Cow;

use bitpacking::BitPacker;

type BitPackerImpl = bitpacking::BitPacker4x;

/// How many elements are packed in a single chunk.
const CHUNK_SIZE: usize = BitPackerImpl::BLOCK_LEN;

pub trait SizedValue: Sized + Copy + std::fmt::Debug {}

impl SizedValue for () {}
impl SizedValue for u32 {}
impl SizedValue for u64 {}

pub trait VarSizedValue: std::fmt::Debug {
    fn to_bytes(&self) -> Cow<'_, [u8]>;

    fn from_bytes(data: &[u8]) -> Self;
}

pub use builder::PostingBuilder;
pub use posting_list::{PostingChunk, PostingElement, PostingList};
pub use visitor::PostingListView;
