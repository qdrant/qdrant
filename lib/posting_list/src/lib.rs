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

pub trait UnsizedValue {
    /// Returns the length of the serialized value in bytes.
    fn write_len(&self) -> usize;

    /// Writes the value to the given buffer.
    fn write_to(&self, dst: &mut [u8]);

    /// Creates a value from the given bytes.
    fn from_bytes(data: &[u8]) -> Self;
}

/// Sized value attached to each id in the posting list.
///
/// Concrete values are usually just `()` or `u32`.
pub type SizedTypeFor<V> = <<V as PostingValue>::Handler as ValueHandler>::Sized;

/// Posting list of ids, where ids are compressed.
pub type IdsPostingList = PostingList<()>;

/// Non-owning posting list of ids, where ids are compressed.
pub type IdsPostingListView<'a> = PostingListView<'a, ()>;

pub use builder::PostingBuilder;
pub use posting_list::{PostingChunk, PostingElement, PostingList, RemainderPosting};
pub use value_handler::{PostingValue, SizedHandler, UnsizedHandler, ValueHandler};
pub use view::{PostingListComponents, PostingListView};
pub use visitor::PostingVisitor;
