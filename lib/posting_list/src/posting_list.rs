use std::marker::PhantomData;

use common::types::PointOffsetType;

use crate::CHUNK_LEN;
use crate::value_handler::ValueHandler;
use crate::view::PostingListView;
use crate::visitor::PostingVisitor;

/// Generic compressed posting list.
///
/// - `PostingList<Sized<()>>` when there are no values (unit type `()`), there are just compressed ids + remainders
/// - `PostingList<Sized<V>>` when there are `SizedValue` values, each id includes one value stored within the
///   fixed-sized chunks
/// - `PostingList<VarSized<V>>` when there are `VarSizedValue` values, each id includes one value in the chunk,
///   which points to the actual value in the var_size_data
pub struct PostingList<H: ValueHandler> {
    pub(crate) id_data: Vec<u8>,
    pub(crate) chunks: Vec<PostingChunk<H::Sized>>,
    pub(crate) remainders: Vec<PostingElement<H::Sized>>,
    pub(crate) var_size_data: Vec<u8>,
    pub(crate) last_id: Option<PointOffsetType>,
    pub(crate) _phantom: PhantomData<H>,
}

#[derive(Clone, Debug)]
pub struct PostingElement<S> {
    pub(crate) id: PointOffsetType,
    pub(crate) value: S,
}

#[derive(Debug, Clone)]
#[repr(C)]
pub struct PostingChunk<S: Sized> {
    /// Initial data point id. Used for decompression.
    pub initial_id: PointOffsetType,

    /// An offset within id_data
    pub offset: u32,

    /// Sized values for the chunk.
    pub sized_values: [S; CHUNK_LEN],
}

impl<S: Sized> PostingChunk<S> {
    /// Get byte size of the compressed ids chunk.
    pub(crate) fn get_compressed_size(
        chunks: &[PostingChunk<S>],
        data: &[u8],
        chunk_index: usize,
    ) -> usize {
        if chunk_index + 1 < chunks.len() {
            chunks[chunk_index + 1].offset as usize - chunks[chunk_index].offset as usize
        } else {
            // Last chunk
            data.len() - chunks[chunk_index].offset as usize
        }
    }
}

impl<H: ValueHandler> PostingList<H> {
    pub fn view(&self) -> PostingListView<H> {
        let PostingList {
            id_data,
            chunks,
            remainders,
            var_size_data,
            last_id,
            _phantom,
        } = self;

        PostingListView {
            id_data,
            chunks,
            var_size_data,
            remainders,
            last_id: *last_id,
            _phantom: PhantomData,
        }
    }

    pub fn visitor(&self) -> PostingVisitor<'_, H> {
        let view = self.view();
        PostingVisitor::new(view)
    }
}
