use std::marker::PhantomData;

use common::counter::conditioned_counter::ConditionedCounter;
use common::types::PointOffsetType;
use zerocopy::little_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::iterator::PostingIterator;
use crate::value_handler::ValueHandler;
use crate::view::PostingListView;
use crate::visitor::PostingVisitor;
use crate::{CHUNK_LEN, PostingBuilder};

/// Generic compressed posting list.
///
/// - `PostingList<Sized<()>>` when there are no values (unit type `()`), there are just compressed ids + remainders
/// - `PostingList<Sized<V>>` when there are `SizedValue` values, each id includes one value stored within the
///   fixed-sized chunks
/// - `PostingList<VarSized<V>>` when there are `VarSizedValue` values, each id includes one value in the chunk,
///   which points to the actual value in the var_size_data
#[derive(Debug, Clone)]
pub struct PostingList<H: ValueHandler> {
    pub(crate) id_data: Vec<u8>,
    pub(crate) chunks: Vec<PostingChunk<H::Sized>>,
    pub(crate) remainders: Vec<RemainderPosting<H::Sized>>,
    pub(crate) var_size_data: Vec<u8>,
    pub(crate) last_id: Option<PointOffsetType>,
    pub(crate) _phantom: PhantomData<H>,
}

/// A single element in the posting list, which contains an id and a value.
///
/// Stores a remainder of the posting list. The difference with [`PostingElement`] is
/// so that this is zerocopy friendly
#[derive(Clone, Debug, FromBytes, Immutable, IntoBytes, KnownLayout)]
#[repr(C)] // Required for IntoBytes to work correctly
pub struct RemainderPosting<S: Sized> {
    /// U32 is required for pinning endianness of the id
    pub id: U32,
    pub value: S,
}

/// A single element in the posting list, which contains an id and a value.
///
/// Output-facing structure.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PostingElement<V> {
    pub id: PointOffsetType,
    pub value: V,
}

#[derive(Debug, Clone, FromBytes, Immutable, IntoBytes, KnownLayout)]
#[repr(C)]
pub struct PostingChunk<S: Sized> {
    /// Initial data point id. Used for decompression.
    pub initial_id: U32,

    /// An offset within id_data
    pub offset: U32,

    /// Sized values for the chunk.
    pub sized_values: [S; CHUNK_LEN],
}

impl<S: Sized> PostingChunk<S> {
    /// Get byte size of the compressed ids chunk.
    pub(crate) fn get_compressed_size(
        chunks: &[PostingChunk<S>],
        ids_data: &[u8],
        chunk_index: usize,
    ) -> usize {
        if chunk_index + 1 < chunks.len() {
            chunks[chunk_index + 1].offset.get() as usize
                - chunks[chunk_index].offset.get() as usize
        } else {
            // Last chunk
            ids_data.len() - chunks[chunk_index].offset.get() as usize
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

        PostingListView::from_components(
            id_data,
            chunks,
            var_size_data,
            remainders,
            *last_id,
            ConditionedCounter::never(),
        )
    }

    pub fn visitor(&self) -> PostingVisitor<'_, H> {
        let view = self.view();
        PostingVisitor::new(view)
    }

    pub fn iter(&self) -> PostingIterator<'_, H>
    where
        H::Value: Clone,
    {
        self.visitor().into_iter()
    }

    pub fn len(&self) -> usize {
        self.chunks.len() * CHUNK_LEN + self.remainders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<H> FromIterator<(PointOffsetType, H::Value)> for PostingList<H>
where
    H: ValueHandler,
{
    fn from_iter<T: IntoIterator<Item = (PointOffsetType, H::Value)>>(iter: T) -> Self {
        let mut builder = PostingBuilder::new();
        for (id, value) in iter {
            builder.add(id, value);
        }
        builder.build_generic::<H>()
    }
}
