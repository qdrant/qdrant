use std::borrow::Borrow;
use std::marker::PhantomData;

use common::types::PointOffsetType;
use zerocopy::little_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::iterator::PostingIterator;
use crate::value_handler::PostingValue;
use crate::view::PostingListView;
use crate::visitor::PostingVisitor;
use crate::{CHUNK_LEN, PostingBuilder, SizedTypeFor};

/// Generic compressed posting list.
///
/// - `PostingList<()>` when there are no values (unit type `()`), there are just compressed ids + remainders
/// - `PostingList<V>` when there are values associated to each id. The value must implement `PostingValue`, which chooses the appropriate value handler.
///   There are two available handlers:
///   - [`SizedHandler`][1]: needs the value to implement [`SizedValue`][3]. Stores the value within the chunk.
///   - [`UnsizedHandler`][2]: needs the value to implement [`UnsizedValue`][4]. Stores the value in the var_size_data.
///
/// [1]: crate::value_handler::SizedHandler
/// [2]: crate::value_handler::UnsizedHandler
/// [3]: crate::SizedValue
/// [4]: crate::UnsizedValue
#[derive(Debug, Clone)]
pub struct PostingList<V: PostingValue> {
    pub(crate) id_data: Vec<u8>,
    pub(crate) chunks: Vec<PostingChunk<SizedTypeFor<V>>>,
    pub(crate) remainders: Vec<RemainderPosting<SizedTypeFor<V>>>,
    pub(crate) var_size_data: Vec<u8>,
    pub(crate) last_id: Option<PointOffsetType>,
    pub(crate) _phantom: PhantomData<V>,
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

impl<V: PostingValue> PostingList<V> {
    pub fn view(&self) -> PostingListView<V> {
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
            var_size_data.borrow(),
            remainders,
            *last_id,
        )
    }

    pub fn visitor(&self) -> PostingVisitor<'_, V> {
        let view = self.view();
        PostingVisitor::new(view)
    }

    pub fn iter(&self) -> PostingIterator<'_, V> {
        self.visitor().into_iter()
    }

    pub fn len(&self) -> usize {
        self.chunks.len() * CHUNK_LEN + self.remainders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<V: PostingValue> FromIterator<(PointOffsetType, V)> for PostingList<V> {
    fn from_iter<T: IntoIterator<Item = (PointOffsetType, V)>>(iter: T) -> Self {
        let mut builder = PostingBuilder::new();
        for (id, value) in iter {
            builder.add(id, value);
        }
        builder.build()
    }
}
