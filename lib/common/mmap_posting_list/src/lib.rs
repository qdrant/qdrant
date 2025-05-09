use std::marker::PhantomData;

use bitpacking::BitPacker;
use common::types::PointOffsetType;

type BitPackerImpl = bitpacking::BitPacker4x;

/// How many elements are packed in a single chunk.
const CHUNK_SIZE: usize = BitPackerImpl::BLOCK_LEN;

trait FixedSizedValue: Sized + std::fmt::Debug {}

trait VarSizedValue {
    /// Size of the value in bytes
    fn size(&self) -> usize;

    fn to_bytes(&self) -> Vec<u8>;

    fn from_bytes(data: &[u8]) -> Self;
}

/// Trait to abstract the handling of values in PostingList
///
/// This trait handles the differences between fixed-size and variable-size value
/// implementations, allowing us to have a unified implementation of `from_builder`.
///
/// - For fixed-size values, the storage type S is the same as the value type V
/// - For variable-size values, S is a pointer/offset (u32) into the var_sized_data
trait ValueHandler<V, S: Sized + Copy> {
    /// Process values before storage and return the necessary var_sized_data
    ///
    /// For fixed-size values, this returns the values themselves and an empty var_sized_data
    /// For variable-size values, this returns offsets and the actual serialized data
    fn process_values(values: Vec<V>) -> (Vec<S>, Vec<u8>);
}

/// Handler for fixed-sized values
struct FixedSizeHandler;

/// Handler for variable-sized values
struct VarSizeHandler;

impl<V: FixedSizedValue + Copy + Default> ValueHandler<V, V> for FixedSizeHandler {
    fn process_values(values: Vec<V>) -> (Vec<V>, Vec<u8>) {
        (values, Vec::new())
    }
}

impl<V: VarSizedValue + Clone> ValueHandler<V, u32> for VarSizeHandler {
    fn process_values(values: Vec<V>) -> (Vec<u32>, Vec<u8>) {
        let mut var_sized_data = Vec::new();
        let mut offsets = Vec::with_capacity(values.len());
        let mut current_offset = 0u32;

        for value in values {
            offsets.push(current_offset);
            let bytes = value.to_bytes();
            current_offset += bytes.len() as u32;
            var_sized_data.extend_from_slice(&bytes);
        }

        (offsets, var_sized_data)
    }
}

#[derive(Clone)]
struct PostingElement<V> {
    id: PointOffsetType,
    value: V,
}

pub struct PostingBuilder<V> {
    elements: Vec<PostingElement<V>>,
}

trait CompressedPostingList<V> {
    type Fixed;
    type Var;

    fn from_builder(builder: PostingBuilder<V>) -> Self;
}

/// V is the value we are interested to store along with the id.
/// S is the type of value we store within the chunk, should be small like an int. For
/// variable-sized values, this acts as a pointer into var_sized_data
struct PostingList<V, S = V> {
    id_data: Vec<u8>,
    chunks: Vec<PostingChunk<S>>,
    remainders: Vec<PostingElement<S>>,
    var_sized_data: Vec<u8>,
    _phantom: std::marker::PhantomData<V>,
}

#[repr(C)]
pub struct PostingChunk<S: Sized> {
    /// Initial data point id. Used for decompression.
    initial: PointOffsetType,

    /// An offset within id_data
    offset: u32,

    /// Sized values for the chunk.
    sized_values: [S; CHUNK_SIZE],
}

impl<S: Sized> PostingChunk<S> {
    /// Get byte size of the compressed ids chunk.
    fn calculate_ids_chunk_size(chunks: &[PostingChunk<S>], data: &[u8], chunk_index: usize) -> usize {
        if chunk_index + 1 < chunks.len() {
            chunks[chunk_index + 1].offset as usize - chunks[chunk_index].offset as usize
        } else {
            // Last chunk
            data.len() - chunks[chunk_index].offset as usize
        }
    }
}

impl<V, S> PostingList<V, S>
where
    S: Sized + Copy,
{
    /// Unified implementation of `from_builder` that works for both fixed-size and variable-size values
    ///
    /// This method uses the `ValueHandler` trait to abstract the differences between
    /// the two implementations, allowing us to share the common logic.
    fn from_builder_generic<H>(mut builder: PostingBuilder<V>) -> Self
    where
        H: ValueHandler<V, S>,
        V: Clone,
    {
        builder.elements.sort_unstable_by_key(|e| e.id);

        let num_elements = builder.elements.len();

        // extract ids and values into separate lists
        let (ids, values): (Vec<_>, Vec<_>) = builder.elements.into_iter().map(|e| (e.id, e.value)).unzip();

        // process values
        let (sized_values, var_sized_data) = H::process_values(values);

        let bitpacker = BitPackerImpl::new();
        let mut chunks = Vec::with_capacity(ids.len() / CHUNK_SIZE);
        let mut id_data_size = 0;

        // process full chunks
        let ids_chunks_iter = ids.chunks_exact(CHUNK_SIZE);
        let values_chunks_iter = sized_values.chunks_exact(CHUNK_SIZE);
        let remainder_ids = ids_chunks_iter.remainder();
        let remainder_values = values_chunks_iter.remainder();

        for (chunk_ids, chunk_values) in ids_chunks_iter.zip(values_chunks_iter) {
            let initial = chunk_ids[0];
            let chunk_bits = bitpacker.num_bits_strictly_sorted(initial.checked_sub(1), &chunk_ids);
            let chunk_size = BitPackerImpl::compressed_block_size(chunk_bits);

            chunks.push(PostingChunk {
                initial,
                offset: id_data_size as u32,
                sized_values: chunk_values.try_into().expect("should be a valid chunk size"),
            });
            id_data_size += chunk_size;
        }

        // now process remainders
        let mut remainders = Vec::with_capacity(num_elements % CHUNK_SIZE);
        for (&id, &value) in remainder_ids.iter().zip(remainder_values) {
            remainders.push(PostingElement {
                id,
                value,
            });
        }

        // compress id_data
        let mut id_data = vec![0u8; id_data_size];
        for (chunk_index, chunk_ids) in ids.chunks_exact(CHUNK_SIZE).enumerate() {
            let chunk = &chunks[chunk_index];
            let chunk_size = PostingChunk::calculate_ids_chunk_size(&chunks, &id_data, chunk_index);
            let chunk_bits = chunk_size * u8::BITS as usize / CHUNK_SIZE;
            bitpacker.compress_strictly_sorted(
                chunk.initial.checked_sub(1),
                &chunk_ids,
                &mut id_data[chunk.offset as usize..chunk.offset as usize + chunk_size],
                chunk_bits as u8,
            );
        }

        Self {
            id_data,
            var_sized_data,
            chunks,
            remainders,
            _phantom: PhantomData,
        }
    }
}

// Fixed-sized value implementation
// For fixed-size values, we store them directly in the PostingChunk
impl<V: FixedSizedValue + Copy + Default> CompressedPostingList<V> for PostingList<V> {
    type Fixed = V;
    type Var = ();

    fn from_builder(builder: PostingBuilder<V>) -> Self {
        Self::from_builder_generic::<FixedSizeHandler>(builder)
    }
}

// Variable-sized value implementation.
// For variable-size values, we store offsets in the PostingChunk that point to
// the actual values stored in var_sized_data.
// Here `chunk.sized_values` are pointing to the start offset of the actual values in `posting.var_sized_values`
impl<V: VarSizedValue + Clone> CompressedPostingList<V> for PostingList<V, u32> {
    type Fixed = u32;
    type Var = V;

    fn from_builder(builder: PostingBuilder<V>) -> Self {
        Self::from_builder_generic::<VarSizeHandler>(builder)
    }
}
