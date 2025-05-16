use std::marker::PhantomData;

use bitpacking::BitPacker;
use common::types::PointOffsetType;

use crate::posting_list::{PostingChunk, PostingElement, PostingList};
use crate::value_handler::{SizedHandler, ValueHandler, UnsizedHandler};
use crate::{BitPackerImpl, CHUNK_LEN, SizedValue, UnsizedValue};

pub struct PostingBuilder<V> {
    elements: Vec<PostingElement<V>>,
}

impl<V> Default for PostingBuilder<V> {
    fn default() -> Self {
        Self {
            elements: Vec::new(),
        }
    }
}

impl<V> PostingBuilder<V> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, id: PointOffsetType, value: V) {
        self.elements.push(PostingElement { id, value });
    }

    /// Unified implementation that works for both fixed-size and variable-size values
    ///
    /// This method uses the `ValueHandler::process_values` trait function to abstract the
    /// differences between the two implementations, allowing us to share the common logic.
    pub(crate) fn build_generic<H>(mut self) -> PostingList<H>
    where
        H: ValueHandler<Value = V>,
    {
        self.elements.sort_unstable_by_key(|e| e.id);

        let num_elements = self.elements.len();

        // extract ids and values into separate lists
        let (ids, values): (Vec<_>, Vec<_>) =
            self.elements.into_iter().map(|e| (e.id, e.value)).unzip();

        // process values
        let (sized_values, var_size_data) = H::process_values(values);

        let bitpacker = BitPackerImpl::new();
        let mut chunks = Vec::with_capacity(ids.len() / CHUNK_LEN);
        let mut id_data_size = 0;

        // process full chunks
        let ids_chunks_iter = ids.chunks_exact(CHUNK_LEN);
        let values_chunks_iter = sized_values.chunks_exact(CHUNK_LEN);
        let remainder_ids = ids_chunks_iter.remainder();
        let remainder_values = values_chunks_iter.remainder();

        for (chunk_ids, chunk_values) in ids_chunks_iter.zip(values_chunks_iter) {
            let initial = chunk_ids[0];
            let chunk_bits = bitpacker.num_bits_strictly_sorted(initial.checked_sub(1), chunk_ids);
            let chunk_size = BitPackerImpl::compressed_block_size(chunk_bits);

            chunks.push(PostingChunk {
                initial_id: initial,
                offset: id_data_size as u32,
                sized_values: chunk_values
                    .try_into()
                    .expect("should be a valid chunk size"),
            });
            id_data_size += chunk_size;
        }

        // now process remainders
        let mut remainders = Vec::with_capacity(num_elements % CHUNK_LEN);
        for (&id, &value) in remainder_ids.iter().zip(remainder_values) {
            remainders.push(PostingElement { id, value });
        }

        // compress id_data
        let mut id_data = vec![0u8; id_data_size];
        for (chunk_index, chunk_ids) in ids.chunks_exact(CHUNK_LEN).enumerate() {
            let chunk = &chunks[chunk_index];
            let compressed_size = PostingChunk::get_compressed_size(&chunks, &id_data, chunk_index);
            let chunk_bits = compressed_size * u8::BITS as usize / CHUNK_LEN;
            bitpacker.compress_strictly_sorted(
                chunk.initial_id.checked_sub(1),
                chunk_ids,
                &mut id_data[chunk.offset as usize..chunk.offset as usize + compressed_size],
                chunk_bits as u8,
            );
        }

        let last_id = ids.last().copied();

        PostingList {
            id_data,
            var_size_data,
            chunks,
            remainders,
            last_id,
            _phantom: PhantomData,
        }
    }
}

impl PostingBuilder<()> {
    /// Add an id without a value.
    pub fn add_id(&mut self, id: PointOffsetType) {
        self.add(id, ());
    }

    /// Build a posting list with just the compressed ids.
    pub fn build(self) -> PostingList<Sized<()>> {
        self.build_generic::<Sized<()>>()
    }
}

impl<V: SizedValue> PostingBuilder<V> {
    /// Build a posting list with fixed-sized values to store them directly in the PostingChunk
    pub fn build_sized(self) -> PostingList<Sized<V>> {
        self.build_generic::<Sized<V>>()
    }
}

// Variable-sized value implementation.
impl<V: VarSizedValue> PostingBuilder<V> {
    /// Build a posting list with variable-sized values to store them in the `var_size_data` field.
    ///
    /// For variable-size values, we store offsets along with each id to point into a flattened array
    /// where the var-sized data lives.
    pub fn build_var_sized(self) -> PostingList<VarSized<V>> {
        self.build_generic::<VarSized<V>>()
    }
}
