use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem::size_of;

use bitpacking::BitPacker as _;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::iterator_hw_measurement::HwMeasurementIteratorExt;
use common::types::PointOffsetType;
#[cfg(debug_assertions)]
use itertools::Itertools as _;

use super::posting_list_common::{
    GenericPostingElement, PostingElement, PostingElementEx, PostingListIter,
};
use crate::common::types::{DimWeight, Weight};
type BitPackerImpl = bitpacking::BitPacker4x;

/// How many elements are packed in a single chunk.
const CHUNK_SIZE: usize = BitPackerImpl::BLOCK_LEN;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct CompressedPostingList<W: Weight> {
    /// Compressed ids data. Chunks refer to subslies of this data.
    id_data: Vec<u8>,

    /// Fixed-size chunks.
    chunks: Vec<CompressedPostingChunk<W>>,

    /// Remainder elements that do not fit into chunks.
    remainders: Vec<GenericPostingElement<W>>,

    /// Id of the last element in the list. Used to avoid unpacking the last chunk.
    last_id: Option<PointOffsetType>,

    /// Quantization parameters.
    quantization_params: W::QuantizationParams,
}

/// A non-owning view of [`CompressedPostingList`].
#[derive(Debug, Clone)]
pub struct CompressedPostingListView<'a, W: Weight> {
    id_data: &'a [u8],
    chunks: &'a [CompressedPostingChunk<W>],
    remainders: &'a [GenericPostingElement<W>],
    last_id: Option<PointOffsetType>,
    multiplier: W::QuantizationParams,
    hw_counter: &'a HardwareCounterCell,
}

#[derive(Debug, Clone, PartialEq)]
#[repr(C)]
pub struct CompressedPostingChunk<W> {
    /// Initial data point id. Used for decompression.
    initial: PointOffsetType,

    /// An offset within id_data
    offset: u32,

    /// Weight values for the chunk.
    weights: [W; CHUNK_SIZE],
}

impl<W: Weight> CompressedPostingList<W> {
    pub(super) fn view<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> CompressedPostingListView<'a, W> {
        CompressedPostingListView {
            id_data: &self.id_data,
            chunks: &self.chunks,
            remainders: &self.remainders,
            last_id: self.last_id,
            multiplier: self.quantization_params,
            hw_counter,
        }
    }

    pub fn iter<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> CompressedPostingListIterator<'a, W> {
        self.view(hw_counter).iter()
    }

    #[cfg(test)]
    pub fn from(records: Vec<(PointOffsetType, DimWeight)>) -> CompressedPostingList<W> {
        let mut posting_list = CompressedPostingBuilder::new();
        for (id, weight) in records {
            posting_list.add(id, weight);
        }
        posting_list.build()
    }
}

pub struct CompressedPostingListStoreSize {
    pub total: usize,
    pub id_data_bytes: usize,
    pub chunks_count: usize,
}

impl CompressedPostingListStoreSize {
    fn new<W: Weight>(id_data_bytes: usize, chunks_count: usize, remainders_count: usize) -> Self {
        CompressedPostingListStoreSize {
            total: id_data_bytes
                + chunks_count * size_of::<CompressedPostingChunk<W>>()
                + remainders_count * size_of::<GenericPostingElement<W>>(),
            id_data_bytes,
            chunks_count,
        }
    }
}

/// Defines possible results of the search for the chunk by the ID.
enum IdChunkPosition {
    /// The Id is smaller than any data in chunks and therefore
    /// not in the posting list.
    Before,
    /// Id if possibly in the chunk, but it is not guaranteed.
    Chunk(usize),
    /// The Id is greater than any data in chunks, but may be in the remainder
    After,
}

impl<'a, W: Weight> CompressedPostingListView<'a, W> {
    pub(super) fn new(
        id_data: &'a [u8],
        chunks: &'a [CompressedPostingChunk<W>],
        remainders: &'a [GenericPostingElement<W>],
        last_id: Option<PointOffsetType>,
        multiplier: W::QuantizationParams,
        hw_counter: &'a HardwareCounterCell,
    ) -> Self {
        CompressedPostingListView {
            id_data,
            chunks,
            remainders,
            last_id,
            multiplier,
            hw_counter,
        }
    }

    pub(super) fn parts(
        &self,
    ) -> (
        &'a [u8],
        &'a [CompressedPostingChunk<W>],
        &'a [GenericPostingElement<W>],
    ) {
        (self.id_data, self.chunks, self.remainders)
    }

    pub fn last_id(&self) -> Option<PointOffsetType> {
        self.last_id
    }

    pub fn multiplier(&self) -> W::QuantizationParams {
        self.multiplier
    }

    pub(super) fn store_size(&self) -> CompressedPostingListStoreSize {
        CompressedPostingListStoreSize::new::<W>(
            self.id_data.len(),
            self.chunks.len(),
            self.remainders.len(),
        )
    }

    pub fn to_owned(&self) -> CompressedPostingList<W> {
        CompressedPostingList {
            id_data: self.id_data.to_vec(),
            chunks: self.chunks.to_vec(),
            remainders: self.remainders.to_vec(),
            last_id: self.last_id,
            quantization_params: self.multiplier,
        }
    }

    pub fn len(&self) -> usize {
        self.chunks.len() * CHUNK_SIZE + self.remainders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty() && self.remainders.is_empty()
    }

    fn decompress_chunk(
        &self,
        chunk_index: usize,
        decompressed_chunk: &mut [PointOffsetType; CHUNK_SIZE],
    ) {
        let chunk = &self.chunks[chunk_index];
        let chunk_size = Self::get_chunk_size(self.chunks, self.id_data, chunk_index);
        self.hw_counter.vector_io_read().incr_delta(chunk_size);
        let chunk_bits = chunk_size * u8::BITS as usize / CHUNK_SIZE;
        BitPackerImpl::new().decompress_strictly_sorted(
            chunk.initial.checked_sub(1),
            &self.id_data[chunk.offset as usize..chunk.offset as usize + chunk_size],
            decompressed_chunk,
            chunk_bits as u8,
        );
    }

    fn chunk_id_by_position(&self, position: usize) -> Option<usize> {
        let chunk_index = position / CHUNK_SIZE;
        if chunk_index < self.chunks.len() {
            Some(chunk_index)
        } else {
            None
        }
    }

    /// Finds the chunk index by the point id.
    /// It doesn't guarantee that the ID is inside the chunk,
    /// but if the ID exists, it would be in the chunk.
    fn chunk_id_by_id(&self, id: PointOffsetType) -> IdChunkPosition {
        let chunk_index_result = self.chunks.binary_search_by(|c| c.initial.cmp(&id));
        match chunk_index_result {
            Ok(chunk_id) => {
                // Found chunk with the first element exactly equal to the id.
                IdChunkPosition::Chunk(chunk_id)
            }
            Err(position) => {
                // ┌────── position 0, before any chunk
                // │       Means first chunk is already greater than required
                // ▼
                //     ┌─────────┬────────┬─────────────────┐
                //     │Chunk-1  │Chunk-2 │....             │
                //     └─────────┴────────┴─────────────────┘   ▲
                //                   ▲                          │
                //                   │                          │
                //   It might────────┘
                //   be inside position-1, if             Position == length
                //   position < length                    it might be either in the chunk (position-1)
                //                                        Or be inside the remainder
                if position == self.chunks.len() {
                    if let Some(first_remainder) = self.remainders.first() {
                        // If first element of remainder is greater than id,
                        // then Id might still be in the last chunk, if it exists
                        if id < first_remainder.record_id {
                            if position > 0 {
                                IdChunkPosition::Chunk(position - 1)
                            } else {
                                IdChunkPosition::Before
                            }
                        } else {
                            IdChunkPosition::After
                        }
                    } else {
                        // There are no remainder, so we don't know the last id of the last chunk
                        // Therefore, it is still possible that the id is in the last chunk
                        IdChunkPosition::Chunk(position - 1)
                    }
                } else if position == 0 {
                    // The id is smaller than the first element of the first chunk
                    IdChunkPosition::Before
                } else {
                    // The id is between two chunks
                    IdChunkPosition::Chunk(position - 1)
                }
            }
        }
    }

    /// Get byte size of the compressed chunk.
    fn get_chunk_size(
        chunks: &[CompressedPostingChunk<W>],
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

    #[inline]
    fn get_remainder_id(&self, index: usize) -> Option<&GenericPostingElement<W>> {
        self.hw_counter
            .vector_io_read()
            .incr_delta(size_of::<GenericPostingElement<W>>());
        self.remainders.get(index)
    }

    #[inline]
    fn iter_remainder_from(
        &self,
        index: usize,
    ) -> impl Iterator<Item = &'_ GenericPostingElement<W>> + '_ {
        self.remainders[index..].iter().measure_hw_with_cell(
            self.hw_counter,
            size_of::<GenericPostingElement<W>>(),
            |hw_counter| hw_counter.vector_io_read(),
        )
    }

    #[inline]
    fn remainder_len(&self) -> usize {
        self.remainders.len()
    }

    #[inline]
    fn chunks_len(&self) -> usize {
        self.chunks.len()
    }

    /// Warning: This function panics if the index is out of bounds.
    #[inline]
    fn get_weight(&self, pos: usize) -> W {
        self.hw_counter.vector_io_read().incr_delta(size_of::<W>());
        let chunk = &self.chunks[pos / CHUNK_SIZE];
        chunk.weights[pos % CHUNK_SIZE]
    }

    #[inline]
    fn weights_range(&self, pos: usize, count: usize) -> &[W] {
        debug_assert!(count <= CHUNK_SIZE);
        self.hw_counter
            .vector_io_read()
            .incr_delta(size_of::<W>() * count);

        let chunk = &self.chunks[pos / CHUNK_SIZE];
        let start = pos % CHUNK_SIZE;
        chunk.weights[start..start + count].as_ref()
    }

    pub fn iter(&self) -> CompressedPostingListIterator<'a, W> {
        CompressedPostingListIterator::new(self)
    }
}

pub struct CompressedPostingBuilder {
    elements: Vec<PostingElement>,
}

impl CompressedPostingBuilder {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        CompressedPostingBuilder {
            elements: Vec::new(),
        }
    }

    /// Add a new record to the posting list.
    pub fn add(&mut self, record_id: PointOffsetType, weight: DimWeight) {
        self.elements.push(PostingElement { record_id, weight });
    }

    pub fn build<W: Weight>(mut self) -> CompressedPostingList<W> {
        self.elements.sort_unstable_by_key(|e| e.record_id);

        let quantization_params =
            W::quantization_params_for(self.elements.iter().map(|e| e.weight));

        // Check for duplicates
        #[cfg(debug_assertions)]
        if let Some(e) = self.elements.iter().duplicates_by(|e| e.record_id).next() {
            panic!("Duplicate id {} in posting list", e.record_id);
        }

        let mut this_chunk = Vec::with_capacity(CHUNK_SIZE);

        let bitpacker = BitPackerImpl::new();
        let mut chunks = Vec::with_capacity(self.elements.len() / CHUNK_SIZE);
        let mut data_size = 0;
        let mut remainders = Vec::with_capacity(self.elements.len() % CHUNK_SIZE);
        for chunk in self.elements.chunks(CHUNK_SIZE) {
            if chunk.len() == CHUNK_SIZE {
                this_chunk.clear();
                this_chunk.extend(chunk.iter().map(|e| e.record_id));

                let initial = this_chunk[0];
                let chunk_bits =
                    bitpacker.num_bits_strictly_sorted(initial.checked_sub(1), &this_chunk);
                let chunk_size = BitPackerImpl::compressed_block_size(chunk_bits);
                chunks.push(CompressedPostingChunk {
                    initial,
                    offset: data_size as u32,
                    weights: chunk
                        .iter()
                        .map(|e| Weight::from_f32(quantization_params, e.weight))
                        .collect::<Vec<_>>()
                        .try_into()
                        .expect("Invalid chunk size"),
                });
                data_size += chunk_size;
            } else {
                for e in chunk {
                    remainders.push(GenericPostingElement {
                        record_id: e.record_id,
                        weight: Weight::from_f32(quantization_params, e.weight),
                    });
                }
            }
        }

        let mut id_data = vec![0u8; data_size];
        for (chunk_index, chunk_data) in self.elements.chunks_exact(CHUNK_SIZE).enumerate() {
            this_chunk.clear();
            this_chunk.extend(chunk_data.iter().map(|e| e.record_id));

            let chunk = &chunks[chunk_index];
            let chunk_size =
                CompressedPostingListView::get_chunk_size(&chunks, &id_data, chunk_index);
            let chunk_bits = chunk_size * u8::BITS as usize / CHUNK_SIZE;
            bitpacker.compress_strictly_sorted(
                chunk.initial.checked_sub(1),
                &this_chunk,
                &mut id_data[chunk.offset as usize..chunk.offset as usize + chunk_size],
                chunk_bits as u8,
            );
        }

        CompressedPostingList {
            id_data,
            chunks,
            remainders,
            last_id: self.elements.last().map(|e| e.record_id),
            quantization_params,
        }
    }
}

#[derive(Clone)]
pub struct CompressedPostingListIterator<'a, W: Weight> {
    list: CompressedPostingListView<'a, W>,

    /// If true, then `decompressed_chunk` contains the unpacked chunk for the current position.
    unpacked: bool,

    decompressed_chunk: [PointOffsetType; CHUNK_SIZE],

    /// Offset inside the posting list along with optional current element.
    /// Defined as a tuple to ensure that we won't forget to update the element
    pos: (usize, Option<PointOffsetType>),
}

impl<'a, W: Weight> CompressedPostingListIterator<'a, W> {
    #[inline]
    fn new(list: &CompressedPostingListView<'a, W>) -> Self {
        Self {
            list: list.clone(),
            unpacked: false,
            decompressed_chunk: [0; CHUNK_SIZE],
            pos: (0, None),
        }
    }

    #[inline]
    fn next_from(&mut self, peek: PostingElementEx) -> PostingElement {
        if self.pos.0 / CHUNK_SIZE < self.list.chunks.len() {
            self.pos = (self.pos.0 + 1, None);
            if self.pos.0 % CHUNK_SIZE == 0 {
                self.unpacked = false;
            }
        } else {
            self.pos = (self.pos.0 + 1, None);
        }

        peek.into()
    }

    #[inline]
    fn next(&mut self) -> Option<PostingElement> {
        let result = self.peek()?;

        Some(self.next_from(result))
    }
}

impl<W: Weight> PostingListIter for CompressedPostingListIterator<'_, W> {
    #[inline]
    fn peek(&mut self) -> Option<PostingElementEx> {
        let pos = self.pos.0;
        if pos / CHUNK_SIZE < self.list.chunks_len() {
            if !self.unpacked {
                self.list
                    .decompress_chunk(pos / CHUNK_SIZE, &mut self.decompressed_chunk);
                self.unpacked = true;
            }

            return Some(PostingElementEx {
                record_id: self.decompressed_chunk[pos % CHUNK_SIZE],
                weight: self.list.get_weight(pos).to_f32(self.list.multiplier),
                max_next_weight: Default::default(),
            });
        }

        self.list
            .get_remainder_id(pos - self.list.chunks_len() * CHUNK_SIZE)
            .map(|e| PostingElementEx {
                record_id: e.record_id,
                weight: e.weight.to_f32(self.list.multiplier),
                max_next_weight: Default::default(),
            })
    }

    #[inline]
    fn last_id(&self) -> Option<PointOffsetType> {
        self.list.last_id
    }

    fn element_size(&self) -> usize {
        size_of::<W>()
    }

    fn skip_to(&mut self, record_id: PointOffsetType) -> Option<PostingElementEx> {
        // 1. Define which chunk we need to unpack (maybe it is current)
        // 2. If current, change the position to the element and do peek

        // Shortcut peeking into memory
        if let Some(current_record_id) = self.pos.1.as_ref() {
            if record_id < *current_record_id {
                // We are already ahead
                return None;
            }
        }

        // If None, we are already reading remainder
        let current_chunk_id_opt = self.list.chunk_id_by_position(self.pos.0);
        // Required chunk id
        let required_chunk_id = self.list.chunk_id_by_id(record_id);

        match (required_chunk_id, current_chunk_id_opt) {
            (IdChunkPosition::Chunk(chunk_id), Some(current_chunk_id)) => {
                match chunk_id.cmp(&current_chunk_id) {
                    Ordering::Less => {
                        // Chunk is already skipped
                        // Return None, don't change the position
                        return None;
                    }
                    Ordering::Equal => {
                        let min_pos = chunk_id * CHUNK_SIZE;
                        self.pos = (std::cmp::max(self.pos.0, min_pos), None);
                    }
                    Ordering::Greater => {
                        // Chunk is ahead, move to it
                        self.pos = (chunk_id * CHUNK_SIZE, None);
                        self.unpacked = false;
                    }
                }
            }
            (IdChunkPosition::Chunk(_), None) => {
                // We are already in the remainder, and we can't go back
                return None;
            }
            (IdChunkPosition::Before, _) => {
                // Don't change anything, as current `pos` is by definition higher
                return None;
            }
            (IdChunkPosition::After, _) => {
                // Go to after the chunks
                let min_pos = self.list.chunks_len() * CHUNK_SIZE;
                self.pos = (std::cmp::max(self.pos.0, min_pos), None);
                self.unpacked = false;
            }
        };

        while let Some(current_element) = self.peek() {
            // Save the current element to avoid further peeking
            self.pos = (self.pos.0, Some(current_element.record_id));
            match current_element.record_id.cmp(&record_id) {
                Ordering::Equal => return Some(current_element),
                Ordering::Greater => return None,
                Ordering::Less => {
                    // Go to the next element
                    self.next_from(current_element);
                }
            }
        }
        None
    }

    #[inline]
    fn skip_to_end(&mut self) {
        self.pos = (
            self.list.chunks_len() * CHUNK_SIZE + self.list.remainder_len(),
            None,
        );
    }

    #[inline]
    fn len_to_end(&self) -> usize {
        self.list.len() - self.pos.0
    }

    #[inline]
    fn current_index(&self) -> usize {
        self.pos.0
    }

    #[inline]
    fn for_each_till_id<Ctx: ?Sized>(
        &mut self,
        id: PointOffsetType,
        ctx: &mut Ctx,
        mut f: impl FnMut(&mut Ctx, PointOffsetType, DimWeight),
    ) {
        let mut pos = self.pos.0;

        // Iterate over compressed chunks
        let mut weights_buf = [0.0; CHUNK_SIZE];

        let mut need_unpack = !self.unpacked;
        while pos / CHUNK_SIZE < self.list.chunks_len() {
            if need_unpack {
                self.list
                    .decompress_chunk(pos / CHUNK_SIZE, &mut self.decompressed_chunk);
            }
            need_unpack = true;

            let start = pos % CHUNK_SIZE;
            let count = count_le_sorted(id, &self.decompressed_chunk[start..]);

            let weights = self.list.weights_range(pos, count);
            let weights =
                W::into_f32_slice(self.list.multiplier, weights, &mut weights_buf[..count]);

            for (idx, weight) in
                std::iter::zip(&self.decompressed_chunk[start..start + count], weights)
            {
                f(ctx, *idx, *weight);
            }
            pos += count;
            if start + count != CHUNK_SIZE {
                self.unpacked = true;
                self.pos = (pos, None);
                return;
            }
        }

        // Iterate over remainders
        for e in self
            .list
            .iter_remainder_from(pos - self.list.chunks_len() * CHUNK_SIZE)
        {
            if e.record_id > id {
                self.pos = (pos, None);
                return;
            }
            f(ctx, e.record_id, e.weight.to_f32(self.list.multiplier));
            pos += 1;
        }
        self.pos = (pos, None);
    }

    fn reliable_max_next_weight() -> bool {
        false
    }

    fn into_std_iter(self) -> impl Iterator<Item = PostingElement> {
        CompressedPostingListStdIterator(self)
    }
}

#[derive(Clone)]
pub struct CompressedPostingListStdIterator<'a, W: Weight>(CompressedPostingListIterator<'a, W>);

impl<W: Weight> Iterator for CompressedPostingListStdIterator<'_, W> {
    type Item = PostingElement;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

/// Find the amount of elements in the sorted array that are less or equal to `val`. In other words,
/// the first index `i` such that `data[i] > val`, or `data.len()` if all elements are less or equal
/// to `val`.
fn count_le_sorted<T: Copy + Eq + Ord>(val: T, data: &[T]) -> usize {
    if data.last().is_none_or(|&x| x < val) {
        // Happy case
        return data.len();
    }

    data.binary_search(&val).map_or_else(|x| x, |x| x + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CASES: [usize; 6] = [0, 64, 128, 192, 256, 320];

    fn mk_case(count: usize) -> Vec<(PointOffsetType, DimWeight)> {
        (0..count)
            .map(|i| (i as u32 + 10000, i as DimWeight))
            .collect()
    }

    fn cases() -> Vec<Vec<(PointOffsetType, DimWeight)>> {
        CASES.iter().copied().map(mk_case).collect()
    }

    #[test]
    fn test_iter() {
        for case in cases() {
            let list = CompressedPostingList::<f32>::from(case.clone());
            let hw_counter = HardwareCounterCell::new();

            let mut iter = list.iter(&hw_counter);

            let mut count = 0;

            assert_eq!(iter.len_to_end(), case.len(), "len_to_end");

            while let Some(e) = iter.next() {
                assert_eq!(e.record_id, case[count].0);
                assert_eq!(e.weight, case[count].1);
                assert_eq!(iter.len_to_end(), case.len() - count - 1);
                count += 1;
            }
        }
    }

    #[test]
    #[allow(clippy::needless_range_loop)] // for consistency
    fn test_try_till_id() {
        let hw_counter = HardwareCounterCell::new();

        for i in 0..CASES.len() {
            for j in i..CASES.len() {
                for k in j..CASES.len() {
                    eprintln!("\n\n\n{} {} {}", CASES[i], CASES[j], CASES[k]);
                    let case = mk_case(CASES[k]);
                    let pl = CompressedPostingList::<f32>::from(case.clone());

                    let mut iter = pl.iter(&hw_counter);

                    let mut data = Vec::new();
                    let mut counter = 0;

                    iter.for_each_till_id(
                        case.get(CASES[i]).map_or(PointOffsetType::MAX, |x| x.0) - 1,
                        &mut (),
                        |_, id, weight| {
                            eprintln!("  {id}");
                            data.push((id, weight));
                            counter += 1;
                        },
                    );
                    assert_eq!(data, &case[..CASES[i]]);
                    eprintln!(" ;");

                    let mut data = Vec::new();
                    let mut counter = 0;
                    iter.for_each_till_id(
                        case.get(CASES[j]).map_or(PointOffsetType::MAX, |x| x.0) - 1,
                        &mut (),
                        |_, id, weight| {
                            eprintln!("  {id}");
                            data.push((id, weight));
                            counter += 1;
                        },
                    );
                    assert_eq!(data, &case[CASES[i]..CASES[j]]);
                }
            }
        }
    }

    #[test]
    fn test_count_le_sorted() {
        let data = [1, 2, 4, 5];
        for val in 0..9 {
            let pos = count_le_sorted(val, &data);
            assert!(data[..pos].iter().all(|&x| x <= val));
            assert!(data[pos..].iter().all(|&x| x > val));
        }
    }
}
