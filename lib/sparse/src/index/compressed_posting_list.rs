use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem::size_of;

use bitpacking::BitPacker as _;
use common::types::PointOffsetType;
#[cfg(debug_assertions)]
use itertools::Itertools as _;

use super::posting_list_common::{
    GenericPostingElement, PostingElement, PostingElementEx, PostingListIter,
};
use crate::common::types::{DimWeight, Weight};
type BitPackerImpl = bitpacking::BitPacker4x;

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

/// A non-owning view of [`GenericCompressedPostingList`].
#[derive(Default, Debug, Clone, PartialEq)]
pub struct CompressedPostingListView<'a, W: Weight> {
    id_data: &'a [u8],
    chunks: &'a [CompressedPostingChunk<W>],
    remainders: &'a [GenericPostingElement<W>],
    last_id: Option<PointOffsetType>,
    multiplier: W::QuantizationParams,
}

#[derive(Debug, Clone, PartialEq)]
#[repr(C)]
pub struct CompressedPostingChunk<W> {
    /// Initial data point id. Used for decompression.
    initial: PointOffsetType,

    /// An offset within id_data
    offset: u32,

    /// Weight values for the chunk.
    weights: [W; BitPackerImpl::BLOCK_LEN],
}

impl<W: Weight> CompressedPostingList<W> {
    pub(super) fn view(&self) -> CompressedPostingListView<W> {
        CompressedPostingListView {
            id_data: &self.id_data,
            chunks: &self.chunks,
            remainders: &self.remainders,
            last_id: self.last_id,
            multiplier: self.quantization_params,
        }
    }

    pub fn iter(&self) -> CompressedPostingListIterator<W> {
        self.view().iter()
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

impl<'a, W: Weight> CompressedPostingListView<'a, W> {
    pub(super) fn new(
        id_data: &'a [u8],
        chunks: &'a [CompressedPostingChunk<W>],
        remainders: &'a [GenericPostingElement<W>],
        last_id: Option<PointOffsetType>,
        multiplier: W::QuantizationParams,
    ) -> Self {
        CompressedPostingListView {
            id_data,
            chunks,
            remainders,
            last_id,
            multiplier,
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
        self.chunks.len() * BitPackerImpl::BLOCK_LEN + self.remainders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty() && self.remainders.is_empty()
    }

    fn decompress_chunk(
        &self,
        chunk_index: usize,
        decompressed_chunk: &mut [PointOffsetType; BitPackerImpl::BLOCK_LEN],
    ) {
        let chunk = &self.chunks[chunk_index];
        let chunk_size = Self::get_chunk_size(self.chunks, self.id_data, chunk_index);
        let chunk_bits = chunk_size * u8::BITS as usize / BitPackerImpl::BLOCK_LEN;
        BitPackerImpl::new().decompress_strictly_sorted(
            chunk.initial.checked_sub(1),
            &self.id_data[chunk.offset as usize..chunk.offset as usize + chunk_size],
            decompressed_chunk,
            chunk_bits as u8,
        );
    }

    fn get_chunk_size(
        chunks: &[CompressedPostingChunk<W>],
        data: &[u8],
        chunk_index: usize,
    ) -> usize {
        if chunk_index + 1 < chunks.len() {
            chunks[chunk_index + 1].offset as usize - chunks[chunk_index].offset as usize
        } else {
            data.len() - chunks[chunk_index].offset as usize
        }
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

        let mut this_chunk = Vec::with_capacity(BitPackerImpl::BLOCK_LEN);

        let bitpacker = BitPackerImpl::new();
        let mut chunks = Vec::with_capacity(self.elements.len() / BitPackerImpl::BLOCK_LEN);
        let mut data_size = 0;
        let mut remainders = Vec::with_capacity(self.elements.len() % BitPackerImpl::BLOCK_LEN);
        for chunk in self.elements.chunks(BitPackerImpl::BLOCK_LEN) {
            if chunk.len() == BitPackerImpl::BLOCK_LEN {
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
        for (chunk_index, chunk_data) in self
            .elements
            .chunks_exact(BitPackerImpl::BLOCK_LEN)
            .enumerate()
        {
            this_chunk.clear();
            this_chunk.extend(chunk_data.iter().map(|e| e.record_id));

            let chunk = &chunks[chunk_index];
            let chunk_size =
                CompressedPostingListView::get_chunk_size(&chunks, &id_data, chunk_index);
            let chunk_bits = chunk_size * u8::BITS as usize / BitPackerImpl::BLOCK_LEN;
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

    decompressed_chunk: [PointOffsetType; BitPackerImpl::BLOCK_LEN],

    pos: usize,
}

impl<'a, W: Weight> CompressedPostingListIterator<'a, W> {
    #[inline]
    fn new(list: &CompressedPostingListView<'a, W>) -> Self {
        Self {
            list: list.clone(),
            unpacked: false,
            decompressed_chunk: [0; BitPackerImpl::BLOCK_LEN],
            pos: 0,
        }
    }

    #[inline]
    fn next(&mut self) -> Option<PostingElement> {
        let result = self.peek()?;

        if self.pos / BitPackerImpl::BLOCK_LEN < self.list.chunks.len() {
            self.pos += 1;
            if self.pos % BitPackerImpl::BLOCK_LEN == 0 {
                self.unpacked = false;
            }
        } else {
            self.pos += 1;
        }

        Some(result.into())
    }
}

impl<W: Weight> PostingListIter for CompressedPostingListIterator<'_, W> {
    #[inline]
    fn peek(&mut self) -> Option<PostingElementEx> {
        let pos = self.pos;
        if pos / BitPackerImpl::BLOCK_LEN < self.list.chunks.len() {
            if !self.unpacked {
                self.list
                    .decompress_chunk(pos / BitPackerImpl::BLOCK_LEN, &mut self.decompressed_chunk);
                self.unpacked = true;
            }

            let chunk = &self.list.chunks[pos / BitPackerImpl::BLOCK_LEN];
            return Some(PostingElementEx {
                record_id: self.decompressed_chunk[pos % BitPackerImpl::BLOCK_LEN],
                weight: chunk.weights[pos % BitPackerImpl::BLOCK_LEN].to_f32(self.list.multiplier),
                max_next_weight: Default::default(),
            });
        }

        self.list
            .remainders
            .get(pos - self.list.chunks.len() * BitPackerImpl::BLOCK_LEN)
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

    #[inline]
    fn skip_to(&mut self, record_id: PointOffsetType) -> Option<PostingElementEx> {
        // TODO: optimize
        while let Some(e) = self.peek() {
            match e.record_id.cmp(&record_id) {
                Ordering::Equal => return Some(e),
                Ordering::Greater => return None,
                Ordering::Less => {
                    self.next();
                }
            }
        }
        None
    }

    #[inline]
    fn skip_to_end(&mut self) {
        self.pos = self.list.chunks.len() * BitPackerImpl::BLOCK_LEN + self.list.remainders.len();
    }

    #[inline]
    fn len_to_end(&self) -> usize {
        self.list.len() - self.pos
    }

    #[inline]
    fn current_index(&self) -> usize {
        self.pos
    }

    #[inline]
    fn for_each_till_id<Ctx: ?Sized>(
        &mut self,
        id: PointOffsetType,
        ctx: &mut Ctx,
        mut f: impl FnMut(&mut Ctx, PointOffsetType, DimWeight),
    ) {
        let mut pos = self.pos;

        // Iterate over compressed chunks
        let mut weights_buf = [0.0; BitPackerImpl::BLOCK_LEN];

        let mut need_unpack = !self.unpacked;
        while pos / BitPackerImpl::BLOCK_LEN < self.list.chunks.len() {
            if need_unpack {
                self.list
                    .decompress_chunk(pos / BitPackerImpl::BLOCK_LEN, &mut self.decompressed_chunk);
            }
            need_unpack = true;

            let chunk = &self.list.chunks[pos / BitPackerImpl::BLOCK_LEN];

            let start = pos % BitPackerImpl::BLOCK_LEN;
            let count = count_le_sorted(id, &self.decompressed_chunk[start..]);
            let weights = W::into_f32_slice(
                self.list.multiplier,
                &chunk.weights[start..start + count],
                &mut weights_buf[..count],
            );

            for (idx, weight) in
                std::iter::zip(&self.decompressed_chunk[start..start + count], weights)
            {
                f(ctx, *idx, *weight);
            }
            pos += count;
            if start + count != BitPackerImpl::BLOCK_LEN {
                self.unpacked = true;
                self.pos = pos;
                return;
            }
        }

        // Iterate over remainders
        for e in &self.list.remainders[pos - self.list.chunks.len() * BitPackerImpl::BLOCK_LEN..] {
            if e.record_id > id {
                self.pos = pos;
                return;
            }
            f(ctx, e.record_id, e.weight.to_f32(self.list.multiplier));
            pos += 1;
        }
        self.pos = pos;
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
    if data.last().map_or(true, |&x| x < val) {
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

            let mut iter = list.iter();

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
        for i in 0..CASES.len() {
            for j in i..CASES.len() {
                for k in j..CASES.len() {
                    eprintln!("\n\n\n{} {} {}", CASES[i], CASES[j], CASES[k]);
                    let case = mk_case(CASES[k]);
                    let pl = CompressedPostingList::<f32>::from(case.clone());

                    let mut iter = pl.iter();

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
