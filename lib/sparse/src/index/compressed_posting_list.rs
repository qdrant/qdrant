use std::cmp::Ordering;
use std::mem::size_of;

use bitpacking::BitPacker as _;
use common::types::PointOffsetType;
#[cfg(debug_assertions)]
use itertools::Itertools as _;

use super::posting_list_common::{PostingElement, PostingElementEx, PostingListIter};
use crate::common::types::DimWeight;
type BitPackerImpl = bitpacking::BitPacker4x;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct CompressedPostingList {
    /// Compressed ids data. Chunks refer to subslies of this data.
    id_data: Vec<u8>,

    /// Fixed-size chunks.
    chunks: Vec<CompressedPostingChunk>,

    /// Remainder elements that do not fit into chunks.
    remainders: Vec<PostingElement>,

    /// Id of the last element in the list. Used to avoid unpacking the last chunk.
    last_id: Option<PointOffsetType>,
}

/// A non-owning view of [`CompressedPostingList`].
#[derive(Default, Debug, Clone, PartialEq)]
pub struct CompressedPostingListView<'a> {
    id_data: &'a [u8],
    chunks: &'a [CompressedPostingChunk],
    remainders: &'a [PostingElement],
    last_id: Option<PointOffsetType>,
}

#[derive(Debug, Clone, PartialEq)]
#[repr(C)]
pub struct CompressedPostingChunk {
    /// Initial data point id. Used for decompression.
    initial: PointOffsetType,

    /// An offset within id_data
    offset: u32,

    /// Weight values for the chunk.
    weights: [DimWeight; BitPackerImpl::BLOCK_LEN],
}

impl CompressedPostingList {
    pub(super) fn view(&self) -> CompressedPostingListView {
        CompressedPostingListView {
            id_data: &self.id_data,
            chunks: &self.chunks,
            remainders: &self.remainders,
            last_id: self.last_id,
        }
    }

    pub fn iter(&self) -> CompressedPostingListIterator {
        self.view().iter()
    }

    #[cfg(test)]
    pub fn from(records: Vec<(PointOffsetType, DimWeight)>) -> CompressedPostingList {
        let mut posting_list = CompressedPostingBuilder::new();
        for (id, weight) in records {
            posting_list.add(id, weight);
        }
        posting_list.build()
    }
}

pub struct CompressedPostingListStoreSize {
    pub id_data_bytes: usize,
    pub chunks_count: usize,
    pub remainders_count: usize,
}

impl CompressedPostingListStoreSize {
    pub fn total(&self) -> usize {
        self.id_data_bytes
            + self.chunks_count * size_of::<CompressedPostingChunk>()
            + self.remainders_count * size_of::<PostingElement>()
    }

    pub fn chunks_bytes(&self) -> usize {
        self.chunks_count * size_of::<CompressedPostingChunk>()
    }
}

impl<'a> CompressedPostingListView<'a> {
    pub(super) fn new(
        id_data: &'a [u8],
        chunks: &'a [CompressedPostingChunk],
        remainders: &'a [PostingElement],
        last_id: Option<PointOffsetType>,
    ) -> Self {
        CompressedPostingListView {
            id_data,
            chunks,
            remainders,
            last_id,
        }
    }

    pub(super) fn parts(&self) -> (&'a [u8], &'a [CompressedPostingChunk], &'a [PostingElement]) {
        (self.id_data, self.chunks, self.remainders)
    }

    pub fn last_id(&self) -> Option<PointOffsetType> {
        self.last_id
    }

    pub(super) fn store_size(&self) -> CompressedPostingListStoreSize {
        CompressedPostingListStoreSize {
            id_data_bytes: self.id_data.len(),
            chunks_count: self.chunks.len(),
            remainders_count: self.remainders.len(),
        }
    }

    pub fn to_owned(&self) -> CompressedPostingList {
        CompressedPostingList {
            id_data: self.id_data.to_vec(),
            chunks: self.chunks.to_vec(),
            remainders: self.remainders.to_vec(),
            last_id: self.last_id,
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

    fn get_chunk_size(chunks: &[CompressedPostingChunk], data: &[u8], chunk_index: usize) -> usize {
        if chunk_index + 1 < chunks.len() {
            chunks[chunk_index + 1].offset as usize - chunks[chunk_index].offset as usize
        } else {
            data.len() - chunks[chunk_index].offset as usize
        }
    }

    pub fn iter(&self) -> CompressedPostingListIterator<'a> {
        CompressedPostingListIterator::new(self)
    }
}

#[derive(Default)]
pub struct CompressedPostingBuilder {
    elements: Vec<PostingElement>,
}

impl CompressedPostingBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    /// Add a new record to the posting list.
    pub fn add(&mut self, record_id: PointOffsetType, weight: DimWeight) {
        self.elements.push(PostingElement { record_id, weight });
    }

    pub fn build(mut self) -> CompressedPostingList {
        self.elements.sort_unstable_by_key(|e| e.record_id);

        // Check for duplicates
        #[cfg(debug_assertions)]
        if let Some(e) = self.elements.iter().duplicates_by(|e| e.record_id).next() {
            panic!("Duplicate id {} in posting list", e.record_id);
        }

        let mut this_chunk = Vec::with_capacity(BitPackerImpl::BLOCK_LEN);

        let bitpacker = BitPackerImpl::new();
        let mut chunks = Vec::with_capacity(self.elements.len() / BitPackerImpl::BLOCK_LEN);
        let mut data_size = 0;
        let mut remainders = Vec::new();
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
                        .map(|e| e.weight)
                        .collect::<Vec<_>>()
                        .try_into()
                        .expect("Invalid chunk size"),
                });
                data_size += chunk_size;
            } else {
                remainders.extend_from_slice(chunk);
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
        }
    }
}

#[derive(Clone)]
pub struct CompressedPostingListIterator<'a> {
    list: CompressedPostingListView<'a>,

    /// If true, then `decompressed_chunk` contains the unpacked chunk for the current
    /// `compressed_idx`.
    unpacked: bool,

    decompressed_chunk: [PointOffsetType; BitPackerImpl::BLOCK_LEN],

    /// Index within compressed chunks.
    compressed_idx: usize,

    remainders_idx: usize,
}

impl<'a> CompressedPostingListIterator<'a> {
    #[inline]
    fn new(list: &CompressedPostingListView<'a>) -> CompressedPostingListIterator<'a> {
        CompressedPostingListIterator {
            list: list.clone(),
            unpacked: false,
            decompressed_chunk: [0; BitPackerImpl::BLOCK_LEN],
            compressed_idx: 0,
            remainders_idx: 0,
        }
    }

    #[inline]
    fn next(&mut self) -> Option<PostingElement> {
        let result = self.peek()?;

        if self.compressed_idx / BitPackerImpl::BLOCK_LEN < self.list.chunks.len() {
            self.compressed_idx += 1;
            if self.compressed_idx % BitPackerImpl::BLOCK_LEN == 0 {
                self.unpacked = false;
            }
        } else {
            self.remainders_idx += 1;
        }

        Some(result.into())
    }
}

impl<'a> PostingListIter for CompressedPostingListIterator<'a> {
    #[inline]
    fn peek(&mut self) -> Option<PostingElementEx> {
        let compressed_idx = self.compressed_idx;
        if compressed_idx / BitPackerImpl::BLOCK_LEN < self.list.chunks.len() {
            if !self.unpacked {
                self.list.decompress_chunk(
                    compressed_idx / BitPackerImpl::BLOCK_LEN,
                    &mut self.decompressed_chunk,
                );
                self.unpacked = true;
            }

            let chunk = &self.list.chunks[compressed_idx / BitPackerImpl::BLOCK_LEN];
            return Some(PostingElementEx {
                record_id: self.decompressed_chunk[compressed_idx % BitPackerImpl::BLOCK_LEN],
                weight: chunk.weights[compressed_idx % BitPackerImpl::BLOCK_LEN],
                max_next_weight: 0.0,
            });
        }

        self.list
            .remainders
            .get(self.remainders_idx)
            .map(|e| PostingElementEx {
                record_id: e.record_id,
                weight: e.weight,
                max_next_weight: 0.0,
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
        self.compressed_idx = self.list.chunks.len() * BitPackerImpl::BLOCK_LEN;
        self.remainders_idx = self.list.remainders.len();
    }

    #[inline]
    fn len_to_end(&self) -> usize {
        self.list.len() - self.compressed_idx - self.remainders_idx
    }

    #[inline]
    fn current_index(&self) -> usize {
        self.compressed_idx + self.remainders_idx
    }

    #[inline]
    fn for_each_till_id<Ctx: ?Sized>(
        &mut self,
        id: PointOffsetType,
        ctx: &mut Ctx,
        mut f: impl FnMut(&mut Ctx, PointOffsetType, DimWeight),
    ) {
        let mut compressed_idx = self.compressed_idx;
        if compressed_idx / BitPackerImpl::BLOCK_LEN < self.list.chunks.len() {
            // 1. Iterate over already decompressed chunk
            if self.unpacked {
                let chunk = &self.list.chunks[compressed_idx / BitPackerImpl::BLOCK_LEN];

                for (idx, weight) in std::iter::zip(
                    &self.decompressed_chunk[compressed_idx % BitPackerImpl::BLOCK_LEN..],
                    &chunk.weights[compressed_idx % BitPackerImpl::BLOCK_LEN..],
                ) {
                    if *idx > id {
                        self.compressed_idx = compressed_idx;
                        return;
                    }
                    f(ctx, *idx, *weight);
                    compressed_idx += 1;
                }
            }

            // 2. Iterate over compressed chunks
            while compressed_idx / BitPackerImpl::BLOCK_LEN < self.list.chunks.len() {
                self.list.decompress_chunk(
                    compressed_idx / BitPackerImpl::BLOCK_LEN,
                    &mut self.decompressed_chunk,
                );
                let chunk = &self.list.chunks[compressed_idx / BitPackerImpl::BLOCK_LEN];

                if *self.decompressed_chunk.last().unwrap() <= id {
                    for (idx, weight) in std::iter::zip(&self.decompressed_chunk, &chunk.weights) {
                        f(ctx, *idx, *weight);
                    }
                    compressed_idx += BitPackerImpl::BLOCK_LEN;
                } else {
                    for (idx, weight) in std::iter::zip(&self.decompressed_chunk, &chunk.weights) {
                        if *idx > id {
                            self.compressed_idx = compressed_idx;
                            self.unpacked = true;
                            return;
                        }
                        compressed_idx += 1;
                        f(ctx, *idx, *weight);
                    }
                }
            }
        }
        self.compressed_idx = compressed_idx;

        // 3. Iterate over remainders
        for e in &self.list.remainders[self.remainders_idx..] {
            if e.record_id > id {
                return;
            }
            f(ctx, e.record_id, e.weight);
            self.remainders_idx += 1;
        }
    }

    fn reliable_max_next_weight() -> bool {
        false
    }

    fn into_std_iter(self) -> impl Iterator<Item = PostingElement> {
        CompressedPostingListStdIterator(self)
    }
}

#[derive(Clone)]
pub struct CompressedPostingListStdIterator<'a>(CompressedPostingListIterator<'a>);

impl Iterator for CompressedPostingListStdIterator<'_> {
    type Item = PostingElement;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
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
            let list = CompressedPostingList::from(case.clone());

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
                    let pl = CompressedPostingList::from(case.clone());

                    let mut iter = pl.iter();

                    let mut data = Vec::new();
                    let mut counter = 0;

                    iter.for_each_till_id(
                        case.get(CASES[i]).map_or(PointOffsetType::MAX, |x| x.0) - 1,
                        &mut (),
                        |_, id, weight| {
                            eprintln!("  {}", id);
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
                            eprintln!("  {}", id);
                            data.push((id, weight));
                            counter += 1;
                        },
                    );
                    assert_eq!(data, &case[CASES[i]..CASES[j]]);
                }
            }
        }
    }
}
