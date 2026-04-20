use std::cell::Cell;
use std::marker::PhantomData;
use std::path::PathBuf;

use common::generic_consts::{Random, Sequential};
use common::universal_io::{OpenOptions, ReadRange, UniversalIoError, UniversalRead};
use posting_list::PostingList;
use zerocopy::FromBytes;

use crate::common::operation_error::OperationResult;
use crate::index::field_index::full_text_index::inverted_index::TokenId;
use crate::index::field_index::full_text_index::inverted_index::mmap_inverted_index::raw_posting_list::RawPostingList;
use crate::index::field_index::full_text_index::inverted_index::mmap_inverted_index::types::{
    PostingListHeader, PostingsHeader, ZerocopyPostingValue,
};

/// Posting lists stored on disk and accessed via the [`UniversalRead`]
/// abstraction. The on-disk layout is produced by
/// [`create_postings_file`](super::create_postings::create_postings_file)
/// and matches the layout documented there.
///
/// `get_header(token_id)` indexes the per-token header array directly:
/// `size_of::<PostingsHeader>() + token_id * size_of::<PostingListHeader>()`.
/// Each [`PostingListHeader`] then points (via absolute `offset`) into the
/// posting-data region.
pub struct UniversalPostings<V: ZerocopyPostingValue, S: UniversalRead<u8>> {
    _path: PathBuf,
    storage: S,
    header: PostingsHeader,
    _value_type: PhantomData<V>,
}

type HeaderResult = Result<(TokenId, PostingListHeader), UniversalIoError>;

/// Output of [`UniversalPostings::headers_iter`]: a lazy iterator over the
/// headers that are in range, paired with the pre-filtered list of token ids
/// that were outside the valid range.
struct HeadersBatch<'a> {
    iter: Box<dyn Iterator<Item = HeaderResult> + 'a>,
    missing: Vec<TokenId>,
}

impl<V: ZerocopyPostingValue, S: UniversalRead<u8>> UniversalPostings<V, S> {
    /// Open the postings file at `path` via the `S` storage backend.
    pub fn open(path: impl Into<PathBuf>, options: OpenOptions) -> OperationResult<Self> {
        let path = path.into();
        let storage = S::open(&path, options)?;

        let header_bytes = storage.read::<Sequential>(ReadRange {
            byte_offset: 0,
            length: size_of::<PostingsHeader>() as u64,
        })?;

        let (header, _) = PostingsHeader::read_from_prefix(header_bytes.as_ref())?;

        Ok(Self {
            _path: path,
            storage,
            header,
            _value_type: PhantomData,
        })
    }

    /// Hint the storage backend to populate any RAM cache backing this file.
    /// For mmap-backed storage this is `madvise(MADV_POPULATE_READ)` and blocks
    /// until pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        self.storage.populate()?;
        Ok(())
    }

    /// Hint the storage backend to drop any RAM cache backing this file.
    /// For mmap-backed storage this is `madvise(MADV_PAGEOUT)`.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_ram_cache()?;
        Ok(())
    }

    /// Stream posting list headers for a batch of token ids.
    ///
    /// The returned [`HeadersBatch`] pairs a lazy iterator over the in-range
    /// `(token_id, header)` pairs with the list of token ids that were
    /// out-of-range. Iterator order is not guaranteed to match the input.
    fn headers_iter(&self, token_ids: &[TokenId]) -> Result<HeadersBatch<'_>, UniversalIoError> {
        let header_length = size_of::<PostingListHeader>() as u64;
        let posting_count = self.header.posting_count;

        let mut valid_ranges: Vec<(TokenId, ReadRange)> = Vec::with_capacity(token_ids.len());
        let mut filtered_out: Vec<TokenId> = Vec::new();

        for &token_id in token_ids {
            if posting_count <= token_id as usize {
                filtered_out.push(token_id);
                continue;
            }
            let header_offset =
                size_of::<PostingsHeader>() as u64 + u64::from(token_id) * header_length;

            valid_ranges.push((
                token_id,
                ReadRange {
                    byte_offset: header_offset,
                    length: header_length,
                },
            ));
        }

        let valid_iter = self
            .storage
            .read_iter::<Random, _>(valid_ranges)?
            .map(|res| {
                let (token_id, bytes) = res?;
                let (header, _) = PostingListHeader::read_from_prefix(bytes.as_ref())?;
                Ok((token_id, header))
            });

        Ok(HeadersBatch {
            iter: Box::new(valid_iter),
            missing: filtered_out,
        })
    }

    fn get_header(&self, token_id: TokenId) -> OperationResult<Option<PostingListHeader>> {
        let HeadersBatch { mut iter, .. } = self.headers_iter(&[token_id])?;
        let Some(entry) = iter.next() else {
            return Ok(None);
        };
        let (_, header) = entry?;
        Ok(Some(header))
    }

    /// Create PostingListView<V> from the given header
    ///
    /// Assume the following layout:
    ///
    /// ```ignore
    /// last_doc_id: &'a PointOffsetType,
    /// chunks_index: &'a [PostingChunk<()>],
    /// data: &'a [u8],
    /// var_size_data: &'a [u8], // might be empty in case of only ids
    /// _alignment: &'a [u8], // 0-3 extra bytes to align the data
    /// remainder_postings: &'a [PointOffsetType],
    /// ```
    #[cfg(test)]
    fn raw_posting<'a>(
        &'a self,
        header: PostingListHeader,
    ) -> Result<RawPostingList<'a>, UniversalIoError> {
        let read_range = ReadRange {
            byte_offset: header.offset,
            length: header.posting_size::<V>() as u64,
        };
        let bytes = self.storage.read::<Sequential>(read_range)?;
        let result = RawPostingList::new(bytes, header);
        Ok(result)
    }

    #[cfg(test)]
    pub fn get(&self, token_id: TokenId) -> OperationResult<Option<RawPostingList<'_>>> {
        let header = self.get_header(token_id)?;
        if let Some(header) = header {
            let posting = self.raw_posting(header).map(Some)?;
            return Ok(posting);
        }
        Ok(None)
    }

    /// Number of elements in the posting list for `token_id`. Reads only the
    /// per-token header, not the posting bytes.
    pub fn posting_len(&self, token_id: TokenId) -> OperationResult<Option<usize>> {
        Ok(self.get_header(token_id)?.map(|h| h.posting_len()))
    }

    /// Read the posting lists for every header yielded by `header_iter` and
    /// return them as `Vec<(TokenId, RawPostingList)>`. Header iteration is
    /// pipelined into the posting reads — submissions are scheduled lazily as
    /// headers arrive.
    fn fetch_raw_postings<'a>(
        &'a self,
        header_iter: Box<dyn Iterator<Item = HeaderResult> + 'a>,
        expected_capacity: usize,
    ) -> OperationResult<Vec<(TokenId, RawPostingList<'a>)>> {
        let header_err: Cell<Option<UniversalIoError>> = Cell::new(None);

        let range_iter = header_iter.filter_map(|header_res| match header_res {
            Ok((token_id, header)) => {
                let range = ReadRange {
                    byte_offset: header.offset,
                    length: header.posting_size::<V>() as u64,
                };
                Some(((token_id, header), range))
            }
            Err(err) => {
                header_err.set(Some(err));
                None
            }
        });

        let mut raw_postings: Vec<(TokenId, RawPostingList<'a>)> =
            Vec::with_capacity(expected_capacity);

        for entry in self.storage.read_iter::<Sequential, _>(range_iter)? {
            let ((token_id, header), bytes) = entry?;
            raw_postings.push((token_id, RawPostingList::new(bytes, header)));
        }

        if let Some(err) = header_err.take() {
            return Err(err.into());
        }

        Ok(raw_postings)
    }

    /// Fetch the raw bytes of posting lists for the given token ids.
    /// Returns `None` if any posting list is missing; otherwise returns
    /// the full set in the order they were read from storage.
    pub fn fetch_all_or_none<'a>(
        &'a self,
        token_ids: &[TokenId],
    ) -> OperationResult<Option<Vec<(TokenId, RawPostingList<'a>)>>> {
        let HeadersBatch {
            iter: header_iter,
            missing,
        } = self.headers_iter(token_ids)?;

        if !missing.is_empty() {
            return Ok(None);
        }

        self.fetch_raw_postings(header_iter, token_ids.len())
            .map(Some)
    }

    /// Fetch the raw bytes of every existing posting list among `token_ids`.
    /// Token ids without a posting list are silently ignored.
    pub fn fetch_existing<'a>(
        &'a self,
        token_ids: &[TokenId],
    ) -> OperationResult<Vec<(TokenId, RawPostingList<'a>)>> {
        let HeadersBatch {
            iter: header_iter, ..
        } = self.headers_iter(token_ids)?;

        self.fetch_raw_postings(header_iter, token_ids.len())
    }

    pub fn all_postings(&self) -> OperationResult<Vec<PostingList<V>>> {
        let all_tokens = (0..self.header.posting_count as TokenId).collect::<Vec<_>>();
        let raw_postings = self.fetch_existing(&all_tokens)?;

        raw_postings
            .iter()
            .map(|(_token_id, raw)| raw.as_view::<V>().map(|view| view.to_owned()))
            .collect()
    }
}
