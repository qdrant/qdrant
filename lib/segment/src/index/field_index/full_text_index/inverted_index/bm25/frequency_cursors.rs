use common::types::PointOffsetType;
use posting_list::{PostingLenIterator, PostingListView};

use super::super::term_frequencies::{BLOCK_LEN, TermFrequencyList};
use super::TermCursors;
use crate::common::operation_error::{OperationError, OperationResult};

/// Cursors over ids-only posting lists, with each term's frequencies read from
/// its own bit-packed list in posting order: an index that scores without
/// positions.
pub struct FrequencyCursors<'a> {
    cursors: Vec<Option<Cursor<'a>>>,
}

struct Cursor<'a> {
    ids: PostingLenIterator<'a, ()>,
    frequencies: TermFrequencyList<'a>,
    /// The block decoded into `block`, if any.
    decoded_block: Option<usize>,
    block: [u32; BLOCK_LEN],
    /// The remainder, decoded the first time it is reached.
    remainder: Option<Vec<u32>>,
}

impl Cursor<'_> {
    fn tf(&mut self, offset: usize) -> u32 {
        let block = offset / BLOCK_LEN;
        let blocks = self.frequencies.blocks();
        if block < blocks {
            if self.decoded_block != Some(block) {
                self.frequencies.decode_block(block, &mut self.block);
                self.decoded_block = Some(block);
            }
            return self.block[offset % BLOCK_LEN];
        }
        let remainder = self.remainder.get_or_insert_with(|| {
            let mut remainder = Vec::with_capacity(BLOCK_LEN);
            self.frequencies.decode_remainder(&mut remainder);
            remainder
        });
        remainder[offset - blocks * BLOCK_LEN]
    }
}

impl<'a> FrequencyCursors<'a> {
    /// One posting list and its frequencies per query term, `None` for a term
    /// this index holds no posting list for. A posting list whose frequencies
    /// are missing or of another length means the two files disagree, and is
    /// an error rather than a term scored as absent or a lookup out of range.
    pub fn new(
        views: Vec<Option<(PostingListView<'a, ()>, Option<TermFrequencyList<'a>>)>>,
    ) -> OperationResult<Self> {
        let cursors = views
            .into_iter()
            .map(|view| {
                let Some((ids, frequencies)) = view else {
                    return Ok(None);
                };
                let frequencies = frequencies.filter(|frequencies| frequencies.len() == ids.len());
                let Some(frequencies) = frequencies else {
                    return Err(OperationError::service_error(
                        "term frequencies do not match the postings",
                    ));
                };
                let mut ids = ids.len_iter();
                if ids.next().is_none() {
                    return Ok(None);
                }
                Ok(Some(Cursor {
                    ids,
                    frequencies,
                    decoded_block: None,
                    block: [0; BLOCK_LEN],
                    remainder: None,
                }))
            })
            .collect::<OperationResult<_>>()?;
        Ok(Self { cursors })
    }
}

impl TermCursors for FrequencyCursors<'_> {
    fn current(&self, term: usize) -> Option<PointOffsetType> {
        self.cursors[term]
            .as_ref()?
            .ids
            .current()
            .map(|elem| elem.id)
    }

    fn advance(&mut self, term: usize) {
        if let Some(cursor) = self.cursors[term].as_mut()
            && cursor.ids.next().is_none()
        {
            self.cursors[term] = None;
        }
    }

    fn seek(&mut self, term: usize, target: PointOffsetType) -> Option<PointOffsetType> {
        let cursor = self.cursors[term].as_mut()?;
        match cursor.ids.advance_until_greater_or_equal(target) {
            Some(elem) => Some(elem.id),
            None => {
                self.cursors[term] = None;
                None
            }
        }
    }

    fn tf(&mut self, term: usize, doc: PointOffsetType) -> u32 {
        let cursor = self.cursors[term]
            .as_mut()
            .expect("tf is only asked for the current document");
        debug_assert_eq!(cursor.ids.current().map(|elem| elem.id), Some(doc));
        let offset = cursor
            .ids
            .current_offset()
            .expect("tf is only asked for the current document");
        cursor.tf(offset)
    }
}
