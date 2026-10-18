use common::types::PointOffsetType;
use posting_list::{PostingLenIterator, PostingListView, PostingValue};

use super::super::term_frequencies::{BLOCK_LEN, TermFrequencyList};
use super::TermCursors;

/// Cursors over posting lists, with each term's frequencies read from its own
/// bit-packed list in posting order. Only the ids of the postings are read.
pub struct FrequencyCursors<'a, V: PostingValue> {
    cursors: Vec<Option<Cursor<'a, V>>>,
}

struct Cursor<'a, V: PostingValue> {
    ids: PostingLenIterator<'a, V>,
    frequencies: TermFrequencyList<'a>,
    /// The block decoded into `block`, if any.
    decoded_block: Option<usize>,
    block: [u32; BLOCK_LEN],
    /// The remainder, decoded the first time it is reached.
    remainder: Option<Vec<u32>>,
}

impl<V: PostingValue> Cursor<'_, V> {
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

impl<'a, V: PostingValue> FrequencyCursors<'a, V> {
    /// One posting list and its frequencies per query term, `None` for a term
    /// this index holds no posting list for. The frequencies of every posting
    /// list are expected to be there, one per entry: the files were checked to
    /// agree at open.
    pub fn new(
        views: Vec<Option<(PostingListView<'a, V>, Option<TermFrequencyList<'a>>)>>,
    ) -> Self {
        let cursors = views
            .into_iter()
            .map(|view| {
                let (ids, frequencies) = view?;
                debug_assert!(
                    frequencies.is_some_and(|frequencies| frequencies.len() == ids.len()),
                    "term frequencies do not match the postings",
                );
                let frequencies = frequencies?;
                let mut ids = ids.len_iter();
                ids.next()?;
                Some(Cursor {
                    ids,
                    frequencies,
                    decoded_block: None,
                    block: [0; BLOCK_LEN],
                    remainder: None,
                })
            })
            .collect();
        Self { cursors }
    }
}

impl<V: PostingValue> TermCursors for FrequencyCursors<'_, V> {
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
