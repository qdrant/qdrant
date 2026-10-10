use common::types::PointOffsetType;
use posting_list::{PostingLenIterator, PostingListView};

use super::super::positions::Positions;
use super::TermCursors;

/// Cursors over compressed posting lists that store positions. A term's
/// frequency is the byte length of its positions divided by their width, read
/// from the offsets alone.
pub struct PositionalCursors<'a> {
    cursors: Vec<Option<PostingLenIterator<'a, Positions>>>,
    lens: Vec<usize>,
}

impl<'a> PositionalCursors<'a> {
    /// One view per query term, `None` for a term this index holds no posting
    /// list for.
    pub fn new(views: Vec<Option<PostingListView<'a, Positions>>>) -> Self {
        let lens = views
            .iter()
            .map(|view| view.as_ref().map_or(0, PostingListView::len))
            .collect();
        let cursors = views
            .into_iter()
            .map(|view| {
                let mut cursor = view?.len_iter();
                cursor.next()?;
                Some(cursor)
            })
            .collect();
        Self { cursors, lens }
    }
}

impl TermCursors for PositionalCursors<'_> {
    fn current(&self, term: usize) -> Option<PointOffsetType> {
        self.cursors[term].as_ref()?.current().map(|elem| elem.id)
    }

    fn advance(&mut self, term: usize) {
        if let Some(cursor) = self.cursors[term].as_mut()
            && cursor.next().is_none()
        {
            self.cursors[term] = None;
        }
    }

    fn posting_len(&self, term: usize) -> usize {
        self.lens[term]
    }

    /// The frequency is read next to the posting.
    fn term_at_a_time(&self) -> bool {
        true
    }

    /// A chunk at a time, without the per-element offset arithmetic.
    fn for_each_posting(&mut self, term: usize, mut f: impl FnMut(PointOffsetType, u32)) {
        if let Some(mut cursor) = self.cursors[term].take() {
            cursor.for_each_remaining(|elem| {
                f(elem.id, (elem.value_len / size_of::<u32>()) as u32);
            });
        }
    }

    fn seek(&mut self, term: usize, target: PointOffsetType) -> Option<PointOffsetType> {
        let cursor = self.cursors[term].as_mut()?;
        match cursor.advance_until_greater_or_equal(target) {
            Some(elem) => Some(elem.id),
            None => {
                self.cursors[term] = None;
                None
            }
        }
    }

    fn tf(&mut self, term: usize, doc: PointOffsetType) -> u32 {
        let elem = self.cursors[term]
            .as_ref()
            .and_then(|cursor| cursor.current())
            .expect("tf is only asked for the current document");
        debug_assert_eq!(elem.id, doc);
        (elem.value_len / size_of::<u32>()) as u32
    }
}
