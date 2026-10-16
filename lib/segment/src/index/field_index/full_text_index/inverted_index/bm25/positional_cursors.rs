use common::types::PointOffsetType;
use posting_list::{PostingLenIterator, PostingListView};

use super::super::positions::Positions;
use super::TermCursors;

/// Cursors over compressed posting lists that store positions. A term's
/// frequency is the byte length of its positions divided by their width, read
/// from the offsets alone.
pub struct PositionalCursors<'a> {
    cursors: Vec<Option<PostingLenIterator<'a, Positions>>>,
}

impl<'a> PositionalCursors<'a> {
    /// One view per query term, `None` for a term this index holds no posting
    /// list for.
    pub fn new(views: Vec<Option<PostingListView<'a, Positions>>>) -> Self {
        let cursors = views
            .into_iter()
            .map(|view| {
                let mut cursor = view?.len_iter();
                cursor.next()?;
                Some(cursor)
            })
            .collect();
        Self { cursors }
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
