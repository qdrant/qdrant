use common::types::PointOffsetType;
use roaring::RoaringBitmap;

use super::super::posting_list::PostingList as MutablePostingList;
use super::TermCursors;

/// A forward cursor over one mutable posting list.
struct MutableCursor<'a> {
    iter: roaring::bitmap::Iter<'a>,
    current: Option<PointOffsetType>,
    frequencies: FrequencyLookup<'a>,
}

/// Finds the term frequency of a cursor's current id.
///
/// The bitmap iterator does not expose its position, so the cursor tracks it.
struct FrequencyLookup<'a> {
    /// The list's ids. Used only to recompute `at` with a rank after a seek.
    ids: &'a RoaringBitmap,
    /// The frequency of each id, in id order: the n-th id's is `values[n]`.
    values: &'a [u32],
    /// The position of the current id in `values`. Moving to the next id
    /// increments it, a seek resets it to `None`, and reading a frequency
    /// recomputes it from `ids` if needed (so seeks never pay for the rank).
    at: Option<usize>,
}

impl MutableCursor<'_> {
    fn advance(&mut self) {
        self.current = self.iter.next();
        self.frequencies.at = self.frequencies.at.map(|at| at + 1);
    }

    fn seek(&mut self, target: PointOffsetType) -> Option<PointOffsetType> {
        if self.current.is_some_and(|current| current >= target) {
            return self.current;
        }
        self.iter.advance_to(target);
        self.current = self.iter.next();
        self.frequencies.at = None;
        self.current
    }
}

/// Cursors over the mutable index. The term frequency is read next to the
/// posting, the same shape as [`PositionalCursors`](super::PositionalCursors).
pub struct MutableCursors<'a> {
    cursors: Vec<Option<MutableCursor<'a>>>,
    lens: Vec<usize>,
}

impl<'a> MutableCursors<'a> {
    /// One posting list per query term, `None` for a term without one. Every
    /// list must carry frequencies.
    pub fn new(postings: Vec<Option<&'a MutablePostingList>>) -> Self {
        let lens = postings
            .iter()
            .map(|posting| posting.map_or(0, |posting| posting.len()))
            .collect();
        let cursors = postings
            .into_iter()
            .map(|posting| {
                let posting = posting?;
                if posting.is_empty() {
                    return None;
                }
                let values = posting
                    .frequencies()
                    .expect("a scored posting list carries frequencies");
                let mut iter = posting.iter();
                let current = iter.next();
                Some(MutableCursor {
                    iter,
                    current,
                    frequencies: FrequencyLookup {
                        ids: posting.ids(),
                        values,
                        at: Some(0),
                    },
                })
            })
            .collect();
        Self { cursors, lens }
    }
}

impl TermCursors for MutableCursors<'_> {
    fn current(&self, term: usize) -> Option<PointOffsetType> {
        self.cursors[term].as_ref()?.current
    }

    fn advance(&mut self, term: usize) {
        if let Some(cursor) = self.cursors[term].as_mut() {
            cursor.advance();
            if cursor.current.is_none() {
                self.cursors[term] = None;
            }
        }
    }

    fn posting_len(&self, term: usize) -> usize {
        self.lens[term]
    }

    /// Document at a time (and its heap) covers the write path; the dense path
    /// is for the positional cursors.
    fn term_at_a_time(&self) -> bool {
        false
    }

    fn seek(&mut self, term: usize, target: PointOffsetType) -> Option<PointOffsetType> {
        let cursor = self.cursors[term].as_mut()?;
        let found = cursor.seek(target);
        if found.is_none() {
            self.cursors[term] = None;
        }
        found
    }

    fn tf(&mut self, term: usize, doc: PointOffsetType) -> u32 {
        let cursor = self.cursors[term]
            .as_mut()
            .expect("tf is asked only of a term positioned on the document");
        debug_assert_eq!(cursor.current, Some(doc));
        let lookup = &mut cursor.frequencies;
        let at = *lookup
            .at
            .get_or_insert_with(|| lookup.ids.rank(doc) as usize - 1);
        lookup.values[at]
    }
}
