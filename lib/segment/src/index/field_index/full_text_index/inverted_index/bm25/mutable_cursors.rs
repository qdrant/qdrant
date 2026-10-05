use common::types::PointOffsetType;
use roaring::RoaringBitmap;

use super::super::posting_list::PostingList as MutablePostingList;
use super::super::{Document, TokenId};
use super::{Bm25Term, TermCursors};

/// A forward cursor over one mutable posting list.
struct MutableCursor<'a> {
    iter: roaring::bitmap::Iter<'a>,
    current: Option<PointOffsetType>,
    /// Set when the list keeps frequencies.
    frequencies: Option<FrequencyLookup<'a>>,
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
        if let Some(lookup) = self.frequencies.as_mut() {
            lookup.at = lookup.at.map(|at| at + 1);
        }
    }

    fn seek(&mut self, target: PointOffsetType) -> Option<PointOffsetType> {
        if self.current.is_some_and(|current| current >= target) {
            return self.current;
        }
        self.iter.advance_to(target);
        self.current = self.iter.next();
        if let Some(lookup) = self.frequencies.as_mut() {
            lookup.at = None;
        }
        self.current
    }
}

/// Cursors over the mutable index.
///
/// With frequencies stored, tf is read next to the posting and this is the same
/// shape as [`PositionalCursors`](super::PositionalCursors). With ids only, the
/// frequency comes from the document itself: the first time a candidate is
/// asked about, its ordered token ids are scanned once and every query term's
/// count is kept, so the scan is paid per document rather than per term.
pub struct MutableCursors<'a> {
    cursors: Vec<Option<MutableCursor<'a>>>,
    documents: &'a [Option<Document>],
    /// `(token id, term index)` sorted by token id, for the scan.
    tokens: Vec<(TokenId, usize)>,
    cached_doc: Option<PointOffsetType>,
    cached_tf: Vec<u32>,
}

impl<'a> MutableCursors<'a> {
    /// One posting list per query term, `None` for a term without one, plus the
    /// documents the frequencies are counted from when the postings carry none.
    pub fn new(
        postings: Vec<Option<&'a MutablePostingList>>,
        documents: &'a [Option<Document>],
        terms: &[Bm25Term],
    ) -> Self {
        debug_assert_eq!(postings.len(), terms.len());
        let cursors = postings
            .into_iter()
            .map(|posting| {
                let posting = posting?;
                if posting.is_empty() {
                    return None;
                }
                let mut iter = posting.iter();
                let current = iter.next();
                let frequencies = posting.frequencies().map(|values| FrequencyLookup {
                    ids: posting.ids(),
                    values,
                    at: Some(0),
                });
                Some(MutableCursor {
                    iter,
                    current,
                    frequencies,
                })
            })
            .collect();
        let mut tokens: Vec<(TokenId, usize)> = terms
            .iter()
            .enumerate()
            .map(|(index, term)| (term.token_id, index))
            .collect();
        tokens.sort_unstable();
        Self {
            cursors,
            documents,
            tokens,
            cached_doc: None,
            cached_tf: vec![0; terms.len()],
        }
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

    fn seek(&mut self, term: usize, target: PointOffsetType) -> Option<PointOffsetType> {
        let cursor = self.cursors[term].as_mut()?;
        let found = cursor.seek(target);
        if found.is_none() {
            self.cursors[term] = None;
        }
        found
    }

    fn tf(&mut self, term: usize, doc: PointOffsetType) -> u32 {
        if let Some(cursor) = self.cursors[term].as_mut()
            && let Some(lookup) = cursor.frequencies.as_mut()
        {
            debug_assert_eq!(cursor.current, Some(doc));
            let at = *lookup
                .at
                .get_or_insert_with(|| lookup.ids.rank(doc) as usize - 1);
            return lookup.values[at];
        }
        if self.cached_doc != Some(doc) {
            self.cached_tf.fill(0);
            let document = self.documents[doc as usize]
                .as_ref()
                .expect("a document in a posting list is stored");
            for token in document.tokens() {
                if let Ok(index) = self.tokens.binary_search_by_key(token, |(t, _)| *t) {
                    self.cached_tf[self.tokens[index].1] += 1;
                }
            }
            self.cached_doc = Some(doc);
        }
        self.cached_tf[term]
    }
}
