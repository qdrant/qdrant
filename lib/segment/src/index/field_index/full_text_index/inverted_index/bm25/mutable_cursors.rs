use common::types::PointOffsetType;

use super::super::posting_list::PostingList as MutablePostingList;
use super::super::{Document, TokenId};
use super::{Bm25Term, TermCursors};

/// A forward cursor over a mutable posting list.
struct BitmapCursor<'a> {
    iter: roaring::bitmap::Iter<'a>,
    current: Option<PointOffsetType>,
}

/// Cursors over the mutable index. Its postings are plain id sets, so a term's
/// frequency comes from the document itself: the first time a candidate is
/// asked about, its ordered token ids are scanned once and every query term's
/// count is kept, so the scan is paid per document rather than per term.
pub struct MutableCursors<'a> {
    cursors: Vec<Option<BitmapCursor<'a>>>,
    lens: Vec<usize>,
    documents: &'a [Option<Document>],
    /// `(token id, term index)` sorted by token id, for the scan.
    tokens: Vec<(TokenId, usize)>,
    cached_doc: Option<PointOffsetType>,
    cached_tf: Vec<u32>,
}

impl<'a> MutableCursors<'a> {
    /// One posting list per query term, `None` for a term without one, plus the
    /// documents the frequencies are counted from.
    pub fn new(
        postings: Vec<Option<&'a MutablePostingList>>,
        documents: &'a [Option<Document>],
        terms: &[Bm25Term],
    ) -> Self {
        debug_assert_eq!(postings.len(), terms.len());
        let lens = postings
            .iter()
            .map(|posting| posting.map_or(0, |posting| posting.len()))
            .collect();
        let cursors = postings
            .into_iter()
            .map(|posting| {
                let mut iter = posting?.iter();
                let current = Some(iter.next()?);
                Some(BitmapCursor { iter, current })
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
            lens,
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
            cursor.current = cursor.iter.next();
            if cursor.current.is_none() {
                self.cursors[term] = None;
            }
        }
    }

    fn posting_len(&self, term: usize) -> usize {
        self.lens[term]
    }

    /// A frequency is counted from the document, cached for one document at a
    /// time: term at a time, every posting would rescan its document.
    fn term_at_a_time(&self) -> bool {
        false
    }

    fn seek(&mut self, term: usize, target: PointOffsetType) -> Option<PointOffsetType> {
        let cursor = self.cursors[term].as_mut()?;
        if cursor.current.is_some_and(|current| current >= target) {
            return cursor.current;
        }
        cursor.iter.advance_to(target);
        cursor.current = cursor.iter.next();
        if cursor.current.is_none() {
            self.cursors[term] = None;
        }
        cursor_current(&self.cursors[term])
    }

    fn tf(&mut self, term: usize, doc: PointOffsetType) -> u32 {
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

fn cursor_current(cursor: &Option<BitmapCursor<'_>>) -> Option<PointOffsetType> {
    cursor.as_ref()?.current
}
