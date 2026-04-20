use common::types::PointOffsetType;
use self_cell::self_cell;

use crate::index::field_index::full_text_index::inverted_index::TokenId;
use crate::index::field_index::full_text_index::inverted_index::mmap_inverted_index::raw_posting_list::RawPostingList;

// Alias is required by `self_cell`: the dependent's lifetime parameter must be
// expressed via a single-lifetime alias.
type BoxedPointOffsetIter<'a> = Box<dyn Iterator<Item = PointOffsetType> + 'a>;

self_cell! {
    /// Lazy iterator over `PointOffsetType` that keeps the underlying raw
    /// posting list bytes alive. The owner is a `Vec<(TokenId, RawPostingList)>`
    /// produced by `UniversalPostings::fetch_*`, and the dependent is a boxed
    /// iterator built on top of views into that vector.
    ///
    /// Construction happens up-front (I/O is done in `fetch_*`), but the
    /// actual query evaluation — posting-list traversal, intersection, phrase
    /// matching — is driven on-demand from `next()`.
    pub struct LazyPostingsIter<'a> {
        owner: Vec<(TokenId, RawPostingList<'a>)>,
        #[covariant]
        dependent: BoxedPointOffsetIter,
    }
}

impl Iterator for LazyPostingsIter<'_> {
    type Item = PointOffsetType;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_owner, dependent| dependent.next())
    }
}

impl<'a> LazyPostingsIter<'a> {
    /// A `LazyPostingsIter` that yields nothing. Used when the query cannot
    /// possibly match (empty posting set, missing tokens, etc.).
    pub fn empty() -> Self {
        Self::new(Vec::new(), |_| Box::new(std::iter::empty()))
    }
}
