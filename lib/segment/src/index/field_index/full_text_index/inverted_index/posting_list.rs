use common::types::PointOffsetType;
use itertools::Either;
use roaring::RoaringBitmap;

/// A document in a posting list and how often the term occurs in it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Posting {
    pub id: PointOffsetType,
    pub tf: u32,
}

/// The mutable index's posting list for one term.
///
/// Membership is all a filter needs, and a bitmap is the compact way to keep
/// it. A scorer also needs the term frequency per document, so an index that
/// scores keeps its postings as a sorted vector of `(id, tf)` instead, the
/// same shape the sparse route's appendable index uses for its weights. The
/// two variants answer the same questions; only one of them can answer
/// [`Self::frequencies`].
#[derive(Clone, Debug)]
pub enum PostingList {
    Ids(RoaringBitmap),
    Frequencies(Vec<Posting>),
}

impl Default for PostingList {
    fn default() -> Self {
        Self::Ids(RoaringBitmap::new())
    }
}

impl PostingList {
    pub fn new(with_frequencies: bool) -> Self {
        match with_frequencies {
            true => Self::Frequencies(Vec::new()),
            false => Self::default(),
        }
    }

    /// Add or replace a document. The frequency is ignored by an ids-only list.
    pub fn insert(&mut self, posting: Posting) {
        match self {
            Self::Ids(list) => {
                list.insert(posting.id);
            }
            Self::Frequencies(list) => {
                // Points arrive in increasing id order almost always, so the
                // append is the common case and the search only runs for an
                // update of an earlier point.
                if list.last().is_none_or(|last| last.id < posting.id) {
                    list.push(posting);
                    return;
                }
                match list.binary_search_by_key(&posting.id, |p| p.id) {
                    Ok(at) => list[at] = posting,
                    Err(at) => list.insert(at, posting),
                }
            }
        }
    }

    pub fn remove(&mut self, id: PointOffsetType) {
        match self {
            Self::Ids(list) => {
                list.remove(id);
            }
            Self::Frequencies(list) => {
                if let Ok(at) = list.binary_search_by_key(&id, |p| p.id) {
                    list.remove(at);
                }
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Ids(list) => list.len() as usize,
            Self::Frequencies(list) => list.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn contains(&self, id: PointOffsetType) -> bool {
        match self {
            Self::Ids(list) => list.contains(id),
            Self::Frequencies(list) => list.binary_search_by_key(&id, |p| p.id).is_ok(),
        }
    }

    /// Document ids in increasing order.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        match self {
            Self::Ids(list) => Either::Left(list.iter()),
            Self::Frequencies(list) => Either::Right(list.iter().map(|p| p.id)),
        }
    }

    /// The postings with their frequencies, sorted by id. `None` for an
    /// ids-only list.
    pub fn frequencies(&self) -> Option<&[Posting]> {
        match self {
            Self::Ids(_) => None,
            Self::Frequencies(list) => Some(list),
        }
    }

    /// The bitmap of an ids-only list. `None` when frequencies are stored.
    pub fn ids(&self) -> Option<&RoaringBitmap> {
        match self {
            Self::Ids(list) => Some(list),
            Self::Frequencies(_) => None,
        }
    }

    pub fn heap_bytes(&self) -> usize {
        match self {
            // Approximate heap usage with serialized size
            Self::Ids(list) => list.serialized_size(),
            Self::Frequencies(list) => list.capacity() * size_of::<Posting>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(id: PointOffsetType, tf: u32) -> Posting {
        Posting { id, tf }
    }

    /// Both shapes answer membership, length and iteration the same; only one
    /// carries frequencies. Out-of-order inserts, updates and removes keep
    /// the frequency list sorted and deduplicated.
    #[test]
    fn frequencies_keep_order_and_replace_on_update() {
        let mut ids = PostingList::new(false);
        let mut freq = PostingList::new(true);
        for posting in [p(5, 2), p(1, 1), p(9, 4), p(5, 3), p(7, 1)] {
            ids.insert(posting);
            freq.insert(posting);
        }
        assert_eq!(ids.iter().collect::<Vec<_>>(), [1, 5, 7, 9]);
        assert_eq!(freq.iter().collect::<Vec<_>>(), [1, 5, 7, 9]);
        assert_eq!(ids.len(), 4);
        assert_eq!(freq.len(), 4);
        assert!(ids.frequencies().is_none());
        assert_eq!(
            freq.frequencies().unwrap(),
            [p(1, 1), p(5, 3), p(7, 1), p(9, 4)],
            "the second insert of point 5 replaces its frequency",
        );
        for id in [1, 5, 7, 9] {
            assert!(ids.contains(id));
            assert!(freq.contains(id));
        }
        assert!(!freq.contains(6));

        ids.remove(5);
        freq.remove(5);
        freq.remove(6);
        assert_eq!(ids.iter().collect::<Vec<_>>(), [1, 7, 9]);
        assert_eq!(freq.frequencies().unwrap(), [p(1, 1), p(7, 1), p(9, 4)]);
        assert!(!freq.is_empty());
    }
}
