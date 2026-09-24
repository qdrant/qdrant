use common::types::PointOffsetType;
use roaring::RoaringBitmap;

/// The mutable index's posting list for one term.
///
/// Membership is all a filter needs, and a bitmap is the compact way to keep
/// it. A scorer also needs the term frequency per document, so an index that
/// scores keeps one next to the bitmap, in id order: the frequency of the
/// `n`-th id is `frequencies[n]`. Filters only ever read the bitmap.
#[derive(Clone, Debug, Default)]
pub struct PostingList {
    ids: RoaringBitmap,
    frequencies: Option<Vec<u32>>,
}

impl PostingList {
    pub fn new(with_frequencies: bool) -> Self {
        Self {
            ids: RoaringBitmap::new(),
            frequencies: with_frequencies.then(Vec::new),
        }
    }

    /// Add or replace a document with how often the term occurs in it. The
    /// frequency is ignored by an ids-only list.
    pub fn insert(&mut self, id: PointOffsetType, tf: u32) {
        let Some(frequencies) = self.frequencies.as_mut() else {
            self.ids.insert(id);
            return;
        };
        // Points arrive in increasing id order almost always, so the append
        // is the common case and the rank only runs for an earlier point.
        if self.ids.max().is_none_or(|max| max < id) {
            self.ids.insert(id);
            frequencies.push(tf);
            return;
        }
        let inserted = self.ids.insert(id);
        let at = self.ids.rank(id) as usize - 1;
        if inserted {
            frequencies.insert(at, tf);
        } else {
            frequencies[at] = tf;
        }
    }

    pub fn remove(&mut self, id: PointOffsetType) {
        if let Some(frequencies) = self.frequencies.as_mut()
            && self.ids.contains(id)
        {
            frequencies.remove(self.ids.rank(id) as usize - 1);
        }
        self.ids.remove(id);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ids.len() as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    #[inline]
    pub fn contains(&self, id: PointOffsetType) -> bool {
        self.ids.contains(id)
    }

    /// Document ids in increasing order.
    #[inline]
    pub fn iter(&self) -> roaring::bitmap::Iter<'_> {
        self.ids.iter()
    }

    pub fn ids(&self) -> &RoaringBitmap {
        &self.ids
    }

    /// The term frequency of every id, in id order. `None` for an ids-only
    /// list.
    pub fn frequencies(&self) -> Option<&[u32]> {
        self.frequencies.as_deref()
    }

    pub fn heap_bytes(&self) -> usize {
        // Approximate the bitmap's heap usage with its serialized size
        self.ids.serialized_size()
            + self
                .frequencies
                .as_ref()
                .map_or(0, |frequencies| frequencies.capacity() * size_of::<u32>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both answer membership, length and iteration the same; only one
    /// carries frequencies. Out-of-order inserts, updates and removes keep
    /// the frequencies aligned with the ids.
    #[test]
    fn frequencies_keep_order_and_replace_on_update() {
        let mut ids = PostingList::new(false);
        let mut freq = PostingList::new(true);
        for (id, tf) in [(5, 2), (1, 1), (9, 4), (5, 3), (7, 1)] {
            ids.insert(id, tf);
            freq.insert(id, tf);
        }
        assert_eq!(ids.iter().collect::<Vec<_>>(), [1, 5, 7, 9]);
        assert_eq!(freq.iter().collect::<Vec<_>>(), [1, 5, 7, 9]);
        assert_eq!(ids.len(), 4);
        assert_eq!(freq.len(), 4);
        assert!(ids.frequencies().is_none());
        assert_eq!(
            freq.frequencies().unwrap(),
            [1, 3, 1, 4],
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
        assert_eq!(freq.iter().collect::<Vec<_>>(), [1, 7, 9]);
        assert_eq!(freq.frequencies().unwrap(), [1, 1, 4]);
        assert!(!freq.is_empty());
    }
}
