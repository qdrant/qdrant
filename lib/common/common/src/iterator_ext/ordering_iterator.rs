use std::collections::BTreeMap;
use std::convert::Infallible;

/// Trait that abstracts over the item type of [`OrderingIterator`], allowing it
/// to accept both plain `(usize, T)` items and `Result<(usize, T), E>` items
/// from a single implementation.
///
/// Errors produced by the upstream iterator bypass the ordering buffer and are
/// forwarded immediately in the order they were observed.
pub trait OrderedItem: Sized {
    type Value;
    type Error;

    fn split(self) -> Result<(usize, Self::Value), Self::Error>;
    fn from_value(idx: usize, value: Self::Value) -> Self;
    fn from_error(error: Self::Error) -> Self;
}

impl<T> OrderedItem for (usize, T) {
    type Value = T;
    type Error = Infallible;

    fn split(self) -> Result<(usize, T), Infallible> {
        Ok(self)
    }

    fn from_value(idx: usize, value: T) -> Self {
        (idx, value)
    }

    fn from_error(error: Infallible) -> Self {
        match error {}
    }
}

impl<T, E> OrderedItem for Result<(usize, T), E> {
    type Value = T;
    type Error = E;

    fn split(self) -> Result<(usize, T), E> {
        self
    }

    fn from_value(idx: usize, value: T) -> Self {
        Ok((idx, value))
    }

    fn from_error(error: E) -> Self {
        Err(error)
    }
}

/// An iterator adapter that takes an iterator of `(idx: usize, element: T)`
/// items — optionally wrapped in `Result` — and yields them in order of
/// ascending `idx`, starting from `0`.
///
/// This is useful when the incoming iterator has local ordering inconsistencies
/// but does not require the whole iterator to be consumed before emitting the
/// first result.
///
/// Out-of-order elements are buffered in a [`BTreeMap`] until their turn comes.
/// If there are gaps in the id sequence (missing ids), the iterator skips over
/// them once it has confirmed the gap by exhausting its input, advancing to the
/// next smallest buffered id.
///
/// If the upstream iterator yields errors (i.e. `Item = Result<_, _>`), the
/// errors are forwarded immediately in the order they were observed and do not
/// participate in the ordering.
///
/// The input ids are assumed to be unique; behavior is unspecified if the same
/// id is produced more than once.
pub struct OrderingIterator<I>
where
    I: Iterator,
    I::Item: OrderedItem,
{
    iter: I,
    next_id: usize,
    buffer: BTreeMap<usize, <I::Item as OrderedItem>::Value>,
}

impl<I> OrderingIterator<I>
where
    I: Iterator,
    I::Item: OrderedItem,
{
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            next_id: 0,
            buffer: BTreeMap::new(),
        }
    }
}

impl<I> Iterator for OrderingIterator<I>
where
    I: Iterator,
    I::Item: OrderedItem,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(value) = self.buffer.remove(&self.next_id) {
                let idx = self.next_id;
                self.next_id += 1;
                return Some(I::Item::from_value(idx, value));
            }

            let Some(item) = self.iter.next() else {
                // Upstream iterator is exhausted: drain the buffer in ascending
                // order, skipping over any gaps between ids.
                let (idx, value) = self.buffer.pop_first()?;
                self.next_id = idx + 1;
                return Some(I::Item::from_value(idx, value));
            };

            match item.split() {
                Ok((idx, value)) => {
                    if idx == self.next_id {
                        self.next_id += 1;
                        return Some(I::Item::from_value(idx, value));
                    }
                    self.buffer.insert(idx, value);
                }
                Err(error) => {
                    return Some(I::Item::from_error(error));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect<I: IntoIterator<Item = (usize, char)>>(input: I) -> Vec<(usize, char)> {
        OrderingIterator::new(input.into_iter()).collect()
    }

    fn collect_res<I, E>(input: I) -> Vec<Result<(usize, char), E>>
    where
        I: IntoIterator<Item = Result<(usize, char), E>>,
    {
        OrderingIterator::new(input.into_iter()).collect()
    }

    #[test]
    fn already_ordered() {
        let input = vec![(0, 'a'), (1, 'b'), (2, 'c'), (3, 'd')];
        assert_eq!(collect(input), vec![(0, 'a'), (1, 'b'), (2, 'c'), (3, 'd')],);
    }

    #[test]
    fn reversed() {
        let input = vec![(3, 'd'), (2, 'c'), (1, 'b'), (0, 'a')];
        assert_eq!(collect(input), vec![(0, 'a'), (1, 'b'), (2, 'c'), (3, 'd')],);
    }

    #[test]
    fn shuffled() {
        let input = vec![(2, 'c'), (0, 'a'), (3, 'd'), (1, 'b')];
        assert_eq!(collect(input), vec![(0, 'a'), (1, 'b'), (2, 'c'), (3, 'd')],);
    }

    #[test]
    fn drains_buffer_after_exhaustion() {
        let input = vec![(0, 'a'), (3, 'd'), (2, 'c'), (1, 'b')];
        assert_eq!(collect(input), vec![(0, 'a'), (1, 'b'), (2, 'c'), (3, 'd')],);
    }

    #[test]
    fn empty() {
        let input: Vec<(usize, char)> = vec![];
        assert!(collect(input).is_empty());
    }

    #[test]
    fn single_element() {
        assert_eq!(collect(vec![(0, 'a')]), vec![(0, 'a')]);
    }

    #[test]
    fn does_not_start_at_zero() {
        let input = vec![(5, 'a'), (6, 'b'), (7, 'c')];
        assert_eq!(collect(input), vec![(5, 'a'), (6, 'b'), (7, 'c')],);
    }

    #[test]
    fn large_gap_in_the_middle() {
        let input = vec![(0, 'a'), (1, 'b'), (1_000_000, 'c'), (1_000_001, 'd')];
        assert_eq!(
            collect(input),
            vec![(0, 'a'), (1, 'b'), (1_000_000, 'c'), (1_000_001, 'd')],
        );
    }

    #[test]
    fn large_gap_with_local_disorder() {
        let input = vec![
            (1000, 'b'),
            (1, 'a'),
            (1_000_001, 'd'),
            (1_000_000, 'c'),
            (2_000_000, 'e'),
        ];
        assert_eq!(
            collect(input),
            vec![
                (1, 'a'),
                (1000, 'b'),
                (1_000_000, 'c'),
                (1_000_001, 'd'),
                (2_000_000, 'e'),
            ],
        );
    }

    #[test]
    fn multiple_gaps() {
        let input = vec![(0, 'a'), (10, 'b'), (20, 'c'), (21, 'd'), (30, 'e')];
        assert_eq!(
            collect(input),
            vec![(0, 'a'), (10, 'b'), (20, 'c'), (21, 'd'), (30, 'e')],
        );
    }

    #[test]
    fn result_items_reordered() {
        let input: Vec<Result<(usize, char), &str>> =
            vec![Ok((2, 'c')), Ok((0, 'a')), Ok((3, 'd')), Ok((1, 'b'))];
        assert_eq!(
            collect_res(input),
            vec![Ok((0, 'a')), Ok((1, 'b')), Ok((2, 'c')), Ok((3, 'd'))],
        );
    }

    #[test]
    fn result_items_error_passes_through_in_observed_order() {
        // Errors are yielded immediately and do not participate in reordering.
        // The Ok items are still reordered around them.
        let input: Vec<Result<(usize, char), &str>> =
            vec![Ok((1, 'b')), Err("boom"), Ok((0, 'a')), Ok((2, 'c'))];
        assert_eq!(
            collect_res(input),
            vec![Err("boom"), Ok((0, 'a')), Ok((1, 'b')), Ok((2, 'c'))],
        );
    }

    #[test]
    fn result_items_error_before_ready_value() {
        // The error is observed while we're still buffering; it should be
        // yielded as soon as it's seen, then ordering resumes.
        let input: Vec<Result<(usize, char), i32>> =
            vec![Ok((2, 'c')), Err(42), Ok((0, 'a')), Ok((1, 'b'))];
        assert_eq!(
            collect_res(input),
            vec![Err(42), Ok((0, 'a')), Ok((1, 'b')), Ok((2, 'c'))],
        );
    }
}
