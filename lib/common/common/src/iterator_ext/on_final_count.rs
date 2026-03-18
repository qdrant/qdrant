pub struct OnFinalCount<I, F>
where
    F: FnMut(usize),
{
    wrapped_iter: I,
    callback: F,
    counter: usize,
}

impl<I, F> OnFinalCount<I, F>
where
    F: FnMut(usize),
{
    pub fn new(iter: I, f: F) -> Self {
        OnFinalCount {
            wrapped_iter: iter,
            callback: f,
            counter: 0,
        }
    }
}

impl<I, F> Drop for OnFinalCount<I, F>
where
    F: FnMut(usize),
{
    fn drop(&mut self) {
        (self.callback)(self.counter);
    }
}

impl<I, F> Iterator for OnFinalCount<I, F>
where
    I: Iterator,
    F: FnMut(usize),
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.wrapped_iter.next();
        self.counter += usize::from(item.is_some());
        item
    }
}

#[cfg(test)]
mod tests {
    use crate::iterator_ext::IteratorExt;

    #[test]
    fn test_on_final_count() {
        let mut iter_counter = 0;

        let count = (0..10).on_final_count(|c| iter_counter = c).count();

        assert_eq!(count, 10);
        assert_eq!(iter_counter, 10);
    }

    #[test]
    fn test_on_final_count_half_full() {
        let mut iter_counter = 0;

        let mut iter = (0..10).on_final_count(|c| iter_counter = c);

        let _item1 = iter.next();
        let _item2 = iter.next();
        let _item3 = iter.next();

        drop(iter);

        assert_eq!(iter_counter, 3);
    }

    #[test]
    fn test_on_final_count_half_full_insist_on_empty() {
        let mut iter_counter = 0;

        let mut iter = (0..3).on_final_count(|c| iter_counter = c);

        let _item = iter.next();
        let _item = iter.next();
        let _item = iter.next();
        let _item = iter.next();
        let _item = iter.next();
        let _item = iter.next();
        let _item = iter.next();

        drop(iter);

        assert_eq!(iter_counter, 3);
    }
}
