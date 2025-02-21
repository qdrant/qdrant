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
        self.counter += 1;
        self.wrapped_iter.next()
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
        assert_eq!(iter_counter, 11);
    }
}
