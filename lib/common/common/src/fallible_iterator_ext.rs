use fallible_iterator::FallibleIterator;

pub trait FallibleIteratorExt: FallibleIterator {
    /// Similar to [`itertools::Itertools::collect_vec`].
    ///
    /// Note: more efficient than [`FallibleIterator::collect`] because it uses
    /// `fold`. The former calls `.next()` in a loop.
    fn collect_vec(self) -> Result<Vec<Self::Item>, Self::Error>
    where
        Self: Sized,
    {
        let vec = Vec::with_capacity(self.size_hint().0);
        self.fold(vec, |mut vec, item| {
            vec.push(item);
            Ok(vec)
        })
    }
}
