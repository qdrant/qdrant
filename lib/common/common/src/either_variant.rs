use std::iter;

/// Variant works similar as Either, but for 4 variants.
pub enum EitherVariant<A, B, C, D> {
    A(A),
    B(B),
    C(C),
    D(D),
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            $crate::either_variant::EitherVariant::A($pattern) => $result,
            $crate::either_variant::EitherVariant::B($pattern) => $result,
            $crate::either_variant::EitherVariant::C($pattern) => $result,
            $crate::either_variant::EitherVariant::D($pattern) => $result,
        }
    };
}

impl<A, B, C, D> Iterator for EitherVariant<A, B, C, D>
where
    A: Iterator,
    B: Iterator<Item = A::Item>,
    C: Iterator<Item = A::Item>,
    D: Iterator<Item = A::Item>,
{
    type Item = A::Item;

    fn next(&mut self) -> Option<Self::Item> {
        for_all!(*self, ref mut inner => inner.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        for_all!(*self, ref inner => inner.size_hint())
    }

    fn count(self) -> usize {
        for_all!(self, inner => inner.count())
    }

    fn last(self) -> Option<Self::Item> {
        for_all!(self, inner => inner.last())
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        for_all!(*self, ref mut inner => inner.nth(n))
    }

    fn for_each<F>(self, f: F)
    where
        F: FnMut(Self::Item),
    {
        for_all!(self, inner => inner.for_each(f))
    }

    fn collect<X>(self) -> X
    where
        X: iter::FromIterator<Self::Item>,
    {
        for_all!(self, inner => inner.collect())
    }

    fn partition<X, F>(self, f: F) -> (X, X)
    where
        X: Default + Extend<Self::Item>,
        F: FnMut(&Self::Item) -> bool,
    {
        for_all!(self, inner => inner.partition(f))
    }

    fn fold<Acc, G>(self, init: Acc, f: G) -> Acc
    where
        G: FnMut(Acc, Self::Item) -> Acc,
    {
        for_all!(self, inner => inner.fold(init, f))
    }

    fn all<F>(&mut self, f: F) -> bool
    where
        F: FnMut(Self::Item) -> bool,
    {
        for_all!(*self, ref mut inner => inner.all(f))
    }

    fn any<F>(&mut self, f: F) -> bool
    where
        F: FnMut(Self::Item) -> bool,
    {
        for_all!(*self, ref mut inner => inner.any(f))
    }

    fn find<P>(&mut self, predicate: P) -> Option<Self::Item>
    where
        P: FnMut(&Self::Item) -> bool,
    {
        for_all!(*self, ref mut inner => inner.find(predicate))
    }

    fn find_map<X, F>(&mut self, f: F) -> Option<X>
    where
        F: FnMut(Self::Item) -> Option<X>,
    {
        for_all!(*self, ref mut inner => inner.find_map(f))
    }

    fn position<P>(&mut self, predicate: P) -> Option<usize>
    where
        P: FnMut(Self::Item) -> bool,
    {
        for_all!(*self, ref mut inner => inner.position(predicate))
    }
}
