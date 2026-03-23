//! Utilities for iterators over `Result`.

use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::iter::{Once, once};

use ahash::AHashMap;
use itertools::Either;

pub trait FallibleIteratorExt: Iterator + Sized {
    /// Like [`itertools::Itertools::unique`], but for iterators over [`Result`].
    fn unique_ok<T, E>(self) -> impl Iterator<Item = Result<T, E>>
    where
        Self: Iterator<Item = Result<T, E>>,
        T: Clone + Eq + Hash + 'static;
}

impl<I: Iterator + Sized> FallibleIteratorExt for I {
    fn unique_ok<T, E>(mut self) -> impl Iterator<Item = Result<T, E>>
    where
        Self: Iterator<Item = Result<T, E>>,
        T: Clone + Eq + Hash + 'static,
    {
        let mut used = AHashMap::new();
        std::iter::from_fn(move || {
            self.find_map(|v| match v {
                Ok(v) => match used.entry(v) {
                    Entry::Occupied(_) => None,
                    Entry::Vacant(entry) => {
                        let elt = entry.key().clone();
                        entry.insert(());
                        Some(Ok(elt))
                    }
                },
                Err(v) => Some(Err(v)),
            })
        })
    }
}

pub trait TransposeResultIter<I, T, E> {
    /// Convert `Result<Iterator<Item = Result<T, E>>, E>`
    /// into    `Iterator<Item = Result<T, E>>`
    fn into_result_iter(self) -> Either<I, Once<Result<T, E>>>;
}

impl<I, T, E> TransposeResultIter<I, T, E> for Result<I, E>
where
    I: Iterator<Item = Result<T, E>>,
{
    fn into_result_iter(self) -> Either<I, Once<Result<T, E>>> {
        match self {
            Ok(iter) => Either::Left(iter),
            Err(err) => Either::Right(once(Err(err))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unique_ok() {
        let a = [Ok(1), Err('a'), Ok(2), Err('b'), Ok(3), Ok(1), Err('a')];
        let b = [Ok(1), Err('a'), Ok(2), Err('b'), Ok(3), Err('a')];
        let output = a.into_iter().unique_ok().collect::<Vec<_>>();
        assert_eq!(output, b);
    }
}
