//! [`std::borrow::Cow`]-like enums.
//!
//! # Comparison table
//!
//! | Type                 | Borrowed | Owned                   |
//! | -------------------- | -------- | ----------------------- |
//! | [`Cow<'a, T>`]       | `&'a T`  | `<T as ToOwned>::Owned` |
//! | [`BoxCow<'a, T>`]    | `&'a T`  | `Box<T>`                |
//! | [`SimpleCow<'a, T>`] | `&'a T`  | `T`                     |
//!
//! [`Cow<'a, T>`]: std::borrow::Cow

use std::borrow::Cow;
use std::ops::Deref;

pub enum BoxCow<'a, T: ?Sized> {
    Borrowed(&'a T),
    Owned(Box<T>),
}

impl<'a, T: ?Sized> BoxCow<'a, T> {
    pub fn as_borrowed(&'a self) -> Self {
        match self {
            BoxCow::Borrowed(v) => BoxCow::Borrowed(v),
            BoxCow::Owned(v) => BoxCow::Borrowed(v.as_ref()),
        }
    }
}

impl<T: ?Sized> Deref for BoxCow<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            BoxCow::Borrowed(t) => t,
            BoxCow::Owned(t) => t,
        }
    }
}

pub enum SimpleCow<'a, T> {
    Borrowed(&'a T),
    Owned(T),
}

impl<T> Deref for SimpleCow<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            SimpleCow::Borrowed(v) => v,
            SimpleCow::Owned(v) => v,
        }
    }
}

/// A newtype wrapping `Cow<'a, [T]>` that implements `IntoIterator<Item = Cow<'a, T>>`.
pub struct CowSlice<'a, T: Clone>(pub Cow<'a, [T]>);

impl<'a, T: Clone> IntoIterator for CowSlice<'a, T> {
    type Item = Cow<'a, T>;
    type IntoIter = CowSliceIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        match self.0 {
            Cow::Borrowed(slice) => CowSliceIter::Borrowed(slice.iter()),
            Cow::Owned(vec) => CowSliceIter::Owned(vec.into_iter()),
        }
    }
}

pub enum CowSliceIter<'a, T> {
    Borrowed(std::slice::Iter<'a, T>),
    Owned(std::vec::IntoIter<T>),
}

impl<'a, T: Clone> Iterator for CowSliceIter<'a, T> {
    type Item = Cow<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            CowSliceIter::Borrowed(iter) => iter.next().map(Cow::Borrowed),
            CowSliceIter::Owned(iter) => iter.next().map(Cow::Owned),
        }
    }
}
