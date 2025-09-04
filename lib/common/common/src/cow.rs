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
