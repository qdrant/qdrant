use std::borrow::Borrow;
use std::ops::Deref;
use std::sync::Arc;

pub type ArcCow<'a, T> = Corrow<'a, T, Arc<T>>;
pub type BoxCow<'a, T> = Corrow<'a, T, Box<T>>;
pub type SimpleCow<'a, T> = Corrow<'a, T, T>;

/// `Cow`-like enum, but based on [`Borrow`] instead of [`ToOwned`].
///
/// Name is a portmanteau of words "borrow" and "cow".
pub enum Corrow<'a, Borrowed: ?Sized, Owned: Borrow<Borrowed>> {
    Borrowed(&'a Borrowed),
    Owned(Owned),
}

impl<'a, B: ?Sized, O: Borrow<B>> Corrow<'a, B, O> {
    #[inline(always)]
    pub fn as_borrowed(&'a self) -> Self {
        Self::Borrowed(self.deref())
    }

    #[inline(always)]
    pub fn owned_from<T: Into<O>>(owned: T) -> Self {
        Self::Owned(owned.into())
    }
}

impl<B: ?Sized, O: Borrow<B>> Deref for Corrow<'_, B, O> {
    type Target = B;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        match self {
            Corrow::Borrowed(t) => t,
            Corrow::Owned(t) => t.borrow(),
        }
    }
}
