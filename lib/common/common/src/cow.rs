use std::borrow::Borrow;
use std::ops::Deref;
use std::sync::Arc;

pub type ArcCow<'a, T> = BorrowCow<'a, T, Arc<T>>;
pub type BoxCow<'a, T> = BorrowCow<'a, T, Box<T>>;
pub type SimpleCow<'a, T> = BorrowCow<'a, T, T>;

/// [`std::borrow::Cow`]-like enum, but based on [`Borrow`] instead of [`ToOwned`].
pub enum BorrowCow<'a, Borrowed: ?Sized, Owned: Borrow<Borrowed>> {
    Borrowed(&'a Borrowed),
    Owned(Owned),
}

impl<'a, B: ?Sized, O: Borrow<B>> BorrowCow<'a, B, O> {
    #[inline(always)]
    pub fn as_borrowed(&'a self) -> Self {
        Self::Borrowed(self.deref())
    }
}

impl<B: ?Sized, O: Borrow<B>> Deref for BorrowCow<'_, B, O> {
    type Target = B;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        match self {
            BorrowCow::Borrowed(t) => t,
            BorrowCow::Owned(t) => t.borrow(),
        }
    }
}
