use std::borrow::Borrow;
use std::ops::Deref;

pub type SimpleCow<'a, T> = BorrowCow<'a, T, T>;

/// [`std::borrow::Cow`]-like enum, but based on [`Borrow`] instead of [`ToOwned`].
pub enum BorrowCow<'a, Borrowed: ?Sized, Owned: Borrow<Borrowed>> {
    Borrowed(&'a Borrowed),
    Owned(Owned),
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
