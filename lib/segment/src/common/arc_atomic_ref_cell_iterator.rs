use std::sync::Arc;

use atomic_refcell::AtomicRefCell;

/// Intermediate data structure intended to help returning an iterator
/// which depends on some guarded internal object.
///
/// The main idea is that we create a special implementation of iterator, which holds
/// all required temporary values, which guarantees that source data for the iterator
/// will be available during the whole time of the iterator existence.
///
/// In this specific case implemented support for `AtomicRefCell` but this code can easily
/// be modified to other structures like `Mutex`, `RwLock`, e.t.c.
pub struct ArcAtomicRefCellIterator<T: ?Sized, I> {
    _holder: Arc<AtomicRefCell<T>>,
    iterator: I,
}

impl<'a, T: 'a + ?Sized, I> ArcAtomicRefCellIterator<T, I> {
    pub fn new<F>(holder: Arc<AtomicRefCell<T>>, f: F) -> Self
    where
        F: FnOnce(&'a T) -> I + 'a,
    {
        // We want to express that it's safe to keep the iterator around for as long as the
        // Arc is around. Unfortunately, we can't say this directly with lifetimes, because
        // we have to return iterator which depends on temporary `.borrow()` value:
        //
        // ```
        // let reference = holder.borrow();
        // //                    ^^^^^^^^^^ - temporary value created
        // return ref.iter()
        // ```
        //
        // Rust does not like that.
        // That is why we use unsafe `.as_ptr()` to bypass the borrow checker.
        // This operation should be safe overall, once we are keeping actual source of data alive
        // with `_holder` in our reference counter.
        // Even if the all other handlers are dropped - iterator will stay valid due to the reference counter.
        let reference = unsafe { holder.as_ptr().as_ref().unwrap() };
        Self {
            _holder: holder,
            iterator: f(reference),
        }
    }
}

impl<T: ?Sized, I: Iterator> Iterator for ArcAtomicRefCellIterator<T, I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.iterator.next()
    }
}
