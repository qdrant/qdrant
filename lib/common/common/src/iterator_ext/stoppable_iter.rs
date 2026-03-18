use std::sync::atomic::{AtomicBool, Ordering};

pub struct StoppableIter<'a, I> {
    iter: I,
    is_stopped: &'a AtomicBool,
}

impl<'a, I> StoppableIter<'a, I> {
    pub fn new(iter: I, is_stopped: &'a AtomicBool) -> Self {
        Self { iter, is_stopped }
    }
}

impl<'a, I> Iterator for StoppableIter<'a, I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        // AI says, that if atomic is not updated frequently,
        // it is cheap enough to check it on every iteration.
        if self.is_stopped.load(Ordering::Relaxed) {
            return None;
        }
        self.iter.next()
    }
}
