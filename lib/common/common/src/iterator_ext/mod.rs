use check_stopped::CheckStopped;

mod check_stopped;

pub trait IteratorExt: Iterator {
    /// Periodically check if the iteration should be stopped.
    /// The closure `f` is called every `every` iterations, and should return `true` if the iteration should be stopped.
    fn check_stop_every<F>(self, every: usize, f: F) -> CheckStopped<Self, F>
    where
        F: FnMut() -> bool,
        Self: Sized,
    {
        CheckStopped::new(self, every, f)
    }

    /// Periodically check if the iteration should be stopped.
    /// The closure `f` is called every 500 iterations, and should return `true` if the iteration should be stopped.
    #[inline]
    fn check_stop<F>(self, f: F) -> CheckStopped<Self, F>
    where
        F: Fn() -> bool,
        Self: Sized,
    {
        self.check_stop_every(500, f)
    }
}

impl<I: Iterator> IteratorExt for I {}
