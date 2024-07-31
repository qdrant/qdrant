pub trait IteratorExt: Iterator {
    /// Periodically check if the iteration should be stopped.
    /// The closure `f` is called every `every` iterations, and should return `true` if the iteration should be stopped.
    fn check_stop_every<F>(self, every: usize, f: F) -> CheckStopped<Self, F>
    where
        F: Fn() -> bool,
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

pub struct CheckStopped<I, F> {
    iter: I,
    f: F,
    every: usize,
    counter: usize,
    done: bool,
}

impl<I, F> CheckStopped<I, F> {
    fn new(iter: I, every: usize, f: F) -> Self {
        CheckStopped {
            iter,
            f,
            every,
            done: false,
            counter: 0,
        }
    }
}

impl<I, F> Iterator for CheckStopped<I, F>
where
    I: Iterator,
    F: Fn() -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        self.counter += 1;

        if self.counter == self.every {
            self.counter = 0;
            if (self.f)() {
                self.done = true;
                return None;
            }
        }
        self.iter.next()
    }
}
