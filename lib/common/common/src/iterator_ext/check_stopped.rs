pub struct CheckStopped<I, F> {
    iter: I,
    f: F,
    every: usize,
    counter: usize,
    done: bool,
}

impl<I, F> CheckStopped<I, F> {
    pub fn new(iter: I, every: usize, f: F) -> Self {
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
    F: FnMut() -> bool,
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

#[cfg(test)]
mod tests {
    use crate::iterator_ext::IteratorExt;

    #[test]
    fn test_it_only_acts_periodically() {
        let mut num_checks = 0;

        let ninety_nine = (0..)
            .check_stop_every(20, || {
                num_checks += 1;

                // stop after 5 checks
                num_checks == 5
            })
            .count();

        assert_eq!(ninety_nine, 99);
        assert_eq!(num_checks, 5);
    }
}
