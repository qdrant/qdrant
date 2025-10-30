/// A helper, similar to golang's `defer` keyword, that executes a function once the current scope finishes.
pub struct DeferCallback {
    callback: Box<dyn FnMut() + Send + Sync>,
}

impl DeferCallback {
    pub fn new(callback: impl FnMut() + Send + Sync + 'static) -> Self {
        Self {
            callback: Box::new(callback),
        }
    }
}

impl Drop for DeferCallback {
    fn drop(&mut self) {
        (self.callback)()
    }
}

#[cfg(test)]
mod test {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use super::*;

    #[test]
    fn test_defer_callback_bool() {
        let counter = Arc::new(AtomicBool::new(false));

        {
            let counter_clone = counter.clone();
            let _ = DeferCallback::new(move || {
                counter_clone.store(true, Ordering::Relaxed);
            });
        }

        assert!(counter.load(Ordering::Relaxed));
    }

    #[test]
    fn test_defer_callback_loop() {
        let counter = Arc::new(AtomicUsize::new(0));

        const LEN: usize = 100;

        {
            for _ in 0..LEN {
                let counter_clone = counter.clone();
                let _ = DeferCallback::new(move || {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                });
            }
        }

        assert_eq!(counter.load(Ordering::Relaxed), LEN);
    }
}
