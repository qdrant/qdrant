use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::{Condvar, Mutex};
use tokio::runtime;
use tokio::runtime::Runtime;

pub fn create_search_runtime(max_search_threads: usize) -> std::io::Result<Runtime> {
    let mut search_threads = max_search_threads;

    if search_threads == 0 {
        let num_cpu = num_cpus::get();
        search_threads = std::cmp::max(1, num_cpu - 1);
    }

    runtime::Builder::new_multi_thread()
        .worker_threads(search_threads)
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("search-{}", id)
        })
        .build()
}

pub struct IsReady {
    condvar: Condvar,
    value: Mutex<bool>,
}

impl Default for IsReady {
    fn default() -> Self {
        Self {
            condvar: Condvar::new(),
            value: Mutex::new(false),
        }
    }
}

impl IsReady {
    pub fn make_ready(&self) {
        let mut is_ready = self.value.lock();
        if !*is_ready {
            *is_ready = true;
            self.condvar.notify_all();
        }
    }

    pub fn make_not_ready(&self) {
        *self.value.lock() = false;
    }

    pub fn check_ready(&self) -> bool {
        *self.value.lock()
    }

    pub fn await_ready(&self) {
        let mut is_ready = self.value.lock();
        if !*is_ready {
            self.condvar.wait(&mut is_ready);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::thread::sleep;
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_is_ready() {
        let is_ready = Arc::new(IsReady::default());
        let is_ready_clone = is_ready.clone();
        let join = thread::spawn(move || {
            is_ready_clone.await_ready();
            eprintln!(
                "is_ready_clone.check_ready() = {:#?}",
                is_ready_clone.check_ready()
            );
        });

        sleep(Duration::from_millis(500));
        eprintln!("Making ready");
        is_ready.make_ready();
        sleep(Duration::from_millis(500));
        join.join().unwrap()
    }
}
