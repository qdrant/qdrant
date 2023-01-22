use std::time::Duration;

use parking_lot::{Condvar, Mutex};

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

    /// Return `true` if ready, `false` if timed out.
    pub fn await_ready_for_timeout(&self, timeout: Duration) -> bool {
        let mut is_ready = self.value.lock();
        if !*is_ready {
            !self.condvar.wait_for(&mut is_ready, timeout).timed_out()
        } else {
            true
        }
    }
}
