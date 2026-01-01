use std::hash::Hash;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use parking_lot::{Condvar, Mutex};

use crate::PoolTasks;

pub(crate) fn thread_worker<GroupId: Eq + Hash + Clone>(
    wait_for_jobs: Arc<Condvar>,
    tasks: Arc<Mutex<PoolTasks<GroupId>>>,
    terminate: Arc<AtomicBool>,
) {
    // TODO restoring a thread if old one panics, or handling the panic.

    // Loop over tasks.
    loop {
        let (task_info, task) = {
            let mut guard = tasks.lock();
            // Loop over condvar wait attempts.
            loop {
                // Both Mutex::lock and Condvar::wait should impose a total ordering per se, so Relaxed ordering here is
                // sufficient.
                //
                // TODO does it apply to `parking_lot` too? (At least it uses Aquire and Release internally.) If not,
                // the thread may end up waiting for the condvar infinitely.
                if terminate.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }
                if let Some(ready_to_run_task) = guard.get_next_task() {
                    break ready_to_run_task;
                } else {
                    // The mutex is released while waiting for the condvar, and other threads can proceed.
                    wait_for_jobs.wait(&mut guard);
                }
            }
        };

        let _result = catch_unwind(task);
        // TODO report the panic if any, or refactor everything, let it panic and create a new thread.

        if terminate.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }

        {
            let mut guard = tasks.lock();
            guard.complete_task(&task_info, wait_for_jobs.as_ref());
            // TODO here lock is released just to be reacquired in the next iteration.
        }
    }
}
