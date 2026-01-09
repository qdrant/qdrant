use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use parking_lot::{Condvar, Mutex};

use crate::{PoolTasks, TaskInfo};

pub(crate) fn thread_worker<GroupId: Eq + Hash + Clone + Send + 'static>(
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

        // Even if the task panics, we want to mark it as completed and proceed.
        // This is a very simple pool!
        let _task_completion_guard = TaskCompletionGuard::new(&tasks, &wait_for_jobs, task_info);

        let _result = task();
        // TODO report the panic if any, or refactor everything, let it panic and create a new thread.

        if terminate.load(std::sync::atomic::Ordering::Relaxed) {
            // We might std::mem::forget(task_completion_guard), but leaving it as is is OK too.
            return;
        }
    }
}

pub(crate) struct TaskCompletionGuard<'env, GroupId: Clone + Eq + Hash + Send + 'static> {
    task_pool: &'env Mutex<PoolTasks<GroupId>>,
    wait_for_jobs: &'env Condvar,
    task_info: Option<TaskInfo<GroupId>>,
}

impl<'env, GroupId: Clone + Eq + Hash + Send + 'static> TaskCompletionGuard<'env, GroupId> {
    pub(crate) fn new(
        task_pool: &'env Mutex<PoolTasks<GroupId>>,
        wait_for_jobs: &'env Condvar,
        task_info: Option<TaskInfo<GroupId>>,
    ) -> Self {
        Self {
            task_pool,
            wait_for_jobs,
            task_info,
        }
    }
}

impl<'env, GroupId: Clone + Eq + Hash + Send + 'static> Drop for TaskCompletionGuard<'env, GroupId> {
    fn drop(&mut self) {
        if let Some(task_info) = &self.task_info {
            let mut guard = self.task_pool.lock();
            guard.complete_task(task_info, self.wait_for_jobs);
        }
    }
}
