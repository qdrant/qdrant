use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread::JoinHandle;

use parking_lot::{Condvar, Mutex};
pub use switch::SwitchToken;
use thread::thread_worker;

#[cfg(feature = "tokio")]
mod async_pool;
mod switch;
mod thread;

#[cfg(feature = "tokio")]
pub use async_pool::{AsyncPool, AsyncTaskError};

// Defines ordering in which tasks are added.
// Tasks which added first are executed first unless something blocks them.
// Smaller task ID means higher priority.
type TaskId = usize;

// The UnwindSafe bound may be removed if we handle panics with recreating a new thread.
pub type Task = Box<dyn FnOnce() + Send + 'static>;

pub enum StalledTask {
    Function(Task),
    Condvar(Arc<Condvar>),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OperationMode {
    Shared,
    // TODO Actually, Exclusive mode doesn't exists; it is deffered with mode switch for particular group ID mid-flight.
    // TODO design an API to
    //      1. check if group is uncontended, and join it
    //      2. if no uncontended group exists, join any with new priority (now or old?)
    //
    //      Also, it worth learning if case 1 every fires on contention (perhaps, on RW contention it is always 2).
    Exclusive,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum GroupState {
    Shared(usize), // number of scheduled or running shared tasks
    Exclusive,
}

pub struct Pool<GroupId> {
    _threads: Vec<JoinHandle<()>>,
    tasks: Arc<Mutex<PoolTasks<GroupId>>>,
    wait_for_jobs_condvar: Arc<Condvar>,
    // TODO make possible using an external termination flag
    terminate: Arc<AtomicBool>,
}

impl<GroupId> Drop for Pool<GroupId> {
    fn drop(&mut self) {
        self.terminate
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.wait_for_jobs_condvar.notify_all();

        // TODO should we really wait for threads to finish?
        for thread in self._threads.drain(..) {
            let _ = thread.join();
        }
    }
}

impl<GroupId> Pool<GroupId> {
    pub fn terminate(&mut self) {
        self.terminate
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.wait_for_jobs_condvar.notify_all();

        for thread in self._threads.drain(..) {
            let _ = thread.join();
        }
    }
}

impl<GroupId: Clone + Hash + Eq + Send + 'static> Pool<GroupId> {
    pub fn new(threads: usize) -> Self {
        let wait_for_jobs = Arc::<Condvar>::default();
        let tasks = Arc::<Mutex<PoolTasks<GroupId>>>::default();
        let terminate = Arc::<AtomicBool>::default();
        let threads = (0..threads)
            .map(|i| {
                let wait_for_jobs = wait_for_jobs.clone();
                let tasks = tasks.clone();
                let terminate = terminate.clone();
                std::thread::Builder::new()
                    .name(format!("qdant-pool-worker-{}", i + 1))
                    .spawn(move || {
                        thread_worker(wait_for_jobs, tasks, terminate);
                    })
                    .expect("failed to spawn thread")
            })
            .collect();
        Self {
            _threads: threads,
            wait_for_jobs_condvar: wait_for_jobs,
            tasks,
            terminate,
        }
    }

    pub fn submit(
        &self,
        group_id: GroupId,
        mode: OperationMode,
        task: impl FnOnce() + Send + 'static,
    ) {
        let mut guard = self.tasks.lock();
        guard.submit(
            group_id,
            mode,
            Box::new(task),
            self.wait_for_jobs_condvar.as_ref(),
        );
    }

    pub fn submit_uncontended(&self, task: impl FnOnce(SwitchToken<GroupId>) + Send + 'static) {
        let mut guard = self.tasks.lock();
        guard.submit_uncontended(task, self.tasks.clone(), self.wait_for_jobs_condvar.clone());
    }
}

struct PoolTasks<GroupId> {
    next_available_priority: TaskId,
    ready_to_run_tasks: BinaryHeap<RevQueuePair<TaskId, (Option<TaskInfo<GroupId>>, Task)>>,
    stalled_tasks: HashMap<GroupId, KeyTaskGroup>,
}

impl<GroupId: Clone + Eq + Hash + Send + 'static> PoolTasks<GroupId> {
    fn submit(&mut self, group_id: GroupId, mode: OperationMode, task: Task, condvar: &Condvar) {
        let task_id = self.next_available_priority;
        self.next_available_priority += 1;

        let PoolTasks {
            stalled_tasks: waiting_tasks,
            ready_to_run_tasks,
            ..
        } = self;

        let task_group = waiting_tasks.entry(group_id.clone()).or_default();
        task_group.stalled_tasks.push(RevQueuePair::new(
            task_id,
            (mode, StalledTask::Function(task)),
        ));
        task_group.refill_ready_to_run_tasks(&group_id, ready_to_run_tasks, condvar);
    }

    fn submit_uncontended(
        &mut self,
        task: impl FnOnce(SwitchToken<GroupId>) + Send + 'static,
        task_pool: Arc<Mutex<PoolTasks<GroupId>>>,
        wait_for_jobs: Arc<Condvar>,
    ) {
        let task_id = self.next_available_priority;
        self.next_available_priority += 1;

        let token = SwitchToken {
            task_pool,
            wait_for_jobs,
            task_id,
        };
        self.ready_to_run_tasks
            .push(RevQueuePair::new(task_id, (None, Box::new(|| task(token)))));
    }

    fn submit_switch(
        &mut self,
        group_id: GroupId,
        mode: OperationMode,
        task_id: TaskId,
        switch_condvar: Arc<Condvar>,
        condvar: &Condvar,
    ) {
        let PoolTasks {
            stalled_tasks: waiting_tasks,
            ready_to_run_tasks,
            ..
        } = self;

        let task_group = waiting_tasks.entry(group_id.clone()).or_default();
        task_group.stalled_tasks.push(RevQueuePair::new(
            task_id,
            (mode, StalledTask::Condvar(switch_condvar)),
        ));
        task_group.refill_ready_to_run_tasks(&group_id, ready_to_run_tasks, condvar);
    }

    fn get_next_task(&mut self) -> Option<(Option<TaskInfo<GroupId>>, Task)> {
        self.ready_to_run_tasks
            .pop()
            .map(|RevQueuePair(_task_id, (task_info, task))| (task_info, task))
    }

    fn complete_task(&mut self, task: &TaskInfo<GroupId>, condvar: &Condvar) {
        let PoolTasks {
            stalled_tasks,
            ready_to_run_tasks,
            ..
        } = self;

        let group = stalled_tasks
            .get_mut(&task.group_id)
            .expect("missing task group");
        group.complete_task(
            task.mode,
            &task.group_id,
            // refill ready to run tasks if possible
            ready_to_run_tasks,
            condvar,
        );
        if group.is_empty() {
            // Includes both running, queued and waiting tasks.
            stalled_tasks.remove(&task.group_id);
        }
    }
}

impl<GroupId> Default for PoolTasks<GroupId> {
    fn default() -> Self {
        Self {
            next_available_priority: Default::default(),
            ready_to_run_tasks: Default::default(),
            stalled_tasks: Default::default(),
        }
    }
}

struct TaskInfo<GroupId> {
    group_id: GroupId,
    // TODO needed only to check correctness.
    mode: OperationMode,
}

#[derive(Debug, Clone, Copy)]
struct RevQueuePair<K, V>(Reverse<K>, V);

impl<K, V> RevQueuePair<K, V> {
    fn new(key: K, val: V) -> Self {
        Self(Reverse(key), val)
    }
}

impl<K: PartialEq, V> PartialEq for RevQueuePair<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<K: Eq, V> Eq for RevQueuePair<K, V> {}

impl<K: PartialOrd, V> PartialOrd for RevQueuePair<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl<K: Ord, V> Ord for RevQueuePair<K, V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

#[derive(Default)]
struct KeyTaskGroup {
    // ready_to_run: current_mode != Some(Exclusive)
    current_mode: Option<GroupState>,
    // it is implicitely ordered by the priority
    stalled_tasks: BinaryHeap<RevQueuePair<TaskId, (OperationMode, StalledTask)>>,
}

impl KeyTaskGroup {
    fn is_empty(&self) -> bool {
        let has_running = match self.current_mode {
            Some(GroupState::Shared(count)) => count > 0, // TODO can it be 0 here?
            Some(GroupState::Exclusive) => true,
            None => false,
        };
        self.stalled_tasks.is_empty() && !has_running
    }

    fn try_get_next_runnable_task(&mut self) -> Option<(OperationMode, TaskId, Task)> {
        if let Some(&RevQueuePair(_task_id, (mode, ref _task))) = self.stalled_tasks.peek() {
            let (mode, task_id, task) = match (self.current_mode.as_ref(), mode) {
                (None, _) => {
                    // no running tasks, can run anything
                    let RevQueuePair(task_id, (_, task)) = self.stalled_tasks.pop().unwrap();
                    self.current_mode = Some(match mode {
                        OperationMode::Shared => GroupState::Shared(1),
                        OperationMode::Exclusive => GroupState::Exclusive,
                    });
                    (mode, task_id.0, task)
                }
                (Some(GroupState::Shared(count)), OperationMode::Shared) => {
                    // can run another shared task
                    let RevQueuePair(task_id, (_, task)) = self.stalled_tasks.pop().unwrap();
                    self.current_mode = Some(GroupState::Shared(count + 1));
                    (mode, task_id.0, task)
                }
                _ => {
                    // cannot run the next task
                    return None;
                }
            };
            match task {
                StalledTask::Function(fn_once) => Some((mode, task_id, fn_once)),
                StalledTask::Condvar(condvar) => {
                    condvar.notify_all();
                    // We've notified the parked thread and it handles both mode and task_id.
                    None
                }
            }
        } else {
            None
        }
    }

    fn complete_task<GroupId: Clone>(
        &mut self,
        complete_task_operation_mode: OperationMode,
        group_id: &GroupId,
        ready_to_run_tasks: &mut BinaryHeap<
            RevQueuePair<TaskId, (Option<TaskInfo<GroupId>>, Task)>,
        >,
        condvar: &Condvar,
    ) {
        match self.current_mode.as_mut() {
            Some(GroupState::Shared(count)) => {
                assert_eq!(complete_task_operation_mode, OperationMode::Shared);
                *count = count.checked_sub(1).expect("shared task count underflow");
                if *count == 0 {
                    self.current_mode = None;
                    self.refill_ready_to_run_tasks(group_id, ready_to_run_tasks, condvar);
                }
            }
            Some(GroupState::Exclusive) => {
                assert_eq!(complete_task_operation_mode, OperationMode::Exclusive);
                self.current_mode = None;
                self.refill_ready_to_run_tasks(group_id, ready_to_run_tasks, condvar);
            }
            None => {
                panic!("task_is_complete called when no tasks are running");
            }
        }
    }

    fn refill_ready_to_run_tasks<GroupId: Clone>(
        &mut self,
        group_id: &GroupId,
        ready_to_run_tasks: &mut BinaryHeap<
            RevQueuePair<TaskId, (Option<TaskInfo<GroupId>>, Task)>,
        >,
        condvar: &Condvar,
    ) {
        // the state is checked and updated by the try_get_next_runnable_task call
        while let Some((mode, task_id, task)) = self.try_get_next_runnable_task() {
            let task_info = TaskInfo::<GroupId> {
                group_id: group_id.clone(),
                mode,
            };
            ready_to_run_tasks.push(RevQueuePair::new(task_id, (Some(task_info), task)));
            condvar.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_shared() {
        let condvar = Condvar::new();
        let mut pool_tasks = PoolTasks::default();

        pool_tasks.submit(1, OperationMode::Shared, Box::new(|| {}), &condvar);

        // thread A
        let job1 = pool_tasks.get_next_task();
        assert!(job1.is_some());

        pool_tasks.submit(1, OperationMode::Shared, Box::new(|| {}), &condvar);
        let job2 = pool_tasks.get_next_task();
        assert!(job2.is_some());

        // No more tasks.
        let job3 = pool_tasks.get_next_task();
        assert!(job3.is_none());
    }

    #[test]
    fn test_exclusive_exclusive() {
        let condvar = Condvar::new();
        let mut pool_tasks = PoolTasks::default();

        pool_tasks.submit(1, OperationMode::Exclusive, Box::new(|| {}), &condvar);

        // thread A
        let job1 = pool_tasks.get_next_task();
        assert!(job1.is_some());

        pool_tasks.submit(1, OperationMode::Exclusive, Box::new(|| {}), &condvar);
        let job2 = pool_tasks.get_next_task();
        assert!(job2.is_none());

        pool_tasks.complete_task(&job1.unwrap().0.unwrap(), &condvar);

        let job2_2 = pool_tasks.get_next_task();
        assert!(job2_2.is_some());

        // No more tasks.
        let job3 = pool_tasks.get_next_task();
        assert!(job3.is_none());
    }

    #[test]
    fn test_shared_exclusive() {
        let condvar = Condvar::new();
        let mut pool_tasks = PoolTasks::default();

        pool_tasks.submit(1, OperationMode::Shared, Box::new(|| {}), &condvar);

        // thread A
        let job1 = pool_tasks.get_next_task();
        assert!(job1.is_some());

        pool_tasks.submit(1, OperationMode::Exclusive, Box::new(|| {}), &condvar);
        let job2 = pool_tasks.get_next_task();
        assert!(job2.is_none());

        pool_tasks.complete_task(&job1.unwrap().0.unwrap(), &condvar);

        let job2_2 = pool_tasks.get_next_task();
        assert!(job2_2.is_some());

        // No more tasks.
        let job3 = pool_tasks.get_next_task();
        assert!(job3.is_none());
    }

    #[test]
    fn test_exclusive_shared() {
        let condvar = Condvar::new();
        let mut pool_tasks = PoolTasks::default();

        pool_tasks.submit(1, OperationMode::Exclusive, Box::new(|| {}), &condvar);

        // thread A
        let job1 = pool_tasks.get_next_task();
        assert!(job1.is_some());

        pool_tasks.submit(1, OperationMode::Shared, Box::new(|| {}), &condvar);
        let job2 = pool_tasks.get_next_task();
        assert!(job2.is_none());

        pool_tasks.complete_task(&job1.unwrap().0.unwrap(), &condvar);

        let job2_2 = pool_tasks.get_next_task();
        assert!(job2_2.is_some());

        // No more tasks.
        let job3 = pool_tasks.get_next_task();
        assert!(job3.is_none());
    }

    #[test]
    fn test_shared_exclusive_shared() {
        let condvar = Condvar::new();
        let mut pool_tasks = PoolTasks::default();

        pool_tasks.submit(1, OperationMode::Shared, Box::new(|| {}), &condvar);

        // thread A
        let job1 = pool_tasks.get_next_task();
        assert!(job1.is_some());

        pool_tasks.submit(1, OperationMode::Exclusive, Box::new(|| {}), &condvar);
        // thread B
        let job2 = pool_tasks.get_next_task();
        assert!(job2.is_none());

        pool_tasks.submit(1, OperationMode::Shared, Box::new(|| {}), &condvar);

        // thread B
        let job2_2 = pool_tasks.get_next_task();
        assert!(job2_2.is_none());
    }
}
