use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::hash::Hash;
use std::panic::{UnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread::JoinHandle;

use parking_lot::{Condvar, Mutex};

// Defines ordering in which tasks are added.
// Tasks which added first are executed first unless something blocks them.
// Smaller task ID means higher priority.
type TaskId = usize;

// The UnwindSafe bound may be removed if we handle panics with recreating a new thread.
pub type Task = Box<dyn FnOnce() + Send + UnwindSafe + 'static>;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OperationMode {
    Shared,
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
}

impl<Id: Clone + Eq + Hash> Pool<Id> {
    pub fn submit(&self, group_id: Id, mode: OperationMode, task: Task) {
        let mut guard = self.tasks.lock();
        guard.submit(group_id, mode, task, self.wait_for_jobs_condvar.as_ref());
    }
}

struct PoolTasks<GroupId> {
    next_available_priority: TaskId,
    ready_to_run_tasks: BinaryHeap<RevQueuePair<TaskId, (TaskInfo<GroupId>, Task)>>,
    stalled_tasks: HashMap<GroupId, KeyTaskGroup>,
}

impl<GroupId: Clone + Eq + Hash> PoolTasks<GroupId> {
    fn submit(&mut self, group_id: GroupId, mode: OperationMode, task: Task, condvar: &Condvar) {
        let task_id = self.next_available_priority;
        self.next_available_priority += 1;

        let PoolTasks {
            stalled_tasks: waiting_tasks,
            ready_to_run_tasks,
            ..
        } = self;

        let task_group = waiting_tasks.entry(group_id.clone()).or_default();
        task_group.waiting_tasks.push_back((mode, task_id, task));
        task_group.refill_ready_to_run_tasks(&group_id, ready_to_run_tasks, condvar);
    }

    fn get_next_task(&mut self) -> Option<(TaskInfo<GroupId>, Task)> {
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
    waiting_tasks: VecDeque<(OperationMode, TaskId, Task)>,
}

impl KeyTaskGroup {
    fn is_empty(&self) -> bool {
        let has_running = match self.current_mode {
            Some(GroupState::Shared(count)) => count > 0, // TODO can it be 0 here?
            Some(GroupState::Exclusive) => true,
            None => false,
        };
        self.waiting_tasks.is_empty() && !has_running
    }

    fn try_get_next_runnable_task(&mut self) -> Option<(OperationMode, TaskId, Task)> {
        if let Some(&(mode, _task_id, ref _task)) = self.waiting_tasks.front() {
            match (self.current_mode.as_ref(), mode) {
                (None, _) => {
                    // no running tasks, can run anything
                    let (_, task_id, task) = self.waiting_tasks.pop_front().unwrap();
                    self.current_mode = Some(match mode {
                        OperationMode::Shared => GroupState::Shared(1),
                        OperationMode::Exclusive => GroupState::Exclusive,
                    });
                    Some((mode, task_id, task))
                }
                (Some(GroupState::Shared(count)), OperationMode::Shared) => {
                    // can run another shared task
                    let (_, task_id, task) = self.waiting_tasks.pop_front().unwrap();
                    self.current_mode = Some(GroupState::Shared(count + 1));
                    Some((mode, task_id, task))
                }
                _ => {
                    // cannot run the next task
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
        ready_to_run_tasks: &mut BinaryHeap<RevQueuePair<TaskId, (TaskInfo<GroupId>, Task)>>,
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
        ready_to_run_tasks: &mut BinaryHeap<RevQueuePair<TaskId, (TaskInfo<GroupId>, Task)>>,
        condvar: &Condvar,
    ) {
        // the state is checked and updated by the try_get_next_runnable_task call
        while let Some((mode, task_id, task)) = self.try_get_next_runnable_task() {
            let task_info = TaskInfo::<GroupId> {
                group_id: group_id.clone(),
                mode,
            };
            ready_to_run_tasks.push(RevQueuePair::new(task_id, (task_info, task)));
            condvar.notify_one();
        }
    }
}
fn thread_worker<GroupId: Eq + Hash + Clone>(
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

        pool_tasks.complete_task(&job1.unwrap().0, &condvar);

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

        pool_tasks.complete_task(&job1.unwrap().0, &condvar);

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

        pool_tasks.complete_task(&job1.unwrap().0, &condvar);

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
