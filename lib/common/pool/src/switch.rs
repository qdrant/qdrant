use std::{hash::Hash, sync::Arc};

use parking_lot::{Condvar, Mutex};

use crate::{OperationMode, PoolTasks, TaskId, TaskInfo, thread::TaskCompletionGuard};

pub struct SwitchToken<GroupId> {
    pub(crate) task_pool: Arc<Mutex<PoolTasks<GroupId>>>,
    pub(crate) wait_for_jobs: Arc<Condvar>,
    pub(crate) task_id: TaskId,
}

impl<GroupId: Clone + Eq + Hash + Send + 'static> SwitchToken<GroupId> {
    pub fn switch_to(&mut self, group: GroupId, mode: OperationMode) -> SwitchGuard<'_, GroupId> {
        let switching_condvar = Arc::new(Condvar::new());

        {
            let mut task_pool_guard = self.task_pool.lock();

            task_pool_guard.submit_switch(
                group.clone(),
                mode,
                self.task_id,
                switching_condvar.clone(),
                &self.wait_for_jobs,
            );

            // Actually, it is not needed.  However, a thread may notify incorrect state on completion.
            // It can be solved by other means, though.
            switching_condvar.wait(&mut task_pool_guard);
        }

        let task_guard = TaskCompletionGuard::new(
            &self.task_pool,
            &self.wait_for_jobs,
            Some(TaskInfo {
                group_id: group,
                mode,
            }),
        );

        SwitchGuard {
            token: self,
            task_guard,
        }
    }
}

pub struct SwitchGuard<'env, GroupId: Clone + Eq + Hash + Send + 'static> {
    token: &'env SwitchToken<GroupId>,
    task_guard: TaskCompletionGuard<'env, GroupId>,
}
