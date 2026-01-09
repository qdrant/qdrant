use std::hash::Hash;
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::thread::TaskCompletionGuard;
use crate::{OperationMode, PoolTasks, TaskId, TaskInfo};

pub struct SwitchTokenReal<GroupId> {
    pub(crate) task_pool: Arc<Mutex<PoolTasks<GroupId>>>,
    pub(crate) wait_for_jobs: Arc<Condvar>,
    pub(crate) task_id: TaskId,
}

pub enum SwitchToken<GroupId> {
    Real(SwitchTokenReal<GroupId>),
    Dummy,
}

impl<GroupId: Clone + Eq + Hash + Send + 'static> SwitchToken<GroupId> {
    pub fn dummy() -> Self {
        SwitchToken::Dummy
    }

    pub fn new(
        task_pool: Arc<Mutex<PoolTasks<GroupId>>>,
        wait_for_jobs: Arc<Condvar>,
        task_id: TaskId,
    ) -> Self {
        SwitchToken::Real(SwitchTokenReal {
            task_pool: task_pool,
            wait_for_jobs: wait_for_jobs,
            task_id: task_id,
        })
    }

    pub fn switch_to(&mut self, group: GroupId, mode: OperationMode) -> SwitchGuard<'_, GroupId> {
        match self {
            SwitchToken::Real(switch_token_real) => {
                let switching_condvar = Arc::new(Condvar::new());

                {
                    let mut task_pool_guard = switch_token_real.task_pool.lock();

                    task_pool_guard.submit_switch(
                        group.clone(),
                        mode,
                        switch_token_real.task_id,
                        switching_condvar.clone(),
                        &switch_token_real.wait_for_jobs,
                    );

                    // Actually, it is not needed.  However, a thread may notify incorrect state on completion.
                    // It can be solved by other means, though.
                    switching_condvar.wait(&mut task_pool_guard);
                }

                let task_guard = TaskCompletionGuard::new(
                    &switch_token_real.task_pool,
                    &switch_token_real.wait_for_jobs,
                    Some(TaskInfo {
                        group_id: group,
                        mode,
                    }),
                );

                SwitchGuard::Real(SwitchGuardReal {
                    token: switch_token_real,
                    task_guard,
                })
            }
            SwitchToken::Dummy => SwitchGuard::Dummy,
        }
    }
}

pub(crate) struct SwitchGuardReal<'env, GroupId: Clone + Eq + Hash + Send + 'static> {
    token: &'env SwitchTokenReal<GroupId>,
    task_guard: TaskCompletionGuard<'env, GroupId>,
}

pub enum SwitchGuard<'env, GroupId: Clone + Eq + Hash + Send + 'static> {
    Real(SwitchGuardReal<'env, GroupId>),
    Dummy,
}
