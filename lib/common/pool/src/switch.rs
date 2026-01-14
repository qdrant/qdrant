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

    pub fn try_switch_to(
        &mut self,
        group: GroupId,
        mode: OperationMode,
    ) -> Option<SwitchGuard<'_, GroupId>> {
        match self {
            SwitchToken::Real(switch_token_real) => {
                let switching_condvar = Arc::new(Condvar::new());

                {
                    let mut task_pool_guard = switch_token_real.task_pool.lock();

                    task_pool_guard.try_submit_switch(
                        group.clone(),
                        mode,
                        switch_token_real.task_id,
                        switching_condvar.clone(),
                        &switch_token_real.wait_for_jobs,
                    )?;
                }

                let task_guard = TaskCompletionGuard::new(
                    &switch_token_real.task_pool,
                    &switch_token_real.wait_for_jobs,
                    Some(TaskInfo {
                        group_id: group,
                        mode,
                    }),
                );

                Some(SwitchGuard::Real(SwitchGuardReal {
                    token: switch_token_real,
                    task_guard,
                }))
            }
            SwitchToken::Dummy => Some(SwitchGuard::Dummy),
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

#[cfg(test)]
mod tests {
    use crate::Pool;

    use super::*;

    #[test]
    fn test_switch_1_shared() {
        let pool = Pool::<i32>::new(1);
        let mutex = Arc::new(Mutex::new(()));
        let mutex_inner = mutex.clone();
        let termination_condvar = Arc::new(Condvar::new());
        let termination_condvar_inner = termination_condvar.clone();

        let mut guard = mutex.lock();
        pool.submit_uncontended(move |mut switch_token| {
            {
                let _guard = mutex_inner.lock();
            }
            let _guard = switch_token.switch_to(1, OperationMode::Shared);
            termination_condvar_inner.notify_one();
        });

        termination_condvar.wait(&mut guard);
    }

    #[test]
    fn test_switch_1_exclusive() {
        let pool = Pool::<i32>::new(1);
        let mutex = Arc::new(Mutex::new(()));
        let mutex_inner = mutex.clone();
        let termination_condvar = Arc::new(Condvar::new());
        let termination_condvar_inner = termination_condvar.clone();

        let mut guard = mutex.lock();
        pool.submit_uncontended(move |mut switch_token| {
            {
                let _guard = mutex_inner.lock();
            }
            let _guard = switch_token.switch_to(1, OperationMode::Exclusive);
            termination_condvar_inner.notify_one();
        });

        termination_condvar.wait(&mut guard);
    }

    #[test]
    fn test_switch_2_shared() {
        let pool = Pool::<i32>::new(2);
        let mutex = Arc::new(Mutex::new(()));
        let mutex_inner = mutex.clone();
        let termination_condvar = Arc::new(Condvar::new());
        let termination_condvar_inner = termination_condvar.clone();

        let mut guard = mutex.lock();
        pool.submit_uncontended(move |mut switch_token| {
            {
                let _guard = mutex_inner.lock();
            }
            let _guard = switch_token.switch_to(1, OperationMode::Shared);
            termination_condvar_inner.notify_one();
        });

        termination_condvar.wait(&mut guard);
    }

    #[test]
    fn test_switch_2_exclusive() {
        let pool = Pool::<i32>::new(2);
        let mutex = Arc::new(Mutex::new(()));
        let mutex_inner = mutex.clone();
        let termination_condvar = Arc::new(Condvar::new());
        let termination_condvar_inner = termination_condvar.clone();

        let mut guard = mutex.lock();
        eprintln!("M: Submiting");
        pool.submit_uncontended(move |mut switch_token| {
            {
                eprintln!("C: Waiting for the lock");
                // wait for the main thread to wait on condvar.
                let _guard = mutex_inner.lock();
                eprintln!("C: Done waiting for the lock");
            }
            eprintln!("C: Switching");
            let _guard = switch_token.switch_to(1, OperationMode::Exclusive);
            eprintln!("C: Switched; notifying");
            termination_condvar_inner.notify_one();
            eprintln!("C: Terminating.")
        });
        eprintln!("M: Waiting for the condvar");
        termination_condvar.wait(&mut guard);
        eprintln!("M: Exiting");
    }
}
