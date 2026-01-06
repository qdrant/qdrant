use std::hash::Hash;

use tokio::sync::oneshot;

use crate::{OperationMode, Pool, Task};

pub struct AsyncPool<GroupId> {
    inner: Pool<GroupId>,
}

pub enum AsyncTaskError {
    Panicked(Box<dyn std::any::Any + Send + 'static>),
    // Can happen if the pool is terminated or dropped.
    Canceled,
}

impl<GroupId> AsyncPool<GroupId>
where
    GroupId: Clone + Hash + Eq + Send + 'static,
{
    pub fn new(threads: usize) -> Self {
        Self {
            inner: Pool::new(threads),
        }
    }

    pub async fn submit<R: Send + 'static>(
        &self,
        group_id: GroupId,
        mode: OperationMode,
        task: impl FnOnce() -> R + Send + 'static,
    ) -> Result<R, AsyncTaskError> {
        let (oneshot_receiver, oneshot_task) = async_wrap(task);
        self.inner.submit(group_id, mode, oneshot_task);
        match oneshot_receiver.await {
            Ok(value) => Ok(value),
            Err(_) => Err(AsyncTaskError::Canceled),
        }
    }

    pub async fn submit_uncontended<R: Send + 'static>(
        &self,
        task: impl FnOnce() -> R + Send + 'static,
    ) -> Result<R, AsyncTaskError> {
        let (oneshot_receiver, oneshot_task) = async_wrap(task);
        self.inner.submit_uncontended(oneshot_task);
        match oneshot_receiver.await {
            Ok(value) => Ok(value),
            Err(_) => Err(AsyncTaskError::Canceled),
        }
    }
}

fn async_wrap<R: Send + 'static>(
    task: impl FnOnce() -> R + Send + 'static,
) -> (oneshot::Receiver<R>, Task) {
    let (oneshot_sender, oneshot_receiver) = oneshot::channel();
    let oneshot_task = move || {
        let result = task();
        let _ = oneshot_sender.send(result);
    };
    (oneshot_receiver, Box::new(oneshot_task))
}
