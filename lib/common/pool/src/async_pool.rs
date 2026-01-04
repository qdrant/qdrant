use crate::{OperationMode, Pool};
use std::hash::Hash;
use std::panic::{UnwindSafe, catch_unwind};

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
        task: impl FnOnce() -> R + Send + UnwindSafe + 'static,
    ) -> Result<R, AsyncTaskError> {
        let (oneshot_sender, oneshot_receiver) = tokio::sync::oneshot::channel();
        let oneshot_task = move || {
            let result = catch_unwind(task);
            let _ = oneshot_sender.send(result);
        };
        self.inner.submit(group_id, mode, Box::new(oneshot_task));
        match oneshot_receiver.await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(panic)) => Err(AsyncTaskError::Panicked(panic)),
            Err(_) => Err(AsyncTaskError::Canceled),
        }
    }
}
