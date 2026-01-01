use crate::{OperationMode, Pool};
use std::{hash::Hash, panic::UnwindSafe};

pub struct AsyncPool<GroupId> {
    inner: Pool<GroupId>,
}

pub struct AsyncPoolError;

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
    ) -> Result<R, AsyncPoolError> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let async_task = move || {
            let result = task();
            let _ = sender.send(result);
        };
        self.inner.submit(group_id, mode, Box::new(async_task));
        receiver.await.map_err(|_| AsyncPoolError)
    }
}
