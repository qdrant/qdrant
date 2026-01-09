use std::hash::Hash;

use tokio::sync::oneshot;

use crate::switch::SwitchToken;
use crate::{OperationMode, Pool, Task};

pub struct AsyncPool<GroupId> {
    inner: Pool<GroupId>,
}

pub enum AsyncTaskError {
    Panicked(Box<dyn std::any::Any + Send + 'static>),
    // Can happen if the pool is terminated or dropped.
    Canceled,
}

use std::fmt;
impl fmt::Debug for AsyncTaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Panicked(_any) => f.debug_tuple("Panicked").field(&"...").finish(),
            Self::Canceled => write!(f, "Canceled"),
        }
    }
}

impl fmt::Display for AsyncTaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            AsyncTaskError::Panicked(_any) => "Pool task error: panicked",
            AsyncTaskError::Canceled => "Pool task error: cancelled",
        };
        write!(f, "{msg}")
    }
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
        task: impl FnOnce(SwitchToken<GroupId>) -> R + Send + 'static,
    ) -> Result<R, AsyncTaskError> {
        let (oneshot_receiver, oneshot_task) = async_wrap1(task);
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

fn async_wrap1<R: Send + 'static, A>(
    task: impl FnOnce(A) -> R + Send + 'static,
) -> (oneshot::Receiver<R>, impl FnOnce(A) -> () + Send + 'static) {
    let (oneshot_sender, oneshot_receiver) = oneshot::channel();
    let oneshot_task = move |a: A| {
        let result = task(a);
        let _ = oneshot_sender.send(result);
    };
    (oneshot_receiver, Box::new(oneshot_task))
}
