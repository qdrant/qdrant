use std::future::{self, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::watch;

#[derive(Clone, Debug)]
pub struct UpdateTracker {
    update_operations: Arc<AtomicUsize>,
    update_notifier: Arc<watch::Sender<()>>,
}

impl Default for UpdateTracker {
    fn default() -> Self {
        let (update_notifier, _) = watch::channel(());

        Self {
            update_operations: Default::default(),
            update_notifier: Arc::new(update_notifier),
        }
    }
}

impl UpdateTracker {
    pub fn is_update_in_progress(&self) -> bool {
        self.update_operations.load(Ordering::Relaxed) > 0
    }

    pub fn watch_for_update(&self) -> impl Future<Output = ()> {
        let mut update_subscriber = self.update_notifier.subscribe();

        async move {
            match update_subscriber.changed().await {
                Ok(()) => (),
                Err(_) => future::pending().await,
            }
        }
    }

    pub fn update(&self) -> UpdateGuard {
        if self.update_operations.fetch_add(1, Ordering::Relaxed) == 0 {
            self.update_notifier.send_replace(());
        }

        UpdateGuard::new(self.update_operations.clone())
    }
}

#[derive(Debug)]
pub struct UpdateGuard {
    update_operations: Arc<AtomicUsize>,
}

impl UpdateGuard {
    fn new(update_operations: Arc<AtomicUsize>) -> Self {
        Self { update_operations }
    }
}

impl Drop for UpdateGuard {
    fn drop(&mut self) {
        self.update_operations.fetch_sub(1, Ordering::Relaxed);
    }
}
