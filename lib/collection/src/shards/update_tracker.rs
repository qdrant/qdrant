use std::future::{self, Future};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::scope_tracker::{ScopeTracker, ScopeTrackerGuard};
use tokio::sync::watch;

#[derive(Clone, Debug)]
pub struct UpdateTracker {
    update_operations: ScopeTracker,
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
    pub fn running_updates(&self) -> usize {
        self.update_operations.get(Ordering::Relaxed)
    }

    pub fn is_update_in_progress(&self) -> bool {
        self.running_updates() > 0
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

    #[must_use]
    pub fn update(&self) -> ScopeTrackerGuard {
        let (old_value, guard) = self.update_operations.get_and_measure();

        if old_value == 0 {
            self.update_notifier.send_replace(());
        }

        guard
    }
}
