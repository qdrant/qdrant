use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::content_manager::toc::TableOfContent;
use tokio::sync::broadcast;
use tokio::{runtime, sync, task};

pub struct Ready {
    toc: Arc<TableOfContent>,
    consensus_state: ConsensusStateRef,
    runtime: runtime::Handle,
    ready: Arc<AtomicBool>,
    task: sync::RwLock<Option<task::JoinHandle<()>>>,
    check_consensus_ready: broadcast::Sender<()>,
}

impl Ready {
    pub fn new(
        toc: Arc<TableOfContent>,
        consensus_state: ConsensusStateRef,
        runtime: runtime::Handle,
    ) -> Self {
        let (check_consensus_ready, _) = broadcast::channel(1);

        Self {
            toc,
            consensus_state,
            runtime,
            ready: Default::default(),
            task: Default::default(),
            check_consensus_ready,
        }
    }

    pub async fn check_ready(&self) -> bool {
        let is_ready = self.is_ready();

        if !is_ready {
            self.notify_task().await;
        } else {
            self.cleanup_task().await;
        }

        is_ready
    }

    fn is_ready(&self) -> bool {
        self.ready.load(atomic::Ordering::Relaxed)
    }

    async fn notify_task(&self) {
        let Err(err) = self.check_consensus_ready.send(()) else {
            return;
        };

        if self.is_ready() {
            return;
        }

        log::warn!("Ready::notify_task: failed to send message to ReadyTask: {err}");
        self.spawn_task().await;
    }

    async fn spawn_task(&self) {
        if self.is_running().await {
            log::warn!("Ready::spawn_task: ReadyTask is already running");
            return;
        }

        let Some(mut task) = self.try_cleanup_task().await else {
            return;
        };

        if self.is_ready() {
            log::error!("TODO");
            return;
        }

        *task = Some(self.runtime.spawn(self.task().exec()));
    }

    async fn cleanup_task(&self) {
        let (is_running, is_spawned) = self.state().await;

        if is_running || !is_spawned {
            if is_running {
                log::warn!("Ready::cleanup_task: ReadyTask is still running");
            }

            return;
        }

        self.try_cleanup_task().await;
    }

    async fn try_cleanup_task(
        &self,
    ) -> Option<sync::RwLockWriteGuard<'_, Option<task::JoinHandle<()>>>> {
        let mut task = self.task.write().await;

        if is_task_running(task.as_ref()) {
            log::debug!("Ready::try_cleanup_task: ReadyTask is running");
            return None;
        }

        let result = match task.take() {
            Some(task) => task.await,
            None => Ok(()),
        };

        if let Err(err) = result {
            log::error!("ReadyTask failed: {err}");
        }

        Some(task)
    }

    async fn state(&self) -> (bool, bool) {
        task_state(self.task.read().await.as_ref())
    }

    async fn is_running(&self) -> bool {
        is_task_running(self.task.read().await.as_ref())
    }

    fn task(&self) -> Task {
        Task {
            ready: self.ready.clone(),
            toc: self.toc.clone(),
            consensus_state: self.consensus_state.clone(),
            check_consensus_ready: self.check_consensus_ready.subscribe(),
        }
    }
}

fn task_state<T>(task: Option<&task::JoinHandle<T>>) -> (bool, bool) {
    (is_task_running(task), is_task_spawned(task))
}

fn is_task_running<T>(task: Option<&task::JoinHandle<T>>) -> bool {
    task.map_or(false, |task| task.is_finished())
}

fn is_task_spawned<T>(task: Option<&task::JoinHandle<T>>) -> bool {
    task.is_some()
}

pub struct Task {
    ready: Arc<AtomicBool>,
    toc: Arc<TableOfContent>,
    consensus_state: ConsensusStateRef,
    check_consensus_ready: broadcast::Receiver<()>,
}

impl Task {
    pub async fn exec(mut self) {
        let transport_channel_pool = self.toc.get_channel_service().channel_pool.clone();
        let peer_address_by_id = self.consensus_state.peer_address_by_id();

        // TODO: Implement readiness check (75% implemented in previous iteration)
        todo!()
    }
}
