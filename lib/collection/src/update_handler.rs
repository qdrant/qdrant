use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::collection_manager::holders::segment_holder::LockedSegmentHolder;
use crate::collection_manager::optimizers::optimize::optimize;
use crate::collection_manager::optimizers::segment_optimizer::SegmentOptimizer;
use crate::operations::types::CollectionResult;
use crate::operations::CollectionUpdateOperations;
use crate::wal::SerdeWal;
use async_channel::{Receiver, Sender};
use itertools::Itertools;
use log::{debug, info};
use segment::types::SeqNumberType;
use std::cmp::min;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

pub type Optimizer = dyn SegmentOptimizer + Sync + Send;

const RECOVER_BATCH: usize = 100;

/// Information, required to perform operation and notify regarding the result
pub struct OperationData {
    /// Sequential number of the operation
    pub op_num: SeqNumberType,
    /// Operation
    pub operation: CollectionUpdateOperations,
    /// Callback notification channel
    pub sender: Option<Sender<CollectionResult<usize>>>,
}

/// Signal, used to inform Updater process
pub enum UpdateSignal {
    /// Requested operation to perform
    Operation(OperationData),
    /// Stop all optimizers and listening
    Stop,
    /// Empty signal used to trigger optimizers
    Nop,
}

/// Signal, used to inform Optimization process
pub enum OptimizerSignal {
    /// Sequential number of the operation
    Operation(SeqNumberType),
    /// Stop all optimizers and listening
    Stop,
    /// Empty signal used to trigger optimizers
    Nop,
}

/// Structure, which holds object, required for processing updates of the collection
pub struct UpdateHandler {
    /// List of used optimizers
    pub optimizers: Arc<Vec<Arc<Optimizer>>>,
    /// How frequent can we flush data
    pub flush_timeout_sec: u64,
    segments: LockedSegmentHolder,
    /// Channel receiver, which is listened by the updater process
    update_receiver: Receiver<UpdateSignal>,
    /// Process, that listens updates signals and perform updates
    update_worker: Option<JoinHandle<()>>,
    /// Process, that listens for post-update signals and performs optimization
    optimizer_worker: Option<JoinHandle<()>>,
    runtime_handle: Handle,
    /// WAL, required for operations
    wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    optimization_handles: Arc<Mutex<Vec<JoinHandle<CollectionResult<bool>>>>>,
}

impl UpdateHandler {
    pub fn new(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        update_receiver: Receiver<UpdateSignal>,
        runtime_handle: Handle,
        segments: LockedSegmentHolder,
        wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
        flush_timeout_sec: u64,
    ) -> UpdateHandler {
        let mut handler = UpdateHandler {
            optimizers,
            segments,
            update_receiver,
            update_worker: None,
            optimizer_worker: None,
            runtime_handle,
            wal,
            flush_timeout_sec,
            optimization_handles: Arc::new(Mutex::new(vec![])),
        };
        handler.run_workers();
        handler
    }

    pub fn run_workers(&mut self) {
        let (tx, rx) = async_channel::unbounded();
        self.optimizer_worker = Some(self.runtime_handle.spawn(Self::optimization_worker_fn(
            self.optimizers.clone(),
            rx,
            self.segments.clone(),
            self.wal.clone(),
            self.flush_timeout_sec,
            self.optimization_handles.clone(),
        )));
        self.update_worker = Some(self.runtime_handle.spawn(Self::update_worker_fn(
            self.update_receiver.clone(),
            tx,
            self.segments.clone(),
        )));
    }

    /// Gracefully wait before all optimizations stop
    /// If some optimization is in progress - it will be finished before shutdown.
    /// Blocking function.
    pub async fn wait_workers_stops(&mut self) -> CollectionResult<()> {
        for handle in self.optimization_handles.lock().await.iter() {
            handle.abort();
        }
        let maybe_handle = self.update_worker.take();
        if let Some(handle) = maybe_handle {
            handle.await?;
        }
        let maybe_handle = self.optimizer_worker.take();
        if let Some(handle) = maybe_handle {
            handle.await?;
        }
        Ok(())
    }

    /// Checks if there are any failed operations.
    /// If so - attempts to re-apply all failed operations.
    async fn try_recover(
        segments: LockedSegmentHolder,
        wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
    ) -> CollectionResult<usize> {
        // Try to re-apply everything starting from the first failed operation
        let first_failed_operation_option =
            segments.read().await.failed_operation.iter().cloned().min();
        match first_failed_operation_option {
            None => {}
            Some(first_failed_op) => {
                let mut recovered_till = first_failed_op;
                // Read operation in batch to prevent Sync error, related to
                // tokio context switch during `.await`
                loop {
                    let operations_to_recover = {
                        let wal_lock = wal.lock().await;
                        wal_lock
                            .read(recovered_till)
                            .take(RECOVER_BATCH)
                            .collect_vec()
                    };

                    if operations_to_recover.is_empty() {
                        break;
                    }

                    for (op_num, operation) in operations_to_recover {
                        CollectionUpdater::update(&segments, op_num, operation).await?;
                        recovered_till = op_num + 1;
                    }
                }
            }
        };
        Ok(0)
    }

    pub(crate) async fn process_optimization(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        segments: LockedSegmentHolder,
    ) -> Vec<JoinHandle<CollectionResult<bool>>> {
        let mut handles = vec![];
        for optimizer in optimizers.iter() {
            loop {
                let nonoptimal_segment_ids = optimizer.check_condition(segments.clone()).await;
                if nonoptimal_segment_ids.is_empty() {
                    break;
                } else {
                    let optim = optimizer.clone();
                    let segs = segments.clone();
                    let nsi = nonoptimal_segment_ids.clone();
                    // Optimize function needs to be async, cause it is working with `segments`,
                    // which are also used in heavily async context and therefore are wrapped into
                    // async RwLocks.
                    //
                    // The whole separate thread is assigned to the optimization,
                    // therefore it is OK to block the whole thread with this function.
                    //
                    // Alternative solution here would be to:
                    //
                    // - to use same tokio pool (optimization won't be limited with task thread):
                    //      ```
                    //      rt_handle = Handle::current();
                    //      ...
                    //      tokio::task::spawn_blocking( move ||
                    //          rt_handle.block_on(optimize(...)))
                    //      ```
                    // - create single thread runtime withing blocking task - looks like a big runtime creation overhead:
                    //      ```
                    //         tokio::task::spawn_blocking(move || {
                    //             tokio::runtime::Builder::new_current_thread()
                    //                 .enable_all()
                    //                 .build()
                    //                 .unwrap()
                    //                 .handle()
                    //                 .block_on(optimize(optim, segs, nsi))
                    //         });
                    //      ```
                    todo!();
                    handles.push(tokio::task::spawn_blocking(move || {
                        futures::executor::block_on(optimize(optim, segs, nsi))
                    }));
                }
            }
        }
        handles
    }

    async fn optimization_worker_fn(
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        receiver: Receiver<OptimizerSignal>,
        segments: LockedSegmentHolder,
        wal: Arc<Mutex<SerdeWal<CollectionUpdateOperations>>>,
        flush_timeout_sec: u64,
        blocking_handles: Arc<Mutex<Vec<JoinHandle<CollectionResult<bool>>>>>,
    ) {
        let flush_timeout = Duration::from_secs(flush_timeout_sec);
        let mut last_flushed = Instant::now();
        loop {
            let recv_res = receiver.recv().await;
            match recv_res {
                Ok(signal) => {
                    match signal {
                        OptimizerSignal::Nop => {
                            if Self::try_recover(segments.clone(), wal.clone())
                                .await
                                .is_err()
                            {
                                continue;
                            }
                            let mut handles = blocking_handles.lock().await;
                            handles.append(
                                &mut Self::process_optimization(
                                    optimizers.clone(),
                                    segments.clone(),
                                )
                                .await,
                            );
                        }
                        OptimizerSignal::Operation(operation_id) => {
                            if Self::try_recover(segments.clone(), wal.clone())
                                .await
                                .is_err()
                            {
                                continue;
                            }
                            {
                                let mut handles = blocking_handles.lock().await;
                                handles.append(
                                    &mut Self::process_optimization(
                                        optimizers.clone(),
                                        segments.clone(),
                                    )
                                    .await,
                                );
                            }

                            let elapsed = last_flushed.elapsed();
                            if elapsed > flush_timeout {
                                debug!("Performing flushing: {}", operation_id);
                                last_flushed = Instant::now();
                                let confirmed_version = {
                                    let read_segments = segments.read().await;
                                    let flushed_version = read_segments.flush_all().unwrap();
                                    match read_segments.failed_operation.iter().cloned().min() {
                                        None => flushed_version,
                                        Some(failed_operation) => {
                                            min(failed_operation, flushed_version)
                                        }
                                    }
                                };
                                wal.lock().await.ack(confirmed_version).unwrap();
                            }
                        }
                        OptimizerSignal::Stop => break, // Stop gracefully
                    }
                }
                Err(_) => break, // Transmitter was destroyed
            }
        }
    }

    async fn update_worker_fn(
        receiver: Receiver<UpdateSignal>,
        optimize_sender: Sender<OptimizerSignal>,
        segments: LockedSegmentHolder,
    ) {
        loop {
            let recv_res = receiver.recv().await;
            match recv_res {
                Ok(signal) => {
                    match signal {
                        UpdateSignal::Operation(OperationData {
                            op_num,
                            operation,
                            sender,
                        }) => {
                            let res = match CollectionUpdater::update(&segments, op_num, operation).await
                            {
                                Ok(update_res) => optimize_sender
                                    .send(OptimizerSignal::Operation(op_num))
                                    .await
                                    .and(Ok(update_res))
                                    .map_err(|send_err| send_err.into()),
                                Err(err) => Err(err),
                            };

                            if let Some(feedback) = sender {
                                feedback.send(res).await.unwrap_or_else(|_| {
                                    info!("Can't report operation {} result. Assume already not required", op_num);
                                });
                            };
                        }
                        UpdateSignal::Stop => {
                            optimize_sender.send(OptimizerSignal::Stop).await.unwrap_or_else(|_| {
                                debug!("Optimizer already stopped")
                            });
                            break;
                        }
                        UpdateSignal::Nop => optimize_sender.send(OptimizerSignal::Nop).await.unwrap_or_else(|_| {
                            info!("Can't notify optimizers, assume process is dead. Restart is required");
                        }),
                    }
                }
                Err(_) => {
                    optimize_sender.send(OptimizerSignal::Stop).await.unwrap_or_else(|_| {
                        debug!("Optimizer already stopped")
                    });
                    break;
                } // Transmitter was destroyed
            }
        }
    }
}
