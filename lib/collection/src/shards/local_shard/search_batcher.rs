//! Bounded, opt-in coalescing of compatible plain dense searches within one shard.
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use common::ambient::hw::HardwareData;
use common::ambient::{self, AmbientContext, AmbientFutureExt, Handoff};
use futures::FutureExt;
use parking_lot::Mutex;
use segment::data_types::vectors::VectorInternal;
use segment::types::ScoredPoint;
use shard::common::stopping_guard::StoppingGuard;
use shard::query::query_enum::QueryEnum;
use shard::search::{CoreSearchRequest, CoreSearchRequestBatch};
use tokio::sync::{Semaphore, oneshot};
use tokio::time::Instant;

use super::LocalShard;
use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::types::{CollectionError, CollectionResult};

type Response = CollectionResult<Vec<Vec<ScoredPoint>>>;
struct Pending {
    request: CoreSearchRequest,
    deadline: Instant,
    handoff: Handoff,
    response: oneshot::Sender<Response>,
}

/// State owned by one shard; no background task borrows its LocalShard.
pub(super) struct SearchBatcher {
    queue: Mutex<VecDeque<Pending>>,
    slots: Arc<Semaphore>,
    segments: shard::segment_holder::locked::LockedSegmentHolder,
    config: Arc<tokio::sync::RwLock<crate::config::CollectionConfigInternal>>,
    runtime: AdaptiveSearchHandle,
    maximum: usize,
    dispatching: std::sync::atomic::AtomicUsize,
}

/// Only plain dense requests enter the queue. Traced requests retain their original scopes.
pub(super) fn eligible(request: &CoreSearchRequestBatch) -> bool {
    request.searches.len() == 1
        && request.searches[0].limit > 0
        && request.searches[0]
            .params
            .as_ref()
            .is_none_or(|p| !p.exact && !p.quantization.is_some_and(|q| q.ignore))
        && matches!(&request.searches[0].query, QueryEnum::Nearest(q) if matches!(&q.query, VectorInternal::Dense(_)))
        && !ambient::current()
            .context()
            .is_some_and(AmbientContext::is_traced)
}

/// Check all execution-affecting fields except the dense query values themselves.
fn compatible(a: &CoreSearchRequest, b: &CoreSearchRequest) -> bool {
    let dimensions = |r: &CoreSearchRequest| match &r.query {
        QueryEnum::Nearest(q) => match &q.query {
            VectorInternal::Dense(v) => Some(v.len()),
            VectorInternal::Sparse(_) | VectorInternal::MultiDense(_) => None,
        },
        QueryEnum::RecommendBestScore(_)
        | QueryEnum::RecommendSumScores(_)
        | QueryEnum::Discover(_)
        | QueryEnum::Context(_)
        | QueryEnum::FeedbackNaive(_) => None,
    };
    a.query.get_vector_name() == b.query.get_vector_name()
        && dimensions(a) == dimensions(b)
        && a.filter == b.filter
        && a.params == b.params
        && a.limit == b.limit
        && a.offset == b.offset
        && a.with_payload == b.with_payload
        && a.with_vector == b.with_vector
        && a.score_threshold == b.score_threshold
}

impl LocalShard {
    /// Enqueue an eligible B1 request, retaining its own deadline and response channel.
    pub(super) async fn coalesced_search(
        &self,
        request: Arc<CoreSearchRequestBatch>,
        runtime: AdaptiveSearchHandle,
        timeout: Duration,
    ) -> Option<Response> {
        let deadline = Instant::now() + timeout;
        let config = match tokio::time::timeout_at(deadline, self.collection_config.read()).await {
            Ok(config) => config,
            Err(_) => {
                return Some(Err(CollectionError::timeout(
                    timeout,
                    "dynamic search configuration",
                )));
            }
        };
        let vector = config
            .params
            .vectors
            .get_params(request.searches[0].query.get_vector_name())?;
        if !matches!(
            vector
                .quantization_config
                .as_ref()
                .or(config.quantization_config.as_ref()),
            Some(segment::types::QuantizationConfig::Scalar(_))
        ) {
            return None;
        }
        let m = vector
            .hnsw_config
            .as_ref()
            .and_then(|h| h.m)
            .unwrap_or(config.hnsw_config.m);
        if m != 0 || vector.multivector_config.is_some() {
            return None;
        }
        drop(config);
        let batcher = self
            .search_batcher
            .get_or_init(|| {
                Arc::new(SearchBatcher {
                    queue: Mutex::new(VecDeque::new()),
                    slots: Arc::new(Semaphore::new(
                        self.shared_storage_config.search_thread_count.max(1),
                    )),
                    segments: self.segments.clone(),
                    config: self.collection_config.clone(),
                    runtime,
                    maximum: self.shared_storage_config.dynamic_search_batch_max.min(64),
                    dispatching: std::sync::atomic::AtomicUsize::new(0),
                })
            })
            .clone();
        let (tx, rx) = oneshot::channel();
        {
            let mut queue = batcher.queue.lock();
            // Overload falls back to the ordinary path instead of growing this queue.
            if queue.len() >= 1024 {
                return None;
            }
            queue.push_back(Pending {
                request: request.searches[0].clone(),
                deadline,
                handoff: ambient::current(),
                response: tx,
            });
        }
        batcher.kick();
        Some(wait_response(rx, deadline, timeout).await)
    }
}

impl SearchBatcher {
    /// Reserve bounded execution slots; yielding permits opportunistic grouping without a timer.
    fn kick(self: &Arc<Self>) {
        loop {
            let Ok(permit) = self.slots.clone().try_acquire_owned() else {
                break;
            };
            if self.queue.lock().is_empty() {
                break;
            }
            self.dispatching
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let this = self.clone();
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                let entries = this.take();
                if !entries.is_empty() {
                    this.execute(entries).await;
                }
                drop(permit);
                this.kick();
            });
        }
    }

    /// Remove canceled/expired requests and select a FIFO-compatible group.
    fn take(&self) -> Vec<Pending> {
        let mut queue = self.queue.lock();
        let available = self
            .dispatching
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed)
            .max(1);
        take_pending(&mut queue, self.maximum, available)
    }

    /// Execute under a separate measured scope, preserving individual cancellation and accounting.
    async fn execute(&self, entries: Vec<Pending>) {
        log::debug!(
            "dynamic search execution: {} compatible B1 requests",
            entries.len()
        );
        let request = Arc::new(CoreSearchRequestBatch {
            searches: entries.iter().map(|p| p.request.clone()).collect(),
        });
        let deadline = entries.iter().map(|p| p.deadline).max().unwrap();
        let timeout = deadline.saturating_duration_since(Instant::now());
        // Share the collection drain directly: timed-out blocking tasks can still
        // charge it after the batch has returned. Split only request counters below.
        let measured = entries
            .iter()
            .find_map(|p| p.handoff.measured())
            .map(|ctx| ctx.root_with_shared_collection("dynamic_search_batch"))
            .unwrap_or_else(|| AmbientContext::root("dynamic_search_batch", None, None));
        let guard = StoppingGuard::new();
        let search = LocalShard::search_owned(
            self.segments.clone(),
            self.config.clone(),
            request,
            &self.runtime,
            timeout,
            &guard,
        )
        .measured(measured.clone());
        let search = AssertUnwindSafe(search).catch_unwind();
        tokio::pin!(search);
        let mut poll = tokio::time::interval(Duration::from_millis(1));
        let result = loop {
            tokio::select! {
                result = &mut search => break match result {
                    Ok(result) => result,
                    Err(_) => Err(CollectionError::service_error("dynamic search panicked")),
                },
                _ = poll.tick() => {
                    if entries.iter().all(|p| p.response.is_closed() || p.deadline <= Instant::now()) {
                        // Stop segment work, then await it so already-used resources are
                        // still attributed before releasing this execution slot.
                        guard.get_is_stopped().store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        };
        let count = entries.len();
        let usage = measured.hw_data();
        let result = result.and_then(|rows| {
            if rows.len() == count {
                Ok(rows)
            } else {
                Err(CollectionError::service_error(
                    "dynamic search result count mismatch",
                ))
            }
        });
        let mut rows = result.as_ref().ok().map(|r| r.iter().cloned());
        for (index, p) in entries.into_iter().enumerate() {
            // Split each metric exactly, including remainders; no leader is charged twice.
            if let Some(ctx) = p.handoff.measured() {
                ctx.accumulate_request(usage_share(usage, count, index));
            }
            let response = match &result {
                Ok(_) => Ok(vec![rows.as_mut().unwrap().next().unwrap()]),
                Err(error) => Err(error.clone()),
            };
            if p.deadline > Instant::now() {
                let _ = p.response.send(response);
            }
        }
    }
}

/// Preserve timeout classification when expiration removes a queued response sender.
async fn wait_response(
    rx: oneshot::Receiver<Response>,
    deadline: Instant,
    timeout: Duration,
) -> Response {
    match tokio::time::timeout_at(deadline, rx).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) if Instant::now() >= deadline => {
            Err(CollectionError::timeout(timeout, "dynamic search"))
        }
        Ok(Err(_)) => Err(CollectionError::service_error(
            "dynamic search worker exited",
        )),
        Err(_) => Err(CollectionError::timeout(timeout, "dynamic search")),
    }
}

/// Select live compatible requests without letting a canceled head block the queue.
fn take_pending(queue: &mut VecDeque<Pending>, maximum: usize, available: usize) -> Vec<Pending> {
    let now = Instant::now();
    queue.retain(|p| !p.response.is_closed() && p.deadline > now);
    let Some(first) = queue.pop_front() else {
        return vec![];
    };
    let compatible_count = 1 + queue
        .iter()
        .filter(|p| compatible(&first.request, &p.request))
        .count();
    let maximum = if compatible_count < 16 || maximum == 1 {
        1
    } else {
        (compatible_count / available / 16 * 16)
            .max(16)
            .min(maximum)
    };
    let mut entries = vec![first];
    let mut rest = VecDeque::new();
    while let Some(p) = queue.pop_front() {
        if entries.len() < maximum && compatible(&entries[0].request, &p.request) {
            entries.push(p);
        } else {
            rest.push_back(p);
        }
    }
    *queue = rest;
    entries
}

/// Allocate integer metric remainders without changing the aggregate collection usage.
fn usage_share(usage: HardwareData, count: usize, index: usize) -> HardwareData {
    HardwareData::from_fn(|metric| {
        usage[metric] / count + usize::from(index < usage[metric] % count)
    })
}

#[cfg(test)]
mod tests {
    use segment::data_types::vectors::NamedQuery;
    use segment::types::SearchParams;

    use super::*;

    /// Build an otherwise compatible plain nearest request with distinct vector values.
    fn request(vector: Vec<f32>) -> CoreSearchRequest {
        CoreSearchRequest {
            query: QueryEnum::Nearest(NamedQuery::default_dense(vector)),
            filter: None,
            params: Some(SearchParams {
                exact: false,
                ..Default::default()
            }),
            limit: 10,
            offset: 0,
            with_payload: None,
            with_vector: None,
            score_threshold: None,
        }
    }

    /// Compatible query values may differ; execution parameters and dimensions must agree.
    #[test]
    fn grouping_preserves_execution_parameters() {
        let a = request(vec![1.0, 2.0]);
        assert!(compatible(&a, &request(vec![2.0, 3.0])));
        assert!(!compatible(&a, &request(vec![1.0])));
        let mut b = a.clone();
        b.limit = 5;
        assert!(!compatible(&a, &b));
        let mut b = a.clone();
        b.offset = 1;
        assert!(!compatible(&a, &b));
        let mut b = a.clone();
        b.params.as_mut().unwrap().exact = true;
        assert!(!compatible(&a, &b));
        let mut b = a.clone();
        b.score_threshold = Some(0.1);
        assert!(!compatible(&a, &b));
        let mut b = a.clone();
        b.with_payload = Some(segment::types::WithPayloadInterface::Bool(true));
        assert!(!compatible(&a, &b));
    }

    /// Splitting metrics must preserve the exact aggregate, including integer remainders.
    #[test]
    fn accounting_partition_preserves_aggregate() {
        for count in [1, 2, 16, 32, 64] {
            for usage in [0, 1, 7, 128, 10001] {
                let shares: Vec<_> = (0..count)
                    .map(|i| {
                        usage_share(HardwareData::from_fn(|_| usage), count, i)
                            [common::ambient::hw::HwMetric::Cpu]
                    })
                    .collect();
                assert_eq!(shares.iter().sum::<usize>(), usage);
                assert!(shares.iter().max().unwrap() - shares.iter().min().unwrap() <= 1);
            }
        }
    }
    /// Expired/canceled entries are removed and incompatible work retains FIFO order.
    #[test]
    fn queue_removes_abandoned_requests_and_preserves_groups() {
        let mut queue = VecDeque::new();
        let mut receivers = Vec::new();
        for index in 0..35 {
            let (tx, rx) = oneshot::channel();
            let mut r = request(vec![index as f32, 1.0]);
            if index == 2 {
                r.limit = 7;
            }
            queue.push_back(Pending {
                request: r,
                deadline: Instant::now()
                    + if index == 1 {
                        Duration::ZERO
                    } else {
                        Duration::from_secs(60)
                    },
                handoff: Handoff::Unmeasured(None),
                response: tx,
            });
            receivers.push(rx);
        }
        drop(receivers.remove(0));
        let group = take_pending(&mut queue, 64, 1);
        assert_eq!(group.len(), 1); // first live request uses the other limit
        assert_eq!(group[0].request.limit, 7);
        let group = take_pending(&mut queue, 64, 1);
        assert_eq!(group.len(), 32);
        assert!(group.iter().all(|p| p.request.limit == 10));
        assert!(queue.is_empty());
    }

    /// Small demand and the control mode remain individual requests.
    #[test]
    fn queue_control_and_low_demand_do_not_batch() {
        for (count, maximum, available, expected) in [
            (15, 64, 3, 1),
            (32, 1, 3, 1),
            (64, 64, 3, 16),
            (128, 64, 1, 64),
        ] {
            let mut receivers = Vec::new();
            let mut queue = VecDeque::new();
            for _ in 0..count {
                let (tx, rx) = oneshot::channel();
                receivers.push(rx);
                queue.push_back(Pending {
                    request: request(vec![1.0, 2.0]),
                    deadline: Instant::now() + Duration::from_secs(60),
                    handoff: Handoff::Unmeasured(None),
                    response: tx,
                });
            }
            assert_eq!(take_pending(&mut queue, maximum, available).len(), expected);
            assert_eq!(queue.len(), count - expected);
        }
    }
    /// Explicit exact/ignored-quantization and externally batched calls bypass coalescing.
    #[test]
    fn eligibility_preserves_exact_and_batch_requests() {
        let _scope = ambient::test_guard();
        let mut batch = CoreSearchRequestBatch {
            searches: vec![request(vec![1.0, 2.0])],
        };
        assert!(eligible(&batch));
        batch.searches[0].params.as_mut().unwrap().exact = true;
        assert!(!eligible(&batch));
        batch.searches[0].params.as_mut().unwrap().exact = false;
        batch.searches[0].params.as_mut().unwrap().quantization =
            Some(segment::types::QuantizationSearchParams {
                ignore: true,
                ..Default::default()
            });
        assert!(!eligible(&batch));
        batch.searches[0].params = None;
        assert!(eligible(&batch));
        batch.searches.push(batch.searches[0].clone());
        assert!(!eligible(&batch));
        batch.searches.clear();
        assert!(!eligible(&batch));
    }
    /// An expired queued request reports a timeout even if its sender has already gone away.
    #[tokio::test]
    async fn expired_sender_reports_timeout() {
        let (tx, rx) = oneshot::channel();
        drop(tx);
        let result = wait_response(rx, Instant::now(), Duration::from_secs(1)).await;
        assert!(matches!(result, Err(CollectionError::Timeout { .. })));
        let (tx, rx) = oneshot::channel();
        drop(tx);
        let result = wait_response(
            rx,
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .await;
        assert!(matches!(result, Err(CollectionError::ServiceError { .. })));
    }
}
