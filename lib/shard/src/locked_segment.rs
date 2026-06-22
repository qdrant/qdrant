use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::entry::{ReadSegmentEntry, SegmentEntry, StorageSegmentEntry as _};
use segment::segment::Segment;

use crate::proxy_segment::ProxySegment;

const DROP_SPIN_TIMEOUT: Duration = Duration::from_millis(10);
const DROP_DATA_TIMEOUT: Duration = Duration::from_secs(60 * 60);
/// Short timeout for the retryable drop ([`LockedSegment::try_drop_data`]): the caller's retry
/// loop provides the real waiting, so we only absorb brief contention here instead of blocking for
/// up to [`DROP_DATA_TIMEOUT`].
const DROP_DATA_RETRY_TIMEOUT: Duration = Duration::from_millis(100);

/// Outcome of a failed [`LockedSegment::try_drop_data`].
pub enum DropDataOutcome {
    /// The segment is still in use; its data is untouched and the segment is handed back so the
    /// caller can retry later.
    StillInUse(LockedSegment, OperationError),
    /// Destroying the data itself failed; the segment was consumed and its data may be partially
    /// gone, so a retry is not possible.
    Failed(OperationError),
}

/// Object, which unifies the access to different types of segments, but still allows to
/// access the original type of the segment if it is required for more efficient operations.
#[derive(Clone, Debug)]
pub enum LockedSegment {
    Original(Arc<RwLock<Segment>>),
    Proxy(Arc<RwLock<ProxySegment>>),
}

fn try_unwrap_with_timeout<T>(
    mut arc: Arc<T>,
    spin: Duration,
    timeout: Duration,
) -> Result<T, Arc<T>> {
    let start = Instant::now();

    loop {
        arc = match Arc::try_unwrap(arc) {
            Ok(unwrapped) => return Ok(unwrapped),
            Err(arc) => arc,
        };

        if start.elapsed() >= timeout {
            return Err(arc);
        }

        sleep(spin);
    }
}

impl LockedSegment {
    pub fn new<T>(segment: T) -> Self
    where
        T: Into<LockedSegment>,
    {
        segment.into()
    }

    /// Get reference to the locked segment
    pub fn get(&self) -> &RwLock<dyn SegmentEntry> {
        match self {
            LockedSegment::Original(segment) => segment.as_ref(),
            LockedSegment::Proxy(proxy) => proxy.as_ref(),
        }
    }

    pub fn get_read(&self) -> &RwLock<dyn ReadSegmentEntry> {
        match self {
            LockedSegment::Original(segment) => segment.as_ref(),
            LockedSegment::Proxy(proxy) => proxy.as_ref(),
        }
    }

    pub fn is_original(&self) -> bool {
        match self {
            LockedSegment::Original(_) => true,
            LockedSegment::Proxy(_) => false,
        }
    }

    /// Consume the LockedSegment and drop the underlying segment data.
    /// Operation fails if the segment is used by other thread for longer than [`DROP_DATA_TIMEOUT`].
    pub fn drop_data(self) -> OperationResult<()> {
        self.drop_data_with_timeout(DROP_DATA_TIMEOUT)
            .map_err(|outcome| match outcome {
                DropDataOutcome::StillInUse(_, err) | DropDataOutcome::Failed(err) => err,
            })
    }

    /// Like [`drop_data`](Self::drop_data), but with a short timeout and the segment handed back on
    /// a [`DropDataOutcome::StillInUse`] failure so the caller can retry on a later attempt.
    pub fn try_drop_data(self) -> Result<(), DropDataOutcome> {
        self.drop_data_with_timeout(DROP_DATA_RETRY_TIMEOUT)
    }

    fn drop_data_with_timeout(self, timeout: Duration) -> Result<(), DropDataOutcome> {
        match self {
            LockedSegment::Original(segment) => {
                match try_unwrap_with_timeout(segment, DROP_SPIN_TIMEOUT, timeout) {
                    Ok(raw_locked_segment) => raw_locked_segment
                        .into_inner()
                        .drop_data()
                        .map_err(DropDataOutcome::Failed),
                    Err(arc) => {
                        let err = OperationError::service_error(format!(
                            "Removing segment which is still in use: {:?}",
                            arc.read().data_path(),
                        ));
                        Err(DropDataOutcome::StillInUse(
                            LockedSegment::Original(arc),
                            err,
                        ))
                    }
                }
            }
            LockedSegment::Proxy(proxy) => {
                match try_unwrap_with_timeout(proxy, DROP_SPIN_TIMEOUT, timeout) {
                    Ok(raw_locked_segment) => raw_locked_segment
                        .into_inner()
                        .drop_data()
                        .map_err(DropDataOutcome::Failed),
                    Err(arc) => {
                        let err = OperationError::service_error(format!(
                            "Removing proxy segment which is still in use: {:?}",
                            arc.read().data_path(),
                        ));
                        Err(DropDataOutcome::StillInUse(LockedSegment::Proxy(arc), err))
                    }
                }
            }
        }
    }
}

impl From<Segment> for LockedSegment {
    fn from(s: Segment) -> Self {
        LockedSegment::Original(Arc::new(RwLock::new(s)))
    }
}

impl From<ProxySegment> for LockedSegment {
    fn from(s: ProxySegment) -> Self {
        LockedSegment::Proxy(Arc::new(RwLock::new(s)))
    }
}
