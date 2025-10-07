use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::entry::entry_point::SegmentEntry;
use segment::segment::Segment;

use crate::proxy_segment::ProxySegment;

const DROP_SPIN_TIMEOUT: Duration = Duration::from_millis(10);
const DROP_DATA_TIMEOUT: Duration = Duration::from_secs(60 * 60);

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

    pub fn is_original(&self) -> bool {
        match self {
            LockedSegment::Original(_) => true,
            LockedSegment::Proxy(_) => false,
        }
    }

    /// Consume the LockedSegment and drop the underlying segment data.
    /// Operation fails if the segment is used by other thread for longer than `timeout`.
    pub fn drop_data(self) -> OperationResult<()> {
        match self {
            LockedSegment::Original(segment) => {
                match try_unwrap_with_timeout(segment, DROP_SPIN_TIMEOUT, DROP_DATA_TIMEOUT) {
                    Ok(raw_locked_segment) => raw_locked_segment.into_inner().drop_data(),
                    Err(locked_segment) => Err(OperationError::service_error(format!(
                        "Removing segment which is still in use: {:?}",
                        locked_segment.read().data_path(),
                    ))),
                }
            }
            LockedSegment::Proxy(proxy) => {
                match try_unwrap_with_timeout(proxy, DROP_SPIN_TIMEOUT, DROP_DATA_TIMEOUT) {
                    Ok(raw_locked_segment) => raw_locked_segment.into_inner().drop_data(),
                    Err(locked_segment) => Err(OperationError::service_error(format!(
                        "Removing proxy segment which is still in use: {:?}",
                        locked_segment.read().data_path(),
                    ))),
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
