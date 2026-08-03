use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// How long a measurement below its limit stays usable, and so how long a
/// resource that just filled up takes to start rejecting updates. Dropping back
/// under the limit takes effect immediately instead — see [`reusable`].
const MEASUREMENT_TTL: Duration = Duration::from_secs(5);

/// Cache of one resource's last measurement, holding the freshness policy
/// [`super::QuotaManager`] applies to every reading.
///
/// Measuring is not free — resident memory advances the jemalloc stats epoch,
/// disk usage costs two `statvfs` calls — and every update checks its quota, so
/// a measurement taken below the limit is reused for [`MEASUREMENT_TTL`].
pub struct Meter<T> {
    last: Mutex<Option<Sample<T>>>,
}

impl<T> Default for Meter<T> {
    fn default() -> Self {
        Meter {
            last: Mutex::new(None),
        }
    }
}

impl<T: Copy> Meter<T> {
    /// The current measurement, taken with `measure` or served from the last one
    /// while `is_reusable` accepts it — see [`reusable`], what every caller passes.
    pub fn measure(&self, is_reusable: impl FnOnce(T) -> bool, measure: impl FnOnce() -> T) -> T {
        let now = Instant::now();

        // Held across `measure` on purpose: under a burst one request should take
        // the syscall and the rest reuse it, not all measure in parallel.
        let mut last = self.last.lock();

        if let Some(sample) = *last
            && now.duration_since(sample.taken_at) < MEASUREMENT_TTL
            && is_reusable(sample.value)
        {
            return sample.value;
        }

        let value = measure();
        *last = Some(Sample {
            value,
            taken_at: now,
        });

        value
    }
}

#[derive(Debug, Clone, Copy)]
struct Sample<T> {
    value: T,
    taken_at: Instant,
}

/// Whether a measurement of `percent` may be reused for a check against `limit`.
///
/// A measurement at or above the limit is never reused: it is rejecting updates,
/// and the clients it rejects retry, so freeing the resource must let them
/// through right away rather than once the TTL runs out.
pub fn reusable(percent: Option<u8>, limit: Option<u8>) -> bool {
    match (percent, limit) {
        (Some(percent), Some(limit)) => percent < limit,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    fn below(limit: u8) -> impl Fn(Option<u8>) -> bool {
        move |percent| reusable(percent, Some(limit))
    }

    #[test]
    fn a_reading_below_the_limit_is_reused() {
        let meter = Meter::default();
        let measurements = Cell::new(0);
        let measure = || {
            measurements.set(measurements.get() + 1);
            Some(50)
        };

        assert_eq!(meter.measure(below(90), measure), Some(50));
        assert_eq!(meter.measure(below(90), measure), Some(50));
        assert_eq!(measurements.get(), 1, "second check should not measure");

        // Unavailable stats are cached too, so a platform without the stat does
        // not pay for a failing read on every request
        let meter = Meter::default();
        assert_eq!(meter.measure(below(90), || None), None);
        assert_eq!(meter.measure(below(90), || unreachable!()), None);
    }

    #[test]
    fn a_reading_at_its_limit_is_measured_again() {
        let meter = Meter::default();
        assert_eq!(meter.measure(below(90), || Some(90)), Some(90));

        // Over the limit the resource is rejecting updates, so freeing it must
        // take effect on the next request instead of after the TTL
        assert_eq!(meter.measure(below(90), || Some(10)), Some(10));
        assert_eq!(meter.measure(below(90), || unreachable!()), Some(10));

        // Judged against the limit of the current call, not the one it was taken
        // for: the same reading is a rejection under a stricter limit
        assert_eq!(meter.measure(below(5), || Some(45)), Some(45));
        // ... and reads with no limit reuse whatever is fresh
        assert_eq!(
            meter.measure(|percent| reusable(percent, None), || unreachable!()),
            Some(45),
        );
    }
}
