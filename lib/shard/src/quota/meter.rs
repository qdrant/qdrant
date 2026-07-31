use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// How long a measurement taken below its limit stays usable.
///
/// Sets how long it takes for a resource that has just filled up to start
/// rejecting updates. Recovery in the other direction is immediate — see
/// [`reusable`].
const MEASUREMENT_TTL: Duration = Duration::from_secs(5);

/// Cache of one resource's last measurement, holding the freshness policy that
/// [`super::QuotaManager`] applies to every reading it takes.
///
/// Measuring is not free — resident memory advances the jemalloc stats epoch,
/// disk usage costs two `statvfs` calls — and updates check their quota on every
/// request, so a measurement taken while the resource sat below its limit is
/// reused for [`MEASUREMENT_TTL`] instead of repeated.
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
    /// while that is still usable.
    ///
    /// `is_reusable` decides whether the previous measurement may stand in for a
    /// fresh one — see [`reusable`], which is what every caller passes.
    pub fn measure(&self, is_reusable: impl FnOnce(T) -> bool, measure: impl FnOnce() -> T) -> T {
        let now = Instant::now();

        // Held across `measure` on purpose: when many requests arrive at once we
        // want one of them to take the syscall and the rest to reuse it, not all
        // of them to measure in parallel.
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
/// A measurement at or above the limit never is. It is rejecting updates, and
/// the clients it rejects retry — so the moment the resource is freed they have
/// to get through, rather than stay locked out for the rest of the TTL. Anything
/// below the limit, or with no limit to compare against, is fine to reuse.
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

    /// The freshness policy every [`super::super::QuotaManager`] reading uses,
    /// bound to one limit.
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
        // not pay for a failing read on every single request.
        let meter = Meter::default();
        assert_eq!(meter.measure(below(90), || None), None);
        assert_eq!(meter.measure(below(90), || unreachable!()), None);
    }

    #[test]
    fn a_reading_at_the_limit_is_measured_again() {
        let meter = Meter::default();
        assert_eq!(meter.measure(below(90), || Some(90)), Some(90));

        // Over the limit the resource is rejecting updates, so freeing it must
        // take effect on the next request instead of after the TTL.
        assert_eq!(meter.measure(below(90), || Some(10)), Some(10));
        assert_eq!(meter.measure(below(90), || unreachable!()), Some(10));
    }

    #[test]
    fn the_limit_a_reading_is_reused_for_is_the_current_one() {
        let meter = Meter::default();
        assert_eq!(meter.measure(below(90), || Some(50)), Some(50));

        // Same reading, stricter limit: it is now a rejection, so it has to be
        // re-measured rather than served from the cache.
        assert_eq!(meter.measure(below(40), || Some(45)), Some(45));

        // Reads with no limit — reporting usage, or asking for free bytes —
        // reuse whatever is fresh.
        assert_eq!(
            meter.measure(|percent| reusable(percent, None), || unreachable!()),
            Some(45),
        );
    }
}
