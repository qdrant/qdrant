use std::time::{Duration, Instant};

use ringbuffer::{ConstGenericRingBuffer, RingBuffer as _};

/// A progress ETA calculator.
/// Calculates the ETA roughly based on the last ten seconds of measurements.
pub struct EtaCalculator(ConstGenericRingBuffer<(Instant, usize), { Self::SIZE }>);

impl EtaCalculator {
    const SIZE: usize = 16;
    const DURATION: Duration = Duration::from_millis(625);

    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self::new_raw(Instant::now())
    }

    /// Capture the current progress and time.
    pub fn set_progress(&mut self, current_progress: usize) {
        self.set_progress_raw(Instant::now(), current_progress);
    }

    /// Calculate the ETA to reach the target progress.
    pub fn estimate(&self, target_progress: usize) -> Option<Duration> {
        self.estimate_raw(Instant::now(), target_progress)
    }

    fn new_raw(now: Instant) -> Self {
        Self([(now, 0)].as_ref().into())
    }

    fn set_progress_raw(&mut self, now: Instant, current_progress: usize) {
        if self.0.back().map_or(false, |(_, l)| current_progress < *l) {
            // Progress went backwards, reset the state.
            *self = Self::new();
        }

        // Consider this progress history: `[recent, older, even_older, ..., oldest]`.
        // Based on the age of `older`, we decide whether to update the `recent` or push a new item.
        //
        // NOTE: When `len() == 1`, calling `get_signed(-2)` would return the same value as
        // `get_signed(-1)`, but this is not what we want. Thus, we explicitly check for length.
        // Unwraps are safe because the length is checked.
        if self.0.len() >= 2 && now - self.0.get_signed(-2).unwrap().0 < Self::DURATION {
            *self.0.back_mut().unwrap() = (now, current_progress);
        } else {
            self.0.push((now, current_progress));
        }
    }

    fn estimate_raw(&self, now: Instant, target_progress: usize) -> Option<Duration> {
        let &(last_time, last_progress) = self.0.back()?;

        // Check if the progress is already reached.
        let value_diff = match target_progress.checked_sub(last_progress) {
            None | Some(0) => return Some(Duration::from_secs(0)),
            Some(value_diff) => value_diff,
        };

        // Find the oldest measurement that is not too old.
        let &(old_time, old_progress) = self
            .0
            .iter()
            .find(|(time, _)| now - *time <= Self::DURATION * Self::SIZE as u32)?;

        if last_progress == old_progress {
            // No progress, no rate.
            return None;
        }

        let rate = (last_progress - old_progress) as f64 / (last_time - old_time).as_secs_f64();
        let elapsed = (now - last_time).as_secs_f64();
        let eta = (value_diff as f64 / rate - elapsed).max(0.0);
        Duration::try_from_secs_f64(eta).ok()
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use super::*;

    #[test]
    fn test_eta_calculator() {
        let mut now = Instant::now();
        let mut eta = EtaCalculator::new();

        let delta = Duration::from_millis(500);
        for i in 0..=40 {
            now += delta;
            eta.set_progress_raw(now, i);
        }
        assert_relative_eq!(
            eta.estimate_raw(now, 100).unwrap().as_secs_f64(),
            ((100 - 40) * delta).as_secs_f64(),
            max_relative = 0.02,
        );
        // Emulate a stall.
        assert!(eta
            .estimate_raw(now + Duration::from_secs(20), 100)
            .is_none());

        // Change the speed.
        let delta = Duration::from_millis(5000);
        for i in 41..=60 {
            now += delta;
            eta.set_progress_raw(now, i);
        }
        assert_relative_eq!(
            eta.estimate_raw(now, 100).unwrap().as_secs_f64(),
            ((100 - 60) * delta).as_secs_f64(),
            max_relative = 0.02,
        );

        // Should be 0 when the target progress is reached or overreached.
        assert_eq!(eta.estimate_raw(now, 60).unwrap(), Duration::from_secs(0));
        assert_eq!(eta.estimate_raw(now, 50).unwrap(), Duration::from_secs(0));
    }
}
