use std::time::{Duration, Instant};

/// A progress ETA calculator.
/// Calculates the ETA roughly based on the last ten seconds of measurements.
pub struct EtaCalculator {
    ring: [(Instant, usize); Self::SIZE],
    ring_pos: usize,
}

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
        Self {
            ring: [(now, 0); Self::SIZE],
            ring_pos: 0,
        }
    }

    fn set_progress_raw(&mut self, now: Instant, current_progress: usize) {
        if current_progress < self.ring[self.ring_pos].1 {
            // Progress went backwards, reset the state.
            *self = Self::new();
        }
        if now - self.ring[(self.ring_pos + Self::SIZE - 1) % Self::SIZE].0 >= Self::DURATION {
            self.ring_pos = (self.ring_pos + 1) % Self::SIZE;
        }
        self.ring[self.ring_pos] = (now, current_progress);
    }

    fn estimate_raw(&self, now: Instant, target_progress: usize) -> Option<Duration> {
        let (last_time, last_progress) = self.ring[self.ring_pos];

        // Check if the progress is already reached.
        let value_diff = match target_progress.checked_sub(last_progress) {
            None | Some(0) => return Some(Duration::from_secs(0)),
            Some(value_diff) => value_diff,
        };

        // Find the oldest measurement that is not too old.
        let mut i = self.ring_pos;
        let (old_time, old_progress) = loop {
            i = (i + 1) % Self::SIZE;
            if i == self.ring_pos {
                // No valid measurements.
                return None;
            }
            if now - self.ring[i].0 <= Self::DURATION * Self::SIZE as u32 {
                break self.ring[i];
            }
        };

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
        let mut eta = EtaCalculator::new_raw(now);

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
