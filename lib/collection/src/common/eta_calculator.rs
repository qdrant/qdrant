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
        let now = Instant::now();
        Self {
            ring: [(now, 0); Self::SIZE],
            ring_pos: 0,
        }
    }

    /// Capture the current progress and time.
    pub fn set_progress(&mut self, current_progress: usize) {
        let now = Instant::now();
        if current_progress < self.ring[self.ring_pos].1 {
            // Progress went backwards, reset the state.
            *self = Self::new();
        }
        if now - self.ring[(self.ring_pos + Self::SIZE - 1) % Self::SIZE].0 >= Self::DURATION {
            self.ring_pos = (self.ring_pos + 1) % Self::SIZE;
        }
        self.ring[self.ring_pos] = (now, current_progress);
    }

    /// Calculate the ETA to reach the target progress.
    pub fn estimate(&self, target_progress: usize) -> Option<Duration> {
        let now = Instant::now();

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
