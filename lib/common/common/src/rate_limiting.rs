use std::time::Instant;

/// A rate limiter based on the token bucket algorithm.
/// Designed to limit the number of requests per minute.
/// The bucket is refilled at a constant rate of `tokens_per_sec` tokens per second.
/// The bucket has a maximum capacity of `capacity_per_minute` tokens to allow for bursts.
#[derive(Debug)]
pub struct RateLimiter {
    // Maximum tokens the bucket can hold.
    capacity_per_minute: u64,
    // Tokens added per second.
    tokens_per_sec: f64,
    // Current tokens in the bucket.
    tokens: f64,
    // Last time tokens were updated.
    last_check: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter for `requests_num` requests per minute.
    pub fn new_per_minute(requests_num: usize) -> Self {
        let tokens_per_sec = requests_num as f64 / 60.0;
        RateLimiter {
            capacity_per_minute: requests_num as u64,
            tokens_per_sec,
            tokens: requests_num as f64, // Start with a full bucket to allow burst at the beginning.
            last_check: Instant::now(),
        }
    }

    /// Attempt to consume a token. Returns `true` if allowed, `false` otherwise.
    pub fn check_and_update(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_check);
        self.last_check = now;

        // Refill tokens based on elapsed time.
        self.tokens += self.tokens_per_sec * elapsed.as_secs_f64();
        if self.tokens > self.capacity_per_minute as f64 {
            self.tokens = self.capacity_per_minute as f64;
        }

        if self.tokens >= 1.0 {
            self.tokens -= 1.0; // Consume one token.
            true // Request allowed.
        } else {
            false // Request denied.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_eq_floats(a: f64, b: f64, tolerance: f64) {
        assert!(
            (a - b).abs() < tolerance,
            "assertion failed: `(left == right)` (left: `{a}`, right: `{b}`, tolerance: `{tolerance}`)",
        );
    }

    #[test]
    fn test_rate_one_per_minute() {
        let mut limiter = RateLimiter::new_per_minute(1);
        assert_eq!(limiter.capacity_per_minute, 1);
        assert_eq_floats(limiter.tokens_per_sec, 0.016, 0.001);
        assert_eq!(limiter.tokens, 1.0);

        assert!(limiter.check_and_update());
        assert_eq!(limiter.tokens, 0.0);

        // rate limit reached
        assert!(!limiter.check_and_update());
    }

    #[test]
    fn test_rate_more_per_minute() {
        let mut limiter = RateLimiter::new_per_minute(600);
        assert_eq!(limiter.capacity_per_minute, 600);
        assert_eq!(limiter.tokens_per_sec, 10.0);
        assert_eq!(limiter.tokens, 600.0);

        assert!(limiter.check_and_update());
        assert_eq!(limiter.tokens, 599.0);
    }
}
