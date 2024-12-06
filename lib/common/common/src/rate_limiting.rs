use std::time::{Duration, Instant};

/// A rate of requests per time period.
#[derive(Debug)]
pub struct Rate {
    requests_num: u64,
    period: Duration,
}

impl Rate {
    /// Create a new rate.
    ///
    /// # Panics
    ///
    /// This function panics if `requests_num` or `period` is 0.
    pub const fn new(requests_num: u64, period: Duration) -> Self {
        assert!(requests_num > 0);
        assert!(period.as_nanos() > 0);

        Rate {
            requests_num,
            period,
        }
    }

    pub(crate) fn requests_num(&self) -> u64 {
        self.requests_num
    }

    pub(crate) fn period(&self) -> Duration {
        self.period
    }
}

/// A rate limiter based on the token bucket algorithm.
#[derive(Debug)]
pub struct RateLimiter {
    // Maximum tokens the bucket can hold.
    capacity: u64,
    // Tokens added per second.
    tokens_per_sec: f64,
    // Current tokens in the bucket.
    tokens: f64,
    // Last time tokens were updated.
    last_check: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter.
    pub fn new(rate: Rate) -> Self {
        let tokens_per_sec = rate.requests_num() as f64 / rate.period().as_secs_f64();
        let capacity = rate.requests_num;
        RateLimiter {
            capacity,
            tokens_per_sec,
            tokens: capacity as f64, // Start with a full bucket.
            last_check: Instant::now(),
        }
    }

    /// Create a new rate limiter from a rate per second.
    pub fn with_rate_per_sec(rate_per_sec: usize) -> Self {
        let rate = Rate::new(rate_per_sec as u64, Duration::from_secs(1));
        Self::new(rate)
    }

    /// Attempt to consume a token. Returns `true` if allowed, `false` otherwise.
    pub fn check(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_check);
        self.last_check = now;

        // Refill tokens based on elapsed time.
        self.tokens += self.tokens_per_sec * elapsed.as_secs_f64();
        if self.tokens > self.capacity as f64 {
            self.tokens = self.capacity as f64;
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

    #[test]
    fn test_rate_limiter() {
        let rate = Rate::new(2, Duration::from_secs(1));
        let mut limiter = RateLimiter::new(rate);

        assert!(limiter.check());
        assert!(limiter.check());
        assert!(!limiter.check());

        std::thread::sleep(Duration::from_secs(1));

        assert!(limiter.check());
        assert!(limiter.check());
        assert!(!limiter.check());
    }
}
