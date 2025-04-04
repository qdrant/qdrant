use std::time::{Duration, Instant};

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

    /// Attempt to consume a given number of tokens.
    ///
    /// Returns:
    /// - `Ok(())` if allowed and consumes the tokens.
    /// - `Err(RateLimitError)` if denied.
    pub fn try_consume(&mut self, tokens: f64) -> Result<(), RateLimitError> {
        // Consumer wants more than maximum capacity, that's impossible
        if tokens > self.capacity_per_minute as f64 {
            return Err(RateLimitError::AlwaysOverBudget(
                "request larger than rate limiter capacity, please try to split your request",
            ));
        }

        let now = Instant::now();
        let elapsed = now.duration_since(self.last_check);
        self.last_check = now;

        // Refill tokens based on elapsed time.
        self.tokens += self.tokens_per_sec * elapsed.as_secs_f64();
        if self.tokens > self.capacity_per_minute as f64 {
            self.tokens = self.capacity_per_minute as f64;
        }

        if self.tokens >= tokens {
            self.tokens -= tokens; // Consume `cost` tokens.
            Ok(()) // Request allowed.
        } else {
            let missing_tokens = tokens - self.tokens;
            let retry_after = Duration::from_secs_f64(missing_tokens / self.tokens_per_sec);
            debug_assert!(retry_after > Duration::from_secs(0));
            let retry_error = RetryError {
                tokens_available: self.tokens,
                retry_after,
            };
            Err(RateLimitError::Retry(retry_error))
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RetryError {
    /// Number of tokens that were available at the time of the request but didn't suffice.
    pub tokens_available: f64,
    /// Estimated time to wait before retrying request
    pub retry_after: Duration,
}

/// Error when too many tokens have been tried to consume from the rate limiter.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RateLimitError {
    /// Operation that will always be over budget.
    AlwaysOverBudget(&'static str),

    /// Operation that can be retried later.
    Retry(RetryError),
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

        assert_eq!(limiter.try_consume(1.0), Ok(()));
        assert_eq!(limiter.tokens, 0.0);

        // rate limit reached
        assert!(limiter.try_consume(1.0).is_err());
    }

    #[test]
    fn test_rate_more_per_minute() {
        let mut limiter = RateLimiter::new_per_minute(600);
        assert_eq!(limiter.capacity_per_minute, 600);
        assert_eq!(limiter.tokens_per_sec, 10.0);
        assert_eq!(limiter.tokens, 600.0);

        assert_eq!(limiter.try_consume(1.0), Ok(()));
        assert_eq!(limiter.tokens, 599.0);

        assert_eq!(limiter.try_consume(10.0), Ok(()));
        assert_eq_floats(limiter.tokens, 589.0, 0.001);
    }

    #[test]
    fn test_rate_huge_request() {
        let mut limiter = RateLimiter::new_per_minute(100);

        // request too large to ever pass the rate limiter
        assert!(limiter.try_consume(99999.0).is_err());
    }
}
