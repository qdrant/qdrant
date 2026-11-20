//! FFI binding for the optimizer resource budget ([`edge::ResourceBudget`]).

use std::sync::Arc;

use crate::error::{EdgeError, Result};

/// CPU and IO permits that optimizations draw from.
///
/// Pass one budget to [`EdgeShard::optimize_with_budget`] on several shards to
/// cap their combined resource use: each optimization waits until the budget
/// has a permit for it.
///
/// [`EdgeShard::optimize_with_budget`]: crate::EdgeShard::optimize_with_budget
#[derive(Debug, uniffi::Object)]
pub struct ResourceBudget(edge::ResourceBudget);

#[uniffi::export]
impl ResourceBudget {
    /// A budget of `cpu` CPU permits and `io` IO permits.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::InvalidArgument`] if either is zero.
    #[uniffi::constructor]
    pub fn new(cpu: u32, io: u32) -> Result<Arc<Self>> {
        // The budget is a tokio semaphore, which panics above
        // `Semaphore::MAX_PERMITS` (`usize::MAX >> 3`). That is above
        // `u32::MAX` on 64-bit targets but not on 32-bit ones.
        const MAX_PERMITS: usize = usize::MAX >> 3;
        let permits = |value: u32, name: &str| {
            if value == 0 {
                return Err(EdgeError::invalid_argument(format!(
                    "resource budget {name} must be at least 1"
                )));
            }
            usize::try_from(value)
                .ok()
                .filter(|&permits| permits <= MAX_PERMITS)
                .ok_or_else(|| {
                    EdgeError::invalid_argument(format!(
                        "resource budget {name} {value} is too large"
                    ))
                })
        };
        Ok(Arc::new(Self(edge::ResourceBudget::new(
            permits(cpu, "cpu")?,
            permits(io, "io")?,
        ))))
    }
}

impl ResourceBudget {
    /// A handle to the same permits: optimizations given clones share them.
    pub(crate) fn share(&self) -> edge::ResourceBudget {
        self.0.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_permits_are_invalid() {
        for (cpu, io) in [(0, 1), (1, 0)] {
            assert!(matches!(
                ResourceBudget::new(cpu, io),
                Err(EdgeError::InvalidArgument { .. })
            ));
        }
        assert!(ResourceBudget::new(2, 2).is_ok());
        assert!(ResourceBudget::new(u32::MAX, u32::MAX).is_ok());
    }
}
