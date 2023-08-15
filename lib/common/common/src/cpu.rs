use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Duration;

#[cfg(target_os = "linux")]
use thiserror::Error;
#[cfg(target_os = "linux")]
use thread_priority::{set_current_thread_priority, ThreadPriority, ThreadPriorityValue};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::time;

use crate::defaults::default_cpu_budget_unallocated;

/// Try to read number of CPUs from environment variable `QDRANT_NUM_CPUS`.
/// If it is not set, use `num_cpus::get()`.
pub fn get_num_cpus() -> usize {
    match std::env::var("QDRANT_NUM_CPUS") {
        Ok(val) => {
            let num_cpus = val.parse::<usize>().unwrap_or(0);
            if num_cpus > 0 {
                num_cpus
            } else {
                num_cpus::get()
            }
        }
        Err(_) => num_cpus::get(),
    }
}

/// Get available CPU budget to use for optimizations as number of CPUs (threads).
///
/// This is user configurable via `cpu_budget` parameter in settings:
/// If 0 - auto selection, keep at least one CPU free when possible.
/// If negative - subtract this number of CPUs from the available CPUs.
/// If positive - use this exact number of CPUs.
///
/// The returned value will always be at least 1.
pub fn get_cpu_budget(cpu_budget_param: isize) -> usize {
    match cpu_budget_param.cmp(&0) {
        // If less than zero, subtract from available CPUs
        Ordering::Less => get_num_cpus()
            .saturating_sub(-cpu_budget_param as usize)
            .max(1),
        // If zero, use automatic selection
        Ordering::Equal => {
            let num_cpus = get_num_cpus();
            num_cpus
                .saturating_sub(-default_cpu_budget_unallocated(num_cpus) as usize)
                .max(1)
        }
        // If greater than zero, use exact number
        Ordering::Greater => cpu_budget_param as usize,
    }
}

/// Structure managing global CPU budget for optimization tasks.
///
/// Assigns CPU permits to tasks to limit overall resource utilization, making optimization
/// workloads more predictable and efficient.
#[derive(Debug, Clone)]
pub struct CpuBudget {
    semaphore: Arc<Semaphore>,
    /// Total CPU budget, available and leased out.
    cpu_budget: usize,
}

impl CpuBudget {
    pub fn new(cpu_budget: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(cpu_budget)),
            cpu_budget,
        }
    }

    /// Returns the total CPU budget.
    pub fn available_cpu_budget(&self) -> usize {
        self.cpu_budget
    }

    /// For the given desired number of CPUs, return the minimum number of required CPUs.
    fn min_permits(&self, desired_cpus: usize) -> usize {
        desired_cpus.min(self.cpu_budget).div_ceil(2)
    }

    /// Try to acquire CPU permit for optimization task from global CPU budget.
    ///
    /// The given `desired_cpus` is not exact, but rather a hint on what we'd like to acquire.
    /// - it will prefer to acquire the maximum number of CPUs
    /// - it will never be higher than the total CPU budget
    /// - it will never be lower than `min_permits(desired_cpus)`
    pub fn try_acquire(&self, desired_cpus: usize) -> Option<CpuPermit> {
        // Determine what number of CPUs to acquire based on available budget
        let min_required = self.min_permits(desired_cpus) as u32;
        let num_cpus = self.semaphore.available_permits().min(desired_cpus) as u32;
        if desired_cpus == 0 || num_cpus < min_required {
            return None;
        }

        // Try to acquire selected number of CPUs
        let result = Semaphore::try_acquire_many_owned(self.semaphore.clone(), num_cpus);
        let permit = match result {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => return None,
            Err(TryAcquireError::Closed) => unreachable!("Cannot acquire CPU permit because CPU budget semaphore is closed, this should never happen"),
        };

        Some(CpuPermit::new(num_cpus, permit))
    }

    /// Check if there is enough CPU budget available for the given `desired_cpus`.
    ///
    /// This checks for the minimum number of required permits based on the given desired CPUs,
    /// based on `min_permits`. To check for an exact number, use `has_budget_exact` instead.
    ///
    /// A desired CPU count of `0` will always return `true`.
    pub fn has_budget(&self, desired_cpus: usize) -> bool {
        self.has_budget_exact(self.min_permits(desired_cpus))
    }

    /// Check if there are at least `budget` available CPUs in this budget.
    ///
    /// A budget of `0` will always return `true`.
    pub fn has_budget_exact(&self, budget: usize) -> bool {
        self.semaphore.available_permits() >= budget
    }

    /// Notify when we have CPU budget available for the given number of desired CPUs.
    ///
    /// This will not resolve until the above condition is met.
    ///
    /// Waits for at least the minimum number of permits based on the given desired CPUs. For
    /// example, if `desired_cpus` is 8, this will wait for at least 4 to be available. See
    /// [`Self::min_permits`].
    ///
    /// - `1` to wait for any CPU budget to be available.
    /// - `0` will always return immediately.
    ///
    /// Uses an exponential backoff strategy up to 10 seconds to avoid busy polling.
    pub async fn notify_on_budget_available(&self, desired_cpus: usize) {
        let min_required = self.min_permits(desired_cpus);
        if self.has_budget_exact(min_required) {
            return;
        }

        // Wait for CPU budget to be available with exponential backoff
        // TODO: find better way, don't busy wait
        let mut delay = Duration::from_micros(100);
        while !self.has_budget_exact(min_required) {
            time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(10));
        }
    }
}

impl Default for CpuBudget {
    fn default() -> Self {
        Self::new(get_cpu_budget(0))
    }
}

/// CPU permit, used to limit number of concurrent CPU-intensive operations
///
/// This permit represents the number of CPUs allocated for an operation, so that the operation can
/// respect other parallel workloads. When dropped or `release()`-ed, the CPUs are given back for
/// other tasks to acquire.
///
/// These CPU permits are used to better balance and saturate resource utilization.
pub struct CpuPermit {
    /// Number of CPUs acquired in this permit.
    pub num_cpus: u32,
    /// Semaphore permit.
    permit: Option<OwnedSemaphorePermit>,
}

impl CpuPermit {
    /// New CPU permit with given CPU count and permit semaphore.
    pub fn new(count: u32, permit: OwnedSemaphorePermit) -> Self {
        Self {
            num_cpus: count,
            permit: Some(permit),
        }
    }

    /// New CPU permit with given CPU count without a backing semaphore for a shared pool.
    #[cfg(feature = "testing")]
    pub fn dummy(count: u32) -> Self {
        Self {
            num_cpus: count,
            permit: None,
        }
    }

    /// Release CPU permit, giving them back to the semaphore.
    pub fn release(&mut self) {
        self.permit.take();
    }

    /// Partial release CPU permit, giving them back to the semaphore.
    pub fn release_count(&mut self, release_count: u32) {
        if self.num_cpus > release_count {
            self.num_cpus -= release_count;
            let permit = self.permit.take();
            self.permit = permit.and_then(|mut permit| permit.split(self.num_cpus as usize));
        } else {
            self.release();
        }
    }
}

impl Drop for CpuPermit {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Error, Debug)]
#[cfg(target_os = "linux")]
pub enum ThreadPriorityError {
    #[error("Failed to set thread priority: {0:?}")]
    SetThreadPriority(thread_priority::Error),
    #[error("Failed to parse thread priority value: {0}")]
    ParseNice(String),
}

/// On Linux, make current thread lower priority (nice: 10).
#[cfg(target_os = "linux")]
pub fn linux_low_thread_priority() -> Result<(), ThreadPriorityError> {
    // 25% corresponds to a nice value of 10
    set_linux_thread_priority(25)
}

/// On Linux, make current thread high priority (nice: -10).
///
/// # Warning
///
/// This is very likely to fail because decreasing the nice value requires special privileges. It
/// is therefore recommended to soft-fail.
/// See: <https://manned.org/renice.1#head6>
#[cfg(target_os = "linux")]
pub fn linux_high_thread_priority() -> Result<(), ThreadPriorityError> {
    // 75% corresponds to a nice value of -10
    set_linux_thread_priority(75)
}

/// On Linux, update priority of current thread.
///
/// Only works on Linux because POSIX threads share their priority/nice value with all process
/// threads. Linux breaks this behaviour though and uses a per-thread priority/nice value.
/// - <https://linux.die.net/man/7/pthreads>
/// - <https://linux.die.net/man/2/setpriority>
#[cfg(target_os = "linux")]
fn set_linux_thread_priority(priority: u8) -> Result<(), ThreadPriorityError> {
    let new_priority = ThreadPriority::Crossplatform(
        ThreadPriorityValue::try_from(priority).map_err(ThreadPriorityError::ParseNice)?,
    );
    set_current_thread_priority(new_priority).map_err(ThreadPriorityError::SetThreadPriority)
}
