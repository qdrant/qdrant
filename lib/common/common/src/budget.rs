use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::time;

use crate::cpu;

/// Get IO budget to use for optimizations as number of parallel IO operations.
pub fn get_io_budget(io_budget: usize, cpu_budget: usize) -> usize {
    if io_budget == 0 {
        // By default, we will use same IO budget as CPU budget
        // This will ensure that we will allocate one IO task ahead of one CPU task
        cpu_budget
    } else {
        io_budget
    }
}

/// Structure managing global CPU/IO/... budget for optimization tasks.
///
/// Assigns CPU/IO/... permits to tasks to limit overall resource utilization, making optimization
/// workloads more predictable and efficient.
#[derive(Debug, Clone)]
pub struct ResourceBudget {
    cpu_semaphore: Arc<Semaphore>,
    /// Total CPU budget, available and leased out.
    cpu_budget: usize,

    io_semaphore: Arc<Semaphore>,
    /// Total IO budget, available and leased out.
    io_budget: usize,
}

impl ResourceBudget {
    pub fn new(cpu_budget: usize, io_budget: usize) -> Self {
        Self {
            cpu_semaphore: Arc::new(Semaphore::new(cpu_budget)),
            cpu_budget,
            io_semaphore: Arc::new(Semaphore::new(io_budget)),
            io_budget,
        }
    }

    /// Returns the total CPU budget.
    pub fn available_cpu_budget(&self) -> usize {
        self.cpu_budget
    }

    /// For the given desired number of CPUs, return the minimum number of required CPUs.
    fn min_cpu_permits(&self, desired_cpus: usize) -> usize {
        desired_cpus.min(self.cpu_budget).div_ceil(2)
    }

    fn min_io_permits(&self, desired_io: usize) -> usize {
        desired_io.min(self.io_budget).div_ceil(2)
    }

    fn try_acquire_cpu(
        &self,
        desired_cpus: usize,
    ) -> Option<(usize, Option<OwnedSemaphorePermit>)> {
        let min_required_cpus = self.min_cpu_permits(desired_cpus) as u32;
        let num_cpus = self.cpu_semaphore.available_permits().min(desired_cpus) as u32;
        if num_cpus < min_required_cpus {
            return None;
        }

        let cpu_permit = if num_cpus > 0 {
            let cpu_result =
                Semaphore::try_acquire_many_owned(self.cpu_semaphore.clone(), num_cpus);
            match cpu_result {
                Ok(permit) => Some(permit),
                Err(TryAcquireError::NoPermits) => return None,
                Err(TryAcquireError::Closed) => unreachable!(
                    "Cannot acquire CPU permit because CPU budget semaphore is closed, this should never happen",
                ),
            }
        } else {
            None
        };

        Some((num_cpus as usize, cpu_permit))
    }

    fn try_acquire_io(&self, desired_io: usize) -> Option<(usize, Option<OwnedSemaphorePermit>)> {
        let min_required_io = self.min_io_permits(desired_io) as u32;
        let num_io = self.io_semaphore.available_permits().min(desired_io) as u32;
        if num_io < min_required_io {
            return None;
        }

        let io_permit = if num_io > 0 {
            let io_result = Semaphore::try_acquire_many_owned(self.io_semaphore.clone(), num_io);
            match io_result {
                Ok(permit) => Some(permit),
                Err(TryAcquireError::NoPermits) => return None,
                Err(TryAcquireError::Closed) => unreachable!(
                    "Cannot acquire IO permit because IO budget semaphore is closed, this should never happen",
                ),
            }
        } else {
            None
        };

        Some((num_io as usize, io_permit))
    }

    /// Try to acquire Resources permit for optimization task from global Resource budget.
    ///
    /// The given `desired_cpus` is not exact, but rather a hint on what we'd like to acquire.
    /// - it will prefer to acquire the maximum number of CPUs
    /// - it will never be higher than the total CPU budget
    /// - it will never be lower than `min_permits(desired_cpus)`
    ///
    /// Warn: only one Resource Permit per thread is allowed. Otherwise, it might lead to deadlocks.
    ///
    pub fn try_acquire(&self, desired_cpus: usize, desired_io: usize) -> Option<ResourcePermit> {
        let (num_cpus, cpu_permit) = self.try_acquire_cpu(desired_cpus)?;
        let (num_io, io_permit) = self.try_acquire_io(desired_io)?;

        Some(ResourcePermit::new(
            num_cpus as u32,
            cpu_permit,
            num_io as u32,
            io_permit,
        ))
    }

    /// Acquire Resources permit for optimization task from global Resource budget.
    ///
    /// This will wait until the required number of permits are available.
    /// This function is blocking.
    pub fn acquire(
        &self,
        desired_cpus: usize,
        desired_io: usize,
        stopped: &AtomicBool,
    ) -> Option<ResourcePermit> {
        let mut delay = Duration::from_micros(100);
        while !stopped.load(std::sync::atomic::Ordering::Relaxed) {
            if let Some(permit) = self.try_acquire(desired_cpus, desired_io) {
                return Some(permit);
            } else {
                std::thread::sleep(delay);
                delay = (delay * 2).min(Duration::from_secs(2));
            }
        }
        None
    }

    pub fn replace_with(
        &self,
        mut permit: ResourcePermit,
        new_desired_cpus: usize,
        new_desired_io: usize,
        stopped: &AtomicBool,
    ) -> Result<ResourcePermit, ResourcePermit> {
        // Make sure we don't exceed the budget, otherwise we might deadlock
        let new_desired_cpus = new_desired_cpus.min(self.cpu_budget);
        let new_desired_io = new_desired_io.min(self.io_budget);

        // Acquire extra resources we don't have yet
        let Some(extra_acquired) = self.acquire(
            new_desired_cpus.saturating_sub(permit.num_cpus as usize),
            new_desired_io.saturating_sub(permit.num_io as usize),
            stopped,
        ) else {
            return Err(permit);
        };
        permit.merge(extra_acquired);

        // Release excess resources we now have
        permit.release(
            permit.num_cpus.saturating_sub(new_desired_cpus as u32),
            permit.num_io.saturating_sub(new_desired_io as u32),
        );

        Ok(permit)
    }

    /// Check if there is enough CPU budget available for the given `desired_cpus`.
    ///
    /// This checks for the minimum number of required permits based on the given desired CPUs,
    /// based on `min_permits`. To check for an exact number, use `has_budget_exact` instead.
    ///
    /// A desired CPU count of `0` will always return `true`.
    pub fn has_budget(&self, desired_cpus: usize, desired_io: usize) -> bool {
        self.has_budget_exact(
            self.min_cpu_permits(desired_cpus),
            self.min_io_permits(desired_io),
        )
    }

    /// Check if there are at least `budget` available CPUs in this budget.
    ///
    /// A budget of `0` will always return `true`.
    pub fn has_budget_exact(&self, cpu_budget: usize, io_budget: usize) -> bool {
        self.cpu_semaphore.available_permits() >= cpu_budget
            && self.io_semaphore.available_permits() >= io_budget
    }

    /// Notify when we have CPU budget available for the given number of desired CPUs.
    ///
    /// This will not resolve until the above condition is met.
    ///
    /// Waits for at least the minimum number of permits based on the given desired CPUs. For
    /// example, if `desired_cpus` is 8, this will wait for at least 4 to be available. See
    /// [`Self::min_cpu_permits`].
    ///
    /// - `1` to wait for any CPU budget to be available.
    /// - `0` will always return immediately.
    ///
    /// Uses an exponential backoff strategy up to 10 seconds to avoid busy polling.
    pub async fn notify_on_budget_available(&self, desired_cpus: usize, desired_io: usize) {
        let min_cpu_required = self.min_cpu_permits(desired_cpus);
        let min_io_required = self.min_io_permits(desired_io);
        if self.has_budget_exact(min_cpu_required, min_io_required) {
            return;
        }

        // Wait for CPU budget to be available with exponential backoff
        // TODO: find better way, don't busy wait
        let mut delay = Duration::from_micros(100);
        while !self.has_budget_exact(min_cpu_required, min_io_required) {
            time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(10));
        }
    }
}

impl Default for ResourceBudget {
    fn default() -> Self {
        let cpu_budget = cpu::get_cpu_budget(0);
        let io_budget = get_io_budget(0, cpu_budget);
        Self::new(cpu_budget, io_budget)
    }
}

/// Resource permit, used to limit number of concurrent resource-intensive operations.
/// For example HNSW indexing (which is CPU-bound) can be limited to a certain number of CPUs.
/// Or an I/O-bound operations like segment moving can be limited by I/O permits.
///
/// This permit represents the number of Resources allocated for an operation, so that the operation can
/// respect other parallel workloads. When dropped or `release()`-ed, the Resources are given back for
/// other tasks to acquire.
///
/// These Resource permits are used to better balance and saturate resource utilization.
pub struct ResourcePermit {
    /// Number of CPUs acquired in this permit.
    pub num_cpus: u32,
    /// Semaphore permit.
    cpu_permit: Option<OwnedSemaphorePermit>,

    /// Number of IO permits acquired in this permit.
    pub num_io: u32,
    /// Semaphore permit.
    io_permit: Option<OwnedSemaphorePermit>,

    /// A callback, which should be called when the permit is changed manually.
    /// Originally used to notify the task manager that a permit is available
    /// and schedule more optimization tasks.
    ///
    /// WARN: is not called on drop, only when `release()` is called.
    on_manual_release: Option<Box<dyn Fn() + Send + Sync>>,

    /// Whether the CPUs are lent back to the budget, see [`Self::suspend_cpu`].
    cpu_suspended: AtomicBool,
    /// Serializes [`Self::suspend_cpu`] and [`Self::resume_cpu`] on a shared permit, so that
    /// CPUs are lent and taken back exactly once each.
    cpu_lending: parking_lot::Mutex<()>,
}

impl ResourcePermit {
    /// New CPU permit with given CPU count and permit semaphore.
    pub fn new(
        cpu_count: u32,
        cpu_permit: Option<OwnedSemaphorePermit>,
        io_count: u32,
        io_permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        // Debug assert that cpu/io count and permit counts match
        debug_assert!(cpu_permit.as_ref().map_or(0, |p| p.num_permits()) == cpu_count as usize);
        debug_assert!(io_permit.as_ref().map_or(0, |p| p.num_permits()) == io_count as usize);

        Self {
            num_cpus: cpu_count,
            cpu_permit,
            num_io: io_count,
            io_permit,
            on_manual_release: None,
            cpu_suspended: AtomicBool::new(false),
            cpu_lending: parking_lot::Mutex::new(()),
        }
    }

    pub fn set_on_manual_release(&mut self, on_release: impl Fn() + Send + Sync + 'static) {
        self.on_manual_release = Some(Box::new(on_release));
    }

    /// Merge the other resource permit into this one
    pub fn merge(&mut self, mut other: Self) {
        debug_assert!(
            !self.is_cpu_suspended() && !other.is_cpu_suspended(),
            "Cannot merge permits with lent CPUs",
        );
        self.num_cpus += other.num_cpus;
        self.num_io += other.num_io;

        // Merge optional semaphore permits
        self.cpu_permit = match (self.cpu_permit.take(), other.cpu_permit.take()) {
            (Some(mut permit), Some(other_permit)) => {
                permit.merge(other_permit);
                Some(permit)
            }
            (permit @ Some(_), None) | (None, permit @ Some(_)) => permit,
            (None, None) => None,
        };
        self.io_permit = match (self.io_permit.take(), other.io_permit.take()) {
            (Some(mut permit), Some(other_permit)) => {
                permit.merge(other_permit);
                Some(permit)
            }
            (permit @ Some(_), None) | (None, permit @ Some(_)) => permit,
            (None, None) => None,
        };

        // Debug assert that cpu/io count and permit counts match
        debug_assert!(
            self.cpu_permit.as_ref().map_or(0, |p| p.num_permits()) == self.num_cpus as usize,
        );
        debug_assert!(
            self.io_permit.as_ref().map_or(0, |p| p.num_permits()) == self.num_io as usize,
        );
    }

    /// New CPU permit with given CPU count without a backing semaphore for a shared pool.
    #[cfg(feature = "testing")]
    pub fn dummy(count: u32) -> Self {
        Self {
            num_cpus: count,
            cpu_permit: None,
            num_io: 0,
            io_permit: None,
            on_manual_release: None,
            cpu_suspended: AtomicBool::new(false),
            cpu_lending: parking_lot::Mutex::new(()),
        }
    }

    /// Lend the CPUs of this permit back to the budget, while the task holding it waits for
    /// work done elsewhere (e.g. a graph built by a remote indexer). Other tasks can acquire
    /// them in the meantime. [`Self::num_cpus`] is unchanged: [`Self::resume_cpu`] gets the
    /// same number back.
    ///
    /// Takes `&self`, because the permit is shared by the vector indexes of one segment build.
    /// Returns whether any CPUs were lent.
    pub fn suspend_cpu(&self) -> bool {
        let Some(permit) = &self.cpu_permit else {
            return false;
        };
        let _lending = self.cpu_lending.lock();
        if permit.num_permits() == 0 || self.cpu_suspended.swap(true, Ordering::AcqRel) {
            return false;
        }
        // The permit keeps holding its own semaphore permits. Adding the same number to the
        // semaphore lends them out; `resume_cpu` takes them back and forgets them again.
        permit.semaphore().add_permits(permit.num_permits());
        if let Some(on_release) = &self.on_manual_release {
            on_release();
        }
        true
    }

    /// Take back the CPUs lent by [`Self::suspend_cpu`]. Blocks until they are available.
    /// Returns `false` if `stopped` was set while waiting: the CPUs stay lent then, and are
    /// accounted for when the permit is dropped or released.
    pub fn resume_cpu(&self, stopped: &AtomicBool) -> bool {
        let Some(permit) = &self.cpu_permit else {
            return true;
        };
        // Held while waiting: a concurrent caller waits too, then finds the CPUs back
        let _lending = self.cpu_lending.lock();
        if !self.cpu_suspended.load(Ordering::Acquire) {
            return true;
        }
        let semaphore = permit.semaphore().clone();
        let num_permits = permit.num_permits() as u32;
        let mut delay = Duration::from_micros(100);
        while !stopped.load(Ordering::Relaxed) {
            match Semaphore::try_acquire_many_owned(semaphore.clone(), num_permits) {
                Ok(lent) => {
                    lent.forget();
                    self.cpu_suspended.store(false, Ordering::Release);
                    return true;
                }
                Err(TryAcquireError::NoPermits) => {
                    std::thread::sleep(delay);
                    delay = (delay * 2).min(Duration::from_secs(2));
                }
                Err(TryAcquireError::Closed) => unreachable!(
                    "Cannot acquire CPU permit because CPU budget semaphore is closed, this should never happen",
                ),
            }
        }
        false
    }

    /// Whether the CPUs are lent back to the budget, see [`Self::suspend_cpu`].
    pub fn is_cpu_suspended(&self) -> bool {
        self.cpu_suspended.load(Ordering::Acquire)
    }

    /// If the CPUs are lent, they are already back in the semaphore: forget the permit instead
    /// of returning it a second time.
    fn settle_suspended_cpu(&mut self) {
        if self.cpu_suspended.swap(false, Ordering::AcqRel)
            && let Some(permit) = self.cpu_permit.take()
        {
            permit.forget();
            self.num_cpus = 0;
        }
    }

    /// Release CPU permit, giving them back to the semaphore.
    fn release_cpu(&mut self) {
        self.settle_suspended_cpu();
        self.num_cpus = 0;
        self.cpu_permit.take();
    }

    /// Release IO permit, giving them back to the semaphore.
    fn release_io(&mut self) {
        self.num_io = 0;
        self.io_permit.take();
    }

    /// Partial release CPU permit, giving them back to the semaphore.
    fn release_cpu_count(&mut self, release_count: u32) {
        if release_count == 0 {
            return;
        }
        // Lent CPUs are released whole
        self.settle_suspended_cpu();

        if self.num_cpus > release_count {
            self.num_cpus -= release_count;
            let permit = self.cpu_permit.take();
            self.cpu_permit = permit.and_then(|mut permit| permit.split(self.num_cpus as usize));
        } else {
            self.release_cpu();
        }
    }

    /// Partial release IO permit, giving them back to the semaphore.
    fn release_io_count(&mut self, release_count: u32) {
        if release_count == 0 {
            return;
        }

        if self.num_io > release_count {
            self.num_io -= release_count;
            let permit = self.io_permit.take();
            self.io_permit = permit.and_then(|mut permit| permit.split(self.num_io as usize));
        } else {
            self.release_io();
        }
    }

    pub fn release(&mut self, cpu: u32, io: u32) {
        self.release_cpu_count(cpu);
        self.release_io_count(io);

        if let Some(on_release) = &self.on_manual_release {
            on_release();
        }
    }
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        self.settle_suspended_cpu();
        let Self {
            num_cpus: _,
            cpu_permit,
            num_io: _,
            io_permit,
            on_manual_release: _, // Only explicit release() should call the callback
            cpu_suspended: _,
            cpu_lending: _,
        } = self;

        let _ = cpu_permit.take();
        let _ = io_permit.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn available_cpus(budget: &ResourceBudget) -> usize {
        budget.cpu_semaphore.available_permits()
    }

    #[test]
    fn suspend_and_resume_cpu() {
        let budget = ResourceBudget::new(4, 4);
        let stopped = AtomicBool::new(false);
        let permit = budget.try_acquire(4, 0).unwrap();
        assert_eq!(available_cpus(&budget), 0);

        assert!(permit.suspend_cpu());
        assert!(!permit.suspend_cpu(), "already suspended");
        assert_eq!(available_cpus(&budget), 4);
        assert_eq!(permit.num_cpus, 4, "nominal count is kept");

        // Somebody else borrows 2 CPUs while we wait
        let other = budget.try_acquire(2, 0).unwrap();
        assert_eq!(available_cpus(&budget), 2);
        drop(other);

        assert!(permit.resume_cpu(&stopped));
        assert_eq!(available_cpus(&budget), 0);
        drop(permit);
        assert_eq!(available_cpus(&budget), 4);
    }

    #[test]
    fn drop_while_suspended_keeps_accounting() {
        let budget = ResourceBudget::new(4, 4);
        let permit = budget.try_acquire(3, 0).unwrap();
        assert!(permit.suspend_cpu());
        assert_eq!(available_cpus(&budget), 4);
        drop(permit);
        assert_eq!(available_cpus(&budget), 4);
    }

    #[test]
    fn release_while_suspended_keeps_accounting() {
        let budget = ResourceBudget::new(4, 4);
        let mut permit = budget.try_acquire(4, 0).unwrap();
        assert!(permit.suspend_cpu());
        permit.release(1, 0);
        assert_eq!(permit.num_cpus, 0);
        assert_eq!(available_cpus(&budget), 4);
        drop(permit);
        assert_eq!(available_cpus(&budget), 4);
    }

    #[test]
    fn concurrent_resume_takes_cpus_back_once() {
        let budget = ResourceBudget::new(4, 4);
        let stopped = AtomicBool::new(false);
        let permit = Arc::new(budget.try_acquire(4, 0).unwrap());
        for _ in 0..20 {
            assert!(permit.suspend_cpu());
            assert_eq!(available_cpus(&budget), 4);
            std::thread::scope(|scope| {
                for _ in 0..4 {
                    scope.spawn(|| assert!(permit.resume_cpu(&stopped)));
                }
            });
            assert_eq!(available_cpus(&budget), 0, "CPUs taken back more than once");
        }
        drop(permit);
        assert_eq!(available_cpus(&budget), 4);
    }

    #[test]
    fn resume_cancelled_by_stop() {
        let budget = ResourceBudget::new(2, 2);
        let stopped = AtomicBool::new(false);
        let permit = budget.try_acquire(2, 0).unwrap();
        assert!(permit.suspend_cpu());
        let other = budget.try_acquire(2, 0).unwrap();
        stopped.store(true, Ordering::Relaxed);
        assert!(!permit.resume_cpu(&stopped));
        drop(other);
        drop(permit);
        assert_eq!(available_cpus(&budget), 2);
    }
}
