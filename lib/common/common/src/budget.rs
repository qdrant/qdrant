use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};

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

/// `Arc<Notify>` wrapper providing `Debug` and `Clone`
///
/// Shared across all clones of a [`ResourceBudget`] to wake waiters when a permit is released
#[derive(Clone)]
pub(crate) struct BudgetNotify(Arc<Notify>);

impl BudgetNotify {
    fn new() -> Self {
        Self(Arc::new(Notify::new()))
    }

    /// wake all waiting tasks
    #[inline]
    fn notify_waiters(&self) {
        self.0.notify_waiters();
    }

    /// future that resolves on the next notification
    #[inline]
    fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.0.notified()
    }
}

impl fmt::Debug for BudgetNotify {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BudgetNotify")
            .field(&Arc::as_ptr(&self.0))
            .finish()
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

    /// Wakes up tasks in [`Self::notify_on_budget_available`] when permits are returned
    ///
    /// This avoids polling and is shared across all `ResourceBudget` clones.
    budget_released: BudgetNotify,
}

impl ResourceBudget {
    pub fn new(cpu_budget: usize, io_budget: usize) -> Self {
        Self {
            cpu_semaphore: Arc::new(Semaphore::new(cpu_budget)),
            cpu_budget,
            io_semaphore: Arc::new(Semaphore::new(io_budget)),
            io_budget,
            budget_released: BudgetNotify::new(),
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
            self.budget_released.clone(),
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

    /// Wait until sufficient budget is available for the given resource requirements.
    ///
    /// Resolves immediately if the budget is available. Otherwise, suspends the
    /// task via [`tokio::sync::Notify`] until a [`ResourcePermit`] is released,
    /// and then rechecks.
    ///
    /// - `1` to wait for any CPU budget to be available.
    /// - `0` will always return immediately.
    pub async fn notify_on_budget_available(&self, desired_cpus: usize, desired_io: usize) {
        let min_cpu_required = self.min_cpu_permits(desired_cpus);
        let min_io_required = self.min_io_permits(desired_io);

        // fast path: budget is already available
        if self.has_budget_exact(min_cpu_required, min_io_required) {
            return;
        }

        // slow path: suspend until a permit is released, then recheck.
        loop {
            // Register the `Notified` future before rechecking availability to
            // avoid missing any release event that happens in between.
            let notified = self.budget_released.notified();

            if self.has_budget_exact(min_cpu_required, min_io_required) {
                return;
            }

            notified.await;
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

    /// Wakes tasks waiting in [`ResourceBudget::notify_on_budget_available`] when permits are returned
    budget_released: Option<BudgetNotify>,

    /// A callback, which should be called when the permit is changed manually.
    /// Originally used to notify the task manager that a permit is available
    /// and schedule more optimization tasks.
    ///
    /// WARN: is not called on drop, only when `release()` is called.
    on_manual_release: Option<Box<dyn Fn() + Send + Sync>>,
}

impl ResourcePermit {
    /// New CPU permit with given CPU count and permit semaphore.
    pub(crate) fn new(
        cpu_count: u32,
        cpu_permit: Option<OwnedSemaphorePermit>,
        io_count: u32,
        io_permit: Option<OwnedSemaphorePermit>,
        budget_released: BudgetNotify,
    ) -> Self {
        // Debug assert that cpu/io count and permit counts match
        debug_assert!(cpu_permit.as_ref().map_or(0, |p| p.num_permits()) == cpu_count as usize);
        debug_assert!(io_permit.as_ref().map_or(0, |p| p.num_permits()) == io_count as usize);

        Self {
            num_cpus: cpu_count,
            cpu_permit,
            num_io: io_count,
            io_permit,
            budget_released: Some(budget_released),
            on_manual_release: None,
        }
    }

    pub fn set_on_manual_release(&mut self, on_release: impl Fn() + Send + Sync + 'static) {
        self.on_manual_release = Some(Box::new(on_release));
    }

    /// Merge the other resource permit into this one
    pub fn merge(&mut self, mut other: Self) {
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

        // Keep one notifier and discard the other, since both point to the same ResourceBudget
        if self.budget_released.is_none() {
            self.budget_released = other.budget_released.take();
        } else {
            other.budget_released.take();
        }

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
            budget_released: None,
            on_manual_release: None,
        }
    }

    /// Release CPU permit, giving them back to the semaphore.
    fn release_cpu(&mut self) {
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

        // Wake waiters on partial release (`Drop` handles full release)
        if let Some(notify) = &self.budget_released {
            notify.notify_waiters();
        }

        if let Some(on_release) = &self.on_manual_release {
            on_release();
        }
    }
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        let Self {
            num_cpus: _,
            cpu_permit,
            num_io: _,
            io_permit,
            budget_released,
            on_manual_release: _, // Only explicit release() should call the callback
        } = self;

        let _ = cpu_permit.take();
        let _ = io_permit.take();

        // Wake any tasks suspended in `notify_on_budget_available`
        if let Some(notify) = budget_released.take() {
            notify.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use tokio::time;

    use super::{ResourceBudget, ResourcePermit};

    #[tokio::test]
    async fn test_notify_returns_immediately_when_budget_available() {
        let budget = ResourceBudget::new(4, 4);
        time::timeout(
            Duration::from_millis(100),
            budget.notify_on_budget_available(2, 2),
        )
        .await
        .expect("must not block when budget available");
    }

    #[tokio::test]
    async fn test_notify_zero_desired_returns_immediately() {
        let budget = ResourceBudget::new(0, 0);
        time::timeout(
            Duration::from_millis(100),
            budget.notify_on_budget_available(0, 0),
        )
        .await
        .expect("notify_on_budget_available(0,0) must always resolve immediately");
    }

    #[tokio::test]
    async fn test_notify_wakes_on_drop() {
        let budget = ResourceBudget::new(2, 2);

        let permit = budget.try_acquire(2, 2).expect("must succeed");

        let budget_clone = budget.clone();
        let waiter = tokio::spawn(async move {
            budget_clone.notify_on_budget_available(1, 1).await;
        });

        // allow waiter to suspend
        time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "waiter must suspend");

        // Release permit, waking waiter
        drop(permit);

        time::timeout(Duration::from_millis(200), waiter)
            .await
            .expect("waiter must wake up")
            .expect("waiter panicked");

        // Verify permits returned
        assert!(
            budget.try_acquire(1, 1).is_some(),
            "permits must be available"
        );
    }

    #[tokio::test]
    async fn test_notify_wakes_on_partial_release() {
        let budget = ResourceBudget::new(4, 4);

        let mut permit = budget.try_acquire(4, 4).expect("must succeed");

        let budget_clone = budget.clone();
        let waiter = tokio::spawn(async move {
            budget_clone.notify_on_budget_available(1, 1).await;
        });

        // Allow waiter to suspend
        time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "waiter must suspend");

        // Partially release permits
        permit.release(2, 2);

        time::timeout(Duration::from_millis(200), waiter)
            .await
            .expect("waiter must wake up")
            .expect("waiter panicked");
    }

    #[tokio::test]
    async fn test_multiple_waiters_all_wake() {
        let budget = ResourceBudget::new(2, 2);
        let permit = budget.try_acquire(2, 2).expect("must succeed");

        // Spawn multiple waiters
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let b = budget.clone();
                tokio::spawn(async move {
                    b.notify_on_budget_available(1, 1).await;
                })
            })
            .collect();

        // Allow waiters to suspend
        time::sleep(Duration::from_millis(20)).await;

        // Release permit, waking all waiters
        drop(permit);

        for handle in handles {
            time::timeout(Duration::from_millis(200), handle)
                .await
                .expect("waiter must wake up")
                .expect("waiter panicked");
        }
    }

    /// dropping a dummy permit (which lacks a `BudgetNotify`) must not panic
    #[cfg(feature = "testing")]
    #[test]
    fn test_dummy_permit_drop_is_safe() {
        let permit = ResourcePermit::dummy(4);
        drop(permit);
    }

    #[tokio::test]
    async fn test_cloned_budgets_share_notifier() {
        let budget_a = ResourceBudget::new(2, 2);
        let budget_b = budget_a.clone();

        // Exhaust via B
        let permit = budget_b.try_acquire(2, 2).expect("must succeed");

        // Wait on A
        let waiter = tokio::spawn(async move {
            budget_a.notify_on_budget_available(1, 1).await;
        });

        time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "waiter must suspend");

        // Release via B, waking A
        drop(permit);

        time::timeout(Duration::from_millis(200), waiter)
            .await
            .expect("waiter on A must wake up")
            .expect("waiter panicked");
    }
}
