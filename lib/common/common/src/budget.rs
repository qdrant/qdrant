use std::sync::Arc;
use std::sync::atomic::AtomicBool;
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

        let mut permit = ResourcePermit::new(num_cpus as u32, cpu_permit, num_io as u32, io_permit);
        permit.budget = Some(self.clone());
        Some(permit)
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

    /// The budget the permit was acquired from, to take lent CPUs back. `None` for a permit
    /// without a budget behind it.
    budget: Option<ResourceBudget>,
    /// CPUs given back with [`ResourcePermit::lend_cpu`], not taken back yet.
    lent_cpus: u32,
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
            budget: None,
            lent_cpus: 0,
        }
    }

    /// The CPUs the holder of this permit works with: those it holds, and those it lent.
    pub fn full_cpus(&self) -> u32 {
        self.num_cpus + self.lent_cpus
    }

    /// Give CPUs back to the budget, down to `keep`, while the holder waits for work done
    /// elsewhere. Other tasks may acquire them. [`Self::restore_cpu`] takes them back.
    pub fn lend_cpu(&mut self, keep: u32) {
        let lend = self.num_cpus.saturating_sub(keep);
        if lend == 0 {
            return;
        }
        self.lent_cpus += lend;
        self.release(lend, 0);
    }

    /// Take back the CPUs lent with [`Self::lend_cpu`], waiting until the budget has them.
    /// Like any acquisition, it may return with fewer, see [`ResourceBudget::try_acquire`].
    ///
    /// Releases the CPUs it still holds first. Holding some while waiting for more would
    /// deadlock tasks that all take theirs back at once, each holding what the others wait
    /// for. So the permit holds no CPU while it waits, and none if `stopped` ends the wait:
    /// then this returns `false`.
    #[must_use]
    pub fn restore_cpu(&mut self, stopped: &AtomicBool) -> bool {
        if self.lent_cpus == 0 {
            return true;
        }
        let desired_cpus = self.full_cpus();
        self.lent_cpus = 0;
        let Some(budget) = self.budget.clone() else {
            // No budget to take them from: nothing was given back either
            self.num_cpus = desired_cpus;
            return true;
        };
        self.release(self.num_cpus, 0);
        match budget.acquire(desired_cpus as usize, 0, stopped) {
            Some(acquired) => {
                self.merge(acquired);
                true
            }
            None => false,
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
            budget: None,
            lent_cpus: 0,
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
            on_manual_release: _, // Only explicit release() should call the callback
            budget: _,
            lent_cpus: _,
        } = self;

        let _ = cpu_permit.take();
        let _ = io_permit.take();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn lent_cpus_go_to_others_and_come_back() {
        let budget = ResourceBudget::new(4, 4);
        let stopped = AtomicBool::new(false);
        let mut permit = budget.try_acquire(4, 0).unwrap();
        assert!(!budget.has_budget_exact(1, 0));

        permit.lend_cpu(1);
        assert_eq!((permit.num_cpus, permit.full_cpus()), (1, 4));
        assert!(budget.has_budget_exact(3, 0));
        // Lending again gives nothing more
        permit.lend_cpu(1);
        assert_eq!((permit.num_cpus, permit.full_cpus()), (1, 4));

        assert!(permit.restore_cpu(&stopped));
        assert_eq!((permit.num_cpus, permit.full_cpus()), (4, 4));
        assert!(!budget.has_budget_exact(1, 0));

        drop(permit);
        assert!(budget.has_budget_exact(4, 0));
    }

    #[test]
    fn restore_waits_for_the_budget_holding_nothing() {
        let budget = ResourceBudget::new(4, 4);
        let stopped = Arc::new(AtomicBool::new(false));
        let mut permit = budget.try_acquire(4, 0).unwrap();
        permit.lend_cpu(1);
        // Another task takes what was lent, and more than leaves room for half of the permit
        let other = budget.try_acquire(3, 0).unwrap();
        assert_eq!(other.num_cpus, 3);

        let restorer = {
            let stopped = stopped.clone();
            std::thread::spawn(move || {
                let restored = permit.restore_cpu(&stopped);
                (restored, permit)
            })
        };
        // While it waits, it holds nothing: the CPU it kept is available
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !budget.has_budget_exact(1, 0) {
            assert!(
                std::time::Instant::now() < deadline,
                "the kept CPU was not released"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(!restorer.is_finished());

        // Half of what it asks for is enough, as for any acquisition
        drop(other);
        let (restored, permit) = restorer.join().unwrap();
        assert!(restored);
        assert_eq!((permit.num_cpus, permit.full_cpus()), (4, 4));
    }

    #[test]
    fn stopped_restore_holds_nothing() {
        let budget = ResourceBudget::new(2, 2);
        let stopped = AtomicBool::new(false);
        let mut permit = budget.try_acquire(2, 0).unwrap();
        permit.lend_cpu(1);
        // Another task takes the lent CPU, so there is nothing to take back
        let other = budget.try_acquire(2, 0).unwrap();
        assert_eq!(other.num_cpus, 1);

        stopped.store(true, Ordering::Relaxed);
        assert!(!permit.restore_cpu(&stopped));
        assert_eq!((permit.num_cpus, permit.full_cpus()), (0, 0));
        drop(other);
        drop(permit);
        assert!(budget.has_budget_exact(2, 0));
    }
}
