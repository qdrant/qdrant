use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::time;

use crate::cpu;

/// Get IO budget to use for optimizations as number of parallel IO operations.
pub fn get_io_budget() -> usize {
    2 // TODO: Make a better guess based on experiments or something like that
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

    /// Returns the total IO budget.
    pub fn available_io_budget(&self) -> usize {
        self.io_budget
    }

    /// For the given desired number of CPUs, return the minimum number of required CPUs.
    fn min_cpu_permits(&self, desired_cpus: usize) -> usize {
        desired_cpus.min(self.cpu_budget).div_ceil(2)
    }

    fn min_io_permits(&self, desired_io: usize) -> usize {
        desired_io.min(self.io_budget) // Use as much IO as requested, not less
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
                Err(TryAcquireError::Closed) => unreachable!("Cannot acquire CPU permit because CPU budget semaphore is closed, this should never happen"),
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
                Err(TryAcquireError::Closed) => unreachable!("Cannot acquire IO permit because IO budget semaphore is closed, this should never happen"),
            }
        } else {
            None
        };

        Some((num_io as usize, io_permit))
    }

    /// Try to acquire CPU permit for optimization task from global CPU budget.
    ///
    /// The given `desired_cpus` is not exact, but rather a hint on what we'd like to acquire.
    /// - it will prefer to acquire the maximum number of CPUs
    /// - it will never be higher than the total CPU budget
    /// - it will never be lower than `min_permits(desired_cpus)`
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

    /// Try to add more resources to the permit.
    pub fn extend_resource_acquisition(
        &self,
        mut permit: ResourcePermit,
        desired_cpus: usize,
        desired_io: usize,
    ) -> Result<ResourcePermit, ResourcePermit> {
        let cpu_result = self.try_acquire_cpu(desired_cpus);
        let io_result = self.try_acquire_io(desired_io);

        match (cpu_result, io_result) {
            (Some((num_cpus, cpu_permit)), Some((num_io, io_permit))) => {
                permit.add_cpu_permit(num_cpus, cpu_permit.unwrap());
                permit.add_io_permit(num_io, io_permit.unwrap());
                Ok(permit)
            }
            _ => Err(permit),
        }
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
        Self::new(cpu::get_cpu_budget(0), get_io_budget())
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
}

impl ResourcePermit {
    /// New CPU permit with given CPU count and permit semaphore.
    pub fn new(
        cpu_count: u32,
        cpu_permit: Option<OwnedSemaphorePermit>,
        io_count: u32,
        io_permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        // Debug assert that if cpu_count is not 0, cpu_permit should be Some
        if cfg!(debug_assertions) && cpu_count > 0 {
            assert!(cpu_permit.is_some());
        }

        if cfg!(debug_assertions) && io_count > 0 {
            assert!(io_permit.is_some());
        }

        Self {
            num_cpus: cpu_count,
            cpu_permit,
            num_io: io_count,
            io_permit,
        }
    }

    /// New CPU permit with given CPU count without a backing semaphore for a shared pool.
    #[cfg(feature = "testing")]
    pub fn dummy(count: u32) -> Self {
        Self {
            num_cpus: count,
            cpu_permit: None,
            num_io: 0,
            io_permit: None,
        }
    }

    /// Release CPU permit, giving them back to the semaphore.
    pub fn release_cpu(&mut self) {
        self.cpu_permit.take();
    }

    /// Release IO permit, giving them back to the semaphore.
    pub fn release_io(&mut self) {
        self.io_permit.take();
    }

    pub fn add_cpu_permit(&mut self, count: usize, permit: OwnedSemaphorePermit) {
        if let Some(cpu_permit) = &mut self.cpu_permit {
            cpu_permit.merge(permit);
        } else {
            self.cpu_permit = Some(permit);
        }
        self.num_cpus += count as u32;
    }

    pub fn add_io_permit(&mut self, count: usize, permit: OwnedSemaphorePermit) {
        if let Some(io_permit) = &mut self.io_permit {
            io_permit.merge(permit);
        } else {
            self.io_permit = Some(permit);
        }
        self.num_io += count as u32;
    }

    /// Partial release CPU permit, giving them back to the semaphore.
    pub fn release_cpu_count(&mut self, release_count: u32) {
        if self.num_cpus > release_count {
            self.num_cpus -= release_count;
            let permit = self.cpu_permit.take();
            self.cpu_permit = permit.and_then(|mut permit| permit.split(self.num_cpus as usize));
        } else {
            self.release_cpu();
        }
    }

    /// Partial release IO permit, giving them back to the semaphore.
    pub fn release_io_count(&mut self, release_count: u32) {
        if self.num_io > release_count {
            self.num_io -= release_count;
            let permit = self.io_permit.take();
            self.io_permit = permit.and_then(|mut permit| permit.split(self.num_io as usize));
        } else {
            self.release_io();
        }
    }
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        self.release_cpu();
    }
}
