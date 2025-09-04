use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};
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
pub struct ResourceBudget(Arc<ResourceBudgetInner>);

#[derive(Debug)]
struct ResourceBudgetInner {
    /// Total budget, available and leased out.
    total_budget: Resources<usize>,
    /// Currently available budget.
    available: Mutex<Resources<usize>>,
    /// Condition variables to notify waiters when budget changes.
    condvar_changed: Resources<Condvar>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Resources<T> {
    pub cpu: T,
    pub io: T,
}

impl ResourceBudget {
    pub fn new(cpu_budget: usize, io_budget: usize) -> ResourceBudget {
        let total_budget = Resources {
            cpu: cpu_budget,
            io: io_budget,
        };
        ResourceBudget(Arc::new(ResourceBudgetInner {
            total_budget,
            available: Mutex::new(total_budget),
            condvar_changed: Resources {
                cpu: Condvar::new(),
                io: Condvar::new(),
            },
        }))
    }

    /// Returns the total budget.
    pub fn total_budget(&self) -> Resources<usize> {
        self.0.total_budget
    }

    /// For the given desired number of CPUs and IO, return the minimum number
    /// of required CPUs and IO.
    fn min_permits(&self, desired_cpus: usize, desired_io: usize) -> Resources<usize> {
        Resources {
            cpu: desired_cpus.min(self.0.total_budget.cpu).div_ceil(2),
            io: desired_io.min(self.0.total_budget.io).div_ceil(2),
        }
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
        let min_required = self.min_permits(desired_cpus, desired_io);

        let mut available = self.0.available.lock();
        if available.cpu < min_required.cpu || available.io < min_required.io {
            return None;
        }
        let taken = Resources {
            cpu: available.cpu.min(desired_cpus),
            io: available.io.min(desired_io),
        };
        *available = Resources {
            cpu: available.cpu - taken.cpu,
            io: available.io - taken.io,
        };
        Some(ResourcePermit::new(taken, self.clone()))
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
        let min_required = self.min_permits(desired_cpus, desired_io);

        let mut available = self.0.available.lock();
        while available.cpu < min_required.cpu || available.io < min_required.io {
            if stopped.load(std::sync::atomic::Ordering::Relaxed) {
                return None;
            }
            if available.cpu < min_required.cpu {
                self.0.condvar_changed.cpu.wait_for(&mut available, delay);
            }
            if available.io < min_required.io {
                self.0.condvar_changed.io.wait_for(&mut available, delay);
            }
            delay = (delay * 2).min(Duration::from_secs(2));
        }
        let taken = Resources {
            cpu: available.cpu.min(desired_cpus),
            io: available.io.min(desired_io),
        };
        *available = Resources {
            cpu: available.cpu - taken.cpu,
            io: available.io - taken.io,
        };
        Some(ResourcePermit::new(taken, self.clone()))
    }

    pub fn replace_with(
        &self,
        mut permit: ResourcePermit,
        new_desired_cpus: usize,
        new_desired_io: usize,
        stopped: &AtomicBool,
    ) -> Result<ResourcePermit, ResourcePermit> {
        // Make sure we don't exceed the budget, otherwise we might deadlock
        let new_desired_cpus = new_desired_cpus.min(self.0.total_budget.cpu);
        let new_desired_io = new_desired_io.min(self.0.total_budget.io);

        // Acquire extra resources we don't have yet
        let Some(extra_acquired) = self.acquire(
            new_desired_cpus.saturating_sub(permit.acquired.cpu),
            new_desired_io.saturating_sub(permit.acquired.io),
            stopped,
        ) else {
            return Err(permit);
        };
        permit.merge(extra_acquired);

        // Release excess resources we now have
        permit.release(
            permit.acquired.cpu.saturating_sub(new_desired_cpus),
            permit.acquired.io.saturating_sub(new_desired_io),
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
        let min_required = self.min_permits(desired_cpus, desired_io);
        self.has_budget_exact(min_required.cpu, min_required.io)
    }

    /// Check if there are at least `budget` available CPUs in this budget.
    ///
    /// A budget of `0` will always return `true`.
    pub fn has_budget_exact(&self, cpu_budget: usize, io_budget: usize) -> bool {
        let Resources { cpu, io } = *self.0.available.lock();
        cpu >= cpu_budget && io >= io_budget
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
    pub async fn notify_on_budget_available(&self, desired_cpus: usize, desired_io: usize) {
        let min_required = self.min_permits(desired_cpus, desired_io);
        if self.has_budget_exact(min_required.cpu, min_required.io) {
            return;
        }

        // Wait for CPU budget to be available with exponential backoff
        // TODO: find better way, don't busy wait
        let mut delay = Duration::from_micros(100);
        while !self.has_budget_exact(min_required.cpu, min_required.io) {
            time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(10));
        }
    }

    /// Release the given number of CPUs and IO back to the budget.
    ///
    /// This private method is intended only for use by [`ResourcePermit`].
    fn release_resources(&self, Resources { cpu, io }: Resources<usize>) {
        if cpu == 0 && io == 0 {
            // Short-circuit to avoid locking
            return;
        }

        let mut available = self.0.available.lock();
        *available = Resources {
            cpu: available.cpu + cpu,
            io: available.io + io,
        };
        if cpu > 0 {
            self.0.condvar_changed.cpu.notify_all();
        }
        if io > 0 {
            self.0.condvar_changed.io.notify_all();
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
    acquired: Resources<usize>,

    /// Backing budget, which will get the resources back when this permit is dropped.
    budget: Option<ResourceBudget>,

    /// A callback, which should be called when the permit is changed manually.
    /// Originally used to notify the task manager that a permit is available
    /// and schedule more optimization tasks.
    ///
    /// WARN: is not called on drop, only when `release()` is called.
    on_manual_release: Option<Box<dyn Fn() + Send + Sync>>,
}

impl ResourcePermit {
    /// New CPU permit with given CPU count and permit semaphore.
    fn new(acquired: Resources<usize>, budget: ResourceBudget) -> Self {
        // Debug assert that cpu/io count and permit counts match
        Self {
            acquired,
            budget: Some(budget),
            on_manual_release: None,
        }
    }

    pub fn acquired(&self) -> Resources<usize> {
        self.acquired
    }

    pub fn set_on_manual_release(&mut self, on_release: impl Fn() + Send + Sync + 'static) {
        self.on_manual_release = Some(Box::new(on_release));
    }

    /// Merge the other resource permit into this one
    pub fn merge(&mut self, mut other: Self) {
        self.acquired = Resources {
            cpu: self.acquired.cpu + other.acquired.cpu,
            io: self.acquired.io + other.acquired.io,
        };
        other.acquired = Resources { cpu: 0, io: 0 };
    }

    /// New CPU permit with given CPU count without a backing semaphore for a shared pool.
    #[cfg(feature = "testing")]
    pub fn dummy(count: usize) -> Self {
        Self {
            acquired: Resources { cpu: count, io: 0 },
            budget: None,
            on_manual_release: None,
        }
    }

    fn release(&mut self, cpu: usize, io: usize) {
        let new = Resources {
            cpu: self.acquired.cpu.saturating_sub(cpu),
            io: self.acquired.io.saturating_sub(io),
        };
        if let Some(base) = &self.budget
            && new != self.acquired
        {
            base.release_resources(Resources {
                cpu: self.acquired.cpu - new.cpu,
                io: self.acquired.io - new.io,
            });
        }
        self.acquired = new;

        if let Some(on_release) = &self.on_manual_release {
            on_release();
        }
    }
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        let Self {
            acquired: taken,
            budget: base,
            on_manual_release: _, // Only explicit release() should call the callback
        } = self;

        if let Some(base) = base.take() {
            base.release_resources(*taken);
        }
    }
}
