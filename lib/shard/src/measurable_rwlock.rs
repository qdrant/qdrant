use std::ops::{Deref, DerefMut};
use std::sync::atomic;
use std::time::Instant;

use parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};

pub mod measurable_parking_lot {
    pub use ::parking_lot::Mutex;

    pub use super::{
        MeasurableRwLock as RwLock, MeasurableRwLockReadGuard as RwLockReadGuard,
        MeasurableRwLockUpgradableReadGuard as RwLockUpgradableReadGuard,
        MeasurableRwLockWriteGuard as RwLockWriteGuard,
    };
}

#[derive(Debug, Default)]
pub struct MeasurableRwLockMetrics {
    pub read_counter: atomic::AtomicU64,
    pub write_counter: atomic::AtomicU64,
    pub upgrade_counter: atomic::AtomicU64,
    pub try_read_for_counter: atomic::AtomicU64,
    pub read_wait_time_us_counter: atomic::AtomicU64,
    pub write_wait_time_us_counter: atomic::AtomicU64,
    pub upgrade_wait_time_us_counter: atomic::AtomicU64,
    pub try_read_for_time_us_counter: atomic::AtomicU64,

    /// lock time
    pub read_lock_time: atomic::AtomicU64,
    pub write_lock_time: atomic::AtomicU64,

    /// Total operation time.
    pub total_time_us_counter: atomic::AtomicU64,
    pub total_counter: atomic::AtomicU64,
}

pub static READ_MEASURABLE_RWLOCK_METRICS: MeasurableRwLockMetrics = MeasurableRwLockMetrics {
    read_counter: atomic::AtomicU64::new(0),
    write_counter: atomic::AtomicU64::new(0),
    upgrade_counter: atomic::AtomicU64::new(0),
    try_read_for_counter: atomic::AtomicU64::new(0),
    read_wait_time_us_counter: atomic::AtomicU64::new(0),
    write_wait_time_us_counter: atomic::AtomicU64::new(0),
    upgrade_wait_time_us_counter: atomic::AtomicU64::new(0),
    try_read_for_time_us_counter: atomic::AtomicU64::new(0),

    read_lock_time: atomic::AtomicU64::new(0),
    write_lock_time: atomic::AtomicU64::new(0),

    total_time_us_counter: atomic::AtomicU64::new(0),
    total_counter: atomic::AtomicU64::new(0),
};

pub static WRITE_MEASURABLE_RWLOCK_METRICS: MeasurableRwLockMetrics = MeasurableRwLockMetrics {
    read_counter: atomic::AtomicU64::new(0),
    write_counter: atomic::AtomicU64::new(0),
    upgrade_counter: atomic::AtomicU64::new(0),
    try_read_for_counter: atomic::AtomicU64::new(0),
    read_wait_time_us_counter: atomic::AtomicU64::new(0),
    write_wait_time_us_counter: atomic::AtomicU64::new(0),
    upgrade_wait_time_us_counter: atomic::AtomicU64::new(0),
    try_read_for_time_us_counter: atomic::AtomicU64::new(0),

    read_lock_time: atomic::AtomicU64::new(0),
    write_lock_time: atomic::AtomicU64::new(0),

    total_time_us_counter: atomic::AtomicU64::new(0),
    total_counter: atomic::AtomicU64::new(0),
};

thread_local! {
    pub static CURRENT_MEASURABLE_RWLOCK_METRICS: atomic::AtomicPtr<MeasurableRwLockMetrics> =
        const { atomic::AtomicPtr::new(std::ptr::null_mut()) };
}

pub fn log_metrics(prefix: &str, m: &MeasurableRwLockMetrics) {
    use std::sync::atomic::Ordering::Relaxed;

    let read_time = m.read_wait_time_us_counter.load(Relaxed) as f64 / 1e6;
    let reads = m.read_counter.load(Relaxed);
    let write_time = m.write_wait_time_us_counter.load(Relaxed) as f64 / 1e6;
    let writes = m.write_counter.load(Relaxed);
    let upgrade_time = m.upgrade_wait_time_us_counter.load(Relaxed) as f64 / 1e6;
    let upgrades = m.upgrade_counter.load(Relaxed);

    let try_read_for_time = m.try_read_for_time_us_counter.load(Relaxed) as f64 / 1e6;
    let try_reads_for = m.try_read_for_counter.load(Relaxed);

    let total_time = m.total_time_us_counter.load(Relaxed) as f64 / 1e6;
    let total_count = m.total_counter.load(Relaxed);

    let locked_read_time: f64 = m.read_lock_time.load(Relaxed) as f64 / 1e6;
    let locked_write_time: f64 = m.write_lock_time.load(Relaxed) as f64 / 1e6;

    log::info!("*** {prefix}, all time in seconds (and number of executions)");
    log::info!("|-> wait times in sec:");
    log::info!("    read: {read_time} ({reads}), ");
    log::info!("    write: {write_time} ({writes}),");
    log::info!("    upgrade: {upgrade_time} ({upgrades})");
    log::info!("    try_read_for times: {try_read_for_time} ({try_reads_for})");
    log::info!("|-> locked times:");
    log::info!("    locked read: {locked_read_time} ({reads}), ");
    log::info!("    locked write: {locked_write_time} ({writes}),");
    log::info!("\\-> total operation time: {total_time} ({total_count})");
}

pub struct MeasureOperation {
    operation_start: Instant,
    prev_metrics: Option<&'static MeasurableRwLockMetrics>,
}

impl Drop for MeasureOperation {
    fn drop(&mut self) {
        let elapsed = self.operation_start.elapsed();
        let elapsed_us = elapsed.as_micros() as u64;
        let current_metrics = get_current_measurable_rwlock_metrics()
            .expect("Unitialized metrics in MeasureOperation drop");
        current_metrics
            .total_time_us_counter
            .fetch_add(elapsed_us, atomic::Ordering::Relaxed);
        current_metrics
            .total_counter
            .fetch_add(1, atomic::Ordering::Relaxed);
        CURRENT_MEASURABLE_RWLOCK_METRICS.with(|ptr| {
            ptr.store(
                match self.prev_metrics {
                    Some(m) => m as *const _ as *mut _,
                    None => std::ptr::null_mut(),
                },
                atomic::Ordering::Release,
            )
        });
    }
}

impl MeasureOperation {
    pub fn new(new_metrics: &'static MeasurableRwLockMetrics) -> Self {
        let prev_metrics = get_current_measurable_rwlock_metrics();
        CURRENT_MEASURABLE_RWLOCK_METRICS
            .with(|ptr| ptr.store(new_metrics as *const _ as *mut _, atomic::Ordering::Release));
        let start = Instant::now();
        Self {
            operation_start: start,
            prev_metrics,
        }
    }
}

#[allow(dead_code)]
pub struct MeasureRead(MeasureOperation);

impl Default for MeasureRead {
    fn default() -> Self {
        Self(MeasureOperation::new(&READ_MEASURABLE_RWLOCK_METRICS))
    }
}

#[allow(dead_code)]
pub struct MeasureWrite(MeasureOperation);

impl Default for MeasureWrite {
    fn default() -> Self {
        Self(MeasureOperation::new(&WRITE_MEASURABLE_RWLOCK_METRICS))
    }
}

fn get_current_measurable_rwlock_metrics() -> Option<&'static MeasurableRwLockMetrics> {
    CURRENT_MEASURABLE_RWLOCK_METRICS.with(|ptr| {
        let p = ptr.load(atomic::Ordering::Acquire);
        if p.is_null() {
            None
        } else {
            Some(unsafe { &*p })
        }
    })
}

#[derive(Debug)]
struct Measurer {
    start_instant: Instant,
    store: &'static atomic::AtomicU64,
}

impl Measurer {
    fn start(store: &'static atomic::AtomicU64) -> Self {
        Self {
            start_instant: Instant::now(),
            store,
        }
    }
}

impl Drop for Measurer {
    fn drop(&mut self) {
        self.store.fetch_add(
            self.start_instant.elapsed().as_micros() as _,
            atomic::Ordering::Relaxed,
        );
    }
}

#[derive(Debug)]
pub struct MeasurableRwLock<T: ?Sized> {
    inner: RwLock<T>,
}

#[derive(Debug)]
pub struct MeasurableRwLockReadGuard<'rwlock, T: ?Sized> {
    #[allow(unused)]
    lock_measurer: Measurer,
    inner: RwLockReadGuard<'rwlock, T>,
}

#[derive(Debug)]
pub struct MeasurableRwLockUpgradableReadGuard<'rwlock, T: ?Sized> {
    upgrade_wait_us_counter: &'static atomic::AtomicU64,
    upgrade_counter: &'static atomic::AtomicU64,
    #[allow(unused)]
    lock_measurer: Measurer,
    inner: RwLockUpgradableReadGuard<'rwlock, T>,
}

#[derive(Debug)]
pub struct MeasurableRwLockWriteGuard<'rwlock, T: ?Sized> {
    upgrade_wait_us_counter: &'static atomic::AtomicU64,
    upgrade_counter: &'static atomic::AtomicU64,
    #[allow(unused)]
    lock_measurer: Measurer,
    inner: RwLockWriteGuard<'rwlock, T>,
}

impl<T> MeasurableRwLock<T> {
    pub fn new(val: T) -> Self {
        Self {
            inner: RwLock::new(val),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }
}

impl<T: ?Sized> MeasurableRwLock<T> {
    pub fn read(&self) -> MeasurableRwLockReadGuard<'_, T> {
        let metrics = get_current_measurable_rwlock_metrics().expect("Unitialized metrics");

        let start = Instant::now();
        let inner = self.inner.read();
        let elapsed = start.elapsed();
        let lock_measurer = Measurer::start(
            &get_current_measurable_rwlock_metrics()
                .expect("Measurer unitialized")
                .read_lock_time,
        );
        metrics.read_wait_time_us_counter.fetch_add(
            elapsed.as_micros() as _, // No way a lock can wait for 584942 years.
            atomic::Ordering::Relaxed,
        );
        metrics.read_counter.fetch_add(1, atomic::Ordering::Relaxed);
        MeasurableRwLockReadGuard {
            lock_measurer,
            inner,
        }
    }

    pub fn try_read(&self) -> Option<MeasurableRwLockReadGuard<'_, T>> {
        let inner = self.inner.try_read();
        inner.map(|inner| {
            let lock_measurer = Measurer::start(
                &get_current_measurable_rwlock_metrics()
                    .expect("Measurer unitialized")
                    .read_lock_time,
            );
            MeasurableRwLockReadGuard {
                lock_measurer,
                inner,
            }
        })
    }

    pub fn try_read_for(
        &self,
        timeout: std::time::Duration,
    ) -> Option<MeasurableRwLockReadGuard<'_, T>> {
        let metrics = get_current_measurable_rwlock_metrics().expect("Unitialized metrics");

        let start = Instant::now();
        let inner = self.inner.try_read_for(timeout);
        let elapsed = start.elapsed();
        let lock_measurer = Measurer::start(
            &get_current_measurable_rwlock_metrics()
                .expect("Measurer unitialized")
                .read_lock_time,
        );
        metrics
            .try_read_for_time_us_counter
            .fetch_add(elapsed.as_micros() as _, atomic::Ordering::Relaxed);
        metrics
            .try_read_for_counter
            .fetch_add(1, atomic::Ordering::Relaxed);
        inner.map(|inner| MeasurableRwLockReadGuard {
            lock_measurer,
            inner,
        })
    }

    pub fn upgradable_read(&self) -> MeasurableRwLockUpgradableReadGuard<'_, T> {
        let metrics = get_current_measurable_rwlock_metrics().expect("Unitialized metrics");

        let start = Instant::now();
        let inner = self.inner.upgradable_read();
        let elapsed = start.elapsed();
        let lock_measurer = Measurer::start(
            &get_current_measurable_rwlock_metrics()
                .expect("Measurer unitialized")
                .read_lock_time,
        );
        metrics.read_wait_time_us_counter.fetch_add(
            elapsed.as_micros() as _, // No way a lock can wait for 584942 years.
            atomic::Ordering::Relaxed,
        );
        metrics.read_counter.fetch_add(1, atomic::Ordering::Relaxed);
        MeasurableRwLockUpgradableReadGuard {
            upgrade_wait_us_counter: &metrics.upgrade_wait_time_us_counter,
            upgrade_counter: &metrics.upgrade_counter,
            lock_measurer,
            inner,
        }
    }

    pub fn write(&self) -> MeasurableRwLockWriteGuard<'_, T> {
        let metrics = get_current_measurable_rwlock_metrics().expect("Unitialized metrics");

        let start = Instant::now();
        let inner = self.inner.write();
        let elapsed = start.elapsed();

        let lock_measurer = Measurer::start(
            &get_current_measurable_rwlock_metrics()
                .expect("Measurer unitialized")
                .write_lock_time,
        );

        metrics
            .write_wait_time_us_counter
            .fetch_add(elapsed.as_micros() as _, atomic::Ordering::Relaxed);
        metrics
            .write_counter
            .fetch_add(1, atomic::Ordering::Relaxed);
        MeasurableRwLockWriteGuard {
            upgrade_wait_us_counter: &metrics.upgrade_wait_time_us_counter,
            upgrade_counter: &metrics.upgrade_counter,
            lock_measurer,
            inner,
        }
    }

    pub fn try_write(&self) -> Option<MeasurableRwLockWriteGuard<'_, T>> {
        let metrics = get_current_measurable_rwlock_metrics().expect("Unitialized metrics");

        self.inner.try_write().map(|inner| {
            let lock_measurer = Measurer::start(
                &get_current_measurable_rwlock_metrics()
                    .expect("Measurer unitialized")
                    .write_lock_time,
            );
            MeasurableRwLockWriteGuard {
                upgrade_wait_us_counter: &metrics.upgrade_wait_time_us_counter,
                upgrade_counter: &metrics.upgrade_counter,
                lock_measurer,
                inner,
            }
        })
    }
}

impl<'rwlock, T: ?Sized + 'rwlock> MeasurableRwLockUpgradableReadGuard<'rwlock, T> {
    pub fn upgrade(s: Self) -> MeasurableRwLockWriteGuard<'rwlock, T> {
        let start = Instant::now();
        let inner = RwLockUpgradableReadGuard::upgrade(s.inner);
        let elapsed = start.elapsed();

        let lock_measurer = Measurer::start(
            &get_current_measurable_rwlock_metrics()
                .expect("Measurer unitialized")
                .write_lock_time,
        );
        s.upgrade_wait_us_counter
            .fetch_add(elapsed.as_micros() as _, atomic::Ordering::Relaxed);
        s.upgrade_counter.fetch_add(1, atomic::Ordering::Relaxed);

        MeasurableRwLockWriteGuard {
            upgrade_wait_us_counter: s.upgrade_wait_us_counter,
            upgrade_counter: s.upgrade_counter,
            lock_measurer,
            inner,
        }
    }

    pub fn with_upgraded<Ret, F>(&mut self, f: F) -> Ret
    where
        F: FnOnce(&mut T) -> Ret,
    {
        let mut pre_duration = 0u64;
        let mut start = Instant::now();
        let result = self.inner.with_upgraded(|t| {
            pre_duration += start.elapsed().as_micros() as u64;

            let metrics = get_current_measurable_rwlock_metrics().expect("Metrics uninitialized");
            let _lock_measurer = Measurer::start(&metrics.write_lock_time);

            let result = f(t);
            start = Instant::now();
            result
        });
        // It include upgrade time and downgrade time without f(...) execution time.
        let elapsed = start.elapsed();
        self.upgrade_wait_us_counter.fetch_add(
            elapsed.as_micros() as u64 + pre_duration,
            atomic::Ordering::Relaxed,
        );
        self.upgrade_counter.fetch_add(1, atomic::Ordering::Relaxed);
        result
    }
}

impl<'rwlock, T: ?Sized> Deref for MeasurableRwLockReadGuard<'rwlock, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'rwlock, T: ?Sized> Deref for MeasurableRwLockUpgradableReadGuard<'rwlock, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'rwlock, T: ?Sized> MeasurableRwLockWriteGuard<'rwlock, T> {
    pub fn unlock_fair(s: Self) {
        RwLockWriteGuard::unlock_fair(s.inner);
    }

    pub fn downgrade_to_upgradable(s: Self) -> MeasurableRwLockUpgradableReadGuard<'rwlock, T> {
        let start = Instant::now();
        let inner = RwLockWriteGuard::downgrade_to_upgradable(s.inner);
        let elapsed = start.elapsed();

        let lock_measurer = Measurer::start(
            &get_current_measurable_rwlock_metrics()
                .expect("Measurer unitialized")
                .read_lock_time,
        );
        s.upgrade_wait_us_counter
            .fetch_add(elapsed.as_micros() as _, atomic::Ordering::Relaxed);
        MeasurableRwLockUpgradableReadGuard {
            upgrade_wait_us_counter: s.upgrade_wait_us_counter,
            upgrade_counter: s.upgrade_counter,
            lock_measurer,
            inner,
        }
    }
}

impl<'rwlock, T: ?Sized> Deref for MeasurableRwLockWriteGuard<'rwlock, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'rwlock, T: ?Sized> DerefMut for MeasurableRwLockWriteGuard<'rwlock, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
