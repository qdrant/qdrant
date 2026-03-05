use std::cell::Cell;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic;
use std::time::Instant;

pub mod parking_lot {
    pub use super::{
        MeasurableMutex as Mutex, MeasurableMutexGuard as MutexGuard, MeasurableRwLock as RwLock,
        MeasurableRwLockReadGuard as RwLockReadGuard,
        MeasurableRwLockUpgradableReadGuard as RwLockUpgradableReadGuard,
        MeasurableRwLockWriteGuard as RwLockWriteGuard,
    };
}

type MeasurableMapMutex = ::parking_lot::Mutex<
    HashMap<&'static str, &'static MeasurableRwLockMetrics, ahash::RandomState>,
>;

pub struct Measurable {
    /// Total operation time.
    total_time_us_counter: atomic::AtomicU64,
    total_counter: atomic::AtomicU64,
    locks: MeasurableMapMutex,
}

impl Measurable {
    const fn new() -> Self {
        Self {
            total_time_us_counter: atomic::AtomicU64::new(0),
            total_counter: atomic::AtomicU64::new(0),
            locks: ::parking_lot::Mutex::new(HashMap::with_hasher(ahash::RandomState::with_seeds(
                1, 42, 3, 0,
            ))),
        }
    }
}

pub static DEFAULT_MEASURABLE: Measurable = Measurable::new();
pub static READ_MEASURABLE: Measurable = Measurable::new();
pub static WRITE_MEASURABLE: Measurable = Measurable::new();
pub static INSTANTIATION_COUNTS: ::parking_lot::Mutex<
    HashMap<&'static str, usize, ahash::RandomState>,
> = ::parking_lot::Mutex::new(HashMap::with_hasher(ahash::RandomState::with_seeds(
    1, 42, 8, 0,
)));

#[derive(Debug, Clone, Copy, Default)]
pub enum OperationType {
    #[default]
    None,
    Read,
    Write,
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

    /// read lock time
    pub read_lock_time: atomic::AtomicU64,
    /// write lock time
    pub write_lock_time: atomic::AtomicU64,
    /// actually is a write time operation, but accounted separately
    pub upgrade_lock_time: atomic::AtomicU64,
}

impl MeasurableRwLockMetrics {
    pub const fn new() -> Self {
        Self {
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
            upgrade_lock_time: atomic::AtomicU64::new(0),
        }
    }
}

thread_local! {
    // pub static CURRENT_MEASURABLE: Cell<*const Measurable> = Cell::new(std::ptr::null());
    pub static CURRENT_MEASURABLE: Cell<*const Measurable> = Cell::new(&DEFAULT_MEASURABLE as *const _);
}

pub fn log_lock_metrics(op: &str, prefix: &str, m: &MeasurableRwLockMetrics) {
    use std::sync::atomic::Ordering::Relaxed;

    let read_time = m.read_wait_time_us_counter.load(Relaxed) as f64 / 1e6;
    let reads = m.read_counter.load(Relaxed);
    let write_time = m.write_wait_time_us_counter.load(Relaxed) as f64 / 1e6;
    let writes = m.write_counter.load(Relaxed);
    let upgrade_time = m.upgrade_wait_time_us_counter.load(Relaxed) as f64 / 1e6;
    let upgrades = m.upgrade_counter.load(Relaxed);

    let try_read_for_time = m.try_read_for_time_us_counter.load(Relaxed) as f64 / 1e6;
    let try_reads_for = m.try_read_for_counter.load(Relaxed);

    let locked_read_time: f64 = m.read_lock_time.load(Relaxed) as f64 / 1e6;
    let locked_write_time: f64 = m.write_lock_time.load(Relaxed) as f64 / 1e6;

    let tab = "   ";
    log::info!("|->*** {op}/{prefix}, all time in seconds (and number of executions)");
    log::info!("{tab}|-> wait times in sec:");
    log::info!("{tab}    read: {read_time} ({reads}), ");
    log::info!("{tab}    write: {write_time} ({writes}),");
    log::info!("{tab}    upgrade: {upgrade_time} ({upgrades})");
    log::info!("{tab}    try_read_for times: {try_read_for_time} ({try_reads_for})");
    log::info!("{tab}|-> locked times:");
    log::info!("{tab}    locked read: {locked_read_time} ({reads}), ");
    log::info!("{tab}    locked write: {locked_write_time} ({writes}),");
}

pub fn log_operation_metrics(op: &str, m: &Measurable) {
    use std::sync::atomic::Ordering::Relaxed;

    log::info!("*** {op}, all time in seconds (and number of executions)");

    let total_time = m.total_time_us_counter.load(Relaxed) as f64 / 1e6;
    let total_count = m.total_counter.load(Relaxed);

    let items = m.locks.lock().clone();
    for (tag, lock_metrics) in items {
        log_lock_metrics(op, tag, lock_metrics);
    }
    log::info!("\\-> total operation time: {total_time} ({total_count})");
}

pub fn log_all_metrics() {
    log::info!("*** Instantiation counts:");
    let counts = INSTANTIATION_COUNTS.lock().clone();
    for (tag, count) in counts {
        log::info!("    {tag}: {count}");
    }

    log_operation_metrics("default", &DEFAULT_MEASURABLE);
    log_operation_metrics("read", &READ_MEASURABLE);
    log_operation_metrics("write", &WRITE_MEASURABLE);
}

pub struct MeasureOperation {
    operation_start: Instant,
    prev_metrics: Option<&'static Measurable>,
}

impl Drop for MeasureOperation {
    fn drop(&mut self) {
        let elapsed = self.operation_start.elapsed();
        let elapsed_us = elapsed.as_micros() as u64;
        let current_measurable =
            get_current_measurable().expect("Unitialized metrics in MeasureOperation drop");
        current_measurable
            .total_time_us_counter
            .fetch_add(elapsed_us, atomic::Ordering::Relaxed);
        current_measurable
            .total_counter
            .fetch_add(1, atomic::Ordering::Relaxed);
        CURRENT_MEASURABLE.with(|ptr| {
            ptr.set(match self.prev_metrics {
                Some(m) => m as *const _,
                None => std::ptr::null_mut(),
            })
        });
    }
}

impl MeasureOperation {
    pub fn new(new_metrics: &'static Measurable) -> Self {
        let prev_metrics = get_current_measurable();
        CURRENT_MEASURABLE.with(|ptr| ptr.set(new_metrics as *const _));
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
        Self(MeasureOperation::new(&READ_MEASURABLE))
    }
}

#[allow(dead_code)]
pub struct MeasureWrite(MeasureOperation);

impl Default for MeasureWrite {
    fn default() -> Self {
        Self(MeasureOperation::new(&WRITE_MEASURABLE))
    }
}

fn get_current_measurable() -> Option<&'static Measurable> {
    CURRENT_MEASURABLE.with(|ptr| unsafe { ptr.get().as_ref() })
}

fn get_current_measurable_rwlock_metrics(
    tag: &'static str,
) -> Option<&'static MeasurableRwLockMetrics> {
    CURRENT_MEASURABLE
        .with(|ptr| unsafe { ptr.get().as_ref() })
        .map(|m| {
            let mut locks = m.locks.lock();
            *locks
                .entry(tag)
                .or_insert(Box::leak(Box::new(MeasurableRwLockMetrics::new())))
        })
}

#[derive(Debug)]
struct Measurer {
    start_instant: Instant,
    correction: u64,
    store: &'static atomic::AtomicU64,
}

impl Measurer {
    fn start(store: &'static atomic::AtomicU64) -> Self {
        Self {
            start_instant: Instant::now(),
            correction: 0,
            store,
        }
    }
}

impl Drop for Measurer {
    fn drop(&mut self) {
        let elapsed = self.start_instant.elapsed().as_micros() as u64;
        let corrected = elapsed.saturating_sub(self.correction);
        self.store.fetch_add(corrected, atomic::Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct MeasurableRwLock<T: ?Sized> {
    tag: &'static str,
    inner: ::parking_lot::RwLock<T>,
}

#[derive(Debug)]
pub struct MeasurableRwLockReadGuard<'rwlock, T: ?Sized> {
    _lock_measurer: Measurer,
    inner: ::parking_lot::RwLockReadGuard<'rwlock, T>,
}

#[derive(Debug)]
pub struct MeasurableRwLockUpgradableReadGuard<'rwlock, T: ?Sized> {
    tag: &'static str,
    upgrade_wait_us_counter: &'static atomic::AtomicU64,
    upgrade_counter: &'static atomic::AtomicU64,
    _lock_measurer: Measurer,
    inner: ::parking_lot::RwLockUpgradableReadGuard<'rwlock, T>,
}

#[derive(Debug)]
pub struct MeasurableRwLockWriteGuard<'rwlock, T: ?Sized> {
    _lock_measurer: Measurer,
    inner: ::parking_lot::RwLockWriteGuard<'rwlock, T>,
}

impl<T> MeasurableRwLock<T> {
    pub fn new(tag: &'static str, val: T) -> Self {
        *INSTANTIATION_COUNTS.lock().entry(tag).or_default() += 1;

        Self {
            tag,
            inner: ::parking_lot::RwLock::new(val),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }
}

impl<T: ?Sized> MeasurableRwLock<T> {
    pub fn read(&self) -> MeasurableRwLockReadGuard<'_, T> {
        let metrics =
            get_current_measurable_rwlock_metrics(&self.tag).expect("Unitialized metrics");

        let start = Instant::now();
        let inner = self.inner.read();
        let elapsed = start.elapsed();
        let lock_measurer = Measurer::start(&metrics.read_lock_time);
        metrics.read_wait_time_us_counter.fetch_add(
            elapsed.as_micros() as _, // No way a lock can wait for 584942 years.
            atomic::Ordering::Relaxed,
        );
        metrics.read_counter.fetch_add(1, atomic::Ordering::Relaxed);
        MeasurableRwLockReadGuard {
            _lock_measurer: lock_measurer,
            inner,
        }
    }

    pub fn try_read(&self) -> Option<MeasurableRwLockReadGuard<'_, T>> {
        let inner = self.inner.try_read();
        inner.map(|inner| {
            let metrics =
                get_current_measurable_rwlock_metrics(self.tag).expect("Measurer unitialized");
            let lock_measurer = Measurer::start(&metrics.read_lock_time);
            MeasurableRwLockReadGuard {
                _lock_measurer: lock_measurer,
                inner,
            }
        })
    }

    pub fn try_read_for(
        &self,
        timeout: std::time::Duration,
    ) -> Option<MeasurableRwLockReadGuard<'_, T>> {
        let metrics = get_current_measurable_rwlock_metrics(self.tag).expect("Unitialized metrics");

        let start = Instant::now();
        let inner = self.inner.try_read_for(timeout);
        let elapsed = start.elapsed();
        let lock_measurer = Measurer::start(&metrics.read_lock_time);
        metrics
            .try_read_for_time_us_counter
            .fetch_add(elapsed.as_micros() as _, atomic::Ordering::Relaxed);
        metrics
            .try_read_for_counter
            .fetch_add(1, atomic::Ordering::Relaxed);
        inner.map(|inner| MeasurableRwLockReadGuard {
            _lock_measurer: lock_measurer,
            inner,
        })
    }

    pub fn upgradable_read(&self) -> MeasurableRwLockUpgradableReadGuard<'_, T> {
        let metrics = get_current_measurable_rwlock_metrics(self.tag).expect("Unitialized metrics");

        let start = Instant::now();
        let inner = self.inner.upgradable_read();
        let elapsed = start.elapsed();
        let lock_measurer = Measurer::start(&metrics.read_lock_time);
        metrics.read_wait_time_us_counter.fetch_add(
            elapsed.as_micros() as _, // No way a lock can wait for 584942 years.
            atomic::Ordering::Relaxed,
        );
        metrics.read_counter.fetch_add(1, atomic::Ordering::Relaxed);
        MeasurableRwLockUpgradableReadGuard {
            tag: self.tag,
            upgrade_wait_us_counter: &metrics.upgrade_wait_time_us_counter,
            upgrade_counter: &metrics.upgrade_counter,
            _lock_measurer: lock_measurer,
            inner,
        }
    }

    pub fn write(&self) -> MeasurableRwLockWriteGuard<'_, T> {
        let metrics = get_current_measurable_rwlock_metrics(self.tag).expect("Unitialized metrics");

        let start = Instant::now();
        let inner = self.inner.write();
        let elapsed = start.elapsed();

        let lock_measurer = Measurer::start(&metrics.write_lock_time);

        metrics
            .write_wait_time_us_counter
            .fetch_add(elapsed.as_micros() as _, atomic::Ordering::Relaxed);
        metrics
            .write_counter
            .fetch_add(1, atomic::Ordering::Relaxed);
        MeasurableRwLockWriteGuard {
            _lock_measurer: lock_measurer,
            inner,
        }
    }

    pub fn try_write(&self) -> Option<MeasurableRwLockWriteGuard<'_, T>> {
        self.inner.try_write().map(|inner| {
            let lock_measurer = Measurer::start(
                &get_current_measurable_rwlock_metrics(self.tag)
                    .expect("Measurer unitialized")
                    .write_lock_time,
            );
            MeasurableRwLockWriteGuard {
                _lock_measurer: lock_measurer,
                inner,
            }
        })
    }
}

impl<'rwlock, T: ?Sized + 'rwlock> MeasurableRwLockUpgradableReadGuard<'rwlock, T> {
    pub fn with_upgraded<Ret, F>(&mut self, f: F) -> Ret
    where
        F: FnOnce(&mut T) -> Ret,
    {
        let mut upgrade_wait_time = 0u64;
        let mut start = Instant::now();
        let metrics =
            get_current_measurable_rwlock_metrics(self.tag).expect("Metrics uninitialized");

        let result = self.inner.with_upgraded(|t| {
            upgrade_wait_time += start.elapsed().as_micros() as u64;

            let _lock_measurer = Measurer::start(&metrics.upgrade_lock_time);

            let result = f(t);
            start = Instant::now();
            result
        });
        // It include upgrade time and f(...) execution time.
        let total_elapsed = start.elapsed();

        // Adjust read lock measurer to not count upgrade time.
        self._lock_measurer.correction += total_elapsed.as_micros() as u64;

        self.upgrade_wait_us_counter
            .fetch_add(upgrade_wait_time, atomic::Ordering::Relaxed);
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
        ::parking_lot::RwLockWriteGuard::unlock_fair(s.inner);
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

#[derive(Debug)]
pub struct MeasurableMutex<T: ?Sized> {
    tag: &'static str,
    inner: ::parking_lot::Mutex<T>,
}

impl<T> MeasurableMutex<T> {
    pub fn new(tag: &'static str, inner: T) -> Self {
        *INSTANTIATION_COUNTS.lock().entry(tag).or_default() += 1;

        Self {
            tag,
            inner: ::parking_lot::Mutex::new(inner),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    pub fn lock(&self) -> MeasurableMutexGuard<'_, T> {
        let metrics = get_current_measurable_rwlock_metrics(self.tag).expect("Unitialized metrics");

        let start = Instant::now();
        let inner = self.inner.lock();
        let elapsed = start.elapsed();
        let lock_measurer = Measurer::start(&metrics.write_lock_time);
        metrics
            .write_wait_time_us_counter
            .fetch_add(elapsed.as_micros() as _, atomic::Ordering::Relaxed);
        metrics
            .write_counter
            .fetch_add(1, atomic::Ordering::Relaxed);
        MeasurableMutexGuard {
            _lock_measurer: lock_measurer,
            inner,
        }
    }
}

pub struct MeasurableMutexGuard<'mutex, T: ?Sized> {
    _lock_measurer: Measurer,
    inner: ::parking_lot::MutexGuard<'mutex, T>,
}

impl<'mutex, T: ?Sized> Deref for MeasurableMutexGuard<'mutex, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'mutex, T: ?Sized> DerefMut for MeasurableMutexGuard<'mutex, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
