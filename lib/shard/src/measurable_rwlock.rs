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

pub type MeasurableRwLockReadGuard<'rwlock, T> = RwLockReadGuard<'rwlock, T>;

#[derive(Debug, Default)]
pub struct MeasurableRwLockMetrics {
    pub read_counter: atomic::AtomicU64,
    pub write_counter: atomic::AtomicU64,
    pub upgrade_counter: atomic::AtomicU64,
    pub read_wait_time_us_counter: atomic::AtomicU64,
    pub write_wait_time_us_counter: atomic::AtomicU64,
    pub upgrade_wait_time_us_counter: atomic::AtomicU64,
}

pub static MEASURABLE_RWLOCK_METRICS_DISABLED: MeasurableRwLockMetrics = MeasurableRwLockMetrics {
    read_counter: atomic::AtomicU64::new(0),
    write_counter: atomic::AtomicU64::new(0),
    upgrade_counter: atomic::AtomicU64::new(0),
    read_wait_time_us_counter: atomic::AtomicU64::new(0),
    write_wait_time_us_counter: atomic::AtomicU64::new(0),
    upgrade_wait_time_us_counter: atomic::AtomicU64::new(0),
};

#[derive(Debug)]
pub struct MeasurableRwLock<T: ?Sized> {
    metrics: &'static MeasurableRwLockMetrics,
    inner: RwLock<T>,
}

#[derive(Debug)]
pub struct MeasurableRwLockUpgradableReadGuard<'rwlock, T: ?Sized> {
    upgrade_wait_us_counter: &'static atomic::AtomicU64,
    upgrade_counter: &'static atomic::AtomicU64,
    inner: RwLockUpgradableReadGuard<'rwlock, T>,
}

#[derive(Debug)]
pub struct MeasurableRwLockWriteGuard<'rwlock, T: ?Sized> {
    upgrade_wait_us_counter: &'static atomic::AtomicU64,
    upgrade_counter: &'static atomic::AtomicU64,
    inner: RwLockWriteGuard<'rwlock, T>,
}

impl<T> MeasurableRwLock<T> {
    pub fn new(val: T) -> Self {
        Self {
            inner: RwLock::new(val),
            metrics: &MEASURABLE_RWLOCK_METRICS_DISABLED,
        }
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }
}

impl<T: ?Sized> MeasurableRwLock<T> {
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        let start = Instant::now();
        let inner = self.inner.read();
        let elapsed = start.elapsed();
        self.metrics.read_wait_time_us_counter.fetch_add(
            elapsed.as_micros() as _, // No way a lock can wait for 584942 years.
            atomic::Ordering::Relaxed,
        );
        self.metrics
            .read_counter
            .fetch_add(1, atomic::Ordering::Relaxed);
        inner
    }

    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        self.inner.try_read()
    }

    pub fn try_read_for(&self, timeout: std::time::Duration) -> Option<RwLockReadGuard<'_, T>> {
        let inner = self.inner.try_read_for(timeout);
        inner
    }

    pub fn upgradable_read(&self) -> MeasurableRwLockUpgradableReadGuard<'_, T> {
        let start = Instant::now();
        let inner = self.inner.upgradable_read();
        let elapsed = start.elapsed();
        self.metrics.read_wait_time_us_counter.fetch_add(
            elapsed.as_micros() as _, // No way a lock can wait for 584942 years.
            atomic::Ordering::Relaxed,
        );
        self.metrics
            .read_counter
            .fetch_add(1, atomic::Ordering::Relaxed);
        MeasurableRwLockUpgradableReadGuard {
            upgrade_wait_us_counter: &self.metrics.upgrade_wait_time_us_counter,
            upgrade_counter: &self.metrics.upgrade_counter,
            inner: inner,
        }
    }

    pub fn write(&self) -> MeasurableRwLockWriteGuard<'_, T> {
        let start = Instant::now();
        let inner = self.inner.write();
        let elapsed = start.elapsed();
        self.metrics
            .write_wait_time_us_counter
            .fetch_add(elapsed.as_micros() as _, atomic::Ordering::Relaxed);
        self.metrics
            .write_counter
            .fetch_add(1, atomic::Ordering::Relaxed);
        MeasurableRwLockWriteGuard {
            upgrade_wait_us_counter: &self.metrics.upgrade_wait_time_us_counter,
            upgrade_counter: &self.metrics.upgrade_counter,
            inner,
        }
    }

    pub fn try_write(&self) -> Option<MeasurableRwLockWriteGuard<'_, T>> {
        self.inner
            .try_write()
            .map(|inner| MeasurableRwLockWriteGuard {
                upgrade_wait_us_counter: &self.metrics.upgrade_wait_time_us_counter,
                upgrade_counter: &self.metrics.upgrade_counter,
                inner,
            })
    }
}

impl<'rwlock, T: ?Sized + 'rwlock> MeasurableRwLockUpgradableReadGuard<'rwlock, T> {
    pub fn upgrade(s: Self) -> MeasurableRwLockWriteGuard<'rwlock, T> {
        let start = Instant::now();
        let inner = RwLockUpgradableReadGuard::upgrade(s.inner);
        let elapsed = start.elapsed();
        s.upgrade_wait_us_counter
            .fetch_add(elapsed.as_micros() as _, atomic::Ordering::Relaxed);
        s.upgrade_counter.fetch_add(1, atomic::Ordering::Relaxed);
        MeasurableRwLockWriteGuard {
            upgrade_wait_us_counter: s.upgrade_wait_us_counter,
            upgrade_counter: s.upgrade_counter,
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

impl<'rwlock, T: ?Sized> Deref for MeasurableRwLockUpgradableReadGuard<'rwlock, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &*self.inner
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
        s.upgrade_wait_us_counter
            .fetch_add(elapsed.as_micros() as _, atomic::Ordering::Relaxed);
        MeasurableRwLockUpgradableReadGuard {
            upgrade_wait_us_counter: s.upgrade_wait_us_counter,
            upgrade_counter: s.upgrade_counter,
            inner,
        }
    }
}

impl<'rwlock, T: ?Sized> Deref for MeasurableRwLockWriteGuard<'rwlock, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &*self.inner
    }
}

impl<'rwlock, T: ?Sized> DerefMut for MeasurableRwLockWriteGuard<'rwlock, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.inner
    }
}
