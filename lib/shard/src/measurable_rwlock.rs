use std::{
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{self, AtomicU64},
    },
    time::Instant,
};

use parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};

pub mod measurable_parking_lot {
    pub use super::MeasurableRwLock as RwLock;
    pub use super::MeasurableRwLockReadGuard as RwLockReadGuard;
    pub use super::MeasurableRwLockUpgradableReadGuard as RwLockUpgradableReadGuard;
    pub use super::MeasurableRwLockWriteGuard as RwLockWriteGuard;
    pub use ::parking_lot::Mutex;
}

pub type MeasurableRwLockReadGuard<'rwlock, T> = RwLockReadGuard<'rwlock, T>;

#[derive(Debug)]
pub struct MeasurableRwLock<T: ?Sized> {
    read_wait_time_us_counter: Arc<AtomicU64>,
    write_wait_time_us_counter: Arc<AtomicU64>,
    upgrade_wait_us_counter: Arc<AtomicU64>,
    inner: RwLock<T>,
}

#[derive(Debug)]
pub struct MeasurableRwLockUpgradableReadGuard<'rwlock, T: ?Sized> {
    upgrade_wait_us_counter: &'rwlock AtomicU64,
    inner: RwLockUpgradableReadGuard<'rwlock, T>,
}

#[derive(Debug)]
pub struct MeasurableRwLockWriteGuard<'rwlock, T: ?Sized> {
    upgrade_wait_us_counter: &'rwlock AtomicU64,
    inner: RwLockWriteGuard<'rwlock, T>,
}

impl<T> MeasurableRwLock<T> {
    pub fn new(
        val: T,
        // read_wait_time_us_counter: Arc<AtomicU64>,
        // write_wait_time_us_counter: Arc<AtomicU64>,
        // upgrade_wait_us_counter: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner: RwLock::new(val),
            read_wait_time_us_counter: todo!(),
            write_wait_time_us_counter: todo!(),
            upgrade_wait_us_counter: todo!(),
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
        self.read_wait_time_us_counter.fetch_add(
            elapsed.as_micros() as _, // No way a lock can wait for 584942 years.
            atomic::Ordering::Relaxed,
        );
        inner
    }

    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        self.inner.try_read()
    }

    pub fn try_read_for(&self, timeout: std::time::Duration) -> Option<RwLockReadGuard<'_, T>> {
        let start = Instant::now();
        let inner = self.inner.try_read_for(timeout);
        let elapsed = start.elapsed();
        self.read_wait_time_us_counter
            .fetch_add(elapsed.as_micros() as _, atomic::Ordering::Relaxed);
        inner
    }

    pub fn upgradable_read(&self) -> MeasurableRwLockUpgradableReadGuard<'_, T> {
        let start = Instant::now();
        let inner = self.inner.upgradable_read();
        let elapsed = start.elapsed();
        self.read_wait_time_us_counter.fetch_add(
            elapsed.as_micros() as _, // No way a lock can wait for 584942 years.
            atomic::Ordering::Relaxed,
        );
        MeasurableRwLockUpgradableReadGuard {
            inner: inner,
            upgrade_wait_us_counter: &self.upgrade_wait_us_counter,
        }
    }

    pub fn write(&self) -> MeasurableRwLockWriteGuard<'_, T> {
        let start = Instant::now();
        let inner = self.inner.write();
        let elapsed = start.elapsed();
        self.write_wait_time_us_counter
            .fetch_add(elapsed.as_micros() as _, atomic::Ordering::Relaxed);
        MeasurableRwLockWriteGuard {
            upgrade_wait_us_counter: &self.upgrade_wait_us_counter,
            inner,
        }
    }

    pub fn try_write(&self) -> Option<MeasurableRwLockWriteGuard<'_, T>> {
        self.inner
            .try_write()
            .map(|inner| MeasurableRwLockWriteGuard {
                upgrade_wait_us_counter: &self.upgrade_wait_us_counter,
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
        MeasurableRwLockWriteGuard {
            upgrade_wait_us_counter: s.upgrade_wait_us_counter,
            inner,
        }
    }

    pub fn with_upgraded<Ret, F>(&mut self, f: F) -> Ret
    where
        F: FnOnce(&mut T) -> Ret,
    {
        let start = Instant::now();
        let result = self.inner.with_upgraded(f);
        // It include both upgrade time, f execution time and downgrade time.
        let elapsed = start.elapsed();
        self.upgrade_wait_us_counter
            .fetch_add(elapsed.as_micros() as _, atomic::Ordering::Relaxed);
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
