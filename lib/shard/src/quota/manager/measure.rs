//! The node's readings of memory and disk, and the freshness policy applied to
//! them. Nothing outside this file calls `statvfs` or reads process RSS.

use std::collections::hash_map::Entry;
use std::path::Path;
use std::sync::Arc;

use ::common::disk_usage::DiskUsage;

use super::QuotaManager;
use crate::quota::check::percent_of;
use crate::quota::meter::{Meter, reusable};
use crate::quota::status::QuotaUsage;

/// Whether the disk has room for a piece of work — see
/// [`QuotaManager::fits_on_disk`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskFit {
    /// Room to spare, with this many bytes free.
    Fits { available: u64 },
    /// Not enough room: `required` bytes are needed, `available` are free.
    TooLarge { available: u64, required: u64 },
    /// Free space could not be measured. Callers proceed: refusing work over a
    /// stat we cannot take would stall any filesystem that does not report one.
    Unknown,
}

impl QuotaManager {
    /// Current utilization of the quota-managed resources.
    pub fn usage(&self) -> QuotaUsage {
        QuotaUsage {
            resident_memory_percent: self.resident_memory_percent(None),
            disk_usage_percent: self.disk_usage_percent(&self.storage_path, None),
        }
    }

    /// Free space in bytes on the filesystem hosting `path`, or `None` when it
    /// cannot be read. Served from the same cache the quota check uses.
    ///
    /// `watch_below` is the level at which the caller starts caring: a reading
    /// under it is never reused, so a disk that is nearly full is tracked call by
    /// call rather than through a sample that may already be stale.
    pub fn available_bytes(&self, path: &Path, watch_below: u64) -> Option<u64> {
        self.disk_usage(path, |usage| usage.available >= watch_below)
            .map(|usage| usage.available)
    }

    /// Total capacity in bytes of the filesystem hosting `path`, or `None` when
    /// it cannot be read. Served from the same cache the quota check uses.
    ///
    /// For reporting rather than deciding, so any reading still within the cache
    /// window will do.
    pub fn disk_capacity_bytes(&self, path: &Path) -> Option<u64> {
        self.disk_usage(path, |_| true).map(|usage| usage.total)
    }

    /// Whether an operation needing `required_bytes` on the filesystem hosting
    /// `path` can go ahead — an optimization sizing up the segment it will build.
    ///
    /// Blind to the configured limits by design: an optimization is what *frees*
    /// a disk the quota has declared full, so only not fitting may stop one.
    pub fn fits_on_disk(&self, path: &Path, required_bytes: u64) -> DiskFit {
        let Some(available) = self.available_bytes(path, required_bytes) else {
            return DiskFit::Unknown;
        };

        if available < required_bytes {
            DiskFit::TooLarge {
                available,
                required: required_bytes,
            }
        } else {
            DiskFit::Fits { available }
        }
    }

    /// Process resident memory as a percentage of total system memory, or `None`
    /// when it cannot be read.
    ///
    /// `limit` is what the reading will be compared against — `None` when reading
    /// for reporting. A reading at or above it is never served from the cache, so
    /// a caller that is rejecting updates sees memory being freed at once.
    pub fn resident_memory_percent(&self, limit: Option<u8>) -> Option<u8> {
        self.memory.measure(
            |percent| reusable(percent, limit),
            || {
                let resident = ::common::memory_usage::resident_bytes()?;
                percent_of(resident as u64, segment::utils::mem::total_memory_bytes())
            },
        )
    }

    /// Used space of the filesystem hosting `path`, as a percentage of capacity.
    pub(super) fn disk_usage_percent(&self, path: &Path, limit: Option<u8>) -> Option<u8> {
        let usage = self.disk_usage(path, |usage| {
            reusable(percent_of(usage.used(), usage.total), limit)
        })?;
        percent_of(usage.used(), usage.total)
    }

    /// Cached disk usage of the filesystem hosting `path`. `still_ample` says
    /// whether the last reading may stand in for a fresh one; it must go false
    /// once the disk is tight enough for the caller to act on, so a caller that
    /// is refusing work re-measures and sees it recover at once.
    fn disk_usage(
        &self,
        path: &Path,
        still_ample: impl Fn(DiskUsage) -> bool,
    ) -> Option<DiskUsage> {
        // Out of the map before measuring, so a slow `statvfs` on one path does
        // not hold up readings for the others.
        let meter = match self.disk.lock().entry(path.to_path_buf()) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => Arc::clone(entry.insert(Arc::new(Meter::default()))),
        };

        meter.measure(
            |usage| usage.is_none_or(&still_ample),
            || ::common::disk_usage::disk_usage(path),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quota::QuotaConfig;

    #[test]
    fn fits_on_disk_answers_from_free_space_not_the_quota() {
        let dir = tempfile::Builder::new().tempdir().unwrap();
        let manager = QuotaManager::load_or_init(
            dir.path(),
            QuotaConfig {
                enabled: true,
                // The strictest quota there is, so if it were consulted at all it
                // would refuse anything this filesystem is asked for
                max_disk_usage_percent: Some(1),
                ..Default::default()
            },
        )
        .unwrap();

        // It is not consulted: an optimization that fits still goes ahead,
        // because it is the work that brings usage back under the limit.
        let fit = manager.fits_on_disk(dir.path(), 0);
        assert!(matches!(fit, DiskFit::Fits { .. }), "{fit:?}");

        // Only not physically fitting stops one, and the verdict carries both
        // numbers so the caller can say by how much.
        let fit = manager.fits_on_disk(dir.path(), u64::MAX);
        let DiskFit::TooLarge {
            available,
            required,
        } = fit
        else {
            panic!("expected the disk to be too small, got {fit:?}");
        };
        assert_eq!(required, u64::MAX);
        assert!(available < u64::MAX);
    }
}
