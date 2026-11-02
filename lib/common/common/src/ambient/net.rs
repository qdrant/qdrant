use std::ops::Index;
use std::sync::atomic::{AtomicUsize, Ordering};

use strum::EnumCount;

use super::slot;

/// 🤖 Remote and disk cache reads.
/// 🤖 Unlike [`super::hw::HwMetric`], these have no opt-out: unmeasured scopes still
/// 🤖 count them into their context, and a scope without a context is a misuse.
#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumCount)]
pub enum NetMetric {
    /// 🤖 Bytes read from the remote.
    RemoteIoRead = 0,
    /// 🤖 Read requests to the remote.
    RemoteRequests = 1,
    /// 🤖 Bytes served by the disk cache, including the ones just fetched from the remote.
    CacheIoRead = 2,
}

impl NetMetric {
    #[inline]
    #[cfg_attr(debug_assertions, track_caller)]
    pub fn bump(self, delta: usize) {
        slot::bump_net(self, delta);
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct NetworkData(pub(crate) [usize; NetMetric::COUNT]);

impl Index<NetMetric> for NetworkData {
    type Output = usize;

    fn index(&self, metric: NetMetric) -> &usize {
        &self.0[metric as usize]
    }
}

/// 🤖 Thread-safe counters, shared as the per-collection drain of multiple
/// 🤖 [`crate::ambient::AmbientContext`]s.
#[derive(Debug, Default)]
pub struct NetSharedDrain([AtomicUsize; NetMetric::COUNT]);

impl NetSharedDrain {
    pub fn load(&self) -> NetworkData {
        NetworkData(self.0.each_ref().map(|c| c.load(Ordering::Relaxed)))
    }

    pub(crate) fn add(&self, src: NetworkData) {
        for (counter, value) in self.0.iter().zip(src.0) {
            counter.fetch_add(value, Ordering::Relaxed);
        }
    }
}
