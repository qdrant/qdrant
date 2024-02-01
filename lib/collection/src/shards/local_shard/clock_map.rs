use std::cmp;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::operations::ClockTag;
use crate::shards::shard::PeerId;

#[derive(Debug, Default)]
pub struct ClockMap {
    clocks: HashMap<ClockId, Clock>,
}

impl ClockMap {
    pub fn advance_clock_and_correct_tag(&mut self, clock_tag: &mut ClockTag) {
        let clock_id = ClockId::from_tag(clock_tag);

        if let Some(clock) = self.clocks.get(&clock_id) {
            if clock_tag.clock_tick < clock.advance_to(clock_tag.clock_tick) {
                clock_tag.clock_tick = clock.tick_once();
            }
        } else {
            self.clocks
                .insert(clock_id, Clock::new(clock_tag.clock_tick));
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
struct ClockId {
    peer_id: PeerId,
    clock_id: u32,
}

impl ClockId {
    pub fn from_tag(clock_tag: &ClockTag) -> Self {
        Self {
            peer_id: clock_tag.peer_id,
            clock_id: clock_tag.clock_id,
        }
    }
}

#[derive(Debug)]
struct Clock {
    clock: AtomicU64,
}

impl Clock {
    pub fn new(tick: u64) -> Self {
        Self {
            clock: AtomicU64::new(tick),
        }
    }

    pub fn tick_once(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn advance_to(&self, new_tick: u64) -> u64 {
        let current_tick = self.clock.fetch_max(new_tick, Ordering::Relaxed);
        cmp::max(current_tick, new_tick)
    }
}
