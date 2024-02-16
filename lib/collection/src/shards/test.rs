use super::local_shard::clock_map::ClockMap;
use super::replica_set::clock_set::ClockSet;
use super::shard::PeerId;
use crate::operations::ClockTag;

#[test]
fn clock_set_clock_map_workflow() {
    let mut helper = Helper::new();

    // `ClockSet` and `ClockMap` "stick" to tick `0`, until `ClockSet` is advanced at least once
    helper.tick_clock().assert(0);
    helper.advance_clock_map(false).assert(0, 0, true);
    helper.advance_clock_map(false).assert(0, 0, true);
    helper.advance_clock(false).assert(0, 0, true);

    // `ClockSet` and `ClockMap` tick sequentially and in sync after that
    for tick in 1..=10 {
        helper.advance_clock(false).assert(tick, tick, true);
    }

    // `ClockMap` advances to newer ticks
    for tick in 11..=50 {
        if tick % 10 != 0 {
            // Tick `ClockSet` few times, without advancing `ClockMap`...
            helper.tick_clock().assert(tick);
        } else {
            // ...then advance both `ClockMap` and `ClockSet`
            helper.advance_clock(false).assert(tick, tick, true);
        }
    }

    // `ClockMap` accepts tick `0` and advances `ClockSet`
    helper.clock_set = Default::default();
    helper.advance_clock(false).assert(0, 50, true);
    helper.tick_clock().assert(51);

    // `ClockMap` rejects older (or current) ticks...
    helper.clock_set = Default::default();
    helper.clock_set.get_clock().advance_to(0);

    for tick in 1..=50 {
        helper.advance_clock_map(false).assert(tick, 50, false);
    }

    // ...and advances `ClockSet`
    helper.clock_set = Default::default();
    helper.clock_set.get_clock().advance_to(42);

    helper.advance_clock(false).assert(43, 50, false);
    helper.tick_clock().assert(51);

    // `ClockMap` advances to newer ticks with `force = true`
    helper.clock_set = Default::default();
    helper.advance_clock(false).assert(0, 50, true);

    for tick in 51..=100 {
        helper.advance_clock(true).assert(tick, tick, true);
    }

    // `ClockMap` accepts older (or current) ticks with `force = true`...
    helper.clock_set = Default::default();

    for tick in 0..=100 {
        helper.advance_clock(true).assert(tick, tick, true);
    }

    // ...but it does not affect current tick of `ClockMap` in any way
    helper.clock_set = Default::default();
    helper.clock_set.get_clock().advance_to(42);

    helper.advance_clock(false).assert(43, 100, false);
    helper.tick_clock().assert(101);
}

#[derive(Clone, Debug)]
struct Helper {
    clock_set: ClockSet,
    clock_map: ClockMap,
}

const PEER_ID: PeerId = 1337;

impl Helper {
    pub fn new() -> Self {
        Self {
            clock_set: ClockSet::default(),
            clock_map: ClockMap::default(),
        }
    }

    pub fn tick_clock(&mut self) -> TickClockStatus {
        let mut clock = self.clock_set.get_clock();
        let clock_tag = ClockTag::new(PEER_ID, clock.id() as _, clock.tick_once());
        TickClockStatus { clock_tag }
    }

    pub fn advance_clock_map(&mut self, force: bool) -> AdvanceStatus {
        self.advance(force, false)
    }

    pub fn advance_clock(&mut self, force: bool) -> AdvanceStatus {
        self.advance(force, true)
    }

    fn advance(&mut self, force: bool, advance_clock: bool) -> AdvanceStatus {
        let mut clock = self.clock_set.get_clock();

        let clock_tag = ClockTag::new(PEER_ID, clock.id() as _, clock.tick_once()).force(force);

        let mut clock_map_tag = clock_tag;
        let accepted = self
            .clock_map
            .advance_clock_and_correct_tag(&mut clock_map_tag);

        assert_eq!(clock_tag.peer_id, clock_map_tag.peer_id);
        assert_eq!(clock_tag.clock_id, clock_map_tag.clock_id);

        if advance_clock {
            clock.advance_to(clock_map_tag.clock_tick);
        }

        AdvanceStatus {
            clock_tag,
            clock_map_tag,
            accepted,
        }
    }
}

#[derive(Copy, Clone, Debug)]
struct TickClockStatus {
    clock_tag: ClockTag,
}

impl TickClockStatus {
    pub fn assert(&self, expected_tick: u64) {
        assert_eq!(self.clock_tag.clock_tick, expected_tick)
    }
}

#[derive(Copy, Clone, Debug)]
struct AdvanceStatus {
    clock_tag: ClockTag,
    clock_map_tag: ClockTag,
    accepted: bool,
}

impl AdvanceStatus {
    pub fn assert(&self, expected_tick: u64, expected_cm_tick: u64, expected_status: bool) {
        assert_eq!(self.clock_tag.clock_tick, expected_tick);
        assert_eq!(self.clock_map_tag.clock_tick, expected_cm_tick);
        assert_eq!(self.accepted, expected_status);
    }
}
