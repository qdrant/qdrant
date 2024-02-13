use std::iter;

use super::local_shard::clock_map::ClockMap;
use super::replica_set::clock_set::ClockSet;
use super::shard::PeerId;
use crate::operations::ClockTag;

#[test]
fn clock_set_clock_map_workflow() {
    let mut helper = Helper::new();

    // `ClockSet` and `ClockMap` "stick" to tick `0`, until `ClockSet` is advanced at least once
    helper.test(Tick, Req(0));
    helper.test(TickAndAdvance, [Req(0), Resp(0), Accepted]);
    helper.test(TickAndAdvance, [Req(0), Resp(0), Accepted]);
    helper.test(RoundTrip, [Req(0), Resp(0), Accepted]);

    // Everything ticks normally after that
    helper.test(RoundTrip, [Req(1), Resp(1), Accepted]);
    helper.test(RoundTrip, [Req(2), Resp(2), Accepted]);
    helper.test(RoundTrip, [Req(3), Resp(3), Accepted]);

    // `ClockMap` always advances to newer tick (regardless if `force` flag is set or not)
    helper.test(Tick, Req(4));
    helper.test(Tick, Req(5));
    helper.test(RoundTrip, [Req(6), Resp(6), Accepted]);
    helper.test(Tick, Req(7));
    helper.test(Tick, Req(8));
    helper.test(RoundTrip.force(), [Req(9), Resp(9), Accepted]);

    // `ClockMap` accepts tick `0` and advances `ClockSet`
    helper.clock_set = Default::default();
    helper.test(RoundTrip, [Req(0), Resp(9), Accepted]);
    helper.test(Tick, Req(10));

    // `ClockMap` rejects older ticks and advances `ClockSet`
    helper.clock_set = Default::default();
    helper.clock_set.get_clock().advance_to(0);
    helper.test(TickAndAdvance, [Req(1), Resp(9), Rejected]);
    helper.test(TickAndAdvance, [Req(2), Resp(9), Rejected]);
    helper.test(RoundTrip, [Req(3), Resp(9), Rejected]);
    helper.test(Tick, Req(10));

    // `ClockMap` accepts older ticks, if `force` flag is set, but does not advance `ClockSet`
    // (But it does "echo" the same tick)
    helper.clock_set = Default::default();
    helper.test(RoundTrip.force(), [Req(0), Resp(0), Accepted]);
    helper.test(RoundTrip.force(), [Req(1), Resp(1), Accepted]);
    helper.test(RoundTrip.force(), [Req(2), Resp(2), Accepted]);
    helper.test(RoundTrip, [Req(3), Resp(9), Rejected]);
    helper.test(Tick, Req(10));
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

    pub fn test(&mut self, test: impl Into<Test>, assert: impl IntoIterator<Item = Assert>) {
        let test = test.into();
        let status = self.execute(test.action, test.force);

        for assert in assert {
            self.assert(&status, assert);
        }
    }

    fn execute(&mut self, action: Action, force: bool) -> Status {
        let mut clock = self.clock_set.get_clock();

        // Tick `ClockSet`
        let request = ClockTag::new(PEER_ID, clock.id() as _, clock.tick_once()).force(force);

        if matches!(action, Action::Tick) {
            return Status::from_req(request);
        }

        // Advance `ClockMap`
        let mut response = request;
        let accepted = self.clock_map.advance_clock_and_correct_tag(&mut response);

        let status = Status::new(request, response, accepted);

        // Advance `ClockSet`
        if matches!(action, Action::RoundTrip) {
            clock.advance_to(status.response.clock_tick);
        }

        status
    }

    fn assert(&self, status: &Status, assert: Assert) {
        match assert {
            Assert::Req(clock_tick) => assert_eq!(status.request.clock_tick, clock_tick),
            Assert::Resp(clock_tick) => assert_eq!(status.response.clock_tick, clock_tick),
            Assert::Accepted => assert!(status.accepted),
            Assert::Rejected => assert!(!status.accepted),
        }
    }
}

use Action::*;
use Assert::*;

#[derive(Copy, Clone, Debug)]
enum Action {
    Tick,
    TickAndAdvance,
    RoundTrip,
}

impl Action {
    pub fn force(self) -> Test {
        Test::new(self, true)
    }
}

#[derive(Copy, Clone, Debug)]
enum Assert {
    Req(u64),
    Resp(u64),
    Accepted,
    Rejected,
}

impl IntoIterator for Assert {
    type Item = Self;
    type IntoIter = iter::Once<Self>;

    fn into_iter(self) -> Self::IntoIter {
        iter::once(self)
    }
}

#[derive(Copy, Clone, Debug)]
struct Test {
    action: Action,
    force: bool,
}

impl Test {
    pub fn new(action: Action, force: bool) -> Self {
        Self { action, force }
    }
}

impl From<Action> for Test {
    fn from(action: Action) -> Self {
        Self::new(action, false)
    }
}

#[derive(Copy, Clone, Debug)]
struct Status {
    request: ClockTag,
    response: ClockTag,
    accepted: bool,
}

impl Status {
    pub fn new(request: ClockTag, response: ClockTag, accepted: bool) -> Self {
        Self {
            request,
            response,
            accepted,
        }
    }

    pub fn from_req(request: ClockTag) -> Self {
        Self::new(request, default_clock_tag(), false)
    }
}

fn default_clock_tag() -> ClockTag {
    ClockTag {
        peer_id: 0,
        clock_id: 0,
        clock_tick: 0,
        force: false,
    }
}
