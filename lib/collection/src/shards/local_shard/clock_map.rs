use std::collections::{hash_map, HashMap};
use std::fmt;
use std::path::Path;

use api::grpc::qdrant::RecoveryPointClockTag;
use io::file_operations;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::operations::types::CollectionError;
use crate::operations::ClockTag;
use crate::shards::shard::PeerId;

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(from = "ClockMapHelper", into = "ClockMapHelper")]
pub struct ClockMap {
    clocks: HashMap<Key, Clock>,
}

impl ClockMap {
    pub fn load_or_default(path: &Path) -> Result<Self> {
        let result = Self::load(path);

        if let Err(Error::Io(err)) = &result {
            if err.kind() == std::io::ErrorKind::NotFound {
                return Ok(Self::default());
            }
        }

        result
    }

    pub fn load(path: &Path) -> Result<Self> {
        let clock_map = file_operations::read_json(path)?;
        Ok(clock_map)
    }

    pub fn store(&self, path: &Path) -> Result<()> {
        file_operations::atomic_save_json(path, self)?;
        Ok(())
    }

    /// Advance clock referenced by `clock_tag` to `clock_tick`, if it's newer than current tick.
    /// Update `clock_tick` to current tick, if it's older.
    ///
    /// Returns whether operation should be accepted by the local shard and written into the WAL
    /// and applied to the storage, or rejected.
    #[must_use = "operation accept status must be used"]
    pub fn advance_clock_and_correct_tag(&mut self, clock_tag: &mut ClockTag) -> bool {
        let (clock_updated, current_tick) = self.advance_clock_impl(*clock_tag);

        // We *accept* an operation, if its `clock_tick` is *newer* than `current_tick`.
        //
        // If we *reject* an operation, we have to update its `clock_tick` to `current_tick`,
        // so that we can return updated clock tag to the sender node, so that the node can
        // correct its clock.
        //
        // There are two special cases:
        // - we always *accept* operations with `force = true`
        //   - (*currently*, this is *stronger* than `clock_tick = 0` condition!)
        // - we always *reject* operations with `clock_tick = 0`
        //   - (this is handled by `advance_clock_impl`, so we don't need to check for `clock_tick = 0` explicitly)
        //
        // TODO: Should we *reject* operations with `force = true`, *if* `clock_tick = 0`!?

        let operation_accepted = clock_updated || clock_tag.force;

        if !operation_accepted {
            clock_tag.clock_tick = current_tick;
        }

        operation_accepted
    }

    /// Advance clock referenced by `clock_tag` to `clock_tick`, if it's newer than current tick.
    ///
    /// If the clock is not yet tracked by the `ClockMap`, it is initialized to
    /// the `clock_tick` and added to the `ClockMap`.
    pub fn advance_clock(&mut self, clock_tag: ClockTag) {
        let _ = self.advance_clock_impl(clock_tag);
    }

    /// Advance clock referenced by `clock_tag` to `clock_tick`, if it's newer than current tick.
    ///
    /// If the clock is not yet tracked by the `ClockMap`, it is initialized to
    /// the `clock_tick` and added to the `ClockMap`.
    ///
    /// Returns whether the clock was updated (or initialized) and the current tick.
    #[must_use = "clock update status and current tick must be used"]
    fn advance_clock_impl(&mut self, clock_tag: ClockTag) -> (bool, u64) {
        let key = Key::from_tag(clock_tag);
        let new_tick = clock_tag.clock_tick;
        let new_token = clock_tag.token;

        match self.clocks.entry(key) {
            hash_map::Entry::Occupied(mut entry) => entry.get_mut().advance_to(new_tick, new_token),
            hash_map::Entry::Vacant(entry) => {
                // Initialize new clock and accept the operation if `new_tick > 0`.
                // Reject the operation if `new_tick = 0`.

                let is_non_zero_tick = new_tick > 0;

                if is_non_zero_tick {
                    entry.insert(Clock::new(new_tick, new_token));
                }

                (is_non_zero_tick, new_tick)
            }
        }
    }

    /// Create a recovery point based on the current clock map state, so that we can recover any
    /// new operations with new clock values
    ///
    /// The recovery point contains every clock that is in this clock map. So, it represents all
    /// the clock ticks we have.
    pub fn to_recovery_point(&self) -> RecoveryPoint {
        RecoveryPoint {
            clocks: self
                .clocks
                .iter()
                .map(|(key, clock)| (*key, clock.current_tick))
                .collect(),
        }
    }

    #[cfg(test)]
    pub fn current_tick(&self, peer_id: PeerId, clock_id: u32) -> Option<u64> {
        self.clocks
            .get(&Key::new(peer_id, clock_id))
            .map(Clock::current_tick)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub struct Key {
    peer_id: PeerId,
    clock_id: u32,
}

impl Key {
    fn new(peer_id: PeerId, clock_id: u32) -> Self {
        Self { peer_id, clock_id }
    }

    fn from_tag(clock_tag: ClockTag) -> Self {
        Self::new(clock_tag.peer_id, clock_tag.clock_id)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct Clock {
    current_tick: u64,
    token: Uuid,
}

impl Clock {
    fn new(current_tick: u64, token: Uuid) -> Self {
        Self {
            current_tick,
            token,
        }
    }

    /// Advance clock to `new_tick` if , if `new_tick` is newer than current tick.
    ///
    /// Returns whether the clock was updated and the current tick.
    ///
    /// The clock is updated when:
    /// - the given `new_tick` is newer than the current tick
    /// - the given `new_tick` and `new_token` are equal to the current tick and token
    #[must_use = "clock update status and current tick must be used"]
    fn advance_to(&mut self, new_tick: u64, new_token: Uuid) -> (bool, u64) {
        // Reject lower ticks
        if new_tick < self.current_tick {
            return (false, self.current_tick);
        }

        // Accept equal tick if it has the same unique token
        if self.current_tick == new_tick {
            return (self.token == new_token, self.current_tick);
        }

        // Accept higher ticks
        self.current_tick = new_tick;
        self.token = new_token;
        (true, self.current_tick)
    }

    #[cfg(test)]
    fn current_tick(&self) -> u64 {
        self.current_tick
    }
}

/// A recovery point, being a list of distributed clocks and their tick value
///
/// The recovery point describes from what point we want to get operations from another node in
/// case of recovery. In other words, the recovery point has the first clock tick values the
/// recovering node has not seen yet.
#[derive(Clone, Debug, Default)]
pub struct RecoveryPoint {
    clocks: HashMap<Key, u64>,
}

impl RecoveryPoint {
    pub fn is_empty(&self) -> bool {
        self.clocks.is_empty()
    }

    /// Iterate over all recovery point entries as clock tags.
    pub fn iter_as_clock_tags(&self) -> impl Iterator<Item = ClockTag> + '_ {
        self.clocks
            .iter()
            // TODO: keep clock token here? currently we generate a new unique one.
            .map(|(key, &tick)| ClockTag::new(key.peer_id, key.clock_id, tick))
    }

    /// Increase all existing clocks in this recovery point by the given amount
    pub fn increase_all_clocks_by(&mut self, ticks: u64) {
        for current_tick in self.clocks.values_mut() {
            *current_tick += ticks;
        }
    }

    /// Check whether this recovery point has any clocks that are not in `other`
    pub fn has_clocks_not_in(&self, other: &Self) -> bool {
        self.clocks
            .keys()
            .any(|key| !other.clocks.contains_key(key))
    }

    /// Check if this recovery point has any clock that is newer than the one in the `other`.
    ///
    /// A clock that is present in this recovery point, but not in the `other`,
    /// is always considered to be *newer*.
    pub fn has_any_newer_clocks_than(&mut self, other: &Self) -> bool {
        self.clocks.iter().any(|(key, &tick)| {
            other
                .clocks
                .get(key)
                .map_or(true, |&other_tick| tick > other_tick)
        })
    }

    /// Check if this recovery point has any clock that is older than the one in the `other`.
    ///
    /// A clock that is present in this recovery point, but not in the `other`,
    /// is always considered to be *newer*.
    pub fn has_any_older_clocks_than(&self, other: &Self) -> bool {
        self.clocks.iter().any(|(key, &tick)| {
            other
                .clocks
                .get(key)
                .map_or(false, |&other_tick| tick < other_tick)
        })
    }

    /// Extend this recovery point with clocks that are only present in the `other`.
    ///
    /// Clocks that are not present in this recovery point are initialized to the tick 1,
    /// because we must recover all operations for them.
    ///
    /// Clocks that are already present in this recovery point are not updated.
    pub fn initialize_clocks_missing_from(&mut self, other: &Self) {
        // Clocks known on our node, that are not in the recovery point, are unknown on the
        // recovering node. Add them here with tick 1, so that we include all records for it.
        for &key in other.clocks.keys() {
            self.clocks.entry(key).or_insert(1);
        }
    }

    /// Remove clocks from this recovery point, that are equal to the clocks in the `other`.
    pub fn remove_clocks_equal_to(&mut self, other: &Self) {
        for (key, other_tick) in &other.clocks {
            if let Some(tick) = self.clocks.get(key) {
                if tick == other_tick {
                    self.clocks.remove(key);
                }
            }
        }
    }

    /// Remove a clock referenced by the clock tag from this recovery point, if the clock is
    /// *newer or equal* than the tick in the tag.
    pub fn remove_clock_if_newer_than_or_equal_to_tag(&mut self, tag: ClockTag) {
        let key = Key::from_tag(tag);

        if let Some(&tick) = self.clocks.get(&key) {
            if tick >= tag.clock_tick {
                self.clocks.remove(&key);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn insert(&mut self, peer_id: PeerId, clock_id: u32, clock_tick: u64) {
        self.clocks.insert(Key::new(peer_id, clock_id), clock_tick);
    }
}

impl fmt::Display for RecoveryPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecoveryPoint[")?;

        let mut separator = "";

        for (key, current_tick) in &self.clocks {
            write!(
                f,
                "{separator}{}({}): {current_tick}",
                key.peer_id, key.clock_id
            )?;

            separator = ", ";
        }

        write!(f, "]")?;

        Ok(())
    }
}

impl From<&RecoveryPoint> for api::grpc::qdrant::RecoveryPoint {
    fn from(rp: &RecoveryPoint) -> Self {
        let clocks = rp
            .clocks
            .iter()
            .map(|(key, &clock_tick)| RecoveryPointClockTag {
                peer_id: key.peer_id,
                clock_id: key.clock_id,
                clock_tick,
            })
            .collect();

        Self { clocks }
    }
}

impl From<RecoveryPoint> for api::grpc::qdrant::RecoveryPoint {
    fn from(rp: RecoveryPoint) -> Self {
        (&rp).into()
    }
}

impl From<api::grpc::qdrant::RecoveryPoint> for RecoveryPoint {
    fn from(rp: api::grpc::qdrant::RecoveryPoint) -> Self {
        let clocks = rp
            .clocks
            .into_iter()
            .map(|tag| (Key::new(tag.peer_id, tag.clock_id), tag.clock_tick))
            .collect();

        Self { clocks }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ClockMapHelper {
    clocks: Vec<KeyClockHelper>,
}

impl From<ClockMap> for ClockMapHelper {
    fn from(clock_map: ClockMap) -> Self {
        Self {
            clocks: clock_map.clocks.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<ClockMapHelper> for ClockMap {
    fn from(helper: ClockMapHelper) -> Self {
        Self {
            clocks: helper.clocks.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
struct KeyClockHelper {
    #[serde(flatten)]
    key: Key,
    #[serde(flatten)]
    clock: Clock,
}

impl From<(Key, Clock)> for KeyClockHelper {
    fn from((key, clock): (Key, Clock)) -> Self {
        Self { key, clock }
    }
}

impl From<KeyClockHelper> for (Key, Clock) {
    fn from(helper: KeyClockHelper) -> Self {
        (helper.key, helper.clock)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
#[error("failed to load/store the clock map: {0}")]
pub enum Error {
    Io(#[from] std::io::Error),
    SerdeJson(#[from] serde_json::Error),
}

impl From<file_operations::Error> for Error {
    fn from(err: file_operations::Error) -> Self {
        match err {
            file_operations::Error::Io(err) => err.into(),
            file_operations::Error::SerdeJson(err) => err.into(),
            _ => unreachable!(),
        }
    }
}

impl From<Error> for CollectionError {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(err) => err.into(),
            Error::SerdeJson(err) => err.into(),
        }
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn clock_map_serde_empty() {
        let input = ClockMap::default();

        let json = serde_json::to_value(&input).unwrap();
        let output = serde_json::from_value(json).unwrap();

        assert_eq!(input, output);
    }

    #[test]
    fn clock_map_serde() {
        let mut input = ClockMap::default();
        input.advance_clock(ClockTag::new(1, 1, 1));
        input.advance_clock(ClockTag::new(1, 2, 8));
        input.advance_clock(ClockTag::new(2, 1, 42));
        input.advance_clock(ClockTag::new(2, 2, 12345));

        let json = serde_json::to_value(&input).unwrap();
        let output = serde_json::from_value(json).unwrap();

        assert_eq!(input, output);
    }

    #[test]
    fn clock_map_accept_last_operation_multiple_times() {
        let mut helper = Helper::empty();

        helper.advance(tag(1)).assert(true, 1);

        // Accept same operation multiple times if it is the last
        // We might send it multiple times due to a forward proxy
        let duplicate = tag(2);
        helper.advance(duplicate).assert(true, 2);
        helper.advance(duplicate).assert(true, 2);
        helper.advance(duplicate).assert(true, 2);

        // Reject same clock tag with different unique token
        helper.advance(tag(2)).assert(false, 2);

        // Still accept the same operation
        helper.advance(duplicate).assert(true, 2);

        // Accept newer operation
        helper.advance(tag(3)).assert(true, 3);

        // Reject duplicated operation now, because a newer one was accepted
        helper.advance(duplicate).assert(false, 3);
    }

    #[test]
    fn clock_map_advance_to_next_tick() {
        let mut helper = Helper::empty();

        // Advance to the next tick
        for tick in 1..10 {
            helper.advance(tag(tick)).assert(true, tick);
        }
    }

    #[test]
    fn clock_map_advance_to_newer_tick() {
        let mut helper = Helper::empty();

        // Advance to a newer tick
        for tick in [10, 20, 30, 40, 50] {
            helper.advance(tag(tick)).assert(true, tick);
        }
    }

    #[test]
    fn clock_map_reject_older_or_current_tick() {
        let mut helper = Helper::default();

        // Advance to a newer tick (already tested in `clock_map_advance_to_newer_tick`)
        helper.advance(tag(10));

        // Reject older tick
        for older_tick in 0..10 {
            helper.advance(tag(older_tick)).assert(false, 10);
        }

        // Reject current tick
        for current_tick in [10, 10, 10, 10, 10] {
            helper.advance(tag(current_tick)).assert(false, 10);
        }
    }

    #[test]
    fn clock_map_reject_tick_0() {
        let mut helper = Helper::empty();

        // Reject tick 0, if clock map is empty
        for _ in 0..5 {
            helper.advance(tag(0)).assert(false, 0);
        }

        // Advance to a newer tick (already tested in `clock_map_advance_to_newer_tick`)
        helper.advance(tag(10));

        // Reject tick 0, if clock map is non-empty
        for _ in 0..5 {
            helper.advance(tag(0)).assert(false, 10);
        }
    }

    #[test]
    fn clock_map_advance_to_newer_tick_with_force_true() {
        let mut helper = Helper::empty();

        // Advance to a newer tick with `force = true`
        for tick in [10, 20, 30, 40, 50] {
            helper.advance(tag(tick).force(true)).assert(true, tick);
            assert_eq!(helper.clock_map.current_tick(PEER_ID, CLOCK_ID), Some(tick));
        }
    }

    #[test]
    fn clock_map_accept_older_or_current_tick_with_force_true() {
        let mut helper = Helper::default();

        // Advance to a newer tick (already tested in `clock_map_advance_to_newer_tick`)
        helper.advance(tag(10));

        // Accept older tick with `force = true`
        for older_tick in 0..10 {
            helper
                .advance(tag(older_tick).force(true))
                .assert(true, older_tick);
        }

        // Accept current tick with `force = true`
        for current_tick in [10, 10, 10, 10, 10] {
            helper
                .advance(tag(current_tick).force(true))
                .assert(true, current_tick);
        }
    }

    proptest! {
        #[test]
        fn clock_map_workflow(execution in proptest::collection::vec(clock_tag(), 0..4096)) {
            let mut helper = Helper::default();

            for clock_tag in execution {
                let current_tick = helper.clock_map.current_tick(clock_tag.peer_id, clock_tag.clock_id);
                assert_ne!(current_tick, Some(0));

                let expected_status =
                    clock_tag.clock_tick > current_tick.unwrap_or(0) || clock_tag.force;

                let expected_tick = if expected_status {
                    clock_tag.clock_tick
                } else {
                    current_tick.unwrap_or(0)
                };

                helper.advance(clock_tag).assert(expected_status, expected_tick);
            }
        }

        #[ignore]
        #[test]
        fn clock_map_clocks_isolation(execution in proptest::collection::vec(clock_tag(), 4096)) {
            let mut helper = Helper::default();

            for clock_tag in execution {
                // Back-up current state
                let backup = helper.clone();

                // Advance the clock map
                helper.advance(clock_tag);

                // Ensure that no more than a single entry in the clock map was updated during advance
                let helper_len = helper.clock_map.clocks.len();
                let backup_len = backup.clock_map.clocks.len();

                assert!(helper_len == backup_len || helper_len == backup_len + 1);

                for (key, clock) in backup.clock_map.clocks {
                    let current_tick = helper.clock_map.current_tick(key.peer_id, key.clock_id);

                    if clock_tag.peer_id == key.peer_id && clock_tag.clock_id == key.clock_id {
                        assert!(current_tick.is_some() || clock_tag.clock_tick == 0);
                    } else {
                        assert_eq!(current_tick, Some(clock.current_tick));
                    }
                }
            }
        }
    }

    prop_compose! {
        fn clock_tag() (
            peer_id in 0..128_u64,
            clock_id in 0..64_u32,
            clock_tick in any::<u64>(),
            force in any::<bool>(),
        ) -> ClockTag {
            ClockTag::new(peer_id, clock_id, clock_tick).force(force)
        }
    }

    #[derive(Clone, Debug, Default)]
    struct Helper {
        clock_map: ClockMap,
    }

    impl Helper {
        pub fn empty() -> Self {
            Self::default()
        }

        pub fn advance(&mut self, mut clock_tag: ClockTag) -> Status {
            let peer_id = clock_tag.peer_id;
            let clock_id = clock_tag.clock_id;
            let token = clock_tag.token;

            let accepted = self.clock_map.advance_clock_and_correct_tag(&mut clock_tag);

            assert_eq!(clock_tag.peer_id, peer_id);
            assert_eq!(clock_tag.clock_id, clock_id);
            assert_eq!(clock_tag.token, token);

            Status {
                accepted,
                clock_tag,
            }
        }
    }

    const PEER_ID: PeerId = 1337;
    const CLOCK_ID: u32 = 42;

    fn tag(tick: u64) -> ClockTag {
        ClockTag::new(PEER_ID, CLOCK_ID, tick)
    }

    #[derive(Copy, Clone, Debug)]
    struct Status {
        accepted: bool,
        clock_tag: ClockTag,
    }

    impl Status {
        pub fn assert(&self, expected_status: bool, expected_tick: u64) {
            assert_eq!(self.accepted, expected_status);
            assert_eq!(self.clock_tag.clock_tick, expected_tick);
        }
    }
}
