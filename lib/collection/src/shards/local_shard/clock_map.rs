use std::collections::{hash_map, HashMap};
use std::path::Path;
use std::{cmp, fmt};

use api::grpc::qdrant::RecoveryPointClockTag;
use io::file_operations;
use serde::{Deserialize, Serialize};

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
        // - we *always* accept operations with `clock_tick = 0`, and also *always* update their `clock_tick`
        // - and we *always* accept operations with `force = true`, but *never* update their `clock_tick`

        if clock_tag.force {
            return true;
        }

        let operation_accepted = clock_updated || clock_tag.clock_tick == 0;
        let update_tag = !operation_accepted || clock_tag.clock_tick == 0;

        if update_tag {
            clock_tag.clock_tick = current_tick;
        }

        operation_accepted
    }

    /// Advance clock referenced by `clock_tag` to `clock_tick`, if it's newer than current tick.
    ///
    /// If the clock is not yet tracked by the `ClockMap`, it is initialized to
    /// the `clock_tag.clock_tick` and added to the `ClockMap`.
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

        match self.clocks.entry(key) {
            hash_map::Entry::Occupied(mut entry) => entry.get_mut().advance_to(new_tick),
            hash_map::Entry::Vacant(entry) => {
                entry.insert(Clock::new(new_tick));
                (true, new_tick)
            }
        }
    }

    /// Create a recovery point based on the current clock map state, so that we can recover any
    /// new operations with new clock values
    ///
    /// The recovery point will contain every clock that is in this clock map, but with a tick of
    /// one higher. That is because we already have an operation for the current clock tick, but
    /// would like to receive the operation with the next tick on recovery.
    ///
    /// In other words, the recovery point will contain the first clock tick values the clock map
    /// has not seen yet.
    pub fn to_recovery_point(&self) -> RecoveryPoint {
        RecoveryPoint {
            clocks: self
                .clocks
                .iter()
                .map(|(key, clock)| (*key, clock.current_tick + 1))
                .collect(),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub struct Key {
    peer_id: PeerId,
    clock_id: u32,
}

impl Key {
    pub fn new(peer_id: PeerId, clock_id: u32) -> Self {
        Self { peer_id, clock_id }
    }

    pub fn from_tag(clock_tag: ClockTag) -> Self {
        Self {
            peer_id: clock_tag.peer_id,
            clock_id: clock_tag.clock_id,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct Clock {
    current_tick: u64,
}

impl Clock {
    fn new(current_tick: u64) -> Self {
        Self { current_tick }
    }

    /// Advance clock to `new_tick`, if `new_tick` is newer than current tick.
    ///
    /// Returns whether the clock was updated and the current tick.
    #[must_use = "clock update status and current tick must be used"]
    fn advance_to(&mut self, new_tick: u64) -> (bool, u64) {
        let clock_updated = self.current_tick < new_tick;
        self.current_tick = cmp::max(self.current_tick, new_tick);
        (clock_updated, self.current_tick)
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
    #[cfg(test)]
    pub(crate) fn insert(&mut self, peer_id: PeerId, clock_id: u32, clock_tick: u64) {
        self.clocks.insert(Key::new(peer_id, clock_id), clock_tick);
    }

    pub fn is_empty(&self) -> bool {
        self.clocks.is_empty()
    }

    /// Iterate over all recovery point entries as clock tags.
    pub fn clock_tag_iter(&self) -> impl Iterator<Item = ClockTag> + '_ {
        self.clocks
            .iter()
            .map(|(key, tick)| ClockTag::new(key.peer_id, key.clock_id, *tick))
    }

    /// Check whether this recovery point has any clocks that are not in `other`
    pub fn has_clocks_not_in(&self, other: &Self) -> bool {
        self.clocks
            .keys()
            .any(|key| !other.clocks.contains_key(key))
    }

    /// Check whether this recovery point has any higher clock value than `other`
    ///
    /// A clock in this recovery point that is not in `other` is always considered to be higher.
    pub fn has_any_higher(&self, other: &Self) -> bool {
        self.clocks.iter().any(|(key, tick)| {
            other
                .clocks
                .get(key)
                .map_or(true, |other_tick| *tick > *other_tick)
        })
    }

    /// Check whether this recovery point has any lower clock value than `other`
    ///
    /// A clock in this recovery point that is not in `other` is not considered lower.
    pub fn has_any_lower(&self, other: &Self) -> bool {
        self.clocks.iter().any(|(key, tick)| {
            other
                .clocks
                .get(key)
                .map_or(false, |other_tick| *tick < *other_tick)
        })
    }

    /// Extend this recovery point with new clocks from `clock_map`
    ///
    /// Clocks that we have not seen yet are added with a tick of `0`, because we must recover all
    /// records for it.
    ///
    /// Clocks that we already have in this recovery point are not updated, regardless of their
    /// tick value.
    pub fn extend_with_missing_clocks(&mut self, other: &Self) {
        // Clocks known on our node, that are not in the recovery point, are unknown on the
        // recovering node. Add them here with tick 0, so that we include all records for it
        for key in other.clocks.keys() {
            self.clocks.entry(*key).or_insert(0);
        }
    }

    /// Remove clocks from this recovery point, if they are equal in `other`
    pub fn remove_equal_clocks(&mut self, other: &Self) {
        for (key, tick) in &other.clocks {
            if let Some(other_tick) = self.clocks.get(key) {
                if tick == other_tick {
                    self.clocks.remove(key);
                }
            }
        }
    }

    /// Remove a clock from this recovery point, if the given `clock_tag` describes an equal or
    /// lower clock value.
    pub fn remove_equal_or_lower(&mut self, clock_tag: ClockTag) {
        let key = Key::from_tag(clock_tag);
        if let Some(tick) = self.clocks.get(&key) {
            if clock_tag.clock_tick <= *tick {
                self.clocks.remove(&key);
            }
        }
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

impl From<RecoveryPoint> for api::grpc::qdrant::RecoveryPoint {
    fn from(rp: RecoveryPoint) -> Self {
        let clocks = rp
            .clocks
            .into_iter()
            .map(|(key, clock_tick)| RecoveryPointClockTag {
                peer_id: key.peer_id,
                clock_id: key.clock_id,
                clock_tick,
            })
            .collect();

        Self { clocks }
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
mod tests {
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
}
