use std::cmp;
use std::collections::{hash_map, HashMap};
use std::path::Path;

use api::grpc::qdrant::RecoveryPointClockTag;
use io::file_operations;
use serde::{Deserialize, Serialize};

use crate::operations::types::CollectionError;
use crate::operations::ClockTag;
use crate::shards::shard::PeerId;

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(transparent)]
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

    /// Create a recovery point from the current clock map state
    pub fn to_recovery_point(&self) -> RecoveryPoint {
        RecoveryPoint {
            clocks: self
                .clocks
                .iter()
                .map(|(key, clock)| (*key, clock.current_tick))
                .collect(),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
struct Key {
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(transparent)]
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

#[derive(Debug, Clone, Default)]
pub struct RecoveryPoint {
    clocks: HashMap<Key, u64>,
}

impl RecoveryPoint {
    /// Extend this recovery point with new clocks from `clock_map`
    ///
    /// Clocks that we already have in this recovery point are not updated, regardless of their
    /// tick value.
    pub fn extend_with_missing_clocks(&mut self, clock_map: &ClockMap) {
        for (key, clock) in &clock_map.clocks {
            self.clocks
                .entry(*key)
                .or_insert_with(|| clock.current_tick);
        }
    }
}

impl From<RecoveryPoint> for api::grpc::qdrant::RecoveryPoint {
    fn from(value: RecoveryPoint) -> Self {
        Self {
            clocks: value
                .clocks
                .into_iter()
                .map(|(key, clock_tick)| RecoveryPointClockTag {
                    peer_id: key.peer_id,
                    clock_id: key.clock_id,
                    clock_tick,
                })
                .collect(),
        }
    }
}

impl From<api::grpc::qdrant::RecoveryPoint> for RecoveryPoint {
    fn from(value: api::grpc::qdrant::RecoveryPoint) -> Self {
        Self {
            clocks: value
                .clocks
                .into_iter()
                .map(|tag| (Key::new(tag.peer_id, tag.clock_id), tag.clock_tick))
                .collect(),
        }
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
