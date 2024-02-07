use std::collections::{hash_map, HashMap};
use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{cmp, fs, io};

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
            if err.kind() == io::ErrorKind::NotFound {
                return Ok(Self::default());
            }
        }

        result
    }

    pub fn load(path: &Path) -> Result<Self> {
        let file = fs::File::open(path)?;
        let clock_map = serde_json::from_reader(io::BufReader::new(file))?;
        Ok(clock_map)
    }

    pub fn store(&self, path: &Path) -> Result<()> {
        let file = fs::File::create(path)?;

        let mut writer = io::BufWriter::new(file);
        serde_json::to_writer(&mut writer, &self)?;
        writer.flush()?;

        Ok(())
    }

    pub fn advance_clock(&mut self, clock_tag: &ClockTag) -> (bool, u64) {
        let key = Key::from_tag(clock_tag);
        let new_tick = clock_tag.clock_tick;

        match self.clocks.entry(key) {
            hash_map::Entry::Occupied(entry) => entry.get().advance_to(new_tick),
            hash_map::Entry::Vacant(entry) => {
                entry.insert(Clock::new(new_tick));
                (true, new_tick)
            }
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
struct Key {
    peer_id: PeerId,
    clock_id: u32,
}

impl Key {
    pub fn from_tag(clock_tag: &ClockTag) -> Self {
        Self {
            peer_id: clock_tag.peer_id,
            clock_id: clock_tag.clock_id,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Clock {
    current_tick: AtomicU64,
}

impl Clock {
    pub fn new(tick: u64) -> Self {
        Self {
            current_tick: tick.into(),
        }
    }

    pub fn advance_to(&self, new_tick: u64) -> (bool, u64) {
        let old_tick = self.current_tick.fetch_max(new_tick, Ordering::Relaxed);

        let clock_updated = old_tick < new_tick;
        let current_tick = cmp::max(old_tick, new_tick);

        (clock_updated, current_tick)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

impl From<Error> for CollectionError {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(err) => err.into(),
            Error::SerdeJson(err) => err.into(),
        }
    }
}
