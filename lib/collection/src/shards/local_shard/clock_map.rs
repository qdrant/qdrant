use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{cmp, fs, io};

use serde::{Deserialize, Serialize};

use crate::operations::types::CollectionError;
use crate::operations::ClockTag;
use crate::shards::shard::PeerId;

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ClockMap {
    clocks: HashMap<ClockId, Clock>,
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
        serde_json::to_writer(io::BufWriter::new(file), &self)?;
        Ok(())
    }

    pub fn advance_clock_and_correct_tag(&mut self, clock_tag: &mut ClockTag) -> u64 {
        let current_tick = self.advance_clock(clock_tag);

        if clock_tag.clock_tick == 0 {
            clock_tag.clock_tick = current_tick;
        }

        current_tick
    }

    pub fn advance_clock(&mut self, clock_tag: &ClockTag) -> u64 {
        let clock_id = ClockId::from_tag(clock_tag);
        let new_tick = clock_tag.clock_tick;

        if let Some(clock) = self.clocks.get(&clock_id) {
            clock.advance_to(new_tick)
        } else {
            self.clocks.insert(clock_id, Clock::new(new_tick));
            new_tick
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
struct Clock {
    clock: AtomicU64,
}

impl Clock {
    pub fn new(tick: u64) -> Self {
        Self {
            clock: AtomicU64::new(tick),
        }
    }

    pub fn advance_to(&self, new_tick: u64) -> u64 {
        let current_tick = self.clock.fetch_max(new_tick, Ordering::Relaxed);
        cmp::max(current_tick, new_tick)
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
