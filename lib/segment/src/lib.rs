#![allow(deprecated)]

pub mod common;
pub mod entry;
pub mod fixtures;
mod id_tracker;
pub mod index;
pub mod payload_storage;
pub mod segment;
pub mod segment_constructor;
pub mod spaces;
pub mod telemetry;

pub mod data_types;
#[allow(deprecated)]
pub mod types;
pub mod vector_storage;

#[macro_use]
extern crate num_derive;
extern crate core;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

pub struct MaxDuration {
    name: String,
    max: parking_lot::Mutex<std::time::Duration>,
}

impl MaxDuration {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            max: parking_lot::Mutex::new(std::time::Duration::from_secs(0)),
        }
    }

    pub fn update(&self, duration: std::time::Duration) {
        use chrono::{Timelike, Utc};
        let now = Utc::now();
        let mut locked = self.max.lock();
        if duration > *locked {
            *locked = duration;
            log::error!("TOP DURATION {}: {:?}s, {:02}:{:02}:{:02}",
                &self.name,
                duration.as_micros() as f64 / 1_000_000.0,
                now.hour(),
                now.minute(),
                now.second(),
            );
        }
    }

    pub fn get(&self) -> std::time::Duration {
        *self.max.lock()
    }
}

lazy_static::lazy_static! {
    pub static ref GRPC_WRITE: MaxDuration = MaxDuration::new("grpc_write");
    pub static ref MMAP_MERGE: MaxDuration = MaxDuration::new("mmap_merge");
    pub static ref COLLECTION_OPERATION: MaxDuration = MaxDuration::new("segment_upsert");
    pub static ref WAL_LOCK: MaxDuration = MaxDuration::new("wal_lock");
    pub static ref WAL_WRITE: MaxDuration = MaxDuration::new("wal_write");
}
