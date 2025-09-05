pub mod locked_segment;
pub mod operations;
pub mod payload_index_schema;
pub mod proxy_segment;
pub mod search;
pub mod segment_holder;
pub mod update;
pub mod wal;

#[cfg(test)]
mod fixtures;

pub type PeerId = u64;
