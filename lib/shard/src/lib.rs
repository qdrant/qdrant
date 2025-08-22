pub mod locked_segment;
pub mod operations;
pub mod payload_index_schema;
pub mod proxy_segment;
pub mod segment_holder;

#[cfg(test)]
mod fixtures;

pub type PeerId = u64;
