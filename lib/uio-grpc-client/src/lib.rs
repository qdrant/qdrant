pub mod generated;
pub mod read;

#[cfg(test)]
mod tests;

pub use generated::qdrant::storage_read_client::StorageReadClient;
pub use generated::qdrant::storage_read_server::{StorageRead, StorageReadServer};
pub use read::Client;
