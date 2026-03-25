pub mod generated;
pub mod remote_read;

pub use generated::qdrant::storage_read_client::StorageReadClient;
pub use remote_read::{RemoteClient, RemoteUniversalRead};
