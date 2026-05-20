mod config;
mod file;
mod pipeline;
mod read;
mod runtime;
pub mod s3;

#[cfg(test)]
mod tests;

pub use config::{S3Config, S3Credentials, build_object_store};
pub use file::BlobFile;
pub use pipeline::{BorrowedBlobPipeline, OwnedBlobPipeline};
pub use read::BlobRead;
pub use runtime::{BridgeRequest, BridgeResponse, BridgeRuntime};
pub use s3::S3Source;

#[cfg(test)]
mod smoke {
    #[test]
    fn crate_links() {
        assert_eq!(2 + 2, 4);
    }
}
