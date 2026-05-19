use std::future::Future;

use common::universal_io::UniversalIoError;

/// Async byte-range read backend. Concrete impls back the [`AsyncDispatcher`] +
/// [`IoBridgeReadPipeline`] machinery so sync `UniversalRead` consumers can read
/// from inherently async sources (S3, HTTP, …) without changing their API.
///
/// The interface is **byte-oriented** — the bridge translates the element-typed
/// `ReadRange` from the trait API (where `length` is in `T` elements) to a
/// concrete byte length before calling the backend, so backends never need to
/// know the destination element type.
///
/// `Send + Sync + 'static`: the dispatcher holds the backend in an [`Arc`] and
/// shares it across spawned tokio tasks.
pub trait AsyncReadBackend: Send + Sync + 'static {
    /// What the backend uses to locate a piece of data — for S3 it would carry
    /// bucket + key; for local async files a `PathBuf`; for the mock backend in
    /// tests, a small identifier. Held by value so the bridge can hand a clone
    /// to each spawned read.
    type Location: Clone + Send + 'static;

    /// Async read of `byte_length` bytes starting at `byte_offset`. Returns an
    /// owned buffer; backends like S3 cannot expose borrowed data, so the
    /// bridge never assumes zero-copy.
    fn read_bytes(
        &self,
        location: Self::Location,
        byte_offset: u64,
        byte_length: u64,
    ) -> impl Future<Output = Result<Vec<u8>, UniversalIoError>> + Send;
}
