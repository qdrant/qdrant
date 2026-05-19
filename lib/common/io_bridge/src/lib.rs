pub mod async_io;

mod adapter;
mod backend;
mod dispatcher;
mod pipeline;
mod runtime;

pub use adapter::{BlockingPipeline, IoBridge};
pub use backend::AsyncReadBackend;
pub use dispatcher::AsyncDispatcher;
pub use pipeline::{IoBridgeFile, IoBridgeReadPipeline};
pub use runtime::{global_async_handle, set_global_async_handle};

#[cfg(test)]
mod tests;
