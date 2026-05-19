mod config;
mod file;
mod pipeline;
mod runtime;

#[cfg(test)]
mod tests;

use std::cell::RefCell;

pub use config::{S3Config, S3Credentials};
pub use file::S3File;
pub use runtime::S3RuntimeHandle;

#[derive(Clone)]
pub struct S3Context {
    pub config: S3Config,
    /// `None` means use the global runtime via `S3RuntimeHandle::global()`.
    pub runtime: Option<S3RuntimeHandle>,
}

thread_local! {
    static S3_CONTEXT: RefCell<Vec<S3Context>> = const { RefCell::new(Vec::new()) };
}

pub struct S3ContextGuard;

impl Drop for S3ContextGuard {
    fn drop(&mut self) {
        S3_CONTEXT.with(|cell| {
            cell.borrow_mut().pop();
        });
    }
}

pub fn push_s3_context(ctx: S3Context) -> S3ContextGuard {
    S3_CONTEXT.with(|cell| cell.borrow_mut().push(ctx));
    S3ContextGuard
}

pub(crate) fn current_s3_context() -> Option<S3Context> {
    S3_CONTEXT.with(|cell| cell.borrow().last().cloned())
}

#[cfg(test)]
mod smoke {
    #[test]
    fn crate_links() {
        assert_eq!(2 + 2, 4);
    }
}
