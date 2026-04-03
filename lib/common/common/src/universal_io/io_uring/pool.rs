use std::cell::RefCell;
use std::{io, ops};

use ::io_uring::{IoUring, Probe, opcode};

use super::*;

/// Submission queue depth for every `IoUring` instance.
pub const IO_URING_QUEUE_LENGTH: u32 = 16;

/// Check that io_uring is supported.
pub fn check_io_uring_support() -> Result<()> {
    IO_URING_POOL
        .with_borrow_mut(IoUringPool::check_support)
        .map_err(UniversalIoError::IoUringNotSupported)
}

/// Take an `IoUring` instance from the thread-local pool.
///
/// The instance is automatically returned to the pool when the guard is dropped.
/// Returns an error if io_uring is not supported on this system.
pub fn get_io_uring() -> Result<IoUringGuard> {
    IO_URING_POOL
        .with_borrow_mut(IoUringPool::get)
        .map_err(UniversalIoError::IoUringNotSupported)
}

thread_local! {
    /// Thread-local pool of reusable `IoUring` instances.
    ///
    /// Each iterator (or `with_uring_runtime` call) takes an instance from the
    /// pool for exclusive use and returns it on drop.  If the pool is empty a
    /// fresh instance is created on-the-fly; if returning would exceed
    /// `POOL_SIZE` the instance is simply dropped.
    static IO_URING_POOL: RefCell<IoUringPool> = RefCell::new(IoUringPool::new());
}

/// Default number of idle `IoUring` instances kept in the thread-local pool.
const IO_URING_POOL_SIZE: usize = 2;

struct IoUringPool {
    io_urings: Vec<IoUring>,
    is_supported: Option<bool>,
}

impl IoUringPool {
    fn new() -> Self {
        Self {
            io_urings: Vec::with_capacity(IO_URING_POOL_SIZE),
            is_supported: None,
        }
    }

    pub fn check_support(&mut self) -> io::Result<()> {
        let io_uring = self.get()?;
        self.put(io_uring.into_inner());
        Ok(())
    }

    pub fn get(&mut self) -> io::Result<IoUringGuard> {
        if self.is_supported == Some(false) {
            return Err(io_uring_unsupported_error());
        }

        let io_uring = self.take_or_init()?;

        if self.probe_support(&io_uring)? {
            Ok(IoUringGuard::new(io_uring))
        } else {
            Err(io_uring_unsupported_error())
        }
    }

    fn take_or_init(&mut self) -> io::Result<IoUring> {
        if let Some(io_uring) = self.io_urings.pop() {
            return Ok(io_uring);
        }

        IoUring::new(IO_URING_QUEUE_LENGTH)
            .map_err(|err| io_error_context(err, "failed to initialize io_uring instance"))
    }

    fn probe_support(&mut self, io_uring: &IoUring) -> io::Result<bool> {
        if let Some(is_supported) = self.is_supported {
            return Ok(is_supported);
        }

        let mut probe = Probe::new();

        io_uring
            .submitter()
            .register_probe(&mut probe)
            .map_err(|err| {
                io_error_context(err, "failed to probe io_uring for supported operations")
            })?;

        let is_supported =
            probe.is_supported(opcode::Read::CODE) && probe.is_supported(opcode::Write::CODE);

        self.is_supported = Some(is_supported);
        Ok(is_supported)
    }

    pub fn put(&mut self, mut io_uring: IoUring) {
        assert_io_uring_empty(&mut io_uring);

        if self.io_urings.len() >= IO_URING_POOL_SIZE {
            return;
        }

        self.io_urings.push(io_uring);
    }
}

fn io_uring_unsupported_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "io_uring does not support required operations",
    )
}

/// RAII guard that owns an `IoUring` instance and returns it to the
/// thread-local pool on drop.
pub struct IoUringGuard {
    io_uring: Option<IoUring>,
}

impl IoUringGuard {
    fn new(io_uring: IoUring) -> Self {
        Self {
            io_uring: Some(io_uring),
        }
    }

    fn into_inner(mut self) -> IoUring {
        self.io_uring.take().expect("io_uring initialized")
    }
}

impl ops::Deref for IoUringGuard {
    type Target = IoUring;

    fn deref(&self) -> &Self::Target {
        self.io_uring.as_ref().expect("io_uring initialized")
    }
}

impl ops::DerefMut for IoUringGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.io_uring.as_mut().expect("io_uring initialized")
    }
}

impl Drop for IoUringGuard {
    fn drop(&mut self) {
        let Some(mut io_uring) = self.io_uring.take() else {
            return;
        };

        assert_io_uring_empty(&mut io_uring);

        IO_URING_POOL.with_borrow_mut(|pool| {
            pool.put(io_uring);
        });
    }
}

fn assert_io_uring_empty(io_uring: &mut IoUring) {
    debug_assert!(
        io_uring.submission().is_empty(),
        "submission queue is not empty"
    );
    debug_assert!(
        io_uring.completion().is_empty(),
        "completion queue is not empty"
    );
}
