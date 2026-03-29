use std::cell::RefCell;
use std::io;

use ::io_uring::{IoUring, Probe, opcode};

/// Default number of idle `IoUring` instances kept in the thread-local pool.
const POOL_SIZE: usize = 2;

/// Submission queue depth for every `IoUring` instance.
pub const IO_URING_QUEUE_LENGTH: u32 = 16;

thread_local! {
    /// Thread-local pool of reusable `IoUring` instances.
    ///
    /// Each iterator (or `with_uring_runtime` call) takes an instance from the
    /// pool for exclusive use and returns it on drop.  If the pool is empty a
    /// fresh instance is created on-the-fly; if returning would exceed
    /// `POOL_SIZE` the instance is simply dropped.
    static POOL: RefCell<IoUringPool> = RefCell::new(IoUringPool::new());
}

struct IoUringPool {
    rings: Vec<IoUring>,
    /// Cached probe result — `None` means we haven't checked yet.
    supported: Option<bool>,
}

impl IoUringPool {
    fn new() -> Self {
        Self {
            rings: Vec::with_capacity(POOL_SIZE),
            supported: None,
        }
    }

    fn check_supported(&mut self) -> io::Result<()> {
        if let Some(true) = self.supported {
            return Ok(());
        }
        if let Some(false) = self.supported {
            return Err(io::Error::other(
                "io_uring does not support required operations",
            ));
        }

        // First call — probe once.
        let ring = IoUring::new(IO_URING_QUEUE_LENGTH)
            .map_err(|err| super::io_uring::io_error_context(err, "failed to setup io_uring"))?;

        let mut probe = Probe::new();
        ring.submitter().register_probe(&mut probe).map_err(|err| {
            super::io_uring::io_error_context(
                err,
                "failed to probe io_uring for supported operations",
            )
        })?;

        let ok = probe.is_supported(opcode::Read::CODE) && probe.is_supported(opcode::Write::CODE);
        self.supported = Some(ok);

        if ok {
            // Keep the ring we just created.
            self.rings.push(ring);
            Ok(())
        } else {
            Err(io::Error::other(
                "io_uring does not support required operations",
            ))
        }
    }

    fn take(&mut self) -> io::Result<IoUring> {
        self.check_supported()?;

        if let Some(ring) = self.rings.pop() {
            Ok(ring)
        } else {
            IoUring::new(IO_URING_QUEUE_LENGTH)
                .map_err(|err| super::io_uring::io_error_context(err, "failed to setup io_uring"))
        }
    }

    fn give_back(&mut self, ring: IoUring) {
        if self.rings.len() < POOL_SIZE {
            self.rings.push(ring);
        }
        // else: drop the excess ring
    }
}

/// RAII guard that owns an `IoUring` instance and returns it to the
/// thread-local pool on drop.
pub(super) struct IoUringGuard {
    ring: Option<IoUring>,
}

impl IoUringGuard {
    pub fn io_uring(&mut self) -> &mut IoUring {
        self.ring.as_mut().expect("IoUringGuard used after drop")
    }
}

impl Drop for IoUringGuard {
    fn drop(&mut self) {
        if let Some(ring) = self.ring.take() {
            POOL.with(|pool| {
                if let Ok(mut pool) = pool.try_borrow_mut() {
                    pool.give_back(ring);
                }
                // If the pool RefCell is already borrowed (e.g. panic during
                // take), just let the ring drop.
            });
        }
    }
}

/// Take an `IoUring` instance from the thread-local pool.
///
/// Returns an error if io_uring is not supported on this system.
/// The instance is automatically returned to the pool when the guard is dropped.
pub(super) fn take_io_uring() -> io::Result<IoUringGuard> {
    POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        let ring = pool.take()?;
        Ok(IoUringGuard { ring: Some(ring) })
    })
}

/// Check that io_uring is supported without taking an instance.
pub(super) fn check_io_uring_supported() -> io::Result<()> {
    POOL.with(|pool| pool.borrow_mut().check_supported())
}
