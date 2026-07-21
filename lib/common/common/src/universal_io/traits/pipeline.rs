//! Read pipelines.
//!
//! Workflow:
//! 1. Push read operations using `schedule()`
//! 2. Pull the completed results using `wait()`
//! 3. Interleave steps 1 and 2 as needed
//!
//! Each backend implements [`ReadPipeline`] where every read is scheduled
//! against an externally-held `&'file File` and [`wait`](ReadPipeline::wait)
//! yields data bounded by that `'file`.
//!
//! The owned variant – a pipeline that *owns* its file so it can live in a
//! long-lived structure independent of any file borrow – is provided generically
//! by [`OwnedPipeline`] on top of the same impl. It is the only place where
//! self-referential `unsafe` is used.
//!
//! ## On the `'file` lifetime
//!
//! For backends whose reads borrow from the file (mmap, disk cache) `'file`
//! genuinely bounds the returned slice. For backends whose reads produce *owned*
//! buffers (`io_uring`, object store) `'file` never bounds returned data — it is
//! a pure *file-safety guard* ensuring the file (and any fd or runtime referencing it)
//! outlives all in-flight operations.

use std::borrow::Cow;
use std::fmt::Debug;
use std::mem::ManuallyDrop;
use std::ops::Range;

use super::{Item, UniversalRead};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::{UioResult, UserData};

/// File-borrowing read pipeline.
///
/// Reads are scheduled against an externally-held `&'file File`. Results from [`wait`]
/// are bound by `'file`, i.e. they may outlive the pipeline itself, but not the file.
///
/// See module for the meaning of `'file` across backends, and [`OwnedPipeline`]
/// for the file-owning variant.
///
/// [`wait`]: Self::wait
pub trait ReadPipeline<'file, U>: Sized
where
    U: UserData,
{
    type File: 'file;

    fn new() -> UioResult<Self>;

    fn can_schedule(&mut self) -> bool;

    /// Schedule a read operation.
    ///
    /// An implementation might add it to an internal queue, but not actually execute it
    /// until [`wait`] is called.
    ///
    /// Should be called only when [`can_schedule`] is `true`.
    /// Returns [`UniversalIoError::QueueIsFull`] otherwise.
    ///
    /// [`wait`]: Self::wait
    /// [`can_schedule`]: Self::can_schedule
    /// [`UniversalIoError::QueueIsFull`]: crate::universal_io::UniversalIoError::QueueIsFull
    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file Self::File,
        range: Range<u64>,
        align: usize,
    ) -> UioResult<()>;

    /// Like `Self::schedule`, but doesn't need to know file length upfront.
    /// Reads starting at `from` offset until the end of file.
    fn schedule_whole(&mut self, user_data: U, file: &'file Self::File, from: u64)
    -> UioResult<()>;

    /// Block until any scheduled operation completes and consume its result.
    fn wait(&mut self) -> UioResult<Option<(U, ACow<'file>)>>;

    #[inline]
    fn wait_bytemuck<T: Item>(&mut self) -> UioResult<Option<(U, Cow<'file, [T]>)>> {
        let Some((user_data, bytes)) = self.wait()? else {
            return Ok(None);
        };

        let items = bytes
            .try_cast_bytemuck()
            .expect("data has compatible layout");

        Ok(Some((user_data, items)))
    }
}

/// File-owning adapter over a [`ReadPipeline`].
///
/// Owns the file so the pipeline can be held in long-lived structures with no file borrow.
///
/// `OwnedPipeline` implementation reborrows file with `'static` lifetime, which is sound,
/// because [`wait`] returns `ACow<'_>` bound to `&mut self`, which guarantees
/// that `OwnedPipeline` (and file that it owns) can't outlive returned `ACow`s.
///
/// [`wait`]: Self::wait
pub struct OwnedPipeline<R, U>
where
    R: UniversalRead + 'static,
    U: UserData,
{
    pipeline: ManuallyDrop<R::ReadPipeline<'static, U>>,

    // file must be wrapped in Box, to provide *stable address* for safe
    // self-referential borrows
    file: ManuallyDrop<Box<R>>,
}

impl<R, U> Debug for OwnedPipeline<R, U>
where
    R: UniversalRead,
    U: UserData,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedPipeline")
            .field("file", &self.file)
            .finish_non_exhaustive()
    }
}

impl<R, U> OwnedPipeline<R, U>
where
    R: UniversalRead + 'static,
    U: UserData,
{
    pub fn new(file: R) -> UioResult<Self> {
        let pipeline = R::ReadPipeline::new()?;

        let pipeline = Self {
            pipeline: ManuallyDrop::new(pipeline),
            file: ManuallyDrop::new(Box::new(file)),
        };

        Ok(pipeline)
    }

    #[inline]
    pub fn can_schedule(&mut self) -> bool {
        self.pipeline.can_schedule()
    }

    pub fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        range: Range<u64>,
        align: usize,
    ) -> UioResult<()> {
        // SAFETY:
        //
        // Safe, because neither inner pipeline, nor returned data can ever outlive file.
        // Drop implementation guarantees that inner pipeline is always dropped before file.
        // Wait returns ACow<'_>, which mutably borrows self, so pipeline can only be dropped
        // after all returned ACow-s are dropped.

        let file: &R = &self.file;
        let file: &'static R = unsafe { (file as *const R).as_ref_unchecked() };

        self.pipeline.schedule::<P>(user_data, file, range, align)
    }

    /// Like [`schedule`](Self::schedule), but reads the entire file
    pub fn schedule_whole(&mut self, user_data: U, from: u64) -> UioResult<()> {
        // SAFETY:
        //
        // Same as schedule

        let file: &R = &self.file;
        let file: &'static R = unsafe { (file as *const R).as_ref_unchecked() };

        self.pipeline.schedule_whole(user_data, file, from)
    }

    #[inline]
    pub fn wait(&mut self) -> UioResult<Option<(U, ACow<'_>)>> {
        // pipeline returns ACow<'static>, but we shorten its lifetime to ACow<'_>,
        // which mutably borrows self, so pipeline can only be dropped after all returned
        // ACow-s are dropped

        self.pipeline.wait()
    }

    #[inline]
    pub fn wait_bytemuck<T: Item>(&mut self) -> UioResult<Option<(U, Cow<'_, [T]>)>> {
        let Some((user_data, bytes)) = self.wait()? else {
            return Ok(None);
        };

        let items = bytes
            .try_cast_bytemuck()
            .expect("data has compatible layout");

        Ok(Some((user_data, items)))
    }

    #[expect(
        clippy::let_and_return,
        reason = "better readability around unsafe code"
    )]
    pub fn into_inner(self) -> R {
        // Wrap self in ManuallyDrop, so Drop doesn't run at the end of into_inner scope
        let mut this = ManuallyDrop::new(self);

        // Drop inner (during destructure), then return file
        let file = unsafe { this.destructure() };
        file
    }
}

impl<R, U> OwnedPipeline<R, U>
where
    R: UniversalRead + 'static,
    U: UserData,
{
    /// Destructure the pipeline. Drop inner `pipeline` and return `file`.
    ///
    /// # Safety:
    ///
    /// Must only be called once. `self` is uninitilized after `destructure` call,
    /// so caller must prevent `Drop` from running and ensure `self` is not accessed
    /// after the call.
    unsafe fn destructure(&mut self) -> R {
        // Inner pipeline might borrow file, so we must drop it before returning file.
        //
        // Take file and pipeline out of ManuallyDrop, so that we can rely on normal drop
        // at the end of scope or during panic.
        //
        // Default drop order is *reverse* of declaration order, so we must take file *first*
        // and pipeline *last*.

        let Self { pipeline, file } = self;

        let file = unsafe { ManuallyDrop::take(file) };
        let pipeline = unsafe { ManuallyDrop::take(pipeline) };

        drop(pipeline);
        *file
    }
}

impl<R, U> Drop for OwnedPipeline<R, U>
where
    R: UniversalRead + 'static,
    U: UserData,
{
    fn drop(&mut self) {
        // Drop inner (during destructure), then drop file explicitly
        let file = unsafe { self.destructure() };
        drop(file);
    }
}
