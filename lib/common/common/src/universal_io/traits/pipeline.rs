//! Read pipelines.
//!
//! Workflow:
//! 1. Push read operations using `schedule()`.
//! 2. Pull the completed results using `wait()`.
//! 3. Interleave steps 1 and 2 as needed.
//!
//! There are two variants of the pipeline traits.
//! The reason to have two traits is in the lifetime of the `wait()` result:
//! - [`BorrowedReadPipeline::wait()`]: `Cow<'file, [T]>`.
//!   I.e. the result might outlive the pipeline itself, but not the file.
//!   Thus, suitable for implementing an [`Iterator`] or other short-living
//!   structures over long-living files.
//! - [`OwnedReadPipeline::wait()`]:    `Cow<'_,    [T]>`.
//!   I.e. the result might outlive the file, but not the pipeline.
//!   Suitable for holding the pipeline in long-living structures.
//!
//! Other differences (e.g. single-file vs multi-file) are not fundamental and
//! can be adjusted later if needed.

use std::borrow::Cow;

use crate::generic_consts::AccessPattern;
use crate::universal_io::{ReadRange, Result, UserData};

pub trait BorrowedReadPipeline<'file, T, U>: Sized
where
    T: bytemuck::Pod,
    U: UserData,
{
    type File: 'file;

    fn new() -> Result<Self>;

    fn can_schedule(&mut self) -> bool;

    /// Schedule read operation.
    ///
    /// Note: an implementation might add it to internal queue, but not actually
    /// execute it until [`UniversalReadPipeline::wait()`] is called.
    ///
    /// Should be called only when [`UniversalReadPipeline::can_schedule()`] is
    /// `true`. Returns [`UniversalIoError::QueueIsFull`] otherwise.
    ///
    /// [`UniversalIoError::QueueIsFull`]: crate::universal_io::UniversalIoError::QueueIsFull
    fn schedule<P>(
        &mut self,
        user_data: U,
        file: &'file Self::File,
        range: ReadRange,
    ) -> Result<()>
    where
        P: AccessPattern;

    /// Block until any of the scheduled operations is completed and consume its
    /// result.
    fn wait(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>>;
}

pub trait OwnedReadPipeline<T, U>: Sized
where
    T: bytemuck::Pod,
    U: UserData,
{
    type File;

    fn new(file: &Self::File) -> Result<Self>;

    fn can_schedule(&mut self) -> bool;

    /// See [`BorrowedReadPipeline::schedule()`].
    fn schedule<P>(&mut self, user_data: U, range: ReadRange) -> Result<()>
    where
        P: AccessPattern;

    /// See [`BorrowedReadPipeline::wait()`].
    fn wait(&mut self) -> Result<Option<(U, Cow<'_, [T]>)>>;
}
