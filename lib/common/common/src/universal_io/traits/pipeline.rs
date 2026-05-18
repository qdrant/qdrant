use std::borrow::Cow;

use crate::generic_consts::AccessPattern;
use crate::universal_io::{ReadRange, Result, UserData};

pub trait UniversalReadPipeline<'file, T, U>: Sized
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
