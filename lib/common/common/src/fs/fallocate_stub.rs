use std::io;
use std::ops::Range;
use std::path::Path;

/// No-op stub of `../fallocate.rs`.
#[derive(Debug, Default)]
pub struct FallocatedPages(());

impl FallocatedPages {
    #[inline]
    pub fn ensure(&mut self, _: &Path, _: usize, _: Range<usize>) -> io::Result<()> {
        Ok(())
    }
}
