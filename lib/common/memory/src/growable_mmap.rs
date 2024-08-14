use std::fs::File;

use anyhow::Result;
use mmap_rs::{MmapFlags, MmapMut, MmapOptions, ReservedMut, UnsafeMmapFlags};

pub struct GrowableMutMmap {
    file: File,
    reserved: Option<ReservedMut>,
    allocated: Option<MmapMut>,
}

impl GrowableMutMmap {
    pub fn new(file: File, max_size: usize) -> Result<Self> {
        anyhow::ensure!(
            max_size % MmapOptions::page_size() == 0,
            "max_size must be a multiple of the page size",
        );

        Ok(Self {
            file,
            reserved: Some(MmapOptions::new(max_size)?.reserve_mut()?),
            allocated: None,
        })
    }

    fn grow_file(&mut self, min_total_size: usize) -> Result<()> {
        let min_total_size = min_total_size.try_into()?;
        let old_file_len = self.file.metadata()?.len();
        if old_file_len < min_total_size {
            self.file.set_len(min_total_size)?;
        }
        Ok(())
    }

    fn mmaped_size(&self) -> usize {
        self.allocated.as_ref().map(|m| m.len()).unwrap_or(0)
    }

    pub fn grow(&mut self, additional_size: usize) -> Result<()> {
        anyhow::ensure!(
            additional_size > 0 && additional_size % MmapOptions::page_size() == 0,
            "size must be a positive multiple of the page size",
        );
        let remaining_len = self.reserved.as_ref().map(|r| r.len()).unwrap_or(0);
        anyhow::ensure!(
            remaining_len >= additional_size,
            "not enough reserved space to grow to size",
        );
        let options = MmapOptions::new(additional_size)?;
        self.grow_file(self.mmaped_size() + additional_size)?;

        let new_chunk_reserved = if remaining_len == additional_size {
            // TODO: put reserved back in case of an error
            self.reserved.take().expect("reserved can't be None")
        } else {
            self.reserved
                .as_mut()
                .expect("reserved can't be None")
                .split_to(additional_size)?
        };

        let res = unsafe {
            options
                .with_address(new_chunk_reserved.start())
                .with_flags(MmapFlags::SHARED)
                .with_unsafe_flags(UnsafeMmapFlags::MAP_FIXED)
                .with_file(&self.file, self.mmaped_size() as u64)
                .map_mut()
        };
        let new_chunk = match res {
            Ok(x) => x,
            Err(e) => {
                // TODO: merge reserved back
                return Err(e.into());
            }
        };
        std::mem::forget(new_chunk_reserved);

        self.allocated = match self.allocated.take() {
            None => Some(new_chunk),
            Some(mut allocated) => {
                match allocated.merge(new_chunk) {
                    Ok(()) => (),
                    Err((e, _chunk)) => {
                        return Err(e.into());
                    }
                }
                Some(allocated)
            }
        };

        Ok(())
    }

    #[allow(clippy::should_implement_trait)]
    pub fn as_mut(&mut self) -> &mut [u8] {
        match self.allocated.as_mut() {
            Some(x) => x.as_mut_slice(),
            None => {
                let ptr = self
                    .reserved
                    .as_mut()
                    .expect("reserved can't be None")
                    .as_mut_ptr();
                unsafe { std::slice::from_raw_parts_mut(ptr, 0) }
            }
        }
    }

    pub fn flush(&self) -> Result<()> {
        if let Some(allocated) = self.allocated.as_ref() {
            allocated.flush(0..allocated.len())?;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: usize = 16384;

    #[test]
    fn test_growable_mut_mmap() {
        let file = tempfile::tempfile().unwrap();

        let mut mmap = GrowableMutMmap::new(file, PAGE * 3).unwrap();
        let start_ptr = mmap.as_mut().as_ptr();
        assert_eq!(mmap.as_mut().len(), 0);

        mmap.grow(PAGE).unwrap();
        assert_eq!(mmap.as_mut().len(), PAGE);
        assert_eq!(mmap.as_mut().as_ptr(), start_ptr);
        mmap.as_mut()[0] = b'a';
        mmap.as_mut()[PAGE - 1] = b'b';

        mmap.grow(PAGE).unwrap();
        assert_eq!(mmap.as_mut().len(), PAGE * 2);
        assert_eq!(mmap.as_mut().as_ptr(), start_ptr);
        assert_eq!(mmap.as_mut()[0], b'a');
        assert_eq!(mmap.as_mut()[PAGE - 1], b'b');
        mmap.as_mut()[PAGE] = b'c';
        mmap.as_mut()[PAGE * 2 - 1] = b'd';
    }
}
