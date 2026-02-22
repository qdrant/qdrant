use crate::mmap::{
    Advice, AdviceSetting, MULTI_MMAP_IS_SUPPORTED, MmapSlice, MmapSliceReadOnly,
    UniversalMmapChunk, open_read_mmap, open_write_mmap,
};
use crate::universal_io::{BytesRange, OpenOptions, UniversalRead};
use std::borrow::Cow;
use std::path::Path;

pub struct MmapUniversal<T: Copy + 'static> {
    /// Main data mmap slice for read/write
    ///
    /// Best suited for random reads.
    mmap: MmapSlice<T>,
    /// Read-only mmap slice best suited for sequential reads
    ///
    /// `None` on platforms that do not support multiple memory maps to the same file.
    /// Use [`as_seq_slice`] utility function to access this mmap slice if available.
    mmap_seq: Option<MmapSliceReadOnly<T>>,
}

impl<T: Copy + 'static> UniversalRead<T> for MmapUniversal<T> {
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> crate::universal_io::Result<Self>
    where
        Self: Sized,
    {
        let OpenOptions {
            need_sequential,
            disk_parallel: _,  // Not applicable for MMAPs
            populate,
        } = options;

        let mmap_file = path.as_ref();
        let advice = AdviceSetting::Global;

        let mmap = open_write_mmap(&mmap_file, advice, populate.unwrap_or_default())?;
        let mmap = unsafe { MmapSlice::try_from(mmap) }?;

        // Only open second mmap for sequential reads if supported
        let mmap_seq = if *MULTI_MMAP_IS_SUPPORTED && need_sequential {
            let mmap_seq =
                open_read_mmap(&mmap_file, AdviceSetting::Advice(Advice::Sequential), false)?;
            Some(unsafe { MmapSliceReadOnly::try_from(mmap_seq) }?)
        } else {
            None
        };

        Ok(MmapUniversal { mmap, mmap_seq })
    }

    fn read<const SEQUENTIAL: bool>(
        &self,
        range: BytesRange,
    ) -> crate::universal_io::Result<Cow<'_, [T]>> {
        todo!()
    }

    fn read_batch<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = BytesRange>,
        callback: impl FnMut(usize, &[T]) -> crate::universal_io::Result<()>,
    ) -> crate::universal_io::Result<()> {
        todo!()
    }

    fn populate(&self) -> crate::universal_io::Result<()> {
        todo!()
    }

    fn clear_ram_cache(&self) -> crate::universal_io::Result<()> {
        todo!()
    }
}

impl<T: Copy + 'static> MmapUniversal<T> {
    /// Helper to get a slice suited for sequential reads if available, otherwise use the main mmap
    pub fn as_seq_slice(&self) -> &[T] {
        self.mmap_seq
            .as_ref()
            .map(|m| m.as_ref())
            .unwrap_or(self.mmap.as_ref())
    }

    pub fn as_slice<const SEQUENTIAL: bool>(&self) -> &[T] {
        if SEQUENTIAL {
            self.as_seq_slice()
        } else {
            self.mmap.as_ref()
        }
    }
}
