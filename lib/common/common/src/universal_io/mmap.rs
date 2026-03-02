use std::borrow::Cow;
use std::path::{Path, PathBuf};

use crate::mmap::{
    Advice, AdviceSetting, MULTI_MMAP_IS_SUPPORTED, MmapSlice, MmapSliceReadOnly, open_read_mmap,
    open_write_mmap,
};
use crate::universal_io::multi_universal_read::{SourceId, StorageRead};
use crate::universal_io::{
    ElementOffset, ElementsRange, Flusher, OpenOptions, Result, StorageWrite, UniversalIoError,
};

#[derive(Debug)]
struct MmapEntry<T: Copy + 'static> {
    path: PathBuf,
    /// Main data mmap slice for read/write
    ///
    /// Best suited for random reads.
    mmap: MmapSlice<T>,
    /// Read-only mmap slice best suited for sequential reads
    ///
    /// `None` on platforms that do not support multiple memory maps to the same file.
    mmap_seq: Option<MmapSliceReadOnly<T>>,
}

impl<T: Copy + 'static> MmapEntry<T> {
    fn open(path: &Path, options: OpenOptions) -> Result<Self> {
        let OpenOptions {
            need_sequential,
            disk_parallel: _,
            populate,
            advice,
        } = options;

        let advice = advice.unwrap_or(AdviceSetting::Global);

        let mmap = open_write_mmap(path, advice, populate.unwrap_or_default()).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                UniversalIoError::NotFound {
                    path: path.to_path_buf(),
                }
            } else {
                e.into()
            }
        })?;
        let mmap = unsafe { MmapSlice::try_from(mmap) }?;

        let mmap_seq = if *MULTI_MMAP_IS_SUPPORTED && need_sequential {
            let mmap_seq = open_read_mmap(path, AdviceSetting::Advice(Advice::Sequential), false)?;
            Some(unsafe { MmapSliceReadOnly::try_from(mmap_seq) }?)
        } else {
            None
        };

        Ok(MmapEntry {
            path: path.to_path_buf(),
            mmap,
            mmap_seq,
        })
    }

    fn as_seq_slice(&self) -> &[T] {
        self.mmap_seq
            .as_ref()
            .map(|m| m.as_ref())
            .unwrap_or(self.mmap.as_ref())
    }

    fn as_slice<const SEQUENTIAL: bool>(&self) -> &[T] {
        if SEQUENTIAL {
            self.as_seq_slice()
        } else {
            self.mmap.as_ref()
        }
    }
}

/// Multi-file mmap-backed storage.
#[derive(Debug)]
pub struct MmapUniversal<T: Copy + 'static> {
    sources: Vec<MmapEntry<T>>,
    open_options: OpenOptions,
}

impl<T: Copy + 'static> MmapUniversal<T> {
    fn get_source(&self, source_id: SourceId) -> Result<&MmapEntry<T>> {
        self.sources
            .get(source_id.0)
            .ok_or(UniversalIoError::InvalidSourceId {
                source_id: source_id.0,
                num_sources: self.sources.len(),
            })
    }

    fn get_source_mut(&mut self, source_id: SourceId) -> Result<&mut MmapEntry<T>> {
        let num_sources = self.sources.len();
        self.sources
            .get_mut(source_id.0)
            .ok_or(UniversalIoError::InvalidSourceId {
                source_id: source_id.0,
                num_sources,
            })
    }
}

impl<T: Copy + 'static> StorageRead<T> for MmapUniversal<T> {
    fn new(options: OpenOptions) -> Self {
        Self {
            sources: Vec::new(),
            open_options: options,
        }
    }

    fn len(&self) -> usize {
        self.sources.len()
    }

    fn attach(&mut self, path: &Path) -> Result<SourceId> {
        let entry = MmapEntry::open(path, self.open_options)?;
        let id = SourceId(self.sources.len());
        self.sources.push(entry);
        Ok(id)
    }

    fn read_batch_multi<'a, const SEQUENTIAL: bool>(
        &'a self,
        requests: impl IntoIterator<Item = (SourceId, ElementsRange)>,
        mut callback: impl FnMut(usize, Cow<'a, [T]>) -> Result<()>,
    ) -> Result<()> {
        for (idx, (source_id, range)) in requests.into_iter().enumerate() {
            let source = self.get_source(source_id)?;
            let data_slice = source.as_slice::<SEQUENTIAL>();
            let start = range.start as usize;
            let end = start + range.length as usize;

            let data_range = data_slice
                .get(start..end)
                .ok_or(UniversalIoError::OutOfBounds {
                    start: start as u64,
                    end: end as u64,
                    data_length: data_slice.len(),
                })?;

            callback(idx, Cow::Borrowed(data_range))?;
        }
        Ok(())
    }

    fn read_whole(&self, source_id: SourceId) -> Result<Cow<'_, [T]>> {
        let source = self.get_source(source_id)?;
        Ok(Cow::Borrowed(source.as_slice::<true>()))
    }

    fn source_len(&self, source_id: SourceId) -> Result<u64> {
        Ok(self.get_source(source_id)?.mmap.len() as u64)
    }

    fn populate(&self) -> Result<()> {
        for source in &self.sources {
            if let Some(mmap_seq) = &source.mmap_seq {
                mmap_seq.populate()?;
            } else {
                source.mmap.populate()?;
            }
        }
        Ok(())
    }

    fn clear_ram_cache(&self) -> Result<()> {
        for source in &self.sources {
            crate::fs::clear_disk_cache(&source.path)?;
        }
        Ok(())
    }
}

impl<T: Copy + 'static> StorageWrite<T> for MmapUniversal<T> {
    fn write_batch_multi<'a>(
        &mut self,
        requests: impl IntoIterator<Item = (SourceId, ElementOffset, &'a [T])>,
    ) -> Result<()> {
        for (source_id, offset, data) in requests {
            let source = self.get_source_mut(source_id)?;
            let mmap_slice: &mut [T] = &mut source.mmap;
            let data_length = mmap_slice.len();
            let start = offset as usize;
            let end = start + data.len();

            let target = mmap_slice
                .get_mut(start..end)
                .ok_or(UniversalIoError::OutOfBounds {
                    start: offset,
                    end: offset + data.len() as u64,
                    data_length,
                })?;

            target.copy_from_slice(data);
        }
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let flushers: Vec<_> = self.sources.iter().map(|s| s.mmap.flusher()).collect();
        Box::new(move || {
            for f in flushers {
                f().map_err(UniversalIoError::Mmap)?;
            }
            Ok(())
        })
    }
}
