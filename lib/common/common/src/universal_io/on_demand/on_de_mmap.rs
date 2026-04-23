use std::borrow::Cow;
use std::path::{Path, PathBuf};

use super::OnDemandConfig;
use super::file::OnDemandFile;
use crate::generic_consts::AccessPattern;
use crate::universal_io::{
    MmapFile, OpenOptions, ReadRange, Result, UniversalKind, UniversalRead, UniversalReadFileOps,
};

/// Read-only storage for immutable files that transparently switches
/// between [`MmapFile`] and [`OnDemandFile<MmapFile>`].
///
/// When [`OnDemandConfig`] has been installed globally, every newly
/// opened file routes through [`OnDemandFile`] (blocks are fetched
/// lazily from the configured path and mirrored into the local cache
/// directory). Otherwise it behaves exactly like a plain [`MmapFile`].
///
/// The decision is made at [`open`](UniversalRead::open) time and
/// frozen into the enum variant; changing the global config afterward
/// does not affect already-opened files.
///
/// This wrapper intentionally does **not** implement
/// [`UniversalWrite`](crate::universal_io::UniversalWrite): it is meant
/// only for immutable on-disk state.
#[derive(Debug)]
pub enum OnDeMmapFile {
    Mmap(MmapFile),
    OnDemand(OnDemandFile<MmapFile>),
}

impl UniversalReadFileOps for OnDeMmapFile {
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        MmapFile::list_files(prefix_path)
    }

    fn exists(path: &Path) -> Result<bool> {
        MmapFile::exists(path)
    }
}

impl<T> UniversalRead<T> for OnDeMmapFile
where
    T: bytemuck::Pod,
{
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        if OnDemandConfig::global().is_some() {
            let inner = <OnDemandFile<MmapFile> as UniversalRead<T>>::open(path, options)?;
            Ok(Self::OnDemand(inner))
        } else {
            let inner = <MmapFile as UniversalRead<T>>::open(path, options)?;
            Ok(Self::Mmap(inner))
        }
    }

    fn read<P: AccessPattern>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        match self {
            Self::Mmap(f) => UniversalRead::<T>::read::<P>(f, range),
            Self::OnDemand(f) => UniversalRead::<T>::read::<P>(f, range),
        }
    }

    fn read_whole(&self) -> Result<Cow<'_, [T]>> {
        match self {
            Self::Mmap(f) => UniversalRead::<T>::read_whole(f),
            Self::OnDemand(f) => UniversalRead::<T>::read_whole(f),
        }
    }

    fn read_batch<'a, P: AccessPattern, Meta: 'a>(
        &'a self,
        ranges: impl IntoIterator<Item = (Meta, ReadRange)>,
        callback: impl FnMut(Meta, &[T]) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Mmap(f) => UniversalRead::<T>::read_batch::<P, Meta>(f, ranges, callback),
            Self::OnDemand(f) => UniversalRead::<T>::read_batch::<P, Meta>(f, ranges, callback),
        }
    }

    fn len(&self) -> Result<u64> {
        match self {
            Self::Mmap(f) => UniversalRead::<T>::len(f),
            Self::OnDemand(f) => UniversalRead::<T>::len(f),
        }
    }

    fn populate(&self) -> Result<()> {
        match self {
            Self::Mmap(f) => UniversalRead::<T>::populate(f),
            Self::OnDemand(f) => UniversalRead::<T>::populate(f),
        }
    }

    fn clear_ram_cache(&self) -> Result<()> {
        match self {
            Self::Mmap(f) => UniversalRead::<T>::clear_ram_cache(f),
            Self::OnDemand(f) => UniversalRead::<T>::clear_ram_cache(f),
        }
    }

    fn kind() -> UniversalKind {
        // Both variants present the same logical surface to callers.
        // Pick `OnDemand` when the global config is live so telemetry
        // reflects the dispatch mode actually in use.
        if OnDemandConfig::global().is_some() {
            UniversalKind::OnDemand
        } else {
            UniversalKind::Mmap
        }
    }
}
