use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::AdviceSetting;
use common::universal_io::{
    OpenOptions, Populate, StoredStruct, UniversalKind, UniversalReadFs, UniversalWrite,
    UniversalWriteFs,
};

use super::ChunkedVectors;
use super::chunks::read_chunks;
use super::config::{Status, ensure_config, status_file};
use super::read_only::ReadOnlyChunkedVectors;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;

impl<T, S> ChunkedVectors<T, S>
where
    T: bytemuck::Pod + Send,
    S: UniversalWrite + Send + 'static,
{
    pub fn storage_kind() -> UniversalKind {
        S::kind()
    }

    pub fn open(
        fs: S::Fs,
        directory: &Path,
        dim: usize,
        advice: AdviceSetting,
        populate: Populate,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        fs_err::create_dir_all(directory)?;
        let status_path = status_file(directory);
        if !fs.exists(&status_path, hw_counter)? {
            fs.create(&status_path, size_of::<Status>(), hw_counter)?;
        }

        let status: StoredStruct<S, Status> = StoredStruct::open(
            &fs,
            status_path,
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate,
                advice: AdviceSetting::Global,
            },
            Default::default(),
            hw_counter,
        )?;

        let config =
            ensure_config::<T, _>(&fs, directory, dim, populate.to_bool::<S>(), hw_counter)?;
        let chunks = read_chunks(&fs, directory, advice, populate, true, hw_counter)?;
        let inner = ReadOnlyChunkedVectors {
            config,
            len: status.len,
            chunks,
            directory: directory.to_owned(),
            advice,
            populate,
        };
        Ok(Self { inner, status, fs })
    }

    pub fn flusher(&self) -> Flusher {
        Box::new({
            let status_flusher = self.status.flusher();
            let chunks_flushers: Vec<_> = self
                .inner
                .chunks
                .iter()
                .map(|chunk| chunk.flusher())
                .collect();
            move || {
                for flusher in chunks_flushers {
                    flusher()?;
                }
                status_flusher()?;
                Ok(())
            }
        })
    }
}
