use std::path::{Path, PathBuf};

use common::fs::atomic_save_json;
use common::mmap::AdviceSetting;
use common::universal_io::{
    OpenOptions, Populate, StoredStruct, UniversalKind, UniversalReadFileOps, UniversalWrite,
};

use super::ChunkedVectors;
use super::chunks::read_chunks;
use super::config::{ChunkedVectorsConfig, Status, config_file, load_config, status_file};
use super::read_only::ReadOnlyChunkedVectors;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::common::CHUNK_SIZE;

impl<T, S> ChunkedVectors<T, S>
where
    T: bytemuck::Pod + Send,
    S: UniversalWrite + Send + 'static,
{
    pub fn storage_kind() -> UniversalKind {
        S::kind()
    }

    pub fn ensure_status_file(fs: &S::Fs, directory: &Path) -> OperationResult<PathBuf> {
        let status_file = status_file(directory);
        if !fs.exists(&status_file)? {
            {
                let length = std::mem::size_of::<Status>();
                // TODO(uio): migrate when UniversalWriteFileOps is available
                common::mmap::create_and_ensure_length(&status_file, length)?;
            }
        }
        Ok(status_file)
    }

    pub(super) fn ensure_config(
        fs: &S::Fs,
        directory: &Path,
        dim: usize,
        populate: bool,
    ) -> OperationResult<ChunkedVectorsConfig> {
        let config_file = config_file(directory);
        match load_config(fs, &config_file) {
            Ok(Some(config)) => {
                if config.dim == dim {
                    Ok(config)
                } else {
                    Err(OperationError::service_error(format!(
                        "Wrong configuration in {}: expected {}, found {dim}",
                        config_file.display(),
                        config.dim,
                    )))
                }
            }
            Ok(None) => Self::create_config(&config_file, dim, populate),
            Err(e) => {
                log::error!("Failed to deserialize config file {config_file:?}: {e}");
                Self::create_config(&config_file, dim, populate)
            }
        }
    }

    fn create_config(
        config_file: &Path,
        dim: usize,
        populate: bool,
    ) -> OperationResult<ChunkedVectorsConfig> {
        if dim == 0 {
            return Err(OperationError::service_error(
                "The vector's dimension cannot be 0",
            ));
        }

        let chunk_size_bytes = CHUNK_SIZE;
        let vector_size_bytes = dim * std::mem::size_of::<T>();
        let chunk_size_vectors = chunk_size_bytes / vector_size_bytes;
        let corrected_chunk_size_bytes = chunk_size_vectors * vector_size_bytes;

        let config = ChunkedVectorsConfig {
            chunk_size_bytes: corrected_chunk_size_bytes,
            chunk_size_vectors,
            dim,
            populate: Some(populate),
        };
        atomic_save_json(config_file, &config)?;
        Ok(config)
    }

    pub fn open(
        fs: S::Fs,
        directory: &Path,
        dim: usize,
        advice: AdviceSetting,
        populate: Populate,
    ) -> OperationResult<Self> {
        fs_err::create_dir_all(directory)?;
        let status_path = Self::ensure_status_file(&fs, directory)?;

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
        )?;

        let config = Self::ensure_config(&fs, directory, dim, populate.to_bool::<S>())?;
        let chunks = read_chunks(&fs, directory, advice, populate, true)?;
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
