use std::path::{Path, PathBuf};

use common::mmap::AdviceSetting;
use common::universal_io::{
    CachedReadFs, Populate, UniversalIoError, UniversalRead, UniversalReadFs,
};

use super::super::chunks::{
    check_mmap_file_name_pattern, chunk_name, chunk_open_options, chunks_prefix, read_chunks,
};
use super::super::config::{config_file, load_config, read_status_len, status_file};
use super::ChunkedVectorsRead;
use crate::common::operation_error::{OperationError, OperationResult};

impl<T: bytemuck::Pod + Send, S: UniversalRead> ChunkedVectorsRead<T, S> {
    /// Schedule background prefetch of every file [`Self::open`] will read.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        directory: &Path,
        advice: AdviceSetting,
        populate: Populate,
    ) -> OperationResult<()> {
        // Config file
        fs.schedule_prefetch(&config_file(directory), None, None)?;

        // Status file
        fs.schedule_prefetch(&status_file(directory), None, None)?;

        // Chunks
        preopen_chunks(fs, directory, advice, populate)?;
        Ok(())
    }

    /// Open an existing chunked-vectors directory in read-only mode.
    ///
    /// Both `config.json` and `status.dat` must already exist; this function
    /// will not create them.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        directory: &Path,
        dim: usize,
        advice: AdviceSetting,
        populate: Populate,
    ) -> OperationResult<Self> {
        let config_file = config_file(directory);
        let config = load_config(fs, &config_file)?.ok_or_else(|| {
            OperationError::service_error(format!(
                "Config file {} is missing",
                config_file.display(),
            ))
        })?;
        if config.dim != dim {
            return Err(OperationError::service_error(format!(
                "Wrong configuration in {}: expected {}, found {dim}",
                config_file.display(),
                config.dim,
            )));
        }

        let len = read_status_len(fs, &status_file(directory))?;
        let chunks = read_chunks(fs, directory, advice, populate, false)?;

        Ok(Self {
            config,
            len,
            chunks,
            directory: directory.to_owned(),
            advice,
            populate,
        })
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut files = Vec::new();
        files.push(config_file(&self.directory));
        files.push(status_file(&self.directory));
        for chunk_idx in 0..self.chunks.len() {
            files.push(chunk_name(&self.directory, chunk_idx));
        }
        files
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        vec![config_file(&self.directory)] // TODO: Is config immutable?
    }

    pub fn populate(&self) -> OperationResult<()> {
        for chunk in &self.chunks {
            chunk.populate()?;
        }
        Ok(())
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            config: _,
            len: _,
            chunks,
            directory: _,
            advice: _,
            populate: _,
        } = self;
        for chunk in chunks {
            chunk.clear_ram_cache()?;
        }
        Ok(())
    }
}

/// Schedule background prefetch of every chunk file [`read_chunks`] will open.
fn preopen_chunks(
    fs: &impl CachedReadFs,
    directory: &Path,
    advice: AdviceSetting,
    populate: Populate,
) -> Result<(), UniversalIoError> {
    for listed in fs.list_files(&chunks_prefix(directory))? {
        let is_chunk = listed
            .path
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .and_then(check_mmap_file_name_pattern)
            .is_some();

        if is_chunk {
            fs.schedule_prefetch(
                &listed.path,
                Some(chunk_open_options(advice, populate, false)),
                None,
            )?;
        }
    }
    Ok(())
}
