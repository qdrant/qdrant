use std::path::{Path, PathBuf};

use common::mmap::AdviceSetting;
use common::universal_io::{
    CachedReadFs, Populate, UniversalIoError, UniversalRead, UniversalReadFs,
};

use super::super::chunks::{
    VisiblePrefix, check_mmap_file_name_pattern, chunk_name, chunk_open_options, chunks_prefix,
    read_chunks_from, visible_chunk_populate,
};
use super::super::config::{
    chunk_size_vectors, config_file, load_config, read_status_len, status_file,
};
use super::ReadOnlyChunkedVectors;
use crate::common::operation_error::{OperationError, OperationResult};

impl<T: bytemuck::Pod + Send, S: UniversalRead> ReadOnlyChunkedVectors<T, S> {
    /// Schedule background prefetch of every file [`Self::open`] will read.
    ///
    /// With a `visible` prefix, only the chunks holding it are populated (see
    /// [`visible_chunk_populate`]): vectors past it are never read.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        directory: &Path,
        advice: AdviceSetting,
        populate: Populate,
        visible: Option<VisiblePrefix>,
    ) -> OperationResult<()> {
        // Config file
        fs.schedule_open(&config_file(directory), None, None);

        // Status file
        fs.schedule_open(&status_file(directory), None, None);

        // Chunks. The config is not read yet, so the chunk capacity is
        // derived the way the writer derives it.
        let chunk_populate = |chunk_id| match visible {
            Some(VisiblePrefix { len, dim }) => {
                let vector_size_bytes = dim * size_of::<T>();
                visible_chunk_populate(
                    populate,
                    chunk_id,
                    chunk_size_vectors(vector_size_bytes),
                    vector_size_bytes,
                    Some(len),
                )
            }
            None => populate,
        };
        preopen_chunks(fs, directory, advice, chunk_populate)?;
        Ok(())
    }

    /// Open an existing chunked-vectors directory in read-only mode.
    ///
    /// Both `config.json` and `status.dat` must already exist; this function
    /// will not create them.
    ///
    /// With `visible_len`, only the first `visible_len` vectors are ever read,
    /// here and by [`live_reload`](crate::common::live_reload::LiveReload::live_reload):
    /// chunks past them open cold, the chunk they end in populates its visible
    /// part only.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        directory: &Path,
        dim: usize,
        advice: AdviceSetting,
        populate: Populate,
        visible_len: Option<usize>,
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
        let chunks = read_chunks_from(
            fs,
            directory,
            0,
            advice,
            |chunk_id| config.chunk_populate(populate, chunk_id, size_of::<T>(), visible_len),
            false,
        )?;

        Ok(Self {
            config,
            len,
            chunks,
            directory: directory.to_owned(),
            advice,
            populate,
            visible_len,
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
            visible_len: _,
        } = self;
        for chunk in chunks {
            chunk.clear_ram_cache()?;
        }
        Ok(())
    }
}

/// Schedule background prefetch of every chunk file
/// [`ReadOnlyChunkedVectors::open`] will open, each with the populate
/// `populate` gives for its id.
fn preopen_chunks(
    fs: &impl CachedReadFs,
    directory: &Path,
    advice: AdviceSetting,
    populate: impl Fn(usize) -> Populate,
) -> Result<(), UniversalIoError> {
    for listed in fs.list_files(&chunks_prefix(directory))? {
        let chunk_id = listed
            .path
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .and_then(check_mmap_file_name_pattern);

        if let Some(chunk_id) = chunk_id {
            fs.schedule_open(
                &listed.path,
                Some(chunk_open_options(advice, populate(chunk_id), false)),
                None,
            );
        }
    }
    Ok(())
}
