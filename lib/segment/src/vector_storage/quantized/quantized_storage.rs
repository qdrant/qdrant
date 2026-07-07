use std::borrow::Cow;
use std::io::BufWriter;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::mmap::{AdviceSetting, MmapFlusher, advice};
use common::types::PointOffsetType;
use common::universal_io::{
    MmapFile, MmapFs, OpenOptions, Populate, ReadOnly, ReadRange, UniversalRead, UniversalReadFs,
};
use fs_err as fs;
use memmap2::MmapMut;

use crate::common::operation_error::{OperationError, OperationResult};

#[derive(Debug)]
pub struct QuantizedStorage<S: UniversalRead> {
    storage: ReadOnly<S>,
    quantized_vector_size: NonZeroUsize,
    path: PathBuf,
}

impl<S: UniversalRead> QuantizedStorage<S> {
    pub fn populate(&self) {
        if let Err(err) = self.storage.populate() {
            log::warn!("Failed to populate quantized storage: {err}")
        };
    }

    pub fn clear_cache(&self) {
        let Self {
            storage: mmap,
            quantized_vector_size: _,
            path: _,
        } = self;
        if let Err(err) = mmap.clear_ram_cache() {
            log::warn!("Failed to clear quantized storage RAM cache: {err}")
        }
    }
}

impl QuantizedStorage<MmapFile> {
    /// Open the backing file for build-time bulk appends, bypassing the read-only mmap.
    pub(crate) fn open_appender(&self) -> std::io::Result<BufWriter<fs::File>> {
        Ok(BufWriter::new(open_append(&self.path)?))
    }

    /// Re-mmap after the file grew so reads observe appended vectors. Build-time only.
    pub(crate) fn reload(&mut self) -> OperationResult<()> {
        let path = self.path.clone();
        *self = Self::from_file(&MmapFs, &path, self.quantized_vector_size.get())?;
        Ok(())
    }
}

/// Open a file shortly for appending.
fn open_append(path: &Path) -> std::io::Result<fs::File> {
    fs::OpenOptions::new().append(true).open(path)
}

pub struct QuantizedStorageBuilder<S> {
    mmap: MmapMut,
    cursor_pos: usize,
    quantized_vector_size: NonZeroUsize,
    path: PathBuf,
    output_storage: PhantomData<S>,
}

impl<S: UniversalRead> QuantizedStorage<S> {
    fn open_options() -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Global,
        }
    }

    pub fn from_file(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        quantized_vector_size: usize,
    ) -> OperationResult<QuantizedStorage<S>> {
        let storage =
            ReadOnly::from_file(fs.open(path, Self::open_options(), Default::default())?);

        let quantized_vector_size = NonZeroUsize::new(quantized_vector_size).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "`quantized_vector_size` must be non-zero",
            )
        })?;
        let len = storage.len::<u8>()? as usize;
        if !len.is_multiple_of(quantized_vector_size.get()) {
            return Err(OperationError::inconsistent_storage(format!(
                "Encoded file size ({len}) is not a multiple of quantized_vector_size ({quantized_vector_size})",
            )));
        }
        Ok(Self {
            storage,
            quantized_vector_size,
            path: path.to_path_buf(),
        })
    }

    /// Open the encoded vectors at `path`, creating an empty storage if the file does not yet exist.
    pub fn open(
        fs: &S::Fs,
        path: &Path,
        quantized_vector_size: usize,
        prefault: bool,
    ) -> OperationResult<QuantizedStorage<S>> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Ensure the backing file exists without clobbering existing data:
        // `from_file` mmaps the file read-only and fails if it is missing.
        fs_err::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let storage = Self::from_file(fs, path, quantized_vector_size)?;

        if prefault {
            storage.populate();
        }

        Ok(storage)
    }
}

impl<S: UniversalRead> quantization::EncodedStorage for QuantizedStorage<S> {
    fn get_vector_data(&self, index: PointOffsetType) -> Cow<'_, [u8]> {
        self.get_vector_data_opt(index).expect("vector exists")
    }

    fn get_vector_data_opt(&self, index: PointOffsetType) -> Option<Cow<'_, [u8]>> {
        let start = (self.quantized_vector_size.get() * index as usize) as u64;
        let length = self.quantized_vector_size.get() as u64;
        self.storage
            .read::<Random, u8>(ReadRange {
                byte_offset: start,
                length,
            })
            .ok()
    }

    fn upsert_vector(
        &mut self,
        _id: PointOffsetType,
        _vector: &[u8],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Cannot upsert vector in mmap storage",
        ))
    }

    fn is_in_ram_or_mmap() -> bool {
        true
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn vectors_count(&self) -> usize {
        self.storage.len::<u8>().unwrap_or(0) as usize / self.quantized_vector_size.get()
    }

    fn flusher(&self) -> MmapFlusher {
        // Mmap storage does not need a flusher, as it is non-appendable and already backed by a file.
        Box::new(|| Ok(()))
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn heap_size_bytes(&self) -> usize {
        let Self {
            storage: _,
            quantized_vector_size: _,
            path: _,
        } = self;

        0
    }
}

impl quantization::EncodedStorageBuilder for QuantizedStorageBuilder<MmapFile> {
    type Storage = QuantizedStorage<MmapFile>;
    type Error = OperationError;

    fn build(self) -> OperationResult<QuantizedStorage<MmapFile>> {
        self.mmap.flush()?;

        let storage = ReadOnly::open(&MmapFs, &self.path, Self::Storage::open_options(), ())?;

        Ok(QuantizedStorage {
            storage,
            quantized_vector_size: self.quantized_vector_size,
            path: self.path,
        })
    }

    fn push_vector_data(&mut self, other: &[u8]) -> OperationResult<()> {
        debug_assert_eq!(
            self.quantized_vector_size.get(),
            other.len(),
            "Pushed vector size does not match expected quantized vector size"
        );
        debug_assert!(
            self.cursor_pos + other.len() <= self.mmap.len(),
            "Overflow allocated quantization storage mmap file (cursor_pos {} + len {} > total {})",
            self.cursor_pos,
            other.len(),
            self.mmap.len()
        );
        self.mmap[self.cursor_pos..self.cursor_pos + other.len()].copy_from_slice(other);
        self.cursor_pos += other.len();
        Ok(())
    }
}

impl<S> QuantizedStorageBuilder<S> {
    pub fn new(
        path: &Path,
        vectors_count: usize,
        quantized_vector_size: usize,
    ) -> std::io::Result<Self> {
        if quantized_vector_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "quantized_vector_size must be > 0",
            ));
        }
        let encoded_storage_size = quantized_vector_size * vectors_count;
        path.parent()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Path must have a parent directory",
                )
            })
            .and_then(fs::create_dir_all)?;

        let file = fs_err::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // Don't truncate because we explicitly set the length later
            .truncate(false)
            .open(path)?;
        file.set_len(encoded_storage_size as u64)?;

        let mmap = unsafe { MmapMut::map_mut(&file) }?;
        advice::madvise(&mmap, advice::get_global())?;
        Ok(Self {
            mmap,
            cursor_pos: 0,
            quantized_vector_size: NonZeroUsize::new(quantized_vector_size).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "`quantized_vector_size` must be non-zero",
                )
            })?,
            path: path.to_path_buf(),
            output_storage: PhantomData,
        })
    }
}
