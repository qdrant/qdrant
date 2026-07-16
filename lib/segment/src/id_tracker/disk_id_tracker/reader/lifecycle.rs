//! Opening and preopening (prefetch scheduling) of the mapping files.

use std::io::Cursor;
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt};
use common::mmap::AdviceSetting;
use common::stored_bitmask::StoredBitmask;
use common::universal_io::{
    CachedReadFs, OkNotFound, OpenOptions, Populate, ReadRange, UniversalRead,
    UniversalReadFileOps, UniversalReadFs,
};

use super::DiskMappingReader;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::disk_id_tracker::on_disk_format::{
    E2I_HEADER_SIZE, E2iHeader, I2E_HEADER_SIZE, I2eHeader, e2i_path, i2e_path, is_uuid_path,
};

type Endian = LittleEndian;

impl<S: UniversalRead> DiskMappingReader<S> {
    fn open_options() -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::Partial(ReadRange::new(
                0,
                size_of::<I2eHeader>().max(size_of::<E2iHeader>()) as u64,
            )),
            advice: AdviceSetting::Global,
        }
    }

    /// The `is_uuid` file is read whole into RAM on open (and its handle
    /// dropped), so it is populated eagerly, unlike the lazily-served
    /// `i2e`/`e2i` handles.
    fn is_uuid_open_options() -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: true,
            populate: Populate::Blocking,
            advice: AdviceSetting::Global,
        }
    }

    /// Schedule background prefetch of the `i2e`/`e2i`/`is_uuid` handles that
    /// [`try_open`](Self::try_open) will open, without reading any bytes
    /// (except the `is_uuid` file, which is populated whole — open loads it
    /// fully into RAM).
    ///
    /// Returns `false` (nothing scheduled) when the mapping is
    /// not in the on-disk format.
    pub fn try_preopen(
        fs: &impl CachedReadFs<File = S>,
        segment_path: &Path,
    ) -> OperationResult<bool> {
        let i2e_path = i2e_path(segment_path);
        if !UniversalReadFileOps::exists(fs, &i2e_path)? {
            return Ok(false);
        }

        let options = Self::open_options();

        fs.schedule_prefetch(&i2e_path, Some(options), None)?;
        fs.schedule_prefetch(&e2i_path(segment_path), Some(options), None)?;
        fs.schedule_prefetch(
            &is_uuid_path(segment_path),
            Some(OpenOptions {
                // Prefetch must not stall on population; only the consuming
                // open blocks on it.
                populate: Populate::PreferBackground,
                ..Self::is_uuid_open_options()
            }),
            None,
        )?;

        Ok(true)
    }

    /// Open the `i2e`/`e2i` handles and read the headers + sparse index into
    /// RAM, along with the whole `is_uuid` bitmap. No other per-point data is
    /// read.
    ///
    /// Errors if the segment is not in the on-disk format (`i2e` absent). Use
    /// [`try_open`](Self::try_open) to probe without erroring.
    pub fn open(fs: &impl UniversalReadFs<File = S>, segment_path: &Path) -> OperationResult<Self> {
        Self::try_open(fs, segment_path)?.ok_or_else(|| {
            OperationError::service_error(format!(
                "on-disk id tracker mapping ({}) not found",
                i2e_path(segment_path).display(),
            ))
        })
    }

    /// Like [`open`](Self::open), but returns `Ok(None)` when the defining `i2e`
    /// file is absent — i.e. the segment is not in the on-disk format. Any other
    /// missing/corrupt file is a hard error. Opening the `i2e` handle also serves
    /// as the format probe, so no separate existence check is issued.
    pub fn try_open(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
    ) -> OperationResult<Option<Self>> {
        let options = Self::open_options();

        let Some(i2e) = fs
            .open(i2e_path(segment_path), options, Default::default())
            .ok_not_found()?
        else {
            return Ok(None);
        };
        let i2e_header_bytes = i2e.read::<common::generic_consts::Random, u8>(ReadRange {
            byte_offset: 0,
            length: I2E_HEADER_SIZE,
        })?;
        let i2e_header = I2eHeader::parse(i2e_header_bytes.as_ref())?;

        let e2i = fs.open(e2i_path(segment_path), options, Default::default())?;
        let e2i_header_bytes = e2i.read::<common::generic_consts::Random, u8>(ReadRange {
            byte_offset: 0,
            length: E2I_HEADER_SIZE,
        })?;
        let e2i_header = E2iHeader::parse(e2i_header_bytes.as_ref())?;

        // Read the whole sparse index (numeric then UUID) in a single range.
        let index_bytes = e2i.read::<common::generic_consts::Random, u8>(ReadRange {
            byte_offset: E2I_HEADER_SIZE,
            length: e2i_header.index_end() - E2I_HEADER_SIZE,
        })?;
        let mut cursor = Cursor::new(index_bytes.as_ref());
        let mut num_sparse = Vec::with_capacity(e2i_header.num_blocks() as usize);
        for _ in 0..e2i_header.num_blocks() {
            num_sparse.push(cursor.read_u64::<Endian>()?);
        }
        // Skip the alignment pad between the numeric and UUID sparse sections.
        cursor.set_position(e2i_header.uuid_sparse_offset - E2I_HEADER_SIZE);
        let mut uuid_sparse = Vec::with_capacity(e2i_header.uuid_blocks() as usize);
        for _ in 0..e2i_header.uuid_blocks() {
            uuid_sparse.push(cursor.read_u128::<Endian>()?);
        }

        // The whole `is_uuid` mask is materialized in RAM here; the handle is
        // dropped right after, so slot decoding never goes back to disk.
        let is_uuid_storage = StoredBitmask::<S>::open(
            fs,
            is_uuid_path(segment_path),
            Self::is_uuid_open_options(),
            Default::default(),
        )?;
        if is_uuid_storage.bit_len() != i2e_header.total {
            return Err(OperationError::inconsistent_storage(format!(
                "is_uuid mask covers {} slots, i2e has {}",
                is_uuid_storage.bit_len(),
                i2e_header.total,
            )));
        }
        let is_uuid = is_uuid_storage.read_ones()?;

        Ok(Some(Self {
            i2e,
            e2i,
            i2e_header,
            e2i_header,
            num_sparse,
            uuid_sparse,
            is_uuid,
        }))
    }
}
