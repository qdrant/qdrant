//! Building (from a `CompressedPointMappings`) and opening the writable
//! disk-resident tracker.

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use common::bitvec::BitVec;
use common::mmap::{AdviceSetting, create_and_ensure_length};
use common::stored_bitslice::StoredBitSlice;
use common::universal_io::{OpenOptions, Populate, UniversalWrite};
use fs_err::File;

use super::on_disk_format::{e2i_path, i2e_path, store_e2i, store_i2e, store_is_uuid};
use super::reader::DiskMappingReader;
use super::versions_file::VersionsFile;
use super::{DiskIdTracker, VersionsFormat};
use crate::common::buffered_update_bitslice::BufferedUpdateBitSlice;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::immutable_id_tracker::deleted_path;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::types::SeqNumberType;

impl<S> DiskIdTracker<S>
where
    S: UniversalWrite + Send + Sync + 'static,
{
    pub fn from_in_memory_tracker(
        fs: &S::Fs,
        in_memory_tracker: InMemoryIdTracker,
        path: &Path,
    ) -> OperationResult<Self> {
        let (internal_to_version, mappings) = in_memory_tracker.into_internal();
        let compressed_mappings = CompressedPointMappings::from_mappings(mappings);
        Self::new(fs, path, &internal_to_version, compressed_mappings)
    }

    /// Open an existing disk-resident id tracker: `deleted` and `versions` are
    /// read into RAM (small, mutated in place), the mapping stays on disk;
    /// `populate` says whether to prime the page cache with it.
    pub fn open(fs: &S::Fs, segment_path: &Path, populate: Populate) -> OperationResult<Self> {
        let deleted_storage = StoredBitSlice::open(
            fs,
            deleted_path(segment_path),
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::Blocking,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?;
        let mut deleted = BitVec::new();
        deleted.extend_from_bitslice(deleted_storage.read_all()?.as_ref());
        let deleted_wrapper = BufferedUpdateBitSlice::new(deleted_storage);

        let (internal_to_version, versions_file) = VersionsFile::open(fs, segment_path)?;

        let reader = DiskMappingReader::open(fs, segment_path, populate)?;

        Ok(Self {
            path: segment_path.to_path_buf(),
            reader,
            deleted,
            deleted_wrapper,
            internal_to_version,
            versions_file,
        })
    }

    /// Build the tracker files at `path`, with versions in the
    /// [format](VersionsFormat::from_feature_flags) of the deployment.
    pub fn new(
        fs: &S::Fs,
        path: &Path,
        internal_to_version: &[SeqNumberType],
        mappings: CompressedPointMappings,
    ) -> OperationResult<Self> {
        let format = VersionsFormat::from_feature_flags();
        Self::new_with_versions_format(fs, path, internal_to_version, mappings, format)
    }

    pub(super) fn new_with_versions_format(
        fs: &S::Fs,
        path: &Path,
        internal_to_version: &[SeqNumberType],
        mappings: CompressedPointMappings,
        versions_format: VersionsFormat,
    ) -> OperationResult<Self> {
        let total = mappings.total_point_count();
        debug_assert!(mappings.deleted().len() <= total);

        // Deleted bitvec file: one bit per point, rounded up to a `u64` multiple.
        let deleted_filepath = deleted_path(path);
        create_and_ensure_length(
            &deleted_filepath,
            total
                .div_ceil(u8::BITS as usize)
                .next_multiple_of(size_of::<u64>()),
        )?;
        let mut deleted_storage = StoredBitSlice::open(
            fs,
            &deleted_filepath,
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::Auto,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?;
        deleted_storage.write_bitslice(mappings.deleted())?;
        deleted_storage.set_ascending_bits_batch(
            (mappings.deleted().len()..total).map(|i| (i as u64, true)),
        )?;
        deleted_storage.flusher()()?;

        // Resident deleted mirror: same bits as on disk (trailing points beyond
        // `mappings.deleted()` are deleted).
        let mut deleted = BitVec::new();
        deleted.extend_from_bitslice(mappings.deleted());
        deleted.resize(total, true);

        let deleted_wrapper = BufferedUpdateBitSlice::new(deleted_storage);

        // Versions: one per point.
        let versions_count = internal_to_version.len().max(total);
        let (internal_to_version, versions_file) = VersionsFile::create(
            fs,
            path,
            internal_to_version,
            versions_count,
            versions_format,
        )?;
        debug_assert_eq!(internal_to_version.len(), versions_count);

        // Mapping files (immutable): i2e + e2i + the is_uuid sidecar.
        write_mapping_file(i2e_path(path), |writer| store_i2e(&mappings, writer))?;
        write_mapping_file(e2i_path(path), |writer| store_e2i(&mappings, writer))?;
        store_is_uuid(fs, path, &mappings)?;

        deleted_wrapper.flusher()()?;

        // Just written, so cached already; the configured placement applies
        // when the built segment is loaded.
        let reader = DiskMappingReader::open(fs, path, Populate::No)?;

        Ok(Self {
            path: path.to_path_buf(),
            reader,
            deleted,
            deleted_wrapper,
            internal_to_version,
            versions_file,
        })
    }
}

/// Create a mapping file and write it with an explicit fsync.
fn write_mapping_file(
    path: PathBuf,
    write: impl FnOnce(&mut BufWriter<File>) -> OperationResult<()>,
) -> OperationResult<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    write(&mut writer)?;
    writer.flush()?;
    writer.into_inner().unwrap().sync_all()?;
    Ok(())
}
