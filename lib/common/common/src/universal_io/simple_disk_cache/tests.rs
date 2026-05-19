use std::borrow::Cow;
use std::path::PathBuf;

use fs_err as fs;

use super::{BLOCK_SIZE, DiskCache, DiskCacheConfig};
use crate::generic_consts::Sequential;
use crate::universal_io::{OpenOptions, ReadRange, UniversalRead};

fn make_test_data(n_bytes: usize) -> Vec<u8> {
    (0..n_bytes).map(|i| (i % 251) as u8).collect()
}

struct Scenario {
    _tmp: tempfile::TempDir,
    remote_path: PathBuf,
    data: Vec<u8>,
    config: DiskCacheConfig,
}

impl Scenario {
    fn new(n_bytes: usize) -> Self {
        let tmp = tempfile::Builder::new()
            .prefix("disk_cache_tests")
            .tempdir()
            .unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");
        fs::create_dir_all(&remote_dir).unwrap();
        fs::create_dir_all(&local_dir).unwrap();

        let remote_path = remote_dir.join("data.bin");
        let data = make_test_data(n_bytes);
        fs::write(&remote_path, &data).unwrap();

        Self {
            _tmp: tmp,
            remote_path,
            data,
            config: DiskCacheConfig::new(remote_dir, local_dir).unwrap(),
        }
    }

    fn expected_local_path(&self) -> PathBuf {
        self.config.local_path_for(&self.remote_path).unwrap()
    }

    fn open<R>(&self) -> DiskCache<R>
    where
        R: UniversalRead,
    {
        DiskCache::open_with_config(
            &self.config,
            &self.remote_path,
            OpenOptions {
                writeable: false,
                ..OpenOptions::default()
            },
        )
        .unwrap()
    }
}

#[duplicate::duplicate_item(
    tests_mod       R               cfg_predicate;
    [tests_mmap]    [MmapFile]      [cfg(all())];
    [tests_uring]   [IoUringFile]   [cfg(target_os = "linux")];
)]
#[cfg_predicate]
#[cfg(test)]
mod tests_mod {
    use super::*;
    #[cfg_predicate]
    use crate::universal_io::R;
    #[test]
    fn basic_read_returns_remote_bytes() {
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = scn.open::<R>();

        // Read inside the first block.
        let bytes = file
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 10,
                length: 20,
            })
            .unwrap();
        assert_eq!(&*bytes, &scn.data[10..30]);

        // Last block includes the 100-byte tail.
        let last = scn.data.len() as u64;
        let bytes = file
            .read::<Sequential, u8>(ReadRange {
                byte_offset: last - 50,
                length: 50,
            })
            .unwrap();
        assert_eq!(&*bytes, &scn.data[scn.data.len() - 50..]);
    }

    #[test]
    fn read_spanning_multiple_blocks_is_contiguous() {
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = scn.open::<R>();

        let start = (BLOCK_SIZE - 50) as u64;
        let len = (BLOCK_SIZE + 100) as u64;
        let bytes = file
            .read::<Sequential, u8>(ReadRange {
                byte_offset: start,
                length: len,
            })
            .unwrap();
        let start = start as usize;
        let end = start + len as usize;
        assert!(matches!(bytes, Cow::Borrowed(_)));
        assert_eq!(bytes.as_ref(), &scn.data[start..end]);
    }

    #[test]
    fn local_file_is_created_on_first_read() {
        let scn = Scenario::new(BLOCK_SIZE * 2);
        let expected_local = scn.expected_local_path();

        let file = scn.open::<R>();

        // Before the first read, the local file doesn't exist yet.
        assert!(
            !expected_local.exists(),
            "local file should not exist before first read: {}",
            expected_local.display(),
        );

        // Trigger one read. This must bring up the local file.
        let _ = file
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 0,
                length: 1,
            })
            .unwrap();

        assert!(
            expected_local.exists(),
            "local file should exist after first read"
        );
        assert_eq!(
            fs::metadata(&expected_local).unwrap().len(),
            scn.data.len() as u64,
            "local file should be sized to the remote",
        );
    }

    #[test]
    fn empty_read_does_not_materialize_local_file() {
        let scn = Scenario::new(BLOCK_SIZE);
        let expected_local = scn.expected_local_path();
        let file = scn.open::<R>();

        let bytes = file
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 0,
                length: 0,
            })
            .unwrap();
        assert!(bytes.is_empty());
        assert!(
            !expected_local.exists(),
            "zero-length read must not trigger local-file initialisation",
        );
    }

    #[test]
    fn populate_fetches_every_block() {
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = scn.open::<R>();

        file.populate().unwrap();

        let bytes = file
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 0,
                length: scn.data.len() as u64,
            })
            .unwrap();
        assert_eq!(&*bytes, &scn.data[..]);
    }

    #[test]
    fn read_past_end_returns_out_of_bounds() {
        let scn = Scenario::new(1024);
        let file = scn.open::<R>();

        let err = file
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 1000,
                length: 100,
            })
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::universal_io::UniversalIoError::OutOfBounds { .. }
            ),
            "expected OutOfBounds, got {err:?}",
        );
    }
}
