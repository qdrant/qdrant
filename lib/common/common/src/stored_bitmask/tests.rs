use std::path::Path;

use roaring::RoaringBitmap;
use tempfile::TempDir;

use super::format::{BitmaskContent, Encoding};
use super::{StoredBitmask, save_bitmask};
use crate::bitvec::BitVec;
use crate::universal_io::{MmapFile, MmapFs, OpenOptions};

fn open(path: &Path) -> StoredBitmask<MmapFile> {
    StoredBitmask::open(&MmapFs, path, OpenOptions::new_for_test(), ()).unwrap()
}

/// Decode the whole mask into a dense [`BitVec`] of [`StoredBitmask::bit_len`]
/// bits.
fn read_to_bitvec(mask: &StoredBitmask<MmapFile>) -> BitVec {
    let len = mask.bit_len() as usize;
    match mask.read().unwrap() {
        BitmaskContent::Dense(bits) => bits.into_owned(),
        BitmaskContent::Ones(ones) => {
            let mut bits = BitVec::repeat(false, len);
            for idx in ones {
                bits.set(idx as usize, true);
            }
            bits
        }
        BitmaskContent::Zeros(zeros) => {
            let mut bits = BitVec::repeat(true, len);
            for idx in zeros {
                bits.set(idx as usize, false);
            }
            bits
        }
    }
}

fn roundtrip(logical_len: u64, ones: &RoaringBitmap) -> (StoredBitmask<MmapFile>, TempDir) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mask.bin");
    save_bitmask(&MmapFs, &path, logical_len, ones.clone()).unwrap();
    (open(&path), dir)
}

fn assert_bits(mask: &StoredBitmask<MmapFile>, logical_len: u64, ones: &RoaringBitmap) {
    assert_eq!(mask.bit_len(), logical_len);
    let bits = read_to_bitvec(mask);
    assert_eq!(bits.len() as u64, logical_len);
    for idx in 0..logical_len {
        assert_eq!(
            bits[idx as usize],
            ones.contains(idx as u32),
            "mismatch at bit {idx}",
        );
    }
}

#[test]
fn sparse_ones_stored_as_roaring_ones() {
    let ones = RoaringBitmap::from_sorted_iter([1u32, 7, 63, 64, 100_000]).unwrap();
    let (mask, _dir) = roundtrip(1_000_000, &ones);
    assert_eq!(mask.encoding, Encoding::RoaringOnes);
    assert!(matches!(mask.read().unwrap(), BitmaskContent::Ones(_)));
    assert_bits(&mask, 1_000_000, &ones);
    // Orders of magnitude below the 125_000-byte dense representation.
    assert!(mask.payload_len < 1_000);
}

#[test]
fn mostly_ones_stored_as_roaring_zeros() {
    let len = 1_000_000u64;
    let mut ones = RoaringBitmap::new();
    ones.insert_range(0..len as u32);
    for zero in [3u32, 500, 65_536, 999_999] {
        ones.remove(zero);
    }
    let (mask, _dir) = roundtrip(len, &ones);
    assert_eq!(mask.encoding, Encoding::RoaringZeros);
    match mask.read().unwrap() {
        BitmaskContent::Zeros(zeros) => {
            assert_eq!(
                zeros.iter().collect::<Vec<_>>(),
                vec![3, 500, 65_536, 999_999]
            );
        }
        BitmaskContent::Dense(_) | BitmaskContent::Ones(_) => {
            panic!("expected zeros encoding")
        }
    }
    assert_bits(&mask, len, &ones);
    assert!(mask.payload_len < 1_000);
}

#[test]
fn incompressible_mask_falls_back_to_dense() {
    // Alternating bits: the worst case for both roaring polarities.
    let len = 100_000u64;
    let ones = RoaringBitmap::from_sorted_iter((0..len as u32).filter(|i| i % 2 == 0)).unwrap();
    let (mask, _dir) = roundtrip(len, &ones);
    assert_eq!(mask.encoding, Encoding::Dense);
    assert!(mask.payload_len <= len.div_ceil(8).next_multiple_of(8));
    assert_bits(&mask, len, &ones);
}

#[test]
fn incompressible_mostly_ones_falls_back_to_dense() {
    // Majority ones with scattered zeros: zeros polarity is chosen, but the
    // zeros are still too scattered for roaring — the dense fallback must
    // reconstruct the majority bits, including the unaligned trailing byte.
    let len = 100_003u64;
    let ones = RoaringBitmap::from_sorted_iter((0..len as u32).filter(|i| i % 5 != 0)).unwrap();
    let (mask, _dir) = roundtrip(len, &ones);
    assert_eq!(mask.encoding, Encoding::Dense);
    assert_bits(&mask, len, &ones);
}

#[test]
fn empty_mask() {
    let ones = RoaringBitmap::new();
    let (mask, _dir) = roundtrip(0, &ones);
    assert_bits(&mask, 0, &ones);

    let (mask, _dir) = roundtrip(12_345, &ones);
    assert_bits(&mask, 12_345, &ones);
}

#[test]
fn full_mask() {
    let len = 12_345u64;
    let mut ones = RoaringBitmap::new();
    ones.insert_range(0..len as u32);
    let (mask, _dir) = roundtrip(len, &ones);
    assert_eq!(mask.encoding, Encoding::RoaringZeros);
    assert_bits(&mask, len, &ones);
}

#[test]
fn unaligned_length() {
    // Length not a multiple of 64: dense truncation must be exact.
    let len = 131u64;
    let ones = RoaringBitmap::from_sorted_iter((0..len as u32).filter(|i| i % 2 == 1)).unwrap();
    let (mask, _dir) = roundtrip(len, &ones);
    assert_eq!(mask.encoding, Encoding::Dense);
    assert_bits(&mask, len, &ones);
}

#[test]
fn read_ones_normalizes_every_encoding() {
    // One mask per encoding, plus the empty edge cases: `read_ones` must
    // return the original set positions regardless of the stored polarity.
    let sparse = RoaringBitmap::from_sorted_iter([1u32, 7, 63, 64, 100_000]).unwrap();
    let mut mostly_ones = RoaringBitmap::new();
    mostly_ones.insert_range(0..1_000_000u32);
    for zero in [3u32, 500, 65_536, 999_999] {
        mostly_ones.remove(zero);
    }
    let alternating =
        RoaringBitmap::from_sorted_iter((0..100_000u32).filter(|i| i % 2 == 0)).unwrap();
    let empty = RoaringBitmap::new();

    for (len, ones, expected_encoding) in [
        (1_000_000u64, &sparse, Encoding::RoaringOnes),
        (1_000_000, &mostly_ones, Encoding::RoaringZeros),
        (100_000, &alternating, Encoding::Dense),
        // Zero-length mask: the empty dense payload (0 bytes) beats roaring.
        (0, &empty, Encoding::Dense),
        (12_345, &empty, Encoding::RoaringOnes),
    ] {
        let (mask, _dir) = roundtrip(len, ones);
        assert_eq!(mask.encoding, expected_encoding);
        assert_eq!(&mask.read_ones().unwrap(), ones);
    }
}

#[test]
fn overwrite_replaces_previous_snapshot() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mask.bin");

    let first = RoaringBitmap::from_sorted_iter(0..50_000u32).unwrap();
    save_bitmask(&MmapFs, &path, 100_000, first).unwrap();

    let second = RoaringBitmap::from_sorted_iter([42u32]).unwrap();
    save_bitmask(&MmapFs, &path, 100_000, second.clone()).unwrap();

    assert_bits(&open(&path), 100_000, &second);
}

#[test]
fn rejects_positions_beyond_logical_len() {
    // Hand-craft a file whose roaring payload contains a position past
    // `logical_len` — the writer refuses to produce this, so build it from
    // format parts directly.
    use super::format::BitmaskHeader;

    let bitmap = RoaringBitmap::from_sorted_iter([100u32]).unwrap();
    let mut payload = Vec::new();
    bitmap.serialize_into(&mut payload).unwrap();

    let mut bytes = bytemuck::bytes_of(&BitmaskHeader::new(
        8,
        Encoding::RoaringOnes,
        payload.len() as u64,
    ))
    .to_vec();
    bytes.extend_from_slice(&payload);

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mask.bin");
    fs_err::write(&path, bytes).unwrap();

    let mask = open(&path); // header itself is valid
    assert!(mask.read().is_err());
}

#[test]
fn rejects_foreign_files() {
    let dir = TempDir::new().unwrap();

    // Too short.
    let path = dir.path().join("short.bin");
    fs_err::write(&path, b"QBMK").unwrap();
    assert!(
        StoredBitmask::<MmapFile>::open(&MmapFs, &path, OpenOptions::new_for_test(), ()).is_err()
    );

    // Right size, wrong magic (e.g. a legacy dense bitslice file).
    let path = dir.path().join("legacy.bin");
    fs_err::write(&path, vec![0u8; 64]).unwrap();
    assert!(
        StoredBitmask::<MmapFile>::open(&MmapFs, &path, OpenOptions::new_for_test(), ()).is_err()
    );
}

mod mutable {
    use super::*;
    use crate::stored_bitmask::MutableStoredBitmask;
    use crate::universal_io::{IsNotFound, OkNotFound};

    fn open_mutable(path: &Path) -> MutableStoredBitmask {
        MutableStoredBitmask::open(&MmapFs, path, OpenOptions::new_for_test(), ()).unwrap()
    }

    #[test]
    fn new_starts_dirty_and_first_save_creates_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("mask.bin");

        let mut mask = MutableStoredBitmask::new(100);
        assert!(mask.is_dirty());
        mask.save(&MmapFs, &path).unwrap();
        assert!(!mask.is_dirty());

        assert_bits(&open(&path), 100, &RoaringBitmap::new());
        let reopened = open_mutable(&path);
        assert!(!reopened.is_dirty());
        assert_eq!(reopened.bit_len(), 100);
        assert_eq!(reopened.count_ones(), 0);
    }

    #[test]
    fn open_is_clean_and_matches_persisted_content() {
        // Same fixtures as `read_ones_normalizes_every_encoding`: one mask
        // per stored encoding, plus an empty one.
        let sparse = RoaringBitmap::from_sorted_iter([1u32, 7, 63, 64, 100_000]).unwrap();
        let mut mostly_ones = RoaringBitmap::new();
        mostly_ones.insert_range(0..1_000_000u32);
        mostly_ones.remove(500);
        let alternating =
            RoaringBitmap::from_sorted_iter((0..100_000u32).filter(|i| i % 2 == 0)).unwrap();

        for (len, ones) in [
            (1_000_000u64, sparse),
            (1_000_000, mostly_ones),
            (100_000, alternating),
            (12_345, RoaringBitmap::new()),
        ] {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("mask.bin");
            save_bitmask(&MmapFs, &path, len, ones.clone()).unwrap();

            let mask = open_mutable(&path);
            assert!(!mask.is_dirty());
            assert_eq!(mask.bit_len(), len);
            assert_eq!(mask.ones(), &ones);
            assert_eq!(mask.count_ones(), ones.len());
            assert_eq!(mask.get(0), ones.contains(0));
            assert_eq!(mask.get(500), ones.contains(500));
            assert!(!mask.get(len as u32)); // beyond the mask: never set
        }
    }

    #[test]
    fn open_missing_file_is_not_found() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("missing.bin");

        let err = MutableStoredBitmask::open(&MmapFs, &path, OpenOptions::new_for_test(), ())
            .unwrap_err();
        assert!(err.is_not_found());

        // The supported caller composition: fall back to an empty mask.
        let mask = MutableStoredBitmask::open(&MmapFs, &path, OpenOptions::new_for_test(), ())
            .ok_not_found()
            .unwrap()
            .unwrap_or_else(|| MutableStoredBitmask::new(100));
        assert!(mask.is_dirty());
        assert_eq!(mask.bit_len(), 100);
    }

    #[test]
    fn set_returns_previous_value() {
        let mut mask = MutableStoredBitmask::new(10);
        assert!(!mask.set(3, true));
        assert!(mask.set(3, true));
        assert!(mask.get(3));
        assert!(mask.set(3, false));
        assert!(!mask.set(3, false));
        assert!(!mask.get(3));
        assert_eq!(mask.count_ones(), 0);
    }

    #[test]
    fn noop_set_stays_clean() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("mask.bin");
        let ones = RoaringBitmap::from_sorted_iter([20u32]).unwrap();
        save_bitmask(&MmapFs, &path, 100, ones).unwrap();

        let mut mask = open_mutable(&path);
        assert!(mask.set(20, true));
        assert!(!mask.set(30, false));
        assert!(!mask.is_dirty());
    }

    #[test]
    fn reverted_change_is_clean_again() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("mask.bin");
        let ones = RoaringBitmap::from_sorted_iter([20u32]).unwrap();
        save_bitmask(&MmapFs, &path, 100, ones).unwrap();

        let mut mask = open_mutable(&path);
        mask.set(20, false);
        mask.set(30, true);
        assert!(mask.is_dirty());
        mask.set(20, true);
        mask.set(30, false);
        assert!(!mask.is_dirty());
    }

    #[test]
    fn save_when_clean_skips_write() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("mask.bin");

        let mut mask = MutableStoredBitmask::new(100);
        mask.set(42, true);
        mask.save(&MmapFs, &path).unwrap();

        // A clean save must not touch storage: with the file deleted behind
        // its back, only an actual write could bring it back.
        fs_err::remove_file(&path).unwrap();
        mask.save(&MmapFs, &path).unwrap();
        assert!(!path.exists());

        // The next effective change writes again — the whole mask, not a
        // delta on the (gone) previous file.
        mask.set(43, true);
        mask.save(&MmapFs, &path).unwrap();
        assert_bits(
            &open(&path),
            100,
            &RoaringBitmap::from_sorted_iter([42u32, 43]).unwrap(),
        );
    }

    #[test]
    fn mutate_save_reopen_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("mask.bin");
        let mut expected = RoaringBitmap::from_sorted_iter(0..50u32).unwrap();
        save_bitmask(&MmapFs, &path, 200, expected.clone()).unwrap();

        let mut mask = open_mutable(&path);
        for (index, value) in [(10u32, false), (49, false), (50, true), (199, true)] {
            mask.set(index, value);
            if value {
                expected.insert(index);
            } else {
                expected.remove(index);
            }
        }
        mask.save(&MmapFs, &path).unwrap();
        assert!(!mask.is_dirty());

        assert_bits(&open(&path), 200, &expected);
        assert_eq!(open_mutable(&path).ones(), &expected);
    }

    #[test]
    fn encoding_transition_sparse_to_dense() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("mask.bin");
        save_bitmask(
            &MmapFs,
            &path,
            100_000,
            RoaringBitmap::from_sorted_iter([3u32, 500]).unwrap(),
        )
        .unwrap();
        assert_eq!(open(&path).encoding, Encoding::RoaringOnes);

        // Alternating bits are incompressible in both polarities.
        let mut mask = open_mutable(&path);
        for index in (0..100_000u32).step_by(2) {
            mask.set(index, true);
        }
        mask.set(3, false);
        mask.save(&MmapFs, &path).unwrap();

        let reopened = open(&path);
        assert_eq!(reopened.encoding, Encoding::Dense);
        let expected =
            RoaringBitmap::from_sorted_iter((0..100_000u32).filter(|i| i % 2 == 0)).unwrap();
        assert_bits(&reopened, 100_000, &expected);
    }

    #[test]
    fn set_len_grows_and_persists() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("mask.bin");
        let ones = RoaringBitmap::from_sorted_iter([0u32, 99]).unwrap();
        save_bitmask(&MmapFs, &path, 100, ones.clone()).unwrap();

        let mut mask = open_mutable(&path);
        mask.set_len(100); // equal length: still clean
        assert!(!mask.is_dirty());

        // Growth alone must persist: the length is part of the file.
        mask.set_len(150);
        assert!(mask.is_dirty());
        mask.save(&MmapFs, &path).unwrap();

        assert_bits(&open(&path), 150, &ones);
        let mut reopened = open_mutable(&path);
        assert!(!reopened.is_dirty());
        assert!(!reopened.set(149, true)); // the grown range is writable
    }

    #[test]
    #[should_panic(expected = "cannot shrink")]
    fn set_len_shrink_panics() {
        MutableStoredBitmask::new(100).set_len(50);
    }

    #[test]
    #[should_panic(expected = "beyond bitmask")]
    fn set_out_of_bounds_panics() {
        MutableStoredBitmask::new(10).set(10, true);
    }

    #[test]
    #[should_panic(expected = "exceeds the u32 position space")]
    fn len_beyond_u32_space_panics() {
        MutableStoredBitmask::new((1 << 32) + 1);
    }

    #[test]
    fn max_len_boundary() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("mask.bin");

        // Sparse ones keep everything roaring-encoded: nothing here may
        // materialize the 512 MiB dense form or the full complement.
        let mut mask = MutableStoredBitmask::new(1 << 32);
        assert!(!mask.set(u32::MAX, true));
        assert!(mask.get(u32::MAX));
        mask.save(&MmapFs, &path).unwrap();

        let reopened = open_mutable(&path);
        assert!(!reopened.is_dirty());
        assert_eq!(reopened.bit_len(), 1 << 32);
        assert_eq!(reopened.count_ones(), 1);
        assert!(reopened.get(u32::MAX));
    }
}
