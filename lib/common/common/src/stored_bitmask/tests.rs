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
