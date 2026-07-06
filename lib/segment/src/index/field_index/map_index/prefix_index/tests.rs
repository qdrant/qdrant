use std::collections::BTreeMap;

use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::{MmapFile, MmapFs, Populate};
use itertools::Itertools as _;
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use tempfile::TempDir;

use super::format::prefix_successor;
use super::{PrefixIndex, build_prefix_index};

fn build_and_open(entries: &BTreeMap<Vec<u8>, usize>) -> (TempDir, PrefixIndex) {
    let dir = TempDir::with_prefix("prefix_index").unwrap();
    build_prefix_index(
        dir.path(),
        entries.iter().map(|(key, &count)| (key.as_slice(), count)),
    )
    .unwrap();
    let index = PrefixIndex::open(
        &common::universal_io::CachedReadFs::new(MmapFs, std::path::Path::new(".")).unwrap(),
        dir.path(),
        Populate::Blocking,
    )
    .unwrap()
    .unwrap();
    (dir, index)
}

fn collect_prefix(index: &PrefixIndex, prefix: &[u8]) -> Vec<(Vec<u8>, usize)> {
    let hw_counter = HardwareCounterCell::disposable();
    let mut result = Vec::new();
    index
        .for_each_key_with_prefix(prefix, &hw_counter, &mut |key, count| {
            result.push((key.to_vec(), count));
            Ok(())
        })
        .unwrap();
    result
}

fn naive_prefix(entries: &BTreeMap<Vec<u8>, usize>, prefix: &[u8]) -> Vec<(Vec<u8>, usize)> {
    entries
        .iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .map(|(key, &count)| (key.clone(), count))
        .collect()
}

fn check_prefix(index: &PrefixIndex, entries: &BTreeMap<Vec<u8>, usize>, prefix: &[u8]) {
    let expected = naive_prefix(entries, prefix);
    assert_eq!(collect_prefix(index, prefix), expected, "prefix {prefix:?}",);

    let hw_counter = HardwareCounterCell::disposable();
    let stats = index.prefix_stats(prefix, &hw_counter).unwrap();
    assert_eq!(stats.keys, expected.len(), "prefix {prefix:?}");
    assert_eq!(
        stats.postings,
        expected.iter().map(|(_, count)| count).sum::<usize>(),
        "prefix {prefix:?}",
    );
}

#[test]
fn missing_file_opens_as_none() {
    let dir = TempDir::with_prefix("prefix_index").unwrap();
    let index = PrefixIndex::<MmapFile>::open(
        &common::universal_io::CachedReadFs::new(MmapFs, std::path::Path::new(".")).unwrap(),
        dir.path(),
        Populate::Blocking,
    )
    .unwrap();
    assert!(index.is_none());
}

#[test]
fn empty_dictionary() {
    let entries = BTreeMap::new();
    let (_dir, index) = build_and_open(&entries);
    assert_eq!(index.key_count(), 0);
    check_prefix(&index, &entries, b"");
    check_prefix(&index, &entries, b"anything");
}

#[test]
fn small_dictionary() {
    let entries: BTreeMap<Vec<u8>, usize> = [
        (&b"https://example.com"[..], 3),
        (b"https://qdrant.tech", 7),
        (b"https://qdrant.tech/docs", 2),
        (b"tag", 1),
        (b"tags", 5),
    ]
    .into_iter()
    .map(|(key, count)| (key.to_vec(), count))
    .collect();
    let (_dir, index) = build_and_open(&entries);

    assert_eq!(index.key_count(), 5);
    for prefix in [
        &b""[..],
        b"h",
        b"https://",
        b"https://qdrant.",
        b"https://qdrant.tech",
        b"https://qdrant.tech/docs/more",
        b"tag",
        b"tags",
        b"tagz",
        b"z",
        b"\xff",
    ] {
        check_prefix(&index, &entries, prefix);
    }
}

#[test]
fn multibyte_and_edge_keys() {
    let entries: BTreeMap<Vec<u8>, usize> = [
        "".as_bytes().to_vec(),
        "α".as_bytes().to_vec(),
        "αβ".as_bytes().to_vec(),
        "яблоко".as_bytes().to_vec(),
        vec![0xFF],
        vec![0xFF, 0xFF],
        vec![0xFF, 0xFF, 0x01],
    ]
    .into_iter()
    .enumerate()
    .map(|(i, key)| (key, i + 1))
    .collect();
    let (_dir, index) = build_and_open(&entries);

    for prefix in [
        &b""[..],
        "α".as_bytes(),
        "я".as_bytes(),
        &[0xCE],
        &[0xFF],
        &[0xFF, 0xFF],
        &[0xFF, 0xFF, 0xFF],
    ] {
        check_prefix(&index, &entries, prefix);
    }
}

#[test]
fn multi_block_random() {
    let mut rng = StdRng::seed_from_u64(42);
    let mut entries = BTreeMap::new();
    // Enough long keys to span many blocks; skewed shared prefixes.
    for _ in 0..5_000 {
        let base = ["https://", "http://", "ftp://", ""][rng.random_range(0..4)];
        let len = rng.random_range(1..40);
        let tail: String = (0..len)
            .map(|_| char::from(rng.random_range(b'a'..=b'e')))
            .collect();
        entries.insert(
            format!("{base}{tail}").into_bytes(),
            rng.random_range(1..100),
        );
    }
    let (_dir, index) = build_and_open(&entries);
    assert!(index.blocks.len() > 3, "test should span multiple blocks");
    assert_eq!(index.key_count(), entries.len());

    // All keys, in order.
    assert_eq!(
        collect_prefix(&index, b""),
        entries
            .iter()
            .map(|(key, &count)| (key.clone(), count))
            .collect_vec(),
    );

    for prefix in [
        &b""[..],
        b"h",
        b"http",
        b"https://",
        b"https://a",
        b"https://ab",
        b"https://abc",
        b"ftp://e",
        b"a",
        b"ab",
        b"nonexistent",
    ] {
        check_prefix(&index, &entries, prefix);
    }

    // Random probes, including prefixes of existing keys.
    let keys = entries.keys().cloned().collect_vec();
    for _ in 0..200 {
        let key = &keys[rng.random_range(0..keys.len())];
        let len = rng.random_range(0..=key.len());
        check_prefix(&index, &entries, &key[..len]);
    }
}

#[test]
fn prefix_successor_edge_cases() {
    assert_eq!(prefix_successor(b""), None);
    assert_eq!(prefix_successor(&[0xFF]), None);
    assert_eq!(prefix_successor(&[0xFF, 0xFF]), None);
    assert_eq!(prefix_successor(b"a"), Some(b"b".to_vec()));
    assert_eq!(prefix_successor(&[b'a', 0xFF]), Some(b"b".to_vec()));
    assert_eq!(
        prefix_successor(&[b'a', 0xFF, b'c']),
        Some(vec![b'a', 0xFF, b'd']),
    );
}
