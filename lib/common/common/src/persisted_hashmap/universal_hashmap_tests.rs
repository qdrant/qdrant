use std::collections::{BTreeMap, BTreeSet};
use std::hash::Hash;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use crate::persisted_hashmap::keys::Key;
use crate::persisted_hashmap::mmap_hashmap::{MmapHashMap, gen_ident, gen_map, repeat_until};
use crate::persisted_hashmap::universal_hashmap::UniversalHashMap;
use crate::universal_io::MmapFile;

type UMap<K, V> = UniversalHashMap<K, V, MmapFile>;

/// Helper: create a file with MmapHashMap, open it with UniversalHashMap.
fn write_and_open<K, V, K1>(
    map: &BTreeMap<K1, BTreeSet<V>>,
    as_ref: impl Fn(&K1) -> &K,
) -> (tempfile::TempDir, UMap<K, V>)
where
    K: Key + ?Sized,
    V: Sized + Copy + zerocopy::FromBytes + zerocopy::Immutable + zerocopy::IntoBytes + zerocopy::KnownLayout,
    K1: Ord + Hash,
{
    let tmpdir = tempfile::Builder::new().tempdir().unwrap();
    let path = tmpdir.path().join("map");
    MmapHashMap::<K, V>::create(&path, map.iter().map(|(k, v)| (as_ref(k), v.iter().copied())))
        .unwrap();
    let umap = UMap::<K, V>::open(&path, Default::default()).unwrap();
    (tmpdir, umap)
}

// ── get / get_with ─────────────────────────────────────────────────────

fn test_get_impl<K: Key + ?Sized, K1: Ord + Hash>(
    generator: impl Clone + Fn(&mut StdRng) -> K1,
    as_ref: impl Fn(&K1) -> &K,
    count: usize,
) {
    let mut rng = StdRng::seed_from_u64(42);
    let map = gen_map(&mut rng, generator.clone(), count);
    let (_tmpdir, umap) = write_and_open(&map, &as_ref);

    for (k, v) in &map {
        let got = umap.get(as_ref(k)).unwrap().unwrap();
        let expected: Vec<u32> = v.iter().copied().collect();
        assert_eq!(got, expected);
    }

    for _ in 0..100 {
        let key = repeat_until(|| generator(&mut rng), |key| !map.contains_key(key));
        assert!(umap.get(as_ref(&key)).unwrap().is_none());
    }
}

#[test]
fn test_get_str_keys() {
    test_get_impl(gen_ident, |s| s.as_str(), 200);
}

#[test]
fn test_get_i64_keys() {
    test_get_impl(|rng| rng.random::<i64>(), |i| i, 200);
}

#[test]
fn test_get_u128_keys() {
    test_get_impl(|rng| rng.random::<u128>(), |i| i, 200);
}

// ── get_with ───────────────────────────────────────────────────────────

#[test]
fn test_get_with() {
    let mut rng = StdRng::seed_from_u64(99);
    let map = gen_map(&mut rng, |rng| rng.random::<i64>(), 100);
    let (_tmpdir, umap) = write_and_open::<i64, u32, _>(&map, |k| k);

    for (k, v) in &map {
        let count = umap.get_with(k, |vals| vals.len()).unwrap().unwrap();
        assert_eq!(count, v.len());
    }
}

// ── get_values_count ───────────────────────────────────────────────────

fn test_get_values_count_impl<K: Key + ?Sized, K1: Ord + Hash>(
    generator: impl Clone + Fn(&mut StdRng) -> K1,
    as_ref: impl Fn(&K1) -> &K,
) {
    let mut rng = StdRng::seed_from_u64(77);
    let map = gen_map(&mut rng, generator.clone(), 150);
    let (_tmpdir, umap) = write_and_open(&map, &as_ref);

    for (k, v) in &map {
        let count = umap.get_values_count(as_ref(k)).unwrap().unwrap();
        assert_eq!(count, v.len());
    }

    let missing = repeat_until(|| generator(&mut rng), |key| !map.contains_key(key));
    assert!(umap.get_values_count(as_ref(&missing)).unwrap().is_none());
}

#[test]
fn test_get_values_count_str() {
    test_get_values_count_impl(gen_ident, |s| s.as_str());
}

#[test]
fn test_get_values_count_i64() {
    test_get_values_count_impl(|rng| rng.random::<i64>(), |i| i);
}

#[test]
fn test_get_values_count_u128() {
    test_get_values_count_impl(|rng| rng.random::<u128>(), |i| i);
}

// ── for_each_entry ─────────────────────────────────────────────────────

fn test_for_each_entry_impl<K: Key + ?Sized, K1: Ord + Hash + Clone>(
    generator: impl Clone + Fn(&mut StdRng) -> K1,
    as_ref: impl Fn(&K1) -> &K,
    from_ref: impl Fn(&K) -> K1,
    count: usize,
) {
    let mut rng = StdRng::seed_from_u64(55);
    let map = gen_map(&mut rng, generator, count);
    let (_tmpdir, umap) = write_and_open(&map, &as_ref);

    let mut visited: BTreeMap<K1, Vec<u32>> = BTreeMap::new();
    umap.for_each_entry(|k, vals| {
        visited.insert(from_ref(k), vals.to_vec());
    })
    .unwrap();

    assert_eq!(visited.len(), map.len());
    for (k, v) in &map {
        let expected: Vec<u32> = v.iter().copied().collect();
        assert_eq!(visited.get(k).unwrap(), &expected);
    }
}

#[test]
fn test_for_each_entry_str() {
    test_for_each_entry_impl(gen_ident, |s| s.as_str(), |s| s.to_owned(), 200);
}

#[test]
fn test_for_each_entry_i64() {
    test_for_each_entry_impl(|rng| rng.random::<i64>(), |i| i, |i| *i, 200);
}

#[test]
fn test_for_each_entry_u128() {
    test_for_each_entry_impl(|rng| rng.random::<u128>(), |i| i, |i| *i, 200);
}

// ── for_each_key ───────────────────────────────────────────────────────

fn test_for_each_key_impl<K: Key + ?Sized, K1: Ord + Hash + Clone + std::fmt::Debug>(
    generator: impl Clone + Fn(&mut StdRng) -> K1,
    as_ref: impl Fn(&K1) -> &K,
    from_ref: impl Fn(&K) -> K1,
    count: usize,
) {
    let mut rng = StdRng::seed_from_u64(33);
    let map = gen_map(&mut rng, generator, count);
    let (_tmpdir, umap) = write_and_open(&map, &as_ref);

    let mut keys: BTreeSet<K1> = BTreeSet::new();
    umap.for_each_key(|k| {
        keys.insert(from_ref(k));
    })
    .unwrap();

    let expected_keys: BTreeSet<K1> = map.keys().cloned().collect();
    assert_eq!(keys, expected_keys);
}

#[test]
fn test_for_each_key_str() {
    test_for_each_key_impl(gen_ident, |s| s.as_str(), |s| s.to_owned(), 200);
}

#[test]
fn test_for_each_key_i64() {
    test_for_each_key_impl(|rng| rng.random::<i64>(), |i| i, |i| *i, 200);
}

#[test]
fn test_for_each_key_u128() {
    test_for_each_key_impl(|rng| rng.random::<u128>(), |i| i, |i| *i, 200);
}

// ── keys_count ─────────────────────────────────────────────────────────

#[test]
fn test_keys_count() {
    let mut rng = StdRng::seed_from_u64(11);
    for count in [0, 1, 10, 500] {
        let map = gen_map(&mut rng, |rng| rng.random::<i64>(), count);
        let (_tmpdir, umap) = write_and_open::<i64, u32, _>(&map, |k| k);
        assert_eq!(umap.keys_count(), count);
    }
}

// ── Different value sizes ──────────────────────────────────────────────

#[test]
fn test_u64_values() {
    let mut rng = StdRng::seed_from_u64(88);

    let mut map: BTreeMap<i64, BTreeSet<u64>> = BTreeMap::new();
    for key in 0..50i64 {
        map.insert(
            key,
            (0..rng.random_range(1..=100))
                .map(|_| rng.random())
                .collect(),
        );
    }

    let (_tmpdir, umap) = write_and_open::<i64, u64, _>(&map, |k| k);

    for (k, v) in &map {
        let got = umap.get(k).unwrap().unwrap();
        let expected: Vec<u64> = v.iter().copied().collect();
        assert_eq!(got, expected);
    }

    assert!(umap.get(&9999).unwrap().is_none());
}

#[test]
fn test_u128_values() {
    let mut rng = StdRng::seed_from_u64(77);

    let mut map: BTreeMap<u128, BTreeSet<u128>> = BTreeMap::new();
    for _ in 0..30 {
        let key = rng.random::<u128>();
        map.insert(
            key,
            (0..rng.random_range(1..=50))
                .map(|_| rng.random())
                .collect(),
        );
    }

    let (_tmpdir, umap) = write_and_open::<u128, u128, _>(&map, |k| k);

    for (k, v) in &map {
        let got = umap.get(k).unwrap().unwrap();
        let expected: Vec<u128> = v.iter().copied().collect();
        assert_eq!(got, expected);
    }
}

// ── Edge: single value per key ─────────────────────────────────────────

#[test]
fn test_single_value_per_key() {
    let mut map: BTreeMap<i64, BTreeSet<u32>> = BTreeMap::new();
    for i in 0..100 {
        let mut s = BTreeSet::new();
        s.insert(i as u32);
        map.insert(i, s);
    }

    let (_tmpdir, umap) = write_and_open::<i64, u32, _>(&map, |k| k);

    for (k, v) in &map {
        let got = umap.get(k).unwrap().unwrap();
        assert_eq!(got, vec![*v.iter().next().unwrap()]);
        assert_eq!(umap.get_values_count(k).unwrap().unwrap(), 1);
    }
}

// ── Edge: many values per key ──────────────────────────────────────────

#[test]
fn test_many_values_per_key() {
    let mut rng = StdRng::seed_from_u64(22);

    let mut map: BTreeMap<i64, BTreeSet<u32>> = BTreeMap::new();
    for i in 0..10 {
        map.insert(
            i,
            (0..1000).map(|_| rng.random_range(0..=100_000)).collect(),
        );
    }

    let (_tmpdir, umap) = write_and_open::<i64, u32, _>(&map, |k| k);

    for (k, v) in &map {
        let got = umap.get(k).unwrap().unwrap();
        let expected: Vec<u32> = v.iter().copied().collect();
        assert_eq!(got, expected);
        assert_eq!(umap.get_values_count(k).unwrap().unwrap(), v.len());
    }
}

// ── Edge: empty map ────────────────────────────────────────────────────

#[test]
fn test_empty_map() {
    let map: BTreeMap<i64, BTreeSet<u32>> = BTreeMap::new();
    let (_tmpdir, umap) = write_and_open::<i64, u32, _>(&map, |k| k);

    assert_eq!(umap.keys_count(), 0);
    assert!(umap.get(&0).unwrap().is_none());
    assert!(umap.get_values_count(&0).unwrap().is_none());

    let mut count = 0;
    umap.for_each_entry(|_, _| count += 1).unwrap();
    assert_eq!(count, 0);

    umap.for_each_key(|_: &i64| count += 1).unwrap();
    assert_eq!(count, 0);
}

// ── Edge: long string keys ─────────────────────────────────────────────

#[test]
fn test_long_string_keys() {
    let mut map: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();
    for i in 0..20 {
        // Keys longer than KEY_READ_CAP (512) to exercise the retry path in for_each_key.
        let key: String = std::iter::repeat_n('x', 600 + i).collect();
        let mut s = BTreeSet::new();
        s.insert(i as u32);
        map.insert(key, s);
    }

    let (_tmpdir, umap) = write_and_open::<str, u32, _>(&map, |k| k.as_str());

    for (k, v) in &map {
        let got = umap.get(k.as_str()).unwrap().unwrap();
        let expected: Vec<u32> = v.iter().copied().collect();
        assert_eq!(got, expected);
    }

    let mut key_count = 0;
    umap.for_each_key(|_| key_count += 1).unwrap();
    assert_eq!(key_count, map.len());

    let mut entry_count = 0;
    umap.for_each_entry(|k, _| {
        assert!(map.contains_key(k));
        entry_count += 1;
    })
    .unwrap();
    assert_eq!(entry_count, map.len());
}

// ── Batch boundary: entry count > ENTRY_BATCH_SIZE ─────────────────────

#[test]
fn test_for_each_entry_exceeds_batch_size() {
    // ENTRY_BATCH_SIZE is 64 inside for_each_entry — use more entries to
    // exercise multi-batch iteration.
    let mut rng = StdRng::seed_from_u64(44);
    let map = gen_map(&mut rng, |rng| rng.random::<i64>(), 300);
    let (_tmpdir, umap) = write_and_open::<i64, u32, _>(&map, |k| k);

    let mut visited: BTreeMap<i64, Vec<u32>> = BTreeMap::new();
    umap.for_each_entry(|k, vals| {
        visited.insert(*k, vals.to_vec());
    })
    .unwrap();

    assert_eq!(visited.len(), map.len());
    for (k, v) in &map {
        let expected: Vec<u32> = v.iter().copied().collect();
        assert_eq!(visited[k], expected);
    }
}

// ── get_with_batch ─────────────────────────────────────────────────────

fn test_get_with_batch_impl<K: Key + ?Sized, K1: Ord + Hash + Clone>(
    generator: impl Clone + Fn(&mut StdRng) -> K1,
    as_ref: impl Fn(&K1) -> &K,
    count: usize,
) {
    let mut rng = StdRng::seed_from_u64(42);
    let map = gen_map(&mut rng, generator.clone(), count);
    let (_tmpdir, umap) = write_and_open(&map, &as_ref);

    // Batch lookup of all existing keys.
    let all_keys: Vec<&K> = map.keys().map(&as_ref).collect();
    let results = umap
        .get_with_batch(&all_keys, |_, vals| vals.to_vec())
        .unwrap();
    assert_eq!(results.len(), map.len());
    for ((_k, v), result) in map.iter().zip(results.iter()) {
        let expected: Vec<u32> = v.iter().copied().collect();
        assert_eq!(result.as_ref().unwrap(), &expected);
    }

    // Batch lookup with mix of existing and non-existing keys.
    let existing: Vec<K1> = map.keys().take(5).cloned().collect();
    let mut missing: Vec<K1> = Vec::new();
    for _ in 0..5 {
        missing.push(repeat_until(
            || generator(&mut rng),
            |key| !map.contains_key(key),
        ));
    }
    // Interleave existing and missing.
    let mixed: Vec<&K> = existing
        .iter()
        .map(|k| as_ref(k))
        .zip(missing.iter().map(|k| as_ref(k)))
        .flat_map(|(a, b)| [a, b])
        .collect();

    let results = umap
        .get_with_batch(&mixed, |_, vals| vals.to_vec())
        .unwrap();
    assert_eq!(results.len(), mixed.len());
    for (i, result) in results.iter().enumerate() {
        if i % 2 == 0 {
            assert!(result.is_some(), "Expected Some for existing key at {i}");
        } else {
            assert!(result.is_none(), "Expected None for missing key at {i}");
        }
    }
}

#[test]
fn test_get_with_batch_str() {
    test_get_with_batch_impl(gen_ident, |s| s.as_str(), 200);
}

#[test]
fn test_get_with_batch_i64() {
    test_get_with_batch_impl(|rng| rng.random::<i64>(), |i| i, 200);
}

#[test]
fn test_get_with_batch_u128() {
    test_get_with_batch_impl(|rng| rng.random::<u128>(), |i| i, 200);
}

#[test]
fn test_get_with_batch_empty_keys() {
    let mut rng = StdRng::seed_from_u64(42);
    let map = gen_map(&mut rng, |rng| rng.random::<i64>(), 50);
    let (_tmpdir, umap) = write_and_open::<i64, u32, _>(&map, |k| k);

    let keys: &[&i64] = &[];
    let results = umap
        .get_with_batch(keys, |_, vals| vals.to_vec())
        .unwrap();
    assert!(results.is_empty());
}

#[test]
fn test_get_with_batch_empty_map() {
    let map: BTreeMap<i64, BTreeSet<u32>> = BTreeMap::new();
    let (_tmpdir, umap) = write_and_open::<i64, u32, _>(&map, |k| k);

    let keys: Vec<&i64> = vec![&1, &2, &3];

    let results = umap
        .get_with_batch(&keys,|_, vals| vals.to_vec())
        .unwrap();
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|r| r.is_none()));
}

#[test]
fn test_get_with_batch_single_value_per_key() {
    let mut map: BTreeMap<i64, BTreeSet<u32>> = BTreeMap::new();
    for i in 0..50 {
        let mut s = BTreeSet::new();
        s.insert(i as u32);
        map.insert(i, s);
    }

    let (_tmpdir, umap) = write_and_open::<i64, u32, _>(&map, |k| k);

    let keys: Vec<&i64> = map.keys().collect();

    let results = umap
        .get_with_batch(&keys,|_, vals| vals.len())
        .unwrap();
    assert!(results.iter().all(|r| *r == Some(1)));
}

#[test]
fn test_get_with_batch_many_values() {
    let mut rng = StdRng::seed_from_u64(22);
    let mut map: BTreeMap<i64, BTreeSet<u32>> = BTreeMap::new();
    for i in 0..10 {
        map.insert(
            i,
            (0..1000).map(|_| rng.random_range(0..=100_000)).collect(),
        );
    }

    let (_tmpdir, umap) = write_and_open::<i64, u32, _>(&map, |k| k);

    let keys: Vec<&i64> = map.keys().collect();

    let results = umap
        .get_with_batch(&keys,|_, vals| vals.to_vec())
        .unwrap();
    for (k, result) in map.keys().zip(results.iter()) {
        let expected: Vec<u32> = map[k].iter().copied().collect();
        assert_eq!(result.as_ref().unwrap(), &expected);
    }
}

#[test]
fn test_get_with_batch_u64_values() {
    let mut rng = StdRng::seed_from_u64(88);
    let mut map: BTreeMap<i64, BTreeSet<u64>> = BTreeMap::new();
    for key in 0..30i64 {
        map.insert(
            key,
            (0..rng.random_range(1..=50))
                .map(|_| rng.random())
                .collect(),
        );
    }

    let (_tmpdir, umap) = write_and_open::<i64, u64, _>(&map, |k| k);

    let keys: Vec<&i64> = map.keys().collect();

    let results = umap
        .get_with_batch(&keys,|_, vals| vals.to_vec())
        .unwrap();
    for (k, result) in map.keys().zip(results.iter()) {
        let expected: Vec<u64> = map[k].iter().copied().collect();
        assert_eq!(result.as_ref().unwrap(), &expected);
    }
}

#[test]
fn test_get_with_batch_preserves_order() {
    let mut rng = StdRng::seed_from_u64(99);
    let map = gen_map(&mut rng, |rng| rng.random::<i64>(), 100);
    let (_tmpdir, umap) = write_and_open::<i64, u32, _>(&map, |k| k);

    // Query keys in reverse order; results must still match input order.
    let keys: Vec<&i64> = map.keys().rev().collect();

    let results = umap
        .get_with_batch(&keys,|_, vals| vals.to_vec())
        .unwrap();
    for (key, result) in keys.iter().zip(results.iter()) {
        let expected: Vec<u32> = map[*key].iter().copied().collect();
        assert_eq!(result.as_ref().unwrap(), &expected);
    }
}

#[test]
fn test_get_with_batch_repeated_calls() {
    let mut rng = StdRng::seed_from_u64(42);
    let map = gen_map(&mut rng, |rng| rng.random::<i64>(), 100);
    let (_tmpdir, umap) = write_and_open::<i64, u32, _>(&map, |k| k);

    let keys: Vec<&i64> = map.keys().collect();

    let results1 = umap
        .get_with_batch(&keys, |_, vals| vals.to_vec())
        .unwrap();
    let results2 = umap
        .get_with_batch(&keys, |_, vals| vals.to_vec())
        .unwrap();

    assert_eq!(results1, results2);
}
