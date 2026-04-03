use std::collections::{BTreeSet, HashMap};
use std::hash::Hash;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use crate::persisted_hashmap::keys::Key;
use crate::persisted_hashmap::mmap_hashmap::{MmapHashMap, gen_ident, gen_map, repeat_until};

#[test]
fn test_mmap_hash() {
    test_mmap_hash_impl(gen_ident, |s| s.as_str(), |s| s.to_owned());
    test_mmap_hash_impl(|rng| rng.random::<i64>(), |i| i, |i| *i);
    test_mmap_hash_impl(|rng| rng.random::<u128>(), |i| i, |i| *i);
}

fn test_mmap_hash_impl<K: Key + ?Sized, K1: Ord + Hash>(
    generator: impl Clone + Fn(&mut StdRng) -> K1,
    as_ref: impl Fn(&K1) -> &K,
    from_ref: impl Fn(&K) -> K1,
) {
    let mut rng = StdRng::seed_from_u64(42);
    let tmpdir = tempfile::Builder::new().tempdir().unwrap();

    let map = gen_map(&mut rng, generator.clone(), 1000);
    MmapHashMap::<K, u32>::create(
        &tmpdir.path().join("map"),
        map.iter().map(|(k, v)| (as_ref(k), v.iter().copied())),
    )
    .unwrap();
    let mmap = MmapHashMap::<K, u32>::open(&tmpdir.path().join("map"), false).unwrap();

    // Non-existing keys should return None
    for _ in 0..1000 {
        let key = repeat_until(|| generator(&mut rng), |key| !map.contains_key(key));
        assert!(mmap.get(as_ref(&key)).unwrap().is_none());
    }

    // check keys iterator
    for key in mmap.keys() {
        let key = from_ref(key);
        assert!(map.contains_key(&key));
    }
    assert_eq!(mmap.keys_count(), map.len());
    assert_eq!(mmap.keys().count(), map.len());

    for (k, v) in mmap.iter() {
        let v = v.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(map.get(&from_ref(k)).unwrap(), &v);
    }

    let keys: Vec<_> = mmap.keys().collect();
    assert_eq!(keys.len(), map.len());

    // Existing keys should return the correct values
    for (k, v) in map {
        assert_eq!(
            mmap.get(as_ref(&k)).unwrap().unwrap(),
            &v.into_iter().collect::<Vec<_>>()
        );
    }
}

#[test]
fn test_mmap_hash_impl_u64_value() {
    let mut rng = StdRng::seed_from_u64(42);
    let tmpdir = tempfile::Builder::new().tempdir().unwrap();

    let mut map: HashMap<i64, BTreeSet<u64>> = Default::default();

    for key in 0..10i64 {
        map.insert(key, (0..100).map(|_| rng.random_range(0..=1000)).collect());
    }

    MmapHashMap::<i64, u64>::create(
        &tmpdir.path().join("map"),
        map.iter().map(|(k, v)| (k, v.iter().copied())),
    )
    .unwrap();

    let mmap = MmapHashMap::<i64, u64>::open(&tmpdir.path().join("map"), true).unwrap();

    for (k, v) in map {
        assert_eq!(
            mmap.get(&k).unwrap().unwrap(),
            &v.into_iter().collect::<Vec<_>>()
        );
    }

    assert!(mmap.get(&100).unwrap().is_none())
}

#[test]
fn test_mmap_hash_impl_u128_value() {
    let mut rng = StdRng::seed_from_u64(42);
    let tmpdir = tempfile::Builder::new().tempdir().unwrap();

    let mut map: HashMap<u128, BTreeSet<u32>> = Default::default();

    map.insert(
        9812384971724u128,
        (0..100).map(|_| rng.random_range(0..=1000)).collect(),
    );

    MmapHashMap::<u128, u32>::create(
        &tmpdir.path().join("map"),
        map.iter().map(|(k, v)| (k, v.iter().copied())),
    )
    .unwrap();

    let mmap = MmapHashMap::<u128, u32>::open(&tmpdir.path().join("map"), true).unwrap();

    let keys: Vec<_> = mmap.keys().collect();
    assert_eq!(keys.len(), map.len());

    for (k, v) in map {
        assert_eq!(
            mmap.get(&k).unwrap().unwrap(),
            &v.into_iter().collect::<Vec<_>>()
        );
    }
    assert!(mmap.get(&100).unwrap().is_none())
}
