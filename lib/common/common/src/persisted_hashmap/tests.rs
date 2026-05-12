use std::collections::BTreeMap;

use itertools::{Itertools, assert_equal};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{RngExt, SeedableRng};

use super::fixtures::{Group, TestKey, TestReport, TestValue};
use super::{Key, MmapHashMap, UniversalHashMap, serialize_hashmap};
#[cfg(target_os = "linux")]
use crate::universal_io::IoUringFile;
use crate::universal_io::{self, MmapFile, UniversalRead};

#[rustfmt::skip] #[test] fn test_k_str_v_i64()   { run_checks::<str,  i64 >(); }
#[rustfmt::skip] #[test] fn test_k_str_v_u128()  { run_checks::<str,  u128>(); }
#[rustfmt::skip] #[test] fn test_k_str_v_u32()   { run_checks::<str,  u32 >(); }

#[rustfmt::skip] #[test] fn test_k_i64_v_i64()   { run_checks::<i64,  i64 >(); }
#[rustfmt::skip] #[test] fn test_k_i64_v_u128()  { run_checks::<i64,  u128>(); }
#[rustfmt::skip] #[test] fn test_k_i64_v_u32()   { run_checks::<i64,  u32 >(); }

#[rustfmt::skip] #[test] fn test_k_u128_v_i64()  { run_checks::<u128, i64 >(); }
#[rustfmt::skip] #[test] fn test_k_u128_v_u128() { run_checks::<u128, u128>(); }
#[rustfmt::skip] #[test] fn test_k_u128_v_u32()  { run_checks::<u128, u32 >(); }

fn run_checks<K: ?Sized + TestKey, V: TestValue>() {
    let mut r = TestReport::new();
    run_checks2::<K, V>(r.group("main"), 1000, false);
    run_checks2::<K, V>(r.group("empty"), 0, false);

    if K::NAME == str::NAME {
        run_checks2::<K, V>(r.group("large_keys"), 50, true);
    }
}

fn run_checks2<K: ?Sized + TestKey, V: TestValue>(
    mut r: Group<'_>,
    entry_count: usize,
    large_keys: bool,
) {
    let mut rng = StdRng::seed_from_u64(42);

    // Ground truth
    let mut orig = BTreeMap::<K::Owned, Vec<V>>::new();
    while orig.len() < entry_count {
        let key = K::gen_key(&mut rng, large_keys);
        if orig.contains_key(&key) {
            continue;
        }
        let values = (0..rng.random_range(1..=100))
            .map(|_| V::gen_value(&mut rng))
            .collect();
        orig.insert(key, values);
    }
    let non_existing_keys = std::iter::repeat_with(|| K::gen_key(&mut rng, large_keys))
        .filter(|key| !orig.contains_key(key))
        .unique()
        .take(1000)
        .collect::<Vec<_>>();

    // File
    let tmpdir = tempfile::Builder::new().tempdir().unwrap();
    let path = tmpdir.path().join("map");
    serialize_hashmap(
        &path,
        orig.iter().map(|(k, v)| (K::as_ref(k), v.iter().copied())),
    )
    .unwrap();

    // implementations under test
    run_mmap_checks(
        r.group("mmap"),
        &MmapHashMap::<K, V>::open(&path, false).unwrap(),
        &orig,
        &non_existing_keys,
    );
    run_uio_checks(
        r.group("uio:mmap"),
        &mut rng,
        &UniversalHashMap::<K, V, MmapFile>::open(&path, Default::default()).unwrap(),
        &orig,
        &non_existing_keys,
    );
    #[cfg(target_os = "linux")]
    run_uio_checks(
        r.group("uio:io_uring"),
        &mut rng,
        &UniversalHashMap::<K, V, IoUringFile>::open(&path, Default::default()).unwrap(),
        &orig,
        &non_existing_keys,
    );
}

fn run_mmap_checks<K: ?Sized + TestKey, V: TestValue>(
    mut r: Group<'_>,
    mmap: &MmapHashMap<K, V>,
    orig: &BTreeMap<K::Owned, Vec<V>>,
    non_existing_keys: &[K::Owned],
) {
    r.check("get() for existing keys", || {
        for (key, values) in orig {
            let val = mmap.get(K::as_ref(key)).unwrap().unwrap();
            assert_eq!(val, values);
        }
    });
    r.check("get() for non-existing keys", || {
        for key in non_existing_keys {
            assert!(mmap.get(K::as_ref(key)).unwrap().is_none());
        }
    });
    r.check("keys_count()", || assert_eq!(mmap.keys_count(), orig.len()));
    r.check("keys()", || assert_equal(mmap.keys().sorted(), orig.keys()));

    r.check("iter()", || {
        assert_equal(
            mmap.iter().sorted(),
            orig.iter().map(|(k, v)| (K::as_ref(k), v.as_slice())),
        );
    });
}

fn run_uio_checks<K: ?Sized + TestKey, V: TestValue, S: UniversalRead>(
    mut r: Group<'_>,
    mut rng: &mut StdRng,
    uio: &UniversalHashMap<K, V, S>,
    orig: &BTreeMap<K::Owned, Vec<V>>,
    non_existing_keys: &[K::Owned],
) {
    r.check("keys_count()", || assert_eq!(uio.keys_count(), orig.len()));

    r.check("get() for existing keys", || {
        for (key, values) in orig {
            let val = uio.unbatched_get(K::as_ref(key)).unwrap().unwrap();
            assert_eq!(&val, values, "uio::unbatched_get()");
        }
    });

    r.check("get_values_count() for existing keys", || {
        for (key, values) in orig {
            let val = uio
                .unbatched_get_values_count(K::as_ref(key))
                .unwrap()
                .unwrap();
            assert_eq!(val, values.len());
        }
    });

    r.check("get() for non-existing keys", || {
        for key in non_existing_keys {
            assert!(uio.unbatched_get(K::as_ref(key)).unwrap().is_none());
        }
    });

    r.check("get_values_count() for non-existing keys", || {
        for key in non_existing_keys {
            assert!(
                uio.unbatched_get_values_count(K::as_ref(key))
                    .unwrap()
                    .is_none()
            );
        }
    });

    r.check("for_each_entry", || {
        let mut res = Vec::new();
        uio.for_each_entry(|k, vals| push_val(&mut res, (K::from_ref(k), vals.to_vec())))
            .unwrap();
        res.sort();
        assert_equal(res.iter().map(|(k, v)| (k, v)), orig.iter());
    });

    r.check("for_each_key", || {
        let mut res = Vec::new();
        uio.for_each_key(|k| push_val(&mut res, K::from_ref(k)))
            .unwrap();
        res.sort();
        assert_equal(res.iter(), orig.keys());
    });

    r.check("for_each_entry_in_iter", || {
        #[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
        struct Entry<KO, V> {
            key: KO,
            values: Option<Vec<V>>,
            meta: u16,
        }

        let mut rng2 = rng.fork();
        let mut shuffled = std::iter::chain(
            orig.iter().map(|(k, v)| Entry {
                key: k.clone(),
                values: Some(v.clone()),
                meta: rng.random::<u16>(),
            }),
            non_existing_keys.iter().map(|k| Entry {
                key: k.clone(),
                values: None,
                meta: rng2.random::<u16>(),
            }),
        )
        .collect_vec();
        shuffled.shuffle(&mut rng);

        let mut collected = Vec::new();
        uio.for_each_entry_in_iter(
            shuffled.iter().map(|entry| {
                let Entry {
                    key,
                    values: _,
                    meta,
                } = entry;
                ((*meta, key.clone()), K::as_ref(key))
            }),
            |(meta, key), vals| {
                push_val(
                    &mut collected,
                    Entry {
                        key,
                        values: vals.map(|v| v.to_vec()),
                        meta,
                    },
                )
            },
        )
        .unwrap();

        shuffled.sort();
        collected.sort();
        assert_equal(collected.iter(), shuffled.iter());
    });
}

#[expect(clippy::unnecessary_wraps, reason = "reduce boilerplate in checks")]
fn push_val<T>(v: &mut Vec<T>, val: T) -> universal_io::Result<()> {
    v.push(val);
    Ok(())
}
