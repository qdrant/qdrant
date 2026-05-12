use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use common::persisted_hashmap::{MmapHashMap, UniversalHashMap, serialize_hashmap};
use common::universal_io::{IoUringFile, OpenOptions, UniversalIoError};
use criterion::{Criterion, criterion_group, criterion_main};
use fs_err as fs;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

#[cfg(target_os = "linux")]
const LIMIT_MEMORY_ENV: &str = "LIMIT_MEMORY";
const LIMIT_MEMORY_ENV_INTERNAL: &str = "_LIMIT_MEMORY_INTERNAL";

fn bench_mmap_hashmap(c: &mut Criterion) {
    #[cfg(target_os = "linux")]
    if std::env::var_os(LIMIT_MEMORY_ENV).is_some() {
        // Without memory limit, the file contents caches in the RAM quickly and
        // we are measuring the "everything fits in kernel page cache" scenario.
        //
        // OTOH, to measure the IO-heavy scenario, we can limit the available
        // process memory. There are many ways to do that, and systemd-run is
        // one of them. If it doesn't work, try containers.

        eprintln!("Dropping cache via `echo 3 > /proc/sys/vm/drop_caches` (requires sudo)...");
        let rc = std::process::Command::new("sudo")
            .args(["sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"])
            .status()
            .expect("Failed to drop caches");
        assert!(rc.success(), "Failed to drop caches");

        eprintln!("Rerunning benchmark with memory limit...");
        let rc = std::process::Command::new("systemd-run")
            .args(["--user", "--scope", "-p", "MemoryMax=48M", "--"])
            .args(std::env::args())
            .env_remove(LIMIT_MEMORY_ENV) // prevent recursion
            .env(LIMIT_MEMORY_ENV_INTERNAL, "") // for `low-mem/` prefix
            .status()
            .expect("Failed to rerun benchmark with memory limit");
        std::process::exit(rc.code().unwrap_or(1));
    }

    let low_mem = std::env::var_os(LIMIT_MEMORY_ENV_INTERNAL).is_some();
    let prefix = if low_mem { "low-mem/" } else { "" };

    let path = make_serialized_hashmap(1_000_000);

    let mmap = MmapHashMap::<str, u32>::open(&path, !low_mem).unwrap();
    let uio = UniversalHashMap::<str, u32, IoUringFile>::open(
        &path,
        OpenOptions {
            writeable: false,
            ..Default::default()
        },
    )
    .unwrap();
    if !low_mem {
        uio.populate().unwrap();
    }

    let mut keys: Vec<String> = mmap.iter().map(|(k, _)| k.to_owned()).collect();
    keys.sort_unstable();

    let mut it = keys.iter().cycle();

    // Scenario: get 1 random key.
    c.bench_function(&format!("{prefix}mmap:get_1"), |b| {
        b.iter(|| {
            mmap.get(it.next().unwrap())
                .unwrap()
                .and_then(|v| v.iter().copied().max())
        })
    });
    c.bench_function(&format!("{prefix}uio:get_1"), |b| {
        b.iter(|| {
            uio.unbatched_get(it.next().unwrap())
                .unwrap()
                .and_then(|v| v.iter().copied().max())
        })
    });

    // Scenario: get 10 random keys.
    c.bench_function(&format!("{prefix}mmap:get_10"), |b| {
        b.iter(|| {
            let mut result = 0;
            for _ in 0..10 {
                let values = mmap.get(it.next().unwrap()).unwrap().unwrap();
                result += values.iter().copied().max().unwrap_or(0);
            }
            result
        })
    });
    c.bench_function(&format!("{prefix}uio:get_10"), |b| {
        b.iter(|| {
            let mut result = 0;
            uio.for_each_entry_in_iter(
                it.by_ref().take(10).map(|k| ((), k.as_str())),
                |(), values| -> Result<(), UniversalIoError> {
                    result += values.unwrap().iter().copied().max().unwrap_or(0);
                    Ok(())
                },
            )
            .unwrap();
            result
        })
    });

    // Scenario: iterate the whole map.
    c.bench_function(&format!("{prefix}mmap:whole"), |b| {
        b.iter(|| {
            let mut result = 0;
            for (_key, values) in mmap.iter() {
                result += values.iter().copied().max().unwrap_or(0);
            }
            result
        })
    });
    c.bench_function(&format!("{prefix}uio:whole"), |b| {
        b.iter(|| {
            let mut result = 0;
            uio.for_each_entry(|_key, values| -> Result<(), UniversalIoError> {
                result += values.iter().copied().max().unwrap_or(0);
                Ok(())
            })
            .unwrap();
            result
        })
    });

    #[cfg(target_os = "linux")]
    if std::env::var_os(LIMIT_MEMORY_ENV_INTERNAL).is_none() {
        eprintln!("hint: Rerun with {LIMIT_MEMORY_ENV}=1 to enable memory limit");
    }
}

fn make_serialized_hashmap(count: usize) -> PathBuf {
    let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(env!("CARGO_PKG_NAME"))
        .join(env!("CARGO_CRATE_NAME"))
        .join(format!("hashmap-{count}.bin"));

    if !path.exists() {
        eprintln!("Building serialized hashmap at {path:?}...");
        fs::create_dir_all(path.parent().unwrap()).unwrap();

        let mut rng = StdRng::seed_from_u64(42);
        let map = gen_map(&mut rng, count);

        serialize_hashmap::<str, u32>(
            &path,
            map.iter().map(|(k, v)| (k.as_str(), v.iter().copied())),
        )
        .unwrap();
    }

    let size = fs::metadata(&path).unwrap().len();
    eprintln!("Serialized hashmap at {path:?} ({size} bytes)");
    path
}

fn gen_map(rng: &mut StdRng, count: usize) -> BTreeMap<String, Vec<u32>> {
    let mut map = BTreeMap::new();
    while map.len() < count {
        let key: String = (0..rng.random_range(5..=32))
            .map(|_| rng.random_range(b'a'..=b'z') as char)
            .collect();
        let values: Vec<u32> = (0..rng.random_range(1..=100))
            .map(|_| rng.random())
            .collect();
        map.insert(key, values);
    }
    map
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_mmap_hashmap,
}

criterion_main!(benches);
