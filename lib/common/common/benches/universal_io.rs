use std::hint::black_box;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use common::generic_consts::{Random, Sequential};
#[cfg(target_os = "linux")]
use common::universal_io::IoUringFile;
use common::universal_io::{MmapFile, OpenOptions, ReadRange, UniversalRead};
use criterion::{Criterion, criterion_group, criterion_main};
use fs_err as fs;
use rand::rngs::StdRng;
use rand::{Rng as _, RngExt, SeedableRng as _};

const FILE_SIZE_BYTES: u64 = 512 * 1024 * 1024;

#[cfg(target_os = "linux")]
const LIMIT_MEMORY_ENV: &str = "LIMIT_MEMORY";
const LIMIT_MEMORY_ENV_INTERNAL: &str = "_LIMIT_MEMORY_INTERNAL";

fn benches(c: &mut Criterion) {
    let path = make_random_file();

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
            .args(["--user", "--scope", "-p", "MemoryMax=64M", "--"])
            .args(std::env::args())
            .env_remove(LIMIT_MEMORY_ENV) // prevent recursion
            .env(LIMIT_MEMORY_ENV_INTERNAL, "") // for `low-mem/` prefix
            .status()
            .expect("Failed to rerun benchmark with memory limit");
        std::process::exit(rc.code().unwrap_or(1));
    }

    #[cfg(target_os = "linux")]
    read_benches::<u64, IoUringFile>(c, "io_uring", "8bytes", &path);
    read_benches::<u64, MmapFile>(c, "mmap", "8bytes", &path);
    #[cfg(target_os = "linux")]
    read_benches::<[u64; 128], IoUringFile>(c, "io_uring", "1KiB", &path);
    read_benches::<[u64; 128], MmapFile>(c, "mmap", "1KiB", &path);

    #[cfg(target_os = "linux")]
    if std::env::var_os(LIMIT_MEMORY_ENV_INTERNAL).is_none() {
        eprintln!("hint: Rerun with {LIMIT_MEMORY_ENV}=1 to enable memory limit");
    }
}

fn read_benches<T: bytemuck::Pod, C: UniversalRead<T>>(
    c: &mut Criterion,
    impl_name: &str, // Corresponds to `C`
    elem_size: &str, // Corresponds to `T`
    path: &Path,
) {
    let options = OpenOptions {
        writeable: false,
        need_sequential: true,
        disk_parallel: None,
        populate: Some(false),
        advice: None,
        prevent_caching: Some(false),
    };
    let storage = C::open(path, options).unwrap();
    let len = FILE_SIZE_BYTES / size_of::<T>() as u64;
    let mut rng = rand::rng();
    assert_eq!(storage.len().unwrap(), len);

    let low_mem = std::env::var_os(LIMIT_MEMORY_ENV_INTERNAL).is_some();

    if !low_mem {
        storage.populate().unwrap();
    }

    let prefix = if low_mem { "low-mem/" } else { "" };
    let group_name = format!("{prefix}{impl_name}/{elem_size}");
    let mut group = c.benchmark_group(&group_name);

    // `read_single` - single non-batched read at a random offset
    group.bench_function("read_single", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            let offset = rng.random_range(0..len) * size_of::<T>() as u64;
            let data = storage
                .read::<Random>(ReadRange {
                    byte_offset: offset,
                    length: 1,
                })
                .unwrap();
            for &item in bytemuck::cast_slice::<T, u64>(&data) {
                sum = sum.wrapping_add(item);
            }
            black_box(sum);
        })
    });

    // `read_batch_small_random` - batch of 8 random reads, each at a random offset
    group.bench_function("read_batch_small_random", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            let ranges = (0..8)
                .map(|_| ReadRange {
                    byte_offset: rng.random_range(0..len) * size_of::<T>() as u64,
                    length: 1,
                })
                .map(|range| ((), range));
            storage
                .read_batch::<Random, ()>(ranges, |(), chunk| {
                    for &item in bytemuck::cast_slice::<T, u64>(chunk) {
                        sum = sum.wrapping_add(item);
                    }
                    Ok(())
                })
                .unwrap();
            black_box(sum);
        })
    });

    // `read_batch_small_sequential` - batch of 8 reads at sequential offsets
    group.bench_function("read_batch_small_sequential", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            let start = rng.random_range(0..len - 8) * size_of::<T>() as u64;
            let ranges = (0..8)
                .map(move |i| ReadRange {
                    byte_offset: start + i * size_of::<T>() as u64,
                    length: 1,
                })
                .map(|range| ((), range));
            storage
                .read_batch::<Sequential, ()>(ranges, |(), chunk| {
                    for &item in bytemuck::cast_slice::<T, u64>(chunk) {
                        sum = sum.wrapping_add(item);
                    }
                    Ok(())
                })
                .unwrap();
            black_box(sum);
        })
    });

    // `read_batch_full` - read the whole file
    // Since it can be very slow, run only a single iteration first.
    let read_batch_full = || {
        let mut sum = 0u64;
        storage
            .read_batch::<Sequential, ()>(ranges_full_file::<T>(), |(), chunk| {
                for &item in bytemuck::cast_slice::<T, u64>(chunk) {
                    sum = sum.wrapping_add(item);
                }
                Ok(())
            })
            .unwrap();
        black_box(sum);
    };
    let duration = time_it(read_batch_full);
    if duration.as_secs_f64() <= 1.0 {
        group.bench_function("read_batch_full", |b| b.iter(read_batch_full));
    } else {
        eprintln!("{group_name}/read_batch_full: {duration:.2?} (single iteration)");
    }
}

fn time_it<F: FnOnce()>(f: F) -> Duration {
    let start = Instant::now();
    f();
    start.elapsed()
}

fn ranges_full_file<T>() -> impl Iterator<Item = ((), ReadRange)> {
    let len = FILE_SIZE_BYTES / size_of::<T>() as u64;
    (0..len)
        .map(move |i| ReadRange {
            byte_offset: i * size_of::<T>() as u64,
            length: 1,
        })
        .map(|range| ((), range))
}

fn make_random_file() -> PathBuf {
    let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(env!("CARGO_PKG_NAME"))
        .join(env!("CARGO_CRATE_NAME"))
        .join(format!("random-{FILE_SIZE_BYTES}.bin"));

    if let Ok(metadata) = fs::metadata(&path)
        && metadata.len() == FILE_SIZE_BYTES
    {
        return path;
    }

    eprintln!("Building random benchmark file at {path:?}...");
    fs_err::create_dir_all(path.parent().unwrap()).unwrap();

    let mut file = fs::File::create(&path).unwrap();
    let mut rng = StdRng::seed_from_u64(42);
    let mut buffer = vec![0; 1024 * 1024];
    let mut bytes_left = FILE_SIZE_BYTES as usize;

    while bytes_left > 0 {
        let len = bytes_left.min(buffer.len());
        rng.fill_bytes(&mut buffer[..len]);
        file.write_all(&buffer[..len]).unwrap();
        bytes_left -= len;
    }

    file.flush().unwrap();
    eprintln!("Random benchmark file cached at {path:?}.");
    path
}

criterion_group! {
    name = bench_group;
    config = Criterion::default();
    targets = benches
}

criterion_main!(bench_group);
