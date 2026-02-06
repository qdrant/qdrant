use std::collections::VecDeque;
use std::io::{Seek, Write};
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::Parser;
use fs_err::File;
use fs_err::os::unix::fs::OpenOptionsExt;
use rand::RngCore;
use rand::distr::Distribution;
use rand_distr::Zipf;

// =============================================================================
// Benchmark parameters
// =============================================================================

#[derive(Parser, Clone)]
struct Args {
    /// Ignored. (`cargo bench` passes this argument)
    #[clap(long)]
    bench: bool,

    /// Path to a file on local SSD.
    /// Will be used as a cache.
    local_ssd_path: PathBuf,

    /// Path to a file on network-attached storage.
    /// If small or non-existent, it will be created and filled with random data.
    network_attached_path: PathBuf,

    /// Controller name. See [`main()`] source for options.
    #[clap(long)]
    controller: String,

    /// Number of worker threads.
    #[clap(long, default_value_t = 4)]
    threads: usize,
}

const VECTOR_SIZE: usize = 4096; // Should be multiple of 4096 for O_DIRECT

// =============================================================================
// I/O stuff
// =============================================================================

struct Storages {
    local_ssd_file: File,
    local_ssd_mmap: memmap2::MmapMut,
    network_attached_file: File,
}

#[repr(C, align(4096))]
struct Buffer([MaybeUninit<u8>; VECTOR_SIZE]);

impl Storages {
    fn read_from_slow_storage(&self, key: u32, buffer: &mut Buffer) -> &[u8] {
        self.network_attached_file
            .file()
            .read_exact_at(
                unsafe {
                    std::slice::from_raw_parts_mut(buffer.0.as_mut_ptr().cast::<u8>(), VECTOR_SIZE)
                },
                u64::from(key) * VECTOR_SIZE as u64,
            )
            .unwrap_or_else(|e| {
                panic!("Failed to read from network-attached storage for key {key}: {e}");
            });
        unsafe { std::slice::from_raw_parts(buffer.0.as_ptr().cast::<u8>(), VECTOR_SIZE) }
    }

    fn read_from_local_ssd(&self, offset: u32) -> &[u8] {
        &self.local_ssd_mmap[(offset as usize * VECTOR_SIZE)..((offset as usize + 1) * VECTOR_SIZE)]
    }

    fn write_to_local_ssd(&self, offset: u32, vector: &[u8]) {
        self.local_ssd_file
            .file()
            .write_all_at(vector, u64::from(offset) * VECTOR_SIZE as u64)
            .expect("Failed to write to local SSD");
    }
}

// =============================================================================
// Cache controller implementations
// =============================================================================

trait Controller: Clone + Send + Sync + 'static {
    /// Create a new controller instance.
    fn new(storages: Arc<Storages>, item_count: usize) -> Self;

    /// The most intresting part.
    /// Call a function and pass vector slice to it.
    fn with_vector<F: Fn(&[u8])>(&self, key: u32, f: F);
}

/// A dumb controller. Don't cache, always read from slow storage.
#[derive(Clone)]
struct ControllerDumb {
    storages: Arc<Storages>,
}

impl Controller for ControllerDumb {
    fn new(storages: Arc<Storages>, _item_count: usize) -> Self {
        Self { storages }
    }

    fn with_vector<F: Fn(&[u8])>(&self, key: u32, f: F) {
        let mut uninit_buffer = Buffer([MaybeUninit::uninit(); VECTOR_SIZE]);
        let buffer = self
            .storages
            .read_from_slow_storage(key, &mut uninit_buffer);
        f(buffer);
    }
}

/// [`trififo`] based controller.
#[derive(Clone)]
struct ControllerTrififo {
    storages: Arc<Storages>,
    cache: Arc<trififo::Cache<u32, u32>>,
    unused_offsets: Arc<Mutex<VecDeque<u32>>>,
    capacity: usize,
    counter: Arc<AtomicUsize>,
}

impl Controller for ControllerTrififo {
    fn new(storages: Arc<Storages>, item_count: usize) -> Self {
        // This is a hack to force the cache to evict a bit earlier.
        let safety_margin = 1_000;

        Self {
            storages,
            cache: Arc::new(trififo::Cache::new(item_count - safety_margin)),
            unused_offsets: Arc::new(Mutex::new((0..item_count as u32).rev().collect())),
            capacity: item_count,
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn with_vector<F: Fn(&[u8])>(&self, key: u32, f: F) {
        match self.cache.get(&key) {
            Some(offset) => {
                f(self.storages.read_from_local_ssd(offset));
            }
            None => {
                let mut uninit_buffer = Buffer([MaybeUninit::uninit(); VECTOR_SIZE]);

                let buffer = self
                    .storages
                    .read_from_slow_storage(key, &mut uninit_buffer);
                f(buffer);

                let mut unused_offsets = self.unused_offsets.lock().unwrap();
                let offset = unused_offsets.pop_front().unwrap_or_else(|| {
                    // FIXME: we need to somehow re-populate unused_offsets on eviction.
                    // But for now, just use a counter. (expect errors)
                    let cnt = self.counter.fetch_add(1, Ordering::Relaxed);
                    (cnt % self.capacity) as u32
                });

                self.storages.write_to_local_ssd(offset, buffer);
                self.cache.insert(key, offset);
            }
        }
    }
}

/// [`quick_cache`] based controller.
#[derive(Clone)]
struct ControllerQuickCache {
    storages: Arc<Storages>,

    cache: Arc<
        quick_cache::sync::Cache<
            u32,
            u32,
            quick_cache::UnitWeighter,       // default
            quick_cache::DefaultHashBuilder, // default
            ControllerQuickCacheLifecycle,
        >,
    >,

    unused_offsets: Arc<Mutex<VecDeque<u32>>>,
}

/// [`quick_cache::Lifecycle`] impl that pushes evicted offsets back to `unused_offsets`.
#[derive(Clone)]
struct ControllerQuickCacheLifecycle {
    base: quick_cache::sync::DefaultLifecycle<u32, u32>,
    unused_offsets: Arc<Mutex<VecDeque<u32>>>,
}

impl quick_cache::Lifecycle<u32, u32> for ControllerQuickCacheLifecycle {
    type RequestState = <quick_cache::sync::DefaultLifecycle<u32, u32> as quick_cache::Lifecycle<
        u32,
        u32,
    >>::RequestState;

    fn begin_request(&self) -> Self::RequestState {
        self.base.begin_request()
    }

    fn on_evict(&self, state: &mut Self::RequestState, key: u32, val: u32) {
        self.base.on_evict(state, key, val);
        self.unused_offsets.lock().unwrap().push_back(val);
    }
}

impl Controller for ControllerQuickCache {
    fn new(storages: Arc<Storages>, item_count: usize) -> Self {
        let unused_offsets = Arc::new(Mutex::new((0..item_count as u32).rev().collect()));

        // This is a hack to force the cache to evict a bit earlier.
        let safety_margin = 1_000;

        Self {
            storages,
            cache: Arc::new(quick_cache::sync::Cache::with(
                item_count - safety_margin,
                (item_count - safety_margin) as u64,
                Default::default(),
                Default::default(),
                ControllerQuickCacheLifecycle {
                    base: Default::default(),
                    unused_offsets: Arc::clone(&unused_offsets),
                },
            )),
            unused_offsets,
        }
    }

    fn with_vector<F: Fn(&[u8])>(&self, key: u32, f: F) {
        use quick_cache::sync::GuardResult;

        match self.cache.get_value_or_guard(&key, None) {
            GuardResult::Value(value) => {
                f(self.storages.read_from_local_ssd(value));
            }
            GuardResult::Guard(guard) => {
                let mut uninit_buffer = Buffer([MaybeUninit::uninit(); VECTOR_SIZE]);

                let buffer = self
                    .storages
                    .read_from_slow_storage(key, &mut uninit_buffer);
                f(buffer);

                let offset = self
                    .unused_offsets
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("No unused offsets available");

                self.storages.write_to_local_ssd(offset, buffer);
                guard.insert(offset).unwrap();
            }
            GuardResult::Timeout => unreachable!(),
        }
    }
}

// =============================================================================
// The benchmark
// =============================================================================

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.controller.as_str() {
        "d" | "dumb" => run_experiment::<ControllerDumb>(args),
        "qc" | "quick_cache" => run_experiment::<ControllerQuickCache>(args),
        "tf" | "trififo" => run_experiment::<ControllerTrififo>(args),
        other => panic!("Unknown controller: {other}"),
    }
}

fn run_experiment<C: Controller>(args: Args) -> anyhow::Result<()> {
    let cache_size = 200_000;
    let cold_size = cache_size * 10;

    let local_ssd_file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&args.local_ssd_path)
        .expect("Failed to open local SSD file");
    local_ssd_file
        .set_len((cache_size * VECTOR_SIZE) as u64)
        .unwrap();
    let local_ssd_mmap = unsafe { memmap2::MmapMut::map_mut(&local_ssd_file).unwrap() };

    {
        let mut file = File::options()
            .append(true)
            .create(true)
            .open(&args.network_attached_path)?;
        let mut pos = file.metadata()?.len();
        pos = pos - (pos % VECTOR_SIZE as u64);
        file.seek(std::io::SeekFrom::Start(pos))?;
        if pos < (cold_size as u64 * VECTOR_SIZE as u64) {
            println!(
                "Filling network-attached file up to {} vectors (~{} MB)...",
                cold_size,
                (cold_size * VECTOR_SIZE) / (1024 * 1024)
            );
        }

        let mut buffer = vec![0u8; VECTOR_SIZE];
        let mut rng = rand::rng();

        while pos < (cold_size as u64 * VECTOR_SIZE as u64) {
            rng.fill_bytes(&mut buffer);
            buffer[0..8].copy_from_slice(&pos.to_le_bytes());
            file.write_all(&buffer)?;
            pos += VECTOR_SIZE as u64;
        }
    }

    let flags = 0;

    #[cfg(target_os = "linux")]
    let flags = nix::libc::O_DIRECT;

    let network_attached_file = File::options()
        .read(true)
        .custom_flags(flags)
        .open(&args.network_attached_path)
        .expect("Failed to open network-attached file");

    #[cfg(target_os = "macos")]
    unsafe {
        nix::libc::fcntl(network_attached_file.as_raw_fd(), nix::libc::F_NOCACHE, 1)
    };

    let storages = Arc::new(Storages {
        local_ssd_file,
        local_ssd_mmap,
        network_attached_file,
    });

    let controller = C::new(storages, cache_size);

    let stop = Arc::new(AtomicBool::new(false));
    let count = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..args.threads {
        let controller = controller.clone();
        let stop = stop.clone();
        let count = count.clone();
        let errors = errors.clone();

        let handle = std::thread::spawn(move || {
            let mut rng = rand::rng();
            let zipf = Zipf::new(cold_size as f64 - 1.0, 1.0).unwrap();

            loop {
                for _ in 0..100 {
                    let key = zipf.sample(&mut rng) as u32;
                    controller.with_vector(key, |vector| {
                        let first_eight = u64::from_le_bytes(vector[0..8].try_into().unwrap());
                        if first_eight != u64::from(key) * VECTOR_SIZE as u64 {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }

                count.fetch_add(100, Ordering::Relaxed);
                if stop.load(Ordering::Relaxed) {
                    break;
                }
            }
        });
        handles.push(handle);
    }

    let mut total_count = 0;
    for i in 0..200 {
        if i % 20 == 0 {
            // Table header
            println!(
                "\x1b[33m{:>4}|{:>12}|{:>12}|{:>12}\x1b[0m",
                "time", "curr ops/sec", "avg ops/sec", "errors"
            );
        }
        std::thread::sleep(Duration::from_secs(1));
        let current_count = count.swap(0, Ordering::Relaxed);
        let current_errors = errors.swap(0, Ordering::Relaxed);
        total_count += current_count;

        println!(
            "{i:>4} {:>12} {:>12} {:>12}",
            pretty_print_int(current_count),
            pretty_print_int(total_count / (i + 1)),
            pretty_print_int(current_errors),
        );
    }

    stop.store(true, Ordering::Relaxed);

    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}

fn pretty_print_int(num: usize) -> String {
    let mut s = String::with_capacity(20);
    let mut num_ = num;
    let mut i = 0;
    while num_ != 0 || i == 0 {
        if i != 0 && i % 3 == 0 {
            s.push('_');
        }
        s.push_str(&(num_ % 10).to_string());
        num_ /= 10;
        i += 1;
    }
    s.chars().rev().collect()
}
