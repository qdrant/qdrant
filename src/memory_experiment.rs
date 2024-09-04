use std::env;
use std::hint::black_box;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use segment;
use segment::segment_constructor::load_segment;
#[cfg(all(
    not(target_env = "msvc"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(
    not(target_env = "msvc"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() {
    // This is a simple routine, which is dedicated to make sure memory is not leaking during
    // load and offload of the segments.
    let mem = segment::utils::mem::Mem::new();

    let available_memory = mem.available_memory_bytes();

    let args: Vec<String> = env::args().collect();
    let segment_path = Path::new(&args[1]);

    for i in 0..100 {
        let stopped = AtomicBool::new(false);

        let segment = load_segment(segment_path, &stopped).unwrap().unwrap();
        black_box(segment);

        let available_memory_after_load = mem.available_memory_bytes();

        println!(
            "Memory used by the segment: {} Mb",
            (available_memory - available_memory_after_load) / 1024 / 1024
        );
    }
}
