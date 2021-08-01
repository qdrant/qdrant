use tokio::runtime;
use tokio::runtime::Runtime;

pub fn create_search_runtime(max_search_threads: usize) -> std::io::Result<Runtime> {
    let mut search_threads = max_search_threads;

    if search_threads == 0 {
        let num_cpu = num_cpus::get();
        search_threads = std::cmp::max(1, num_cpu - 1);
    }

    runtime::Builder::new_multi_thread()
        .worker_threads(search_threads)
        .build()
}
