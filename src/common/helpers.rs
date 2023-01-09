use std::fs::canonicalize;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::runtime;
use tokio::runtime::Runtime;

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LocksOption {
    pub error_message: Option<String>,
    pub write: bool,
}

pub fn home_dir() -> std::io::Result<PathBuf> {
    dirs::home_dir().ok_or(Error::new(ErrorKind::Other, "Home directory not found"))
}

pub fn tilde_expand(path: &str) -> std::io::Result<PathBuf> {
    match path {
        "~" => home_dir(),
        p if p.starts_with("~/") => Ok(home_dir()?.join(&path[2..])),
        _ => Ok(PathBuf::from(path)),
    }
}

// Parsing the given path
// Ex: ~/a/../foo.txt -> /home/user/foo.txt
// Return io::ErrorKind::NotFound if the path does not exist
pub fn parse_path(path: &str) -> std::io::Result<PathBuf> {
    canonicalize(tilde_expand(path)?)
}

pub fn create_search_runtime(max_search_threads: usize) -> std::io::Result<Runtime> {
    let mut search_threads = max_search_threads;

    if search_threads == 0 {
        let num_cpu = num_cpus::get();
        search_threads = std::cmp::max(1, num_cpu - 1);
    }

    runtime::Builder::new_multi_thread()
        .worker_threads(search_threads)
        .enable_time()
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("search-{}", id)
        })
        .build()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;
    use std::{env, thread};

    use storage::content_manager::consensus::is_ready::IsReady;

    use super::*;

    #[test]
    fn test_is_ready() {
        let is_ready = Arc::new(IsReady::default());
        let is_ready_clone = is_ready.clone();
        let join = thread::spawn(move || {
            is_ready_clone.await_ready();
            eprintln!(
                "is_ready_clone.check_ready() = {:#?}",
                is_ready_clone.check_ready()
            );
        });

        sleep(Duration::from_millis(500));
        eprintln!("Making ready");
        is_ready.make_ready();
        sleep(Duration::from_millis(500));
        join.join().unwrap()
    }

    #[test]
    fn test_tilde_expansion() {
        let home_env = "HOME";
        env::set_var(home_env, "/home/user");

        assert_eq!(
            tilde_expand("~/qdrant").unwrap(),
            PathBuf::from("/home/user/qdrant")
        );

        assert_eq!(tilde_expand("").unwrap(), PathBuf::from(""));
        assert_eq!(tilde_expand("qdrant").unwrap(), PathBuf::from("qdrant"));
        assert_eq!(tilde_expand("~").unwrap(), PathBuf::from("/home/user"));
    }
}
