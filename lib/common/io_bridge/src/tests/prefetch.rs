#![cfg(test)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::generic_consts::Random;
use common::mmap::AdviceSetting;
use common::universal_io::{
    CachedFs, CachedReadFs, OpenOptions, Populate, UniversalReadAsync, UniversalReadFs,
};

use super::whole_read::CountingSource;
use crate::{BlobFs, BridgeRuntime};

#[test]
fn scheduled_open_continuation_runs_while_parked() {
    let source = CountingSource::new(b"header|offsets");
    let fs = CachedFs::new(BlobFs::new(source, BridgeRuntime::global()), Path::new("")).unwrap();
    let options = OpenOptions {
        writeable: false,
        need_sequential: false,
        populate: Populate::No,
        advice: AdviceSetting::Global,
    };

    let reached = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&reached);
    fs.schedule_open_with(
        Path::new("obj"),
        Some(options),
        None,
        move |file| async move {
            let header = file.read_bytes_async(0..6, Random, 1).await?;
            assert_eq!(&header[..], b"header");
            // Dependent second read, only reachable after the first one landed.
            file.read_bytes_async(7..14, Random, 1).await?;
            flag.store(true, Ordering::Release);
            Ok(file)
        },
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    while !reached.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "the continuation waited for the pool to poll it"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    fs.open("obj", options, ()).unwrap();
}
