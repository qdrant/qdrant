use std::path::Path;
use std::sync::OnceLock;

use common::universal_io::{OpenOptions, UniversalRead, UniversalReadFileOps};
use tokio::runtime::{Handle, Runtime};

use super::mock_async_file::MockAsyncFile;
use crate::{IoBridge, global_async_handle, set_global_async_handle};

fn process_test_handle() -> Handle {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().expect("process test runtime"))
        .handle()
        .clone()
}

#[test]
fn global_async_handle_installs_once_and_is_visible() {
    let handle = process_test_handle();
    let _ = set_global_async_handle(handle);
    assert!(
        global_async_handle().is_some(),
        "global handle must be visible once set",
    );
}

#[test]
fn io_bridge_resolves_global_handle_outside_tokio_context() {
    let handle = process_test_handle();
    let _ = set_global_async_handle(handle);

    let exists = <IoBridge<MockAsyncFile> as UniversalReadFileOps>::exists(Path::new("/x"));
    assert!(!exists.expect("exists ok"));
}

#[test]
fn io_bridge_open_uses_global_handle() {
    let handle = process_test_handle();
    let _ = set_global_async_handle(handle);

    let io_bridge = <IoBridge<MockAsyncFile> as UniversalRead>::open(
        Path::new("/dummy"),
        OpenOptions::default(),
    )
    .expect("open through global handle");

    assert_eq!(io_bridge.len::<u8>().expect("len"), 0);
}
