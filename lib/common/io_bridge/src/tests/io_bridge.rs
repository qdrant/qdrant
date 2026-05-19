use std::borrow::Cow;

use common::generic_consts::Random;
use common::universal_io::{Flusher, ReadRange, UniversalRead, UniversalWrite};
use tokio::runtime::Runtime;

use super::mock_async_file::MockAsyncFile;
use crate::IoBridge;

#[test]
fn io_bridge_async_read_exposes_universal_read() {
    let runtime = Runtime::new().expect("runtime");
    let handle = runtime.handle().clone();

    let async_file = MockAsyncFile::with_contents(b"hello world".to_vec());
    let io_bridge = IoBridge::new(async_file, handle);

    let bytes: Cow<[u8]> = io_bridge
        .read::<Random, u8>(ReadRange::new(6, 5))
        .expect("read");
    assert_eq!(bytes.as_ref(), b"world");

    assert_eq!(io_bridge.len::<u8>().expect("len"), 11);
}

#[test]
fn io_bridge_routes_read_iter_through_blocking_pipeline() {
    let runtime = Runtime::new().expect("runtime");
    let handle = runtime.handle().clone();

    let async_file = MockAsyncFile::with_contents((0u8..=9u8).collect());
    let io_bridge = IoBridge::new(async_file, handle);

    let mut results: Vec<(u64, Vec<u8>)> = Vec::new();
    for r in io_bridge
        .read_iter::<Random, u8, u64>([
            (0u64, ReadRange::new(0, 3)),
            (1u64, ReadRange::new(3, 3)),
            (2u64, ReadRange::new(6, 4)),
        ])
        .expect("read_iter")
    {
        let (u, cow) = r.expect("range");
        results.push((u, cow.into_owned()));
    }

    assert_eq!(
        results,
        vec![
            (0, vec![0, 1, 2]),
            (1, vec![3, 4, 5]),
            (2, vec![6, 7, 8, 9]),
        ]
    );
}

#[test]
fn io_bridge_async_write_exposes_universal_write() {
    let runtime = Runtime::new().expect("runtime");
    let handle = runtime.handle().clone();

    let file = MockAsyncFile::with_contents(b"hello".to_vec());
    let verify = file.contents_handle();
    let mut io_bridge = IoBridge::new(file, handle);

    UniversalWrite::write::<u8>(&mut io_bridge, 5, b" world").expect("write");

    assert_eq!(verify.lock().unwrap().as_slice(), b"hello world");

    let bytes = io_bridge
        .read::<Random, u8>(ReadRange::new(0, 11))
        .expect("read");
    assert_eq!(bytes.as_ref(), b"hello world");
}

#[test]
fn io_bridge_write_batch_applies_each_offset() {
    let runtime = Runtime::new().expect("runtime");
    let handle = runtime.handle().clone();

    let file = MockAsyncFile::with_contents(vec![0u8; 16]);
    let verify = file.contents_handle();
    let mut io_bridge = IoBridge::new(file, handle);

    UniversalWrite::write_batch::<u8>(
        &mut io_bridge,
        [
            (0u64, b"AAA".as_slice()),
            (5u64, b"BBB".as_slice()),
            (10u64, b"CCC".as_slice()),
        ],
    )
    .expect("write_batch");

    let result = verify.lock().unwrap().clone();
    assert_eq!(&result[0..3], b"AAA");
    assert_eq!(&result[5..8], b"BBB");
    assert_eq!(&result[10..13], b"CCC");
}

#[test]
fn io_bridge_flusher_returns_invocable_sync_closure() {
    let runtime = Runtime::new().expect("runtime");
    let handle = runtime.handle().clone();

    let file = MockAsyncFile::with_contents(Vec::new());
    let io_bridge = IoBridge::new(file, handle);

    let flusher: Flusher = io_bridge.flusher();
    flusher().expect("sync flush ok");
}
