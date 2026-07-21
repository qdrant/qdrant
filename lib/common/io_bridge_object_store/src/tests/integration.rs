#![cfg(test)]

use std::assert_matches;
use std::path::Path;

use common::generic_consts::Random;
use common::universal_io::{
    ListedFile, ReadRange, UniversalAppend, UniversalIoError, UniversalRead,
};
use io_bridge::{AsyncRead, BridgeRuntime};
use object_store::aws::AmazonS3;

use crate::tests::rustfs::{rustfs_aws_config, rustfs_enabled, setup_bucket};
use crate::{BlobFile, ObjectStoreSource};

fn maybe_skip() -> bool {
    if !rustfs_enabled() {
        eprintln!("skipping rustfs integration test; set S3_INTEGRATION_TEST=1 to enable");
        true
    } else {
        false
    }
}

/// The native append RPC needs a store implementing the write-offset
/// `PutObject` API (S3 Express One Zone / MinIO AiStor); RustFS and plain S3
/// buckets reject it, hence the separate opt-in.
fn maybe_skip_append() -> bool {
    if std::env::var("S3_APPEND_INTEGRATION_TEST").as_deref() != Ok("1") {
        eprintln!(
            "skipping append integration test; set S3_APPEND_INTEGRATION_TEST=1 (and the \
             RUSTFS_* endpoint vars) to run against an append-capable store"
        );
        true
    } else {
        false
    }
}

#[test]
#[ignore]
fn test_open_and_read_whole() {
    if maybe_skip() {
        return;
    }
    let runtime = BridgeRuntime::global();
    setup_bucket(&runtime, &[("hello.bin", b"hello rustfs")]);

    let file =
        BlobFile::<ObjectStoreSource<AmazonS3>>::open(&rustfs_aws_config(), runtime, "hello.bin")
            .expect("open");
    let bytes = file.read_whole::<u8>().expect("read_whole");
    assert_eq!(&bytes[..], b"hello rustfs");
}

#[test]
#[ignore]
fn test_read_range() {
    if maybe_skip() {
        return;
    }
    let runtime = BridgeRuntime::global();
    setup_bucket(
        &runtime,
        &[("ranged.bin", &(0u8..=63u8).collect::<Vec<u8>>())],
    );

    let file =
        BlobFile::<ObjectStoreSource<AmazonS3>>::open(&rustfs_aws_config(), runtime, "ranged.bin")
            .expect("open");
    let bytes = file
        .read::<Random, u8>(ReadRange::new(16, 16))
        .expect("read");
    assert_eq!(bytes.len(), 16);
    assert_eq!(bytes[0], 16);
    assert_eq!(bytes[15], 31);
}

#[test]
#[ignore]
fn test_read_batch_parallel() {
    if maybe_skip() {
        return;
    }
    let runtime = BridgeRuntime::global();
    setup_bucket(&runtime, &[("blob", &(0u8..=255u8).collect::<Vec<u8>>())]);

    let file = BlobFile::<ObjectStoreSource<AmazonS3>>::open(&rustfs_aws_config(), runtime, "blob")
        .expect("open");
    let inputs: Vec<(u32, ReadRange)> = (0u32..16)
        .map(|i| (i, ReadRange::new(u64::from(i) * 16, 16)))
        .collect();
    let mut got: std::collections::HashMap<u32, Vec<u8>> = Default::default();
    file.read_batch::<Random, u8, _>(inputs, |user_data, slice| {
        got.insert(user_data, slice.to_vec());
        Ok(())
    })
    .expect("read_batch");
    assert_eq!(got.len(), 16);
    for i in 0u32..16 {
        let chunk = &got[&i];
        assert_eq!(chunk.len(), 16);
        assert_eq!(chunk[0], (i * 16) as u8);
    }
}

#[test]
#[ignore]
fn test_not_found() {
    if maybe_skip() {
        return;
    }
    let runtime = BridgeRuntime::global();
    let _ = setup_bucket(&runtime, &[]);

    // `open` no longer touches the network, so the missing object only surfaces
    // when we actually read it (the `len` HEAD inside `read_whole`).
    let file = BlobFile::<ObjectStoreSource<AmazonS3>>::open(
        &rustfs_aws_config(),
        runtime,
        "does-not-exist",
    )
    .expect("open builds the store without IO");
    let err = file.read_whole::<u8>().unwrap_err();
    assert_matches!(err, UniversalIoError::NotFound { .. });
}

/// End-to-end native append flow against a real append-capable store:
/// create-on-first-append at offset 0, sequential appends, read-back, and a
/// stale-offset conflict that writes nothing, recovered by re-deriving the
/// offset from the actual length.
#[test]
#[ignore]
fn test_native_append_flow() {
    if maybe_skip_append() {
        return;
    }
    let runtime = BridgeRuntime::global();
    let _ = setup_bucket(&runtime, &[]);

    // Fresh key per run so reruns do not collide with leftover objects.
    let key = format!("append-{}.log", std::process::id());

    let mut file = BlobFile::<ObjectStoreSource<AmazonS3>>::open(
        &rustfs_aws_config(),
        runtime.clone(),
        key.as_str(),
    )
    .expect("open");
    file.append(0, b"hello ".as_slice())
        .expect("first append creates the object");
    file.append(6, b"world".as_slice()).expect("append");

    let bytes = file.read_whole::<u8>().expect("read_whole");
    assert_eq!(&bytes[..], b"hello world");

    // A second handle appends behind this handle's back...
    let mut interloper =
        BlobFile::<ObjectStoreSource<AmazonS3>>::open(&rustfs_aws_config(), runtime, key.as_str())
            .expect("open");
    interloper.append(11, b"A".as_slice()).expect("append");

    // ...so an append at the stale offset conflicts without writing, and
    // re-deriving the offset from the actual length recovers.
    let err = file.append(11, b"B".as_slice()).unwrap_err();
    assert_matches!(err, UniversalIoError::AppendOffsetConflict { .. });
    let eof = file.len::<u8>().expect("len");
    assert_eq!(eof, 12);
    file.append(eof, b"B".as_slice()).expect("append");

    let bytes = file.read_whole::<u8>().expect("read_whole");
    assert_eq!(&bytes[..], b"hello worldAB");
}

#[test]
#[ignore]
fn test_list_files() {
    if maybe_skip() {
        return;
    }
    let runtime = BridgeRuntime::global();
    setup_bucket(
        &runtime,
        &[
            ("listed/a", b"x"),
            ("listed/b", b"x"),
            ("listed/c", b"x"),
            ("other/z", b"x"),
        ],
    );

    let store = <ObjectStoreSource<AmazonS3> as AsyncRead>::open(&rustfs_aws_config())
        .expect("build store");
    let files = runtime
        .block_on(store.list_files(Path::new("listed")))
        .expect("list_files");
    assert_eq!(files.len(), 3);
    for ListedFile {
        path,
        size,
        last_modified: _,
    } in &files
    {
        assert!(path.to_string_lossy().starts_with("listed/"));
        assert_eq!(*size, 1);
    }
}
