#![cfg(test)]

use std::assert_matches;
use std::path::Path;

use common::universal_io::{ReadRange, UniversalIoError, UniversalRead};
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
        .read::<common::generic_consts::Random, u8>(ReadRange::new(16, 16))
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
    file.read_batch::<common::generic_consts::Random, u8, _>(inputs, |user_data, slice| {
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
    for f in &files {
        assert!(f.to_string_lossy().starts_with("listed/"));
    }
}
