use std::path::Path;

use aws_sdk_s3::{
    operation::{
        complete_multipart_upload::CompleteMultipartUploadOutput,
        create_multipart_upload::CreateMultipartUploadOutput,
    },
    types::{CompletedMultipartUpload, CompletedPart},
};
use aws_smithy_types::byte_stream::{ByteStream, Length};
use aws_smithy_types_convert::date_time::DateTimeExt;

use super::{snapshot_ops::SnapshotDescription, types::CollectionResult};

pub fn get_key(path: &Path) -> Option<String> {
    let key = path
        .to_str()
        .expect("path is invalid")
        .trim_start_matches("./")
        .to_string();
    Some(key)
}

pub async fn multi_part_upload(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
    source_path: &str,
) -> CompleteMultipartUploadOutput {
    const CHUNK_SIZE: u64 = 1024 * 1024 * 5;
    const MAX_CHUNKS: u64 = 10000;
    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = multipart_upload_res.upload_id().unwrap();
    let file_size = tokio::fs::metadata(source_path)
        .await
        .expect("it exists I swear")
        .len();

    let mut chunk_count = (file_size / CHUNK_SIZE) + 1;
    let mut size_of_last_chunk = file_size % CHUNK_SIZE;
    if size_of_last_chunk == 0 {
        size_of_last_chunk = CHUNK_SIZE;
        chunk_count -= 1;
    }

    if file_size == 0 {
        panic!("Bad file size.");
    }
    if chunk_count > MAX_CHUNKS {
        panic!("Too many chunks! Try increasing your chunk size.")
    }

    let mut upload_parts: Vec<CompletedPart> = Vec::new();
    for chunk_index in 0..chunk_count {
        let this_chunk = if chunk_count - 1 == chunk_index {
            size_of_last_chunk
        } else {
            CHUNK_SIZE
        };
        let stream = ByteStream::read_from()
            .path(source_path)
            .offset(chunk_index * CHUNK_SIZE)
            .length(Length::Exact(this_chunk))
            .build()
            .await
            .unwrap();
        //Chunk index needs to start at 0, but part numbers start at 1.
        let part_number = (chunk_index as i32) + 1;
        let upload_part_res = client
            .upload_part()
            .key(key)
            .bucket(bucket_name)
            .upload_id(upload_id)
            .body(stream)
            .part_number(part_number)
            .send()
            .await
            .unwrap();
        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );
    }
    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket_name)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await
        .unwrap()
}

pub async fn get_snapshot_description(
    client: &aws_sdk_s3::Client,
    bucket_name: String,
    key: String,
) -> CollectionResult<SnapshotDescription> {
    let file_meta = client
        .head_object()
        .bucket(bucket_name)
        .key(&key)
        .send()
        .await
        .unwrap();
    let creation_time = file_meta
        .last_modified
        .map(|t| t.to_chrono_utc().unwrap().naive_utc());
    let checksum = file_meta.checksum_sha256.clone();
    let size = file_meta.content_length().unwrap() as u64;
    Ok(SnapshotDescription {
        name: key,
        creation_time,
        size,
        checksum,
    })
}

pub async fn list_snapshots_in_bucket_with_key(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
) -> CollectionResult<Vec<SnapshotDescription>> {
    let entries = client
        .list_objects_v2()
        .bucket(bucket_name)
        .prefix(key)
        .send()
        .await
        .unwrap()
        .contents
        .unwrap();

    let snapshot_futures: Vec<_> = entries
        .into_iter()
        .filter_map(|entry| {
            entry
                .key
                .map(|key| get_snapshot_description(client, bucket_name.to_string(), key.clone()))
        })
        .collect();

    let snapshots = futures::future::join_all(snapshot_futures).await;
    Ok(snapshots.into_iter().collect::<Result<_, _>>().unwrap())
}

pub async fn delete_snapshot(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
) -> CollectionResult<bool> {
    println!("Deleting snapshot: {}", key);
    client
        .delete_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    Ok(true)
}
