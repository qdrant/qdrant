use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use api::grpc::qdrant::shard_storage_read_server::ShardStorageRead;
use api::grpc::qdrant::{
    FileExistsResponse, FileLengthResponse, ListFilesResponse, ReadBatchResponse,
    ReadBytesResponse, ReadBytesStreamResponse, ReadMultiResponse, ReadWholeResponse,
    ShardFileExistsRequest, ShardFileLengthRequest, ShardListFilesRequest, ShardReadBatchRequest,
    ShardReadBytesRequest, ShardReadBytesStreamRequest, ShardReadMultiRequest,
    ShardReadWholeRequest,
};
use common::universal_io::mmap::MmapFile;
use common::universal_io::{ReadRange, UniversalRead};
use futures::{Stream, StreamExt};
use storage::content_manager::toc::TableOfContent;
use tonic::{Request, Response, Status, async_trait};

use crate::tonic::api::{storage_read_common as common_ops, validate};
use crate::tonic::auth::extract_auth;

mod helpers;
#[cfg(test)]
mod tests;

pub struct ShardStorageReadService<S: UniversalRead<u8> + Send + Sync + 'static = MmapFile> {
    toc: Arc<TableOfContent>,
    _marker: PhantomData<S>,
}

#[async_trait]
impl<S: UniversalRead<u8> + Send + Sync + 'static> ShardStorageRead for ShardStorageReadService<S> {
    async fn file_exists(
        &self,
        mut request: Request<ShardFileExistsRequest>,
    ) -> Result<Response<FileExistsResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ShardFileExistsRequest {
            collection_name,
            shard_id,
            path,
        } = request.into_inner();
        let (base, safe_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "file_exists")
            .await?;
        let path = common_ops::resolve_path(&base, &safe_root, &path)?;

        let exists = common_ops::file_exists::<S>(path).await?;
        Ok(Response::new(FileExistsResponse { exists }))
    }

    async fn list_files(
        &self,
        mut request: Request<ShardListFilesRequest>,
    ) -> Result<Response<ListFilesResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ShardListFilesRequest {
            collection_name,
            shard_id,
            prefix_path,
        } = request.into_inner();
        let (base, safe_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "list_files")
            .await?;
        let prefix_path = common_ops::resolve_path(&base, &safe_root, &prefix_path)?;

        let paths = common_ops::list_files::<S>(base, prefix_path).await?;
        Ok(Response::new(ListFilesResponse { paths }))
    }

    async fn file_length(
        &self,
        mut request: Request<ShardFileLengthRequest>,
    ) -> Result<Response<FileLengthResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ShardFileLengthRequest {
            collection_name,
            shard_id,
            path,
        } = request.into_inner();
        let (base, safe_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "file_length")
            .await?;
        let path = common_ops::resolve_path(&base, &safe_root, &path)?;

        let length = common_ops::file_length::<S>(path).await?;
        Ok(Response::new(FileLengthResponse { length }))
    }

    async fn read_bytes(
        &self,
        mut request: Request<ShardReadBytesRequest>,
    ) -> Result<Response<ReadBytesResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ShardReadBytesRequest {
            collection_name,
            shard_id,
            path,
            byte_offset,
            length,
        } = request.into_inner();

        let (base, safe_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "read_bytes")
            .await?;
        let path = common_ops::resolve_path(&base, &safe_root, &path)?;
        let data = common_ops::read_bytes::<S>(path, ReadRange::new(byte_offset, length)).await?;

        Ok(Response::new(ReadBytesResponse { data }))
    }

    type ReadBytesStreamStream =
        Pin<Box<dyn Stream<Item = Result<ReadBytesStreamResponse, Status>> + Send>>;

    async fn read_bytes_stream(
        &self,
        mut request: Request<ShardReadBytesStreamRequest>,
    ) -> Result<Response<Self::ReadBytesStreamStream>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ShardReadBytesStreamRequest {
            collection_name,
            shard_id,
            path,
            byte_offset,
            length,
        } = request.into_inner();

        let (base, safe_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "read_bytes_stream")
            .await?;
        let path = common_ops::resolve_path(&base, &safe_root, &path)?;
        let stream =
            common_ops::read_bytes_stream::<S>(path, ReadRange::new(byte_offset, length)).await?;

        let mapped = stream.map(|chunk| chunk.map(|data| ReadBytesStreamResponse { data }));
        Ok(Response::new(Box::pin(mapped)))
    }

    async fn read_whole(
        &self,
        mut request: Request<ShardReadWholeRequest>,
    ) -> Result<Response<ReadWholeResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ShardReadWholeRequest {
            collection_name,
            shard_id,
            path,
        } = request.into_inner();
        let (base, safe_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "read_whole")
            .await?;
        let path = common_ops::resolve_path(&base, &safe_root, &path)?;

        let data = common_ops::read_whole::<S>(path).await?;
        Ok(Response::new(ReadWholeResponse { data }))
    }

    async fn read_batch(
        &self,
        mut request: Request<ShardReadBatchRequest>,
    ) -> Result<Response<ReadBatchResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ShardReadBatchRequest {
            collection_name,
            shard_id,
            path,
            ranges,
        } = request.into_inner();
        let (base, safe_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "read_batch")
            .await?;
        let path = common_ops::resolve_path(&base, &safe_root, &path)?;

        let ranges = ranges
            .iter()
            .map(|r| ReadRange::new(r.byte_offset, r.length))
            .collect::<Vec<_>>();

        let data = common_ops::read_batch::<S>(path, ranges).await?;
        Ok(Response::new(ReadBatchResponse { data }))
    }

    async fn read_multi(
        &self,
        mut request: Request<ShardReadMultiRequest>,
    ) -> Result<Response<ReadMultiResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ShardReadMultiRequest {
            collection_name,
            shard_id,
            reads,
        } = request.into_inner();
        let (base, safe_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "read_multi")
            .await?;

        let resolved = reads
            .iter()
            .map(|entry| {
                let path = common_ops::resolve_path(&base, &safe_root, &entry.path)?;
                Ok::<(PathBuf, ReadRange), Status>((
                    path,
                    ReadRange::new(entry.byte_offset, entry.length),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let data = common_ops::read_multi::<S>(resolved).await?;
        Ok(Response::new(ReadMultiResponse { data }))
    }
}
