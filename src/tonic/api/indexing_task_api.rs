//! `qdrant.IndexingTask`: serves remote indexing tasks to an indexing service (§4.5).
//!
//! Authorization is the one-time task token in the `x-qdrant-indexing-token` header, checked
//! here, so it holds whether or not the internal port has an auth layer. The auth layer lets
//! these paths through without an API key, which the indexer does not have.

use std::pin::Pin;

use api::grpc::qdrant::indexing_task_server::IndexingTask;
use api::grpc::qdrant::{
    FailIndexingTaskRequest, FailIndexingTaskResponse, GetIndexingTaskRequest, IndexingDeletedMask,
    IndexingFileChunk, IndexingHeartbeatRequest, IndexingHeartbeatResponse,
    IndexingTaskDescription, IndexingVectorsChunk, ReadIndexingDeletedRequest,
    ReadIndexingFileRequest, ReadIndexingVectorsRequest, UploadIndexingGraphRequest,
    UploadIndexingGraphResponse,
};
use api::grpc::remote_indexing::{
    GraphAssembler, INDEXING_TOKEN_HEADER, deleted_to_grpc, heartbeat_action_to_grpc,
    heartbeat_from_grpc, task_description_to_grpc,
};
use futures::Stream;
use segment::index::hnsw_index::remote::registry::{
    READ_CHUNK_BYTES, REMOTE_INDEXING_TASKS, RemoteTaskError, TaskToken,
};
use segment::index::hnsw_index::remote::wire::SUPPORTED_GRAPH_FORMAT_VERSIONS;
use tonic::{Request, Response, Status, Streaming, async_trait};

/// Upper bound on links per point in an upload, before validation: generous, validation
/// applies the exact limit.
const MAX_UPLOAD_LINKS_PER_POINT: usize = 4096;

#[derive(Default)]
pub struct IndexingTaskService;

fn token<T>(request: &Request<T>) -> Result<TaskToken, Status> {
    request
        .metadata()
        .get(INDEXING_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(TaskToken::from_hex)
        .ok_or_else(|| Status::permission_denied("invalid or revoked indexing task token"))
}

fn status(err: RemoteTaskError) -> Status {
    match err {
        RemoteTaskError::PermissionDenied => Status::permission_denied(err.to_string()),
        RemoteTaskError::InvalidArgument(msg) => Status::invalid_argument(msg),
        RemoteTaskError::Internal(msg) => Status::internal(msg),
    }
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, RemoteTaskError> + Send + 'static,
) -> Result<T, Status> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|err| Status::internal(format!("task panicked: {err}")))?
        .map_err(status)
}

type VectorsStream = Pin<Box<dyn Stream<Item = Result<IndexingVectorsChunk, Status>> + Send>>;
type FileStream = Pin<Box<dyn Stream<Item = Result<IndexingFileChunk, Status>> + Send>>;

fn receiver_stream<T: Send + 'static>(
    receiver: tokio::sync::mpsc::Receiver<Result<T, Status>>,
) -> Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>> {
    Box::pin(futures::stream::unfold(
        receiver,
        |mut receiver| async move { receiver.recv().await.map(|item| (item, receiver)) },
    ))
}

#[async_trait]
impl IndexingTask for IndexingTaskService {
    async fn get_task(
        &self,
        request: Request<GetIndexingTaskRequest>,
    ) -> Result<Response<IndexingTaskDescription>, Status> {
        let token = token(&request)?;
        let description = REMOTE_INDEXING_TASKS.get_task(&token).map_err(status)?;
        let response = task_description_to_grpc(&description, SUPPORTED_GRAPH_FORMAT_VERSIONS)
            .map_err(Status::internal)?;
        Ok(Response::new(response))
    }

    type ReadVectorsStream = VectorsStream;

    async fn read_vectors(
        &self,
        request: Request<ReadIndexingVectorsRequest>,
    ) -> Result<Response<Self::ReadVectorsStream>, Status> {
        let token = token(&request)?;
        let ReadIndexingVectorsRequest { start, end } = request.into_inner();
        // Fail fast on a bad token, before streaming
        let (first, next) =
            blocking(move || REMOTE_INDEXING_TASKS.read_vectors(&token, start, end)).await?;

        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        tokio::task::spawn_blocking(move || {
            let mut chunk = Some((start, first, next));
            while let Some((chunk_start, vectors, next)) = chunk.take() {
                let done = vectors.is_empty() || next >= end;
                let message = IndexingVectorsChunk {
                    start: chunk_start,
                    vectors,
                };
                if sender.blocking_send(Ok(message)).is_err() {
                    // The indexer went away
                    return;
                }
                if done {
                    return;
                }
                match REMOTE_INDEXING_TASKS.read_vectors(&token, next, end) {
                    Ok((vectors, after)) => chunk = Some((next, vectors, after)),
                    Err(err) => {
                        let _ = sender.blocking_send(Err(status(err)));
                        return;
                    }
                }
            }
        });
        Ok(Response::new(receiver_stream(receiver)))
    }

    type ReadFileStream = FileStream;

    async fn read_file(
        &self,
        request: Request<ReadIndexingFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        let token = token(&request)?;
        let ReadIndexingFileRequest { name } = request.into_inner();
        // Fail fast on a bad token or file, before streaming
        let first = {
            let name = name.clone();
            blocking(move || REMOTE_INDEXING_TASKS.read_file(&token, &name, 0, READ_CHUNK_BYTES))
                .await?
        };

        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        tokio::task::spawn_blocking(move || {
            let mut offset = 0u64;
            let mut data = first;
            while !data.is_empty() {
                let len = data.len() as u64;
                if sender
                    .blocking_send(Ok(IndexingFileChunk { offset, data }))
                    .is_err()
                {
                    return;
                }
                offset += len;
                match REMOTE_INDEXING_TASKS.read_file(&token, &name, offset, READ_CHUNK_BYTES) {
                    Ok(next) => data = next,
                    Err(err) => {
                        let _ = sender.blocking_send(Err(status(err)));
                        return;
                    }
                }
            }
        });
        Ok(Response::new(receiver_stream(receiver)))
    }

    async fn read_deleted(
        &self,
        request: Request<ReadIndexingDeletedRequest>,
    ) -> Result<Response<IndexingDeletedMask>, Status> {
        let token = token(&request)?;
        let deleted = REMOTE_INDEXING_TASKS.read_deleted(&token).map_err(status)?;
        Ok(Response::new(deleted_to_grpc(&deleted)))
    }

    async fn heartbeat(
        &self,
        request: Request<IndexingHeartbeatRequest>,
    ) -> Result<Response<IndexingHeartbeatResponse>, Status> {
        let token = token(&request)?;
        let progress = heartbeat_from_grpc(request.into_inner());
        let action = REMOTE_INDEXING_TASKS
            .heartbeat(&token, progress)
            .map_err(status)?;
        Ok(Response::new(heartbeat_action_to_grpc(action)))
    }

    async fn upload_graph(
        &self,
        request: Request<Streaming<UploadIndexingGraphRequest>>,
    ) -> Result<Response<UploadIndexingGraphResponse>, Status> {
        let token = token(&request)?;
        // Bound the upload by the task, so a token cannot be used to exhaust memory
        let description = REMOTE_INDEXING_TASKS.get_task(&token).map_err(status)?;
        let mut assembler =
            GraphAssembler::new(description.params.point_count, MAX_UPLOAD_LINKS_PER_POINT);
        let mut stream = request.into_inner();
        while let Some(message) = stream.message().await? {
            assembler.push(message).map_err(Status::invalid_argument)?;
        }
        let graph = assembler.finish().map_err(Status::invalid_argument)?;
        // Validation walks the whole graph
        blocking(move || REMOTE_INDEXING_TASKS.upload(&token, graph)).await?;
        Ok(Response::new(UploadIndexingGraphResponse {}))
    }

    async fn fail(
        &self,
        request: Request<FailIndexingTaskRequest>,
    ) -> Result<Response<FailIndexingTaskResponse>, Status> {
        let token = token(&request)?;
        let FailIndexingTaskRequest { reason } = request.into_inner();
        REMOTE_INDEXING_TASKS.fail(&token, reason).map_err(status)?;
        Ok(Response::new(FailIndexingTaskResponse {}))
    }
}
