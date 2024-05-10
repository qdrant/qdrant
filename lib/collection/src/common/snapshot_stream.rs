use std::path::PathBuf;

use actix_files::NamedFile;
use actix_web::{HttpRequest, HttpResponse, Responder};
use futures::Stream;

pub struct SnapShotStreamLocalFS {
    pub snapshot_path: PathBuf,
    pub req: HttpRequest,
}
pub struct SnapShotStreamCloudStrage {
    pub streamer:
        std::pin::Pin<Box<dyn Stream<Item = Result<bytes::Bytes, object_store::Error>> + Send>>,
}

pub enum SnapshotStream {
    LocalFS(SnapShotStreamLocalFS),
    CloudStorage(SnapShotStreamCloudStrage),
}

impl Responder for SnapshotStream {
    type Body = actix_web::body::BoxBody;

    fn respond_to(self, _: &actix_web::HttpRequest) -> HttpResponse<Self::Body> {
        match self {
            SnapshotStream::LocalFS(stream) => {
                let file = NamedFile::open(stream.snapshot_path).unwrap();
                file.into_response(&stream.req)
            }

            SnapshotStream::CloudStorage(stream) => HttpResponse::Ok()
                .content_type("application/octet-stream")
                .streaming(stream.streamer),
        }
    }
}
