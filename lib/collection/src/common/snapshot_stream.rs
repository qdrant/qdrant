use std::error::Error;
use std::path::PathBuf;
use std::pin::Pin;

use actix_files::NamedFile;
use actix_web::{HttpResponse, Responder};
use bytes::Bytes;
use futures::{Stream, TryStreamExt};

pub struct SnapShotStreamLocalFS {
    pub snapshot_path: PathBuf,
}

type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, Box<dyn Error>>>>>;

pub struct SnapShotStreamCloudStrage {
    stream: ByteStream,
}

pub enum SnapshotStream {
    LocalFS(SnapShotStreamLocalFS),
    ByteStream(SnapShotStreamCloudStrage),
}

impl SnapshotStream {
    pub fn new_stream<S, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes, E>> + 'static,
        E: Into<Box<dyn Error>>,
    {
        SnapshotStream::ByteStream(SnapShotStreamCloudStrage {
            stream: Box::pin(stream.map_err(|e| e.into())),
        })
    }
}

impl Responder for SnapshotStream {
    type Body = actix_web::body::BoxBody;

    fn respond_to(self, req: &actix_web::HttpRequest) -> HttpResponse<Self::Body> {
        match self {
            SnapshotStream::LocalFS(stream) => match NamedFile::open(stream.snapshot_path) {
                Ok(file) => file.into_response(req),
                Err(e) => match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        HttpResponse::NotFound().body(format!("File not found: {e}"))
                    }
                    _ => HttpResponse::InternalServerError()
                        .body(format!("Failed to open file: {e}")),
                },
            },

            SnapshotStream::ByteStream(stream) => HttpResponse::Ok()
                .content_type("application/octet-stream")
                .streaming(stream.stream),
        }
    }
}
