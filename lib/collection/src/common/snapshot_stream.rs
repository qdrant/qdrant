use std::error::Error;
use std::path::PathBuf;
use std::pin::Pin;

use actix_files::NamedFile;
use actix_web::http::header::ContentDisposition;
use actix_web::{HttpResponse, Responder};
use bytes::Bytes;
use futures::{Stream, TryStreamExt};

pub struct SnapShotStreamLocalFS {
    pub snapshot_path: PathBuf,
}

type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, Box<dyn Error>>>>>;

pub struct SnapShotStreamCloudStrage {
    stream: ByteStream,
    filename: Option<String>,
}

pub enum SnapshotStream {
    LocalFS(SnapShotStreamLocalFS),
    ByteStream(SnapShotStreamCloudStrage),
}

impl SnapshotStream {
    /// Create a new snapshot stream from a byte stream.
    ///
    /// The `filename` is used as the `Content-Disposition` header and only
    /// makes sense when the snapshot needs to be saved under a different
    /// name than the endpoint path.
    pub fn new_stream<S, E>(stream: S, filename: Option<String>) -> Self
    where
        S: Stream<Item = Result<Bytes, E>> + 'static,
        E: Into<Box<dyn Error>>,
    {
        SnapshotStream::ByteStream(SnapShotStreamCloudStrage {
            stream: Box::pin(stream.map_err(|e| e.into())),
            filename,
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

            SnapshotStream::ByteStream(SnapShotStreamCloudStrage { stream, filename }) => {
                let mut response = HttpResponse::Ok();
                response.content_type("application/octet-stream");
                if let Some(filename) = filename {
                    response.insert_header(ContentDisposition::attachment(filename));
                }
                response.streaming(stream)
            }
        }
    }
}
