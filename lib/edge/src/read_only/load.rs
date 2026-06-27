use std::path::PathBuf;
use std::thread::{self, JoinHandle};

use segment::common::operation_error::{OperationError, OperationResult};
use segment::index::UniversalReadExt;
use segment::segment::read_only::ReadOnlySegment;
use uuid::Uuid;

type SegmentLoadHandle<S> = JoinHandle<OperationResult<(Uuid, ReadOnlySegment<S>)>>;

/// Load each segment in a dedicated thread and return the opened segments.
pub(crate) fn load_segments_parallel<S>(
    fs: &S::Fs,
    segments: impl IntoIterator<Item = (Uuid, PathBuf)>,
) -> OperationResult<Vec<(Uuid, ReadOnlySegment<S>)>>
where
    S: UniversalReadExt + 'static,
    S::Fs: Send + Sync + Clone + 'static,
{
    let handles: Vec<SegmentLoadHandle<S>> = segments
        .into_iter()
        .map(|(uuid, segment_path)| {
            let fs = fs.clone();
            thread::spawn(move || {
                let segment = ReadOnlySegment::<S>::open(&fs, &segment_path, uuid, None)?;
                Ok((uuid, segment))
            })
        })
        .collect();

    let mut loaded = Vec::with_capacity(handles.len());
    for handle in handles {
        loaded.push(
            handle
                .join()
                .map_err(|_| OperationError::service_error("segment load thread panicked"))??,
        );
    }
    Ok(loaded)
}
