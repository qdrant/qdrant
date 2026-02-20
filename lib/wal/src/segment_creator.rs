use crate::{OpenSegment, Segment};
use log::warn;
use std::cmp::max;
#[cfg(not(target_os = "windows"))]
use std::fs::File;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::thread;

pub struct SegmentCreatorV2 {
    /// Where to create segments
    dir: PathBuf,
    /// Segments, which are already created, but not yet handed out.
    pending_segments: Vec<OpenSegment>,

    /// Capacity of segments to create.
    segment_capacity: usize,

    /// How many segments to create ahead of time.
    segment_queue_len: usize,

    /// Current id to create
    current_id: u64,

    /// The segment creator thread.
    /// Used to create segments asynchronously.
    thread: Option<thread::JoinHandle<std::io::Result<Vec<OpenSegment>>>>,
}

fn create_segments(
    path: PathBuf,
    start_id: u64,
    count: usize,
    segment_capacity: usize,
) -> std::io::Result<Vec<OpenSegment>> {
    // Directory being a file only applies to Linux
    #[cfg(not(target_os = "windows"))]
    let dir = File::open(&path)?;

    let mut segments = Vec::with_capacity(count);

    for i in 0..count {
        let segment_id = start_id + i as u64;
        let segment_path = path.join(format!("open-{segment_id}"));
        let segment = OpenSegment {
            id: segment_id,
            segment: Segment::create(segment_path, segment_capacity)?,
        };
        segments.push(segment);
    }

    #[cfg(not(target_os = "windows"))]
    dir.sync_all()?;

    Ok(segments)
}

impl SegmentCreatorV2 {
    /// Creates a new segment creator.
    ///
    /// The segment creator must be started before new segments will be created.
    pub fn new<P>(
        dir: P,
        open_segment: Option<&OpenSegment>,
        mut unused_segments: Vec<OpenSegment>,
        segment_capacity: usize,
        segment_queue_len: usize,
    ) -> SegmentCreatorV2
    where
        P: AsRef<Path>,
    {
        // Any open segment must have lower ID than all unused segments
        debug_assert!(
            unused_segments
                .iter()
                .all(|s| open_segment.as_ref().is_none_or(|o| s.id > o.id)),
            "open_segment must have lower ID than all unused_segments",
        );

        let dir = dir.as_ref().to_path_buf();

        unused_segments.sort_by_key(|segment| segment.id);

        // Find the current id by incrementing either last unused segment or open segment
        // If neither exists, start at 1
        let current_id = unused_segments
            .last()
            .or(open_segment)
            .map_or(1, |s| s.id + 1);

        let mut result = Self {
            dir,
            pending_segments: unused_segments,
            segment_capacity,
            segment_queue_len: max(segment_queue_len, 1), // Always create at least one segment
            current_id,
            thread: None,
        };

        result.schedule_creation();

        result
    }

    fn schedule_creation(&mut self) {
        if self.thread.is_none() && self.pending_segments.len() < self.segment_queue_len {
            let count = self
                .segment_queue_len
                .saturating_sub(self.pending_segments.len());
            let dir = self.dir.clone();
            let start_id = self.current_id;
            let segment_capacity = self.segment_capacity;

            self.thread = Some(
                thread::Builder::new()
                    .name("wal-segment-creator".to_string())
                    .spawn(move || create_segments(dir, start_id, count, segment_capacity))
                    .unwrap(),
            );
        }
    }

    fn refill_pending(&mut self) -> std::io::Result<()> {
        if let Some(thread) = self.thread.take() {
            if self.pending_segments.is_empty() || thread.is_finished() {
                let new_segments = thread
                    .join()
                    .map_err(|_| Error::other("segment creation thread panicked"))??;
                self.current_id += new_segments.len() as u64;
                self.pending_segments.extend(new_segments);
            } else {
                // Put the thread back if it's not finished, and we don't need segments yet.
                self.thread = Some(thread);
            }
        }

        Ok(())
    }

    pub fn next(&mut self) -> std::io::Result<OpenSegment> {
        self.refill_pending()?;

        if self.pending_segments.is_empty() {
            return Err(Error::other("no segments available"));
        }

        let segment = self.pending_segments.remove(0);
        self.schedule_creation();
        Ok(segment)
    }
}

impl Drop for SegmentCreatorV2 {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            match thread.join() {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => {
                    warn!("Error while shutting down segment creator: {err:?}");
                }
                Err(err) => {
                    warn!("Segment creator thread panicked: {err:?}");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::Builder;

    fn init_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn test_segment_creator_v2() {
        init_logger();
        let dir = Builder::new().prefix("segment").tempdir().unwrap();

        let segment = Some(OpenSegment {
            id: 3,
            segment: Segment::create(dir.path().join("open-3"), 1024).unwrap(),
        });

        let unused_segments = vec![OpenSegment {
            id: 4,
            segment: Segment::create(dir.path().join("open-4"), 1024).unwrap(),
        }];

        let mut creator =
            SegmentCreatorV2::new(dir.path(), segment.as_ref(), unused_segments, 1024, 1);
        for i in 4..10 {
            assert_eq!(i, creator.next().unwrap().id);
        }

        // This awaits for the thread to finish
        drop(creator);

        // List directory contents
        let mut entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|res| res.map(|e| e.file_name()))
            .collect::<Result<_, std::io::Error>>()
            .unwrap();

        entries.sort();

        for entry in &entries {
            eprintln!("{:?}", entry);
        }

        assert_eq!(entries.len(), 10 - 3 + 1); // open-3 and open-4 existed, open-5 to open-9 created + open-10 created ahead
    }

    #[test]
    fn test_segment_creator_v2_no_unused() {
        init_logger();
        let dir = Builder::new().prefix("segment").tempdir().unwrap();

        let segment = Some(OpenSegment {
            id: 3,
            segment: Segment::create(dir.path().join("open-3"), 1024).unwrap(),
        });

        let unused_segments = vec![];

        let mut creator =
            SegmentCreatorV2::new(dir.path(), segment.as_ref(), unused_segments, 1024, 1);
        for i in 4..10 {
            let another_segment = creator.next().unwrap();
            eprintln!("another_segment = {:#?}", another_segment);
            assert_eq!(i, another_segment.id);
        }

        // This awaits for the thread to finish
        drop(creator);

        // List directory contents
        let mut entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|res| res.map(|e| e.file_name()))
            .collect::<Result<_, std::io::Error>>()
            .unwrap();

        entries.sort();

        for entry in &entries {
            eprintln!("{:?}", entry);
        }

        assert_eq!(entries.len(), 10 - 3 + 1); // open-3 existed, open-4 to open-9 created + open-10 created ahead
    }

    #[test]
    #[should_panic(expected = "open_segment must have lower ID than all unused_segments")]
    fn test_segment_creator_v2_invalid_open() {
        init_logger();
        let dir = Builder::new().prefix("segment").tempdir().unwrap();

        let segment = Some(OpenSegment {
            id: 4,
            segment: Segment::create(dir.path().join("open-3"), 1024).unwrap(),
        });

        let unused_segments = vec![OpenSegment {
            id: 4,
            segment: Segment::create(dir.path().join("open-4"), 1024).unwrap(),
        }];

        // Expected to panic, open segment cannot have same or higher ID than unused segments
        SegmentCreatorV2::new(dir.path(), segment.as_ref(), unused_segments, 1024, 1);
    }
}
