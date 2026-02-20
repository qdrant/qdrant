use crate::test_utils::EntryGenerator;
use crate::{Wal, WalOptions};
use std::num::NonZeroUsize;
use tempfile::Builder;

#[test]
fn test_handling_missing_empty_segment() {
    let entry_count = 5;

    let dir = Builder::new().prefix("wal").tempdir().unwrap();
    let options = WalOptions {
        segment_capacity: 1024,
        segment_queue_len: 0,
        retain_closed: NonZeroUsize::new(1).unwrap(),
    };

    let mut wal = Wal::with_options(dir.path(), &options).unwrap();
    let entries = EntryGenerator::new().take(entry_count).collect::<Vec<_>>();

    for entry in &entries {
        wal.append(entry).unwrap();
    }

    wal.flush_open_segment().unwrap();

    drop(wal);

    eprintln!("------------ first WAL created ------------");

    let wal = Wal::open(dir.path()).unwrap();

    let num = wal.num_entries();

    assert_eq!(num, entry_count as u64);

    drop(wal);

    eprintln!("------------ WAL opened and closed ------------");

    // List directory contents

    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|res| res.map(|e| e.file_name()))
        .collect::<Result<_, std::io::Error>>()
        .unwrap();

    for entry in &entries {
        eprintln!("{:?}", entry);
    }

    // Remove the last segment file (which is empty)
    assert!(
        !dir.path().join("open-3").exists(),
        "open-3 should not exist",
    );
    let last_segment_file = dir.path().join("open-2");
    std::fs::remove_file(last_segment_file).unwrap();

    eprintln!("------------ removed last segment ------------");

    let wal = Wal::open(dir.path()).unwrap();

    let num = wal.num_entries();

    assert_eq!(num, entry_count as u64);

    // sleep, to account for background process creating more segments
    std::thread::sleep(std::time::Duration::from_millis(100));

    drop(wal);

    eprintln!("------------ WAL opened and closed again ------------");

    let wal = Wal::open(dir.path()).unwrap();

    let num = wal.num_entries();

    assert_eq!(num, entry_count as u64);
}
