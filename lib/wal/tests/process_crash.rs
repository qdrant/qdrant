//! Simulates a process crash while writing to a write ahead log segment.
//!
//! Steps:
//!
//! 1. The test process creates a seed and a temporary directory
//! 2. The test process creates a subprocess with the seed and directory as
//!    arguments.
//! 3. The subprocess creates a segment in the directory, and appends entries to
//!    the segment based on the seed.
//! 4. The subprocess aborts without closing resources or cleaning up state.
//! 5. The test process reads the entries from the segment, ensuring that no
//!    corruption or data loss has occurred.

use std::collections::HashMap;
use std::env;
use std::process;

use rand::RngCore;
use tempfile::Builder;
use wal::test_utils::EntryGenerator;

const SEGMENT_CAPACITY: usize = 32 * 1024 * 1024;
const EXIT_CODE: i32 = 42;

#[test]
fn process_crash() {
    let vars: HashMap<String, String> = env::vars().collect();
    if vars.contains_key("SEED") && vars.contains_key("SEGMENT_PATH") {
        let seed: usize = vars["SEED"].parse().unwrap();
        let path = vars["SEGMENT_PATH"].clone();
        subprocess(path, seed);
    } else {
        test()
    }
}

fn test() {
    let tempdir = Builder::new().prefix("segment").tempdir().unwrap();
    let seed: usize = rand::rng().next_u32() as usize;
    let path = tempdir.path().join("segment");

    println!("Spawning subprocess; path: {path:?}; seed: {seed}");

    let exit_code = process::Command::new(env::current_exe().unwrap())
        .env("SEED", format!("{seed}"))
        .env("SEGMENT_PATH", format!("{}", path.display()))
        .status()
        .unwrap()
        .code()
        .unwrap();

    assert_eq!(EXIT_CODE, exit_code);

    let segment = wal::Segment::open(path).unwrap();
    let entries = EntryGenerator::with_seed_and_segment_capacity(seed, SEGMENT_CAPACITY);

    for (i, entry) in entries.into_iter().enumerate() {
        assert_eq!(&entry[..], &*segment.entry(i).unwrap());
    }
}

fn subprocess(path: String, seed: usize) {
    let mut segment = wal::Segment::create(path, SEGMENT_CAPACITY).unwrap();

    for entry in EntryGenerator::with_seed_and_segment_capacity(seed, SEGMENT_CAPACITY) {
        segment.append(&entry).expect("segment capacity exhausted");
    }

    process::exit(EXIT_CODE);
}
