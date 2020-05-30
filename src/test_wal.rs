extern crate wal;

use std::env;
use wal::Entry;
use wal::Wal;
use wal::WalOptions;

pub fn write_wal() {
  let path = env::args()
    .skip(1)
    .next()
    .unwrap_or("./wal_log".to_string());
  let options = WalOptions {
    segment_capacity: 1024,
    segment_queue_len: 0,
  };

  let mut wal = Wal::with_options(&path, &options).unwrap();

  for i in 0..10 {
    let entry: &[u8] = &[42u8 + i; 200];
    wal.append(&entry).unwrap();
  }
}

pub fn truncate_wal() {
  let path = env::args()
    .skip(1)
    .next()
    .unwrap_or("./wal_log".to_string());

  let options = WalOptions {
    segment_capacity: 1024,
    segment_queue_len: 0,
  };

  let mut wal = Wal::with_options(&path, &options).unwrap();

  wal.prefix_truncate(6).unwrap();

}

pub fn read_wal() {
  let path = env::args()
    .skip(1)
    .next()
    .unwrap_or("./wal_log".to_string());

  let options = WalOptions {
    segment_capacity: 1024,
    segment_queue_len: 0,
  };

  let wal = Wal::with_options(&path, &options).unwrap();

  let first_index = wal.first_index();
  let num_entries = wal.num_entries();



  println!(
    "WAL with {} entities starting from {}",
    num_entries, first_index
  );

  for i in first_index..(first_index + num_entries) {
    match wal.entry(i) {
      Some(ref entry) => println!(
        "entry for {} of size {} with elem {}",
        i,
        entry.len(),
        entry[0]
      ),
      None => print!("No entry {}", i),
    }
  }
}
