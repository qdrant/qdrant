use std::env;

use wal::Wal;

fn main() {
    env_logger::init();
    let path = env::args().nth(1).unwrap_or_else(|| ".".to_owned());
    println!("path: {path}");
    let mut wal = Wal::open(&path).unwrap();

    let entry: &[u8] = &[42u8; 4096];

    for _ in 1..100 {
        wal.append(&entry).unwrap();
    }

    // wal.flush();
}
