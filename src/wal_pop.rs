use std::env;
use std::path::Path;

use wal::Wal;

fn main() {
    let args: Vec<String> = env::args().collect();
    let wal_path = Path::new(&args[1]);

    let mut wal = Wal::open(wal_path).expect("Can't open consensus WAL");

    let last_index = wal.last_index();

    eprintln!("last_index = {last_index}");

    wal.truncate(last_index).unwrap();
    wal.flush_open_segment().unwrap();
}
