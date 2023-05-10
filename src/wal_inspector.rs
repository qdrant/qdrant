use std::env;
use std::path::Path;

use collection::operations::CollectionUpdateOperations;
use collection::wal::SerdeWal;
use wal::WalOptions;

/// Executable to inspect the content of a write ahead log folder.
/// e.g `cargo run --bin wal_inspector storage/collections/test-collection/0/wal/`
fn main() {
    let args: Vec<String> = env::args().collect();
    let wal_path = Path::new(&args[1]);
    let wal: Result<SerdeWal<CollectionUpdateOperations>, _> =
        SerdeWal::new(wal_path.to_str().unwrap(), WalOptions::default());

    match wal {
        Err(error) => {
            eprintln!("Unable to open write ahead log in directory {wal_path:?}: {error}.");
        }
        Ok(wal) => {
            // print all entries
            let mut count = 0;
            for (idx, op) in wal.read_all() {
                println!("==========================");
                println!("Entry {}", idx);
                println!("{:?}", op);
                count += 1;
            }
            println!("==========================");
            println!("End of WAL.");
            println!("Found {} entries.", count);
        }
    }
}
