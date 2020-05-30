mod test_wal;
mod settings;

mod common;
mod operations;

fn main() {
    // let settings = settings::Settings::new().expect("Can't read config.");
    // println!("{:?}", settings);
    
    test_wal::write_wal();
    test_wal::read_wal();
    test_wal::truncate_wal();
    test_wal::read_wal();
}
