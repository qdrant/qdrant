mod settings;
mod test_wal;

mod common;
mod operations;

use common::index_def::{BaseIndexParams, Distance, Indexes};
use operations::collection_ops::CollectionOps;

extern crate bincode;
extern crate serde_json;

fn main() {
    // let settings = settings::Settings::new().expect("Can't read config.");
    // println!("{:?}", settings);

    let op1 = CollectionOps::CreateCollection {
        collection_name: String::from("my_collection"),
        dim: 50,
        index: Some(Indexes::PlainIndex {
            params: BaseIndexParams {
                distance: Distance::Cosine,
            },
        }),
    };

    println!("{:?}", op1);

    let ops_bin = bincode::serialize(&op1).unwrap();

    let dec_ops: CollectionOps = bincode::deserialize(&ops_bin).expect("Can't deserialize");

    println!("{:?}", dec_ops);
    // test_wal::write_wal();
    // test_wal::read_wal();
    // test_wal::truncate_wal();
    // test_wal::read_wal();
}
