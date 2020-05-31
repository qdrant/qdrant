mod test_wal;
mod settings;

mod common;
mod operations;

use operations::collection_ops::CreateCollection;
use common::index_def::{IndexType, BaseIndexParams, Distance, PlainIndex};

extern crate serde_json;
extern crate bincode;

fn main() {
    // let settings = settings::Settings::new().expect("Can't read config.");
    // println!("{:?}", settings);

    let op1 = CreateCollection {
        collection_name: String::from("my_collection"),
        dim: 50,
        index: Some(IndexType::Plain(PlainIndex{
            params: BaseIndexParams {
                distance: Distance::Cosine
            }
        }))
    };

    println!("{:?}", op1);

    let ops_json = bincode::serialize(&op1);

    match ops_json {
        Ok(json) => println!("{:?}", json),
        Err(x) => println!("Error {:?}", x),
    }
    
    // test_wal::write_wal();
    // test_wal::read_wal();
    // test_wal::truncate_wal();
    // test_wal::read_wal();
}

