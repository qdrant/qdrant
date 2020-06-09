mod settings;

mod common;
mod operations;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpResponse, HttpServer, Responder};

use env_logger;


async fn index() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

async fn index2() -> impl Responder {
    HttpResponse::Ok().body("Hello world again!")
}


#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    let settings = settings::Settings::new().expect("Can't read config.");
    std::env::set_var("RUST_LOG",  settings.log_level);
    env_logger::init();
    
    HttpServer::new(|| {
        App::new()
            .wrap(Logger::default())
            .route("/", web::get().to(index))
            .route("/again", web::get().to(index2))
    })
    .workers(1)
    .bind(format!("{}:{}", settings.service.host, settings.service.port))?
    .run()
    .await
}


// extern crate bincode;
// extern crate serde_json;

// use common::index_def::{BaseIndexParams, Distance, Indexes};
// use operations::collection_ops::CollectionOps;
// fn main() {
//     // 
//     // println!("{:?}", settings);

//     let op1 = CollectionOps::CreateCollection {
//         collection_name: String::from("my_collection"),
//         dim: 50,
//         index: Some(Indexes::PlainIndex {
//             params: BaseIndexParams {
//                 distance: Distance::Cosine,
//             },
//         }),
//     };

//     println!("{:?}", op1);

//     let ops_bin = bincode::serialize(&op1).unwrap();

//     let dec_ops: CollectionOps = bincode::deserialize(&ops_bin).expect("Can't deserialize");

//     println!("{:?}", dec_ops);
//     // test_wal::write_wal();
//     // test_wal::read_wal();
//     // test_wal::truncate_wal();
//     // test_wal::read_wal();
// }
