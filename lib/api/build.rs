fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir("src/grpc/") // saves generated structures at this location
        .compile(
            &["src/grpc/proto/qdrant.proto"], // proto entry point
            &["src/grpc/proto"], // specify the root location to search proto dependencies
        )
        .unwrap();

    Ok(())
}
