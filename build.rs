fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir("lib/api/src/grpc/") // saves generated structures at this location
        .compile(
            &["lib/api/src/grpc/proto/qdrant.proto"], // proto entry point
            &["lib/api/src/grpc/proto"], // specify the root location to search proto dependencies
        )
        .unwrap();

    Ok(())
}
