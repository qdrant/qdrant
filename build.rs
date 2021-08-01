fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "grpc")]
    {
        tonic_build::compile_protos("src/tonic/proto/qdrant.proto")?;
    }
    Ok(())
}
