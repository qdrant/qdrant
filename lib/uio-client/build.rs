fn main() -> std::io::Result<()> {
    tonic_build::configure().out_dir("src/generated/").compile(
        &["../api/src/grpc/proto/storage_read_service.proto"],
        &["../api/src/grpc/proto"],
    )?;
    Ok(())
}
