use std::path::Path;

fn main() -> std::io::Result<()> {
    let source = Path::new("../api/src/grpc/proto/storage_read_service.proto");
    let local = "proto/storage_read_service.proto";

    println!("cargo:rerun-if-changed={local}");

    // When building inside the qdrant workspace, copy the proto from the api
    // crate (source of truth) into the local directory.  The copy is tracked
    // by git so that the crate also builds standalone (e.g. from crates.io).
    if source.exists() {
        println!("cargo:rerun-if-changed={}", source.display());
        fs_err::copy(source, local)?;
    }

    tonic_build::configure()
        .out_dir("src/generated/")
        .compile(&[local], &["proto"])?;

    Ok(())
}
