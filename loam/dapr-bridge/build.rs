fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Dapr's app callback protos, vendored from dapr/dapr v1.18.4 (Apache-2.0).
    // Server side only: the sidecar calls us.
    tonic_prost_build::configure()
        .build_client(false)
        .compile_protos(
            &["proto/dapr/proto/runtime/v1/appcallback.proto"],
            &["proto"],
        )?;
    Ok(())
}
