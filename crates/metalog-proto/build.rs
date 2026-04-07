fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "../../proto/ingestion.proto",
                "../../proto/splits.proto",
                "../../proto/admin.proto",
                "../../proto/metadata.proto",
            ],
            &["../../proto/"],
        )?;
    Ok(())
}
