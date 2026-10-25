fn main() -> Result<(), Box<dyn std::error::Error>> {
    // protoc from the crate: no system install required
    if std::env::var_os("PROTOC").is_none() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/gaip.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/gaip.proto");
    Ok(())
}
