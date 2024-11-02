use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .file_descriptor_set_path(PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is not set")).join("tempgrpcd.bin"))
        .out_dir("src/pb")
        .compile(&["src/pb/tempgrpcd.proto"], &["src/"])?;
    Ok(())
}
