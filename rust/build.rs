use std::path::PathBuf;

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();

    let proto_file = PathBuf::from(&manifest_dir)
        .join("cockatiel_proto")
        .join("cockatiel_protobuf.proto"); // <-- Updated filename

    let proto_include = PathBuf::from(&manifest_dir).join("cockatiel_proto");

    println!("cargo:rerun-if-changed={}", proto_file.display());

    prost_build::compile_protos(
        &[proto_file.to_str().unwrap()],
        &[proto_include.to_str().unwrap()],
    )
    .unwrap();
}
