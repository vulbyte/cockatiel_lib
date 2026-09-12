fn main() {
    println!("cargo:rerun-if-changed=proto/cockatiel_protobuf.proto");
    prost_build::compile_protos(&["../../proto/cockatiel_protobuf.proto"], &["proto/"])
        .expect("failed to compile cockatiel_protobuf.proto");
}
