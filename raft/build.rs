fn main() -> Result<(), Box<dyn std::error::Error>> {
    // compiling protos using path on build time
    let protobuf_definitions = ["proto/raft.proto"];
    for def in protobuf_definitions.iter() {
        tonic_build::compile_protos(def)?;
    }
    Ok(())
}
