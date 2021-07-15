use tonic_build::compile_protos;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // compiling protos using path on build time
    let protobuf_definitions = ["proto/engine.proto"];
    for def in protobuf_definitions.iter() {
        compile_protos(def)?;
    }
    Ok(())
}
