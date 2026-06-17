fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("proto/helloworld/v1/helloworld.proto")?;
    tonic_prost_build::compile_protos("proto/echo/v1/echo.proto")?;
    tonic_prost_build::compile_protos("proto/broadcaster/v1/broadcaster.proto")?;
    tonic_prost_build::compile_protos("proto/crdt/v1/crdt.proto")?;
    Ok(())
}
