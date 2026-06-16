use crdt::server::CrdtService;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    CrdtService::serve("[::1]:50051").await
}
