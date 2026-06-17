use clap::Parser;
use crdt::server::CrdtService;
use std::error::Error;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Server address to connect to
    #[arg(short, long, default_value = "[::1]:50051")]
    addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    CrdtService::serve(&args.addr).await
}
