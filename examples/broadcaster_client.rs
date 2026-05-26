use std::error::Error;

use tokio_stream::{Stream, StreamExt};
use tonic::transport::Channel;

use crate::pb::{PingRequest, broadcaster_service_client::BroadcasterServiceClient};

pub mod pb {
    tonic::include_proto!("broadcaster.v1");
}

fn ping_request_iter() -> impl Stream<Item = PingRequest> {
    tokio_stream::iter(1..i64::MAX).map(|i| PingRequest { value: i })
}

async fn ping(client: &mut BroadcasterServiceClient<Channel>, num: usize) {
    let in_stream = ping_request_iter().take(num);

    let response = client.ping(in_stream).await.unwrap();

    let mut resp_stream = response.into_inner();

    while let Some(received) = resp_stream.next().await {
        let received = received.unwrap();
        println!("received value: {}", received.value)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut client = BroadcasterServiceClient::connect("http://[::1]:50051").await?;

    println!("Sending a ping to the server;");
    ping(&mut client, 5).await;

    Ok(())
}
