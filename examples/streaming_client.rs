pub mod pb {
    tonic::include_proto!("echo.v1");
}

use std::error::Error;
use std::time::Duration;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::Channel;

use pb::{
    BidirectionalStreamingEchoRequest, ServerStreamingEchoRequest, UnaryEchoRequest,
    echo_service_client::EchoServiceClient,
};

// fn unary_echo_requests_iter() -> impl Stream<Item = UnaryEchoRequest> {
//     tokio_stream::iter(1..usize::MAX).map(|i| UnaryEchoRequest {
//         message: format!("msg {i:02}"),
//     })
// }

fn bidirectional_streaming_echo_requests_iter()
-> impl Stream<Item = BidirectionalStreamingEchoRequest> {
    tokio_stream::iter(1..usize::MAX).map(|i| BidirectionalStreamingEchoRequest {
        message: format!("msg {i:02}"),
    })
}

async fn streaming_echo(client: &mut EchoServiceClient<Channel>, num: usize) {
    let stream = client
        .server_streaming_echo(ServerStreamingEchoRequest {
            message: "foo".into(),
        })
        .await
        .unwrap()
        .into_inner();

    let mut stream = stream.take(num);

    while let Some(item) = stream.next().await {
        println!("\treceived: {}", item.unwrap().message);
    }
}

async fn bidirectional_streaming_echo(client: &mut EchoServiceClient<Channel>, num: usize) {
    let in_stream = bidirectional_streaming_echo_requests_iter().take(num);

    let response = client
        .bidirectional_streaming_echo(in_stream)
        .await
        .unwrap();

    let mut resp_stream = response.into_inner();

    while let Some(received) = resp_stream.next().await {
        let received = received.unwrap();
        println!("\treceived message: {}", received.message);
    }
}

async fn bidirectional_streaming_echo_throttle(
    client: &mut EchoServiceClient<Channel>,
    dur: Duration,
) {
    let in_stream = bidirectional_streaming_echo_requests_iter().throttle(dur);

    let response = client
        .bidirectional_streaming_echo(in_stream)
        .await
        .unwrap();

    let mut resp_stream = response.into_inner();

    while let Some(received) = resp_stream.next().await {
        let received = received.unwrap();
        println!("\treceived message: {}", received.message);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut client = EchoServiceClient::connect("http://[::1]:50051")
        .await
        .unwrap();

    println!("Streaming echo:");
    streaming_echo(&mut client, 5).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("\r\nBidirectional stream echo");
    bidirectional_streaming_echo(&mut client, 17).await;

    // Send usize::MAX amount of requests. One per 2s.
    println!("\r\nBidirectional stream echo with usize::MAX amount of requests");
    bidirectional_streaming_echo_throttle(&mut client, Duration::from_secs(2)).await;

    Ok(())
}
