use std::pin::Pin;

use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming, transport::Server};

use crate::pb::{
    BidirectionalStreamingEchoRequest, BidirectionalStreamingEchoResponse,
    ClientStreamingEchoRequest, ClientStreamingEchoResponse, ServerStreamingEchoRequest,
    ServerStreamingEchoResponse, UnaryEchoRequest, UnaryEchoResponse,
};

pub mod pb {
    tonic::include_proto!("echo.v1");
}

type ServerResponseStream =
    Pin<Box<dyn Stream<Item = Result<ServerStreamingEchoResponse, Status>> + Send>>;

type BidirectionalResponseStream =
    Pin<Box<dyn Stream<Item = Result<BidirectionalStreamingEchoResponse, Status>> + Send>>;

#[derive(Debug)]
pub struct EchoServer {}

#[tonic::async_trait]
impl pb::echo_service_server::EchoService for EchoServer {
    async fn unary_echo(
        &self,
        _: Request<UnaryEchoRequest>,
    ) -> Result<Response<UnaryEchoResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    type ServerStreamingEchoStream = ServerResponseStream;

    async fn server_streaming_echo(
        &self,
        req: Request<ServerStreamingEchoRequest>,
    ) -> Result<Response<Self::ServerStreamingEchoStream>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn client_streaming_echo(
        &self,
        _: Request<Streaming<ClientStreamingEchoRequest>>,
    ) -> Result<Response<ClientStreamingEchoResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    type BidirectionalStreamingEchoStream = BidirectionalResponseStream;

    async fn bidirectional_streaming_echo(
        &self,
        req: Request<Streaming<BidirectionalStreamingEchoRequest>>,
    ) -> Result<Response<BidirectionalResponseStream>, Status> {
        Err(Status::unimplemented("not implemented"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = EchoServer {};

    Server::builder().add_service(pb::echo_service_server::EchoServiceServer::new(server));

    Ok(())
}
