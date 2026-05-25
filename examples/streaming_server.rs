use std::{error::Error, io::ErrorKind, net::ToSocketAddrs, pin::Pin, time::Duration};

use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
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

fn match_for_io_error(err_status: &Status) -> Option<&std::io::Error> {
    let mut err: &(dyn Error + 'static) = err_status;

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        if let Some(h2_err) = err.downcast_ref::<h2::Error>()
            && let Some(io_err) = h2_err.get_io()
        {
            return Some(io_err);
        }

        err = err.source()?;
    }
}

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
        println!("server_streaming_echo");
        println!("\nClient connected: {:?}", req.remote_addr());

        let repeat = std::iter::repeat(ServerStreamingEchoResponse {
            message: req.into_inner().message,
        });
        let mut stream = Box::pin(tokio_stream::iter(repeat).throttle(Duration::from_millis(200)));

        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match tx.send(Result::<_, Status>::Ok(item)).await {
                    Ok(_) => {}
                    Err(_item) => {
                        break;
                    }
                }
            }
            println!("\nclient disconnected");
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ServerStreamingEchoStream
        ))
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
        println!("bidirectional_streaming_echo");
        println!("\nClient connected: {:?}", req.remote_addr());

        let mut in_stream = req.into_inner();
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(v) => tx
                        .send(Ok(BidirectionalStreamingEchoResponse {
                            message: v.message,
                        }))
                        .await
                        .expect("working rx"),
                    Err(err) => {
                        if let Some(io_err) = match_for_io_error(&err)
                            && io_err.kind() == ErrorKind::BrokenPipe
                        {
                            eprintln!("\tclient disconnected: broken pipe");
                            break;
                        }

                        match tx.send(Err(err)).await {
                            Ok(_) => (),
                            Err(_err) => break,
                        }
                    }
                }
            }

            println!("\tstream ended\n");
        });

        let out_stream = ReceiverStream::new(rx);

        Ok(Response::new(
            Box::pin(out_stream) as Self::BidirectionalStreamingEchoStream
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = EchoServer {};

    Server::builder()
        .add_service(pb::echo_service_server::EchoServiceServer::new(server))
        .serve("[::1]:50051".to_socket_addrs()?.next().unwrap())
        .await?;

    Ok(())
}
