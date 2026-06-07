use std::{net::ToSocketAddrs, pin::Pin};

use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, wrappers::BroadcastStream};
use tonic::{Request, Response, Status, Streaming, transport::Server};
use uuid::Uuid;

use crate::pb::{PingRequest, PingResponse, broadcaster_service_server};

pub mod pb {
    tonic::include_proto!("broadcaster.v1");
}

type PingResponseStream = Pin<Box<dyn Stream<Item = Result<PingResponse, Status>> + Send>>;

#[derive(Clone)]
struct InternalPingResponse {
    uuid: Uuid,
    ping_response: PingResponse,
}

#[derive(Debug)]
struct BroadcasterService {
    tx: broadcast::Sender<InternalPingResponse>,
}

#[tonic::async_trait]
impl broadcaster_service_server::BroadcasterService for BroadcasterService {
    type PingStream = PingResponseStream;

    async fn ping(
        &self,
        req: Request<Streaming<PingRequest>>,
    ) -> Result<Response<Self::PingStream>, Status> {
        let uuid = Uuid::new_v4();

        let mut stream = req.into_inner();
        let tx = self.tx.clone();

        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(request) => {
                        let response = InternalPingResponse {
                            uuid: uuid,
                            ping_response: PingResponse {
                                value: request.value,
                            },
                        };

                        match tx.send(response) {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("Error writing to broadcaster: {}", e);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Client disconnected or error reading stream: {}", e);
                        break;
                    }
                }
            }
        });

        // rx is a receiver of self.tx (broadcaster). We want to
        // stream this receiver (messages from broadcaster) to the client.
        let rx = self.tx.subscribe();

        // We need to convert Receiver type into gRPC compatible Stream. This
        // can be done by using BroadcastStream from tokio_stream::wrappers.
        let out_stream = BroadcastStream::new(rx)
            // filter_map makes sure we don't send some random error to the client.
            .filter_map(move |internal_response| match internal_response {
                Ok(res) => {
                    if res.uuid != uuid {
                        return Some(Ok(res.ping_response));
                    }

                    None
                }
                Err(_) => Some(Err(Status::internal(""))),
            });

        Ok(Response::new(Box::pin(out_stream) as Self::PingStream))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, _) = broadcast::channel(16);

    let server = BroadcasterService {
        // TODO: Does it mean that we can loose messages?
        // How does it affect CRDT structures.
        tx,
    };

    Server::builder()
        .add_service(pb::broadcaster_service_server::BroadcasterServiceServer::new(server))
        .serve("[::1]:50051".to_socket_addrs()?.next().unwrap())
        .await?;

    Ok(())
}
