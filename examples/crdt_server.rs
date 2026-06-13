use std::{
    net::ToSocketAddrs,
    pin::Pin,
    sync::{Arc, Mutex},
};

use crdt::crdt::Crdt;
use crdt::crdt::g_counter::{GCounter, GCounterReplica};
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, wrappers::BroadcastStream};
use tonic::{Request, Response, Status, Streaming, transport::Server};
use uuid::Uuid;

use crate::pb::{SyncRequest, SyncResponse, g_counter_service_server};

pub mod pb {
    tonic::include_proto!("crdt.v1");
}

type SyncResponseStream = Pin<Box<dyn Stream<Item = Result<SyncResponse, Status>> + Send>>;

#[derive(Clone)]
struct InternalSyncResponse {
    uuid: Uuid,
    response: SyncResponse,
}

#[derive(Debug)]
struct GCounterService {
    crdt: Arc<Mutex<GCounterReplica>>,
    tx: broadcast::Sender<InternalSyncResponse>,
}

#[tonic::async_trait]
impl g_counter_service_server::GCounterService for GCounterService {
    type SyncStream = SyncResponseStream;

    async fn sync(
        &self,
        req: Request<Streaming<SyncRequest>>,
    ) -> Result<Response<Self::SyncStream>, Status> {
        let uuid = Uuid::new_v4();
        let mut stream = req.into_inner();

        // rx is a receiver of self.tx (broadcaster). We want to
        // stream this receiver (messages from broadcaster) to the client.
        let rx = self.tx.subscribe();

        let sync_message = {
            let crdt = self.crdt.lock().unwrap();

            match bincode::serialize(&crdt.crdt) {
                Ok(crdt_bytes) => Some(Ok(SyncResponse { crdt_bytes })),
                Err(e) => {
                    eprintln!("Failed to serialize initial state: {}", e);
                    None
                }
            }
        };

        let broadcast_stream =
            BroadcastStream::new(rx).filter_map(move |internal_response| match internal_response {
                Ok(res) => {
                    if res.uuid != uuid {
                        return Some(Ok(res.response));
                    }
                    None
                }
                Err(_) => Some(Err(Status::internal(""))),
            });

        let out_stream: Self::SyncStream = if let Some(msg) = sync_message {
            Box::pin(tokio_stream::once(msg).chain(broadcast_stream))
        } else {
            Box::pin(broadcast_stream)
        };

        let tx = self.tx.clone();
        let crdt = self.crdt.clone();

        tokio::spawn(async move {
            while let Some(req_result) = stream.next().await {
                let req = match req_result {
                    Ok(req) => req,
                    Err(e) => {
                        eprintln!("Client disconnected or error reading stream: {}", e);
                        break;
                    }
                };

                let remote_crdt = match bincode::deserialize::<GCounter>(&req.crdt_bytes) {
                    Ok(remote_crdt) => remote_crdt,
                    Err(e) => {
                        eprintln!("Failed to deserialize remote CRDT: {}", e);
                        continue;
                    }
                };

                let mut crdt = crdt.lock().unwrap();
                crdt.merge(&remote_crdt);
                println!("Server state is {}", crdt.value());

                // Propagate current source-of-truth to other clients.

                let crdt_bytes = match bincode::serialize(&crdt.crdt) {
                    Ok(crdt_bytes) => crdt_bytes,
                    Err(e) => {
                        eprintln!("Failed to serialize CRDT: {}", e);
                        continue;
                    }
                };

                let response = InternalSyncResponse {
                    uuid: uuid,
                    response: SyncResponse { crdt_bytes },
                };

                match tx.send(response) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Error writing to broadcaster: {}", e);
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(out_stream) as Self::SyncStream))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crdt = GCounterReplica::new();

    let (tx, _) = broadcast::channel(16);

    let server = GCounterService {
        crdt: Arc::new(Mutex::new(crdt)),
        tx,
    };

    Server::builder()
        .add_service(g_counter_service_server::GCounterServiceServer::new(server))
        .serve("[::1]:50051".to_socket_addrs()?.next().unwrap())
        .await?;

    Ok(())
}
