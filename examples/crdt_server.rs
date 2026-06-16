use crdt::crdt::{Crdt, lww_set::LWWSetReplica};
use crdt::crdt::{
    g_counter::{GCounter, GCounterReplica},
    lww_set::LWWSet,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Display,
    net::ToSocketAddrs,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, wrappers::BroadcastStream};
use tonic::{Request, Response, Status, Streaming, transport::Server};
use uhlc::HLC;
use uuid::Uuid;

use crate::pb::{
    CreateVariableRequest, SyncStreamRequest, SyncStreamResponse, crdt_service_server,
};

pub mod pb {
    tonic::include_proto!("crdt.v1");
}

type SyncStreamResponseStream =
    Pin<Box<dyn Stream<Item = Result<SyncStreamResponse, Status>> + Send>>;

#[derive(Clone)]
struct InternalSyncResponse {
    uuid: Uuid,
    response: SyncStreamResponse,
}

#[derive(Debug, Deserialize)]
pub enum AnyCrdt {
    GCounter(GCounter),
    LWWSet(LWWSet<String>),
}

#[derive(Debug, Serialize)]
pub enum AnyCrdtRef<'a> {
    GCounter(&'a GCounter),
    LWWSet(&'a LWWSet<String>),
}

pub enum AnyReplica {
    GCounter(GCounterReplica),
    LWWSet(LWWSetReplica<String>),
}

impl AnyReplica {
    pub fn as_crdt_ref(&self) -> AnyCrdtRef<'_> {
        match self {
            AnyReplica::GCounter(replica) => AnyCrdtRef::GCounter(&replica.crdt),
            AnyReplica::LWWSet(replica) => AnyCrdtRef::LWWSet(&replica.crdt),
        }
    }
}

impl Display for AnyReplica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyReplica::GCounter(_) => write!(f, "GCounter"),
            AnyReplica::LWWSet(_) => write!(f, "LWWSet"),
        }
    }
}

struct CrdtService {
    hlc: Arc<HLC>,
    state_map: Arc<Mutex<HashMap<String, AnyReplica>>>,
    tx: broadcast::Sender<InternalSyncResponse>,
}

#[tonic::async_trait]
impl crdt_service_server::CrdtService for CrdtService {
    type SyncStreamStream = SyncStreamResponseStream;

    async fn create_variable(
        &self,
        req: Request<CreateVariableRequest>,
    ) -> Result<Response<()>, Status> {
        let inner_req = req.into_inner();
        let var_name = inner_req.var;

        let remote_crdt = match bincode::deserialize::<AnyCrdt>(&inner_req.crdt_bytes) {
            Ok(crdt) => crdt,
            Err(e) => {
                eprintln!("Failed to deserialize CRDT on creation: {}", e);
                return Err(Status::invalid_argument("Invalid CRDT bytes"));
            }
        };

        let replica = match remote_crdt {
            AnyCrdt::GCounter(crdt) => AnyReplica::GCounter(GCounterReplica {
                crdt,
                ..Default::default()
            }),
            AnyCrdt::LWWSet(crdt) => AnyReplica::LWWSet(LWWSetReplica {
                hlc: self.hlc.clone(),
                crdt,
            }),
        };

        let mut state_map = self.state_map.lock().unwrap();

        if state_map.contains_key(&var_name) {
            return Err(Status::already_exists(format!(
                "Variable '{}' already exists",
                var_name
            )));
        }

        state_map.insert(var_name.clone(), replica);

        let inserted_replica = state_map.get(&var_name).unwrap();
        if let Ok(crdt_bytes) = bincode::serialize(&inserted_replica.as_crdt_ref()) {
            let response = InternalSyncResponse {
                uuid: Uuid::new_v4(), // Generate a server-side event UUID
                response: SyncStreamResponse {
                    var: var_name,
                    crdt_bytes,
                },
            };

            // We ignore send errors here because it just means no one is currently subscribed
            let _ = self.tx.send(response);
        }

        // 5. Return Empty
        Ok(Response::new(()))
    }

    async fn sync_stream(
        &self,
        req: Request<Streaming<SyncStreamRequest>>,
    ) -> Result<Response<Self::SyncStreamStream>, Status> {
        let uuid = Uuid::new_v4();
        let mut stream = req.into_inner();

        // rx is a receiver of self.tx (broadcaster). We want to
        // stream this receiver (messages from broadcaster) to the client.
        let rx = self.tx.subscribe();

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

        let state_map = self.state_map.lock().unwrap();

        // Collect initial state to avoid use-after-move errors on stream chaining
        let mut initial_states = Vec::new();

        for (var, replica) in state_map.iter() {
            match bincode::serialize(&replica.as_crdt_ref()) {
                Ok(crdt_bytes) => {
                    initial_states.push(Ok(SyncStreamResponse {
                        var: var.clone(),
                        crdt_bytes,
                    }));
                }
                Err(e) => {
                    eprintln!("Failed to serialize initial state: {}", e);
                }
            };
        }

        let out_stream = tokio_stream::iter(initial_states).chain(broadcast_stream);
        drop(state_map); // Drop the mutex guard before spawning

        let tx = self.tx.clone();
        let state_map_arc = Arc::clone(&self.state_map);

        tokio::spawn(async move {
            while let Some(req_result) = stream.next().await {
                let req = match req_result {
                    Ok(req) => req,
                    Err(e) => {
                        eprintln!("Client disconnected or error reading stream: {}", e);
                        break;
                    }
                };

                let var_name = req.var;
                let deserialized = match bincode::deserialize::<AnyCrdt>(&req.crdt_bytes) {
                    Ok(remote_crdt) => remote_crdt,
                    Err(e) => {
                        eprintln!("Failed to deserialize remote CRDT: {}", e);
                        continue;
                    }
                };

                // 3. USE THE CLONED ARC HERE INSTEAD OF `self`
                let mut state_map = state_map_arc.lock().unwrap();

                let merge_result = match deserialized {
                    AnyCrdt::GCounter(crdt) => match state_map.get_mut(&var_name) {
                        Some(AnyReplica::GCounter(var)) => {
                            var.merge(&crdt);
                            Ok(())
                        }
                        _ => Err(()),
                    },
                    AnyCrdt::LWWSet(crdt) => match state_map.get_mut(&var_name) {
                        Some(AnyReplica::LWWSet(var)) => {
                            var.merge(&crdt);
                            Ok(())
                        }
                        _ => Err(()),
                    },
                };

                if merge_result.is_err() {
                    eprintln!(
                        "Failed to merge CRDT: variable '{}' not found or type mismatch.",
                        var_name
                    );
                    continue;
                }

                println!("Server state is:");
                for (name, replica) in state_map.iter() {
                    println!("{}: {}", name, replica);
                }

                let updated_replica = state_map.get(&var_name).unwrap();

                let crdt_bytes = match bincode::serialize(&updated_replica.as_crdt_ref()) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        eprintln!("Failed to serialize CRDT: {}", e);
                        continue;
                    }
                };

                let response = InternalSyncResponse {
                    uuid,
                    response: SyncStreamResponse {
                        var: var_name,
                        crdt_bytes,
                    },
                };

                if let Err(e) = tx.send(response) {
                    eprintln!("Error writing to broadcaster: {}", e);
                }
            }
        });

        Ok(Response::new(Box::pin(out_stream) as Self::SyncStreamStream))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let state_map = Arc::new(Mutex::new(HashMap::<String, AnyReplica>::new()));
    let hlc = Arc::new(HLC::default());

    let (tx, _) = broadcast::channel(16);
    let server = CrdtService { hlc, state_map, tx };

    Server::builder()
        .add_service(crdt_service_server::CrdtServiceServer::new(server))
        .serve("[::1]:50051".to_socket_addrs()?.next().unwrap())
        .await?;

    Ok(())
}
