use bincode;
use clap::{Parser, ValueEnum};
use clap_repl::ClapEditor;
use clap_repl::reedline::DefaultPrompt;
use crdt::crdt::Crdt;
use crdt::crdt::enums::CrdtState;
use crdt::crdt::g_counter::{GCounter, GCounterReplica};
use crdt::crdt::lww_set::{LWWBias, LWWSetReplica};
use std::collections::HashMap;
use std::{
    error::Error,
    // TODO: Difference between tokio Mutex and std Mutex
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::transport::Channel;
use uhlc::HLC;

use crate::pb::{SyncRequest, g_counter_service_client::GCounterServiceClient};

pub mod pb {
    tonic::include_proto!("crdt.v1");
}

#[derive(Debug, Clone, ValueEnum)]
enum CrdtType {
    GCounter,
    LWWSet,
}

#[derive(Debug, Parser)]
#[command(name = "")]
enum Command {
    New { name: String, crdt_type: CrdtType },
    Value { name: String },
    Inc { name: String },
    Add { name: String, element: String },
    Remove { name: String, element: String },
    Vars,
    Connect,
    Disconnect,
    Quit,
}

async fn handle_connection(
    mut client: GCounterServiceClient<Channel>,
    state: Arc<Mutex<GCounterReplica>>,
    rx: Receiver<SyncRequest>,
) {
    let req_stream = ReceiverStream::new(rx);
    let response = client.sync(req_stream).await.unwrap();
    let mut resp_stream = response.into_inner();

    // Server sync
    while let Some(received) = resp_stream.next().await {
        let received = received.unwrap();

        match bincode::deserialize::<GCounter>(&received.crdt_bytes) {
            Ok(deserialized) => {
                let mut state = state.lock().unwrap();
                state.merge(&deserialized);
            }
            Err(e) => {
                eprintln!("Failed to deserialize: {}", e);
            }
        }
    }
}

fn establish_connection(
    state: Arc<Mutex<GCounterReplica>>,
) -> (Sender<SyncRequest>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<SyncRequest>(16);
    let state_copy = state.clone();

    let handle = tokio::spawn(async move {
        match GCounterServiceClient::connect("http://[::1]:50051").await {
            Ok(client) => {
                handle_connection(client, state_copy, rx).await;
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    });

    (tx, handle)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let state_map = Arc::new(Mutex::new(HashMap::<String, CrdtState>::new()));

    // let (tx, connection_handle) = establish_connection(state.clone());
    // let mut active_tx: Option<mpsc::Sender<SyncRequest>> = Some(tx);
    // let mut active_connection_handle: Option<JoinHandle<()>> = Some(connection_handle);

    let prompt = DefaultPrompt {
        ..DefaultPrompt::default()
    };

    let rl = ClapEditor::<Command>::builder()
        .with_prompt(Box::new(prompt))
        .build();

    rl.repl(|command| match command {
        Command::New { name, crdt_type } => {
            //Create a new variable with name
            let mut state_map = state_map.lock().unwrap();

            //If we want variables to be mutable, we need to get rid of this check
            if let Some(_) = state_map.get(&name) {
                eprintln!("Structure with name: {} already exists", &name);
                return;
            }

            let new_crdt = match crdt_type {
                CrdtType::GCounter => CrdtState::GCounter(GCounterReplica::new()),
                CrdtType::LWWSet => CrdtState::LWWSet(LWWSetReplica::<String>::new(
                    Arc::new(HLC::default()),
                    LWWBias::Add,
                )),
            };

            state_map.insert(name, new_crdt);

            //TODO: Broadcast CRDT structure creation to all users
        }
        //TODO: Refactor commands beneath avoid repeating code (closure?)
        Command::Inc { name } => {
            let mut state_map = state_map.lock().unwrap();

            match state_map.get_mut(&name) {
                Some(crdt) => {
                    if let Err(e) = crdt.try_inc(1) {
                        eprintln!("{}", e);
                    }
                }
                None => eprintln!("Structure with name: {} doesn't extist", name),
            }

            //TODO: Refactor to broadcast G-counter incrementation to all users

            // let crdt_bytes = match bincode::serialize(&state.crdt) {
            //     Ok(crdt_bytes) => crdt_bytes,
            //     Err(e) => {
            //         eprintln!("Failed to serialize CRDT: {}", e);
            //         return;
            //     }
            // };

            // // TODO: Why there is &
            // if let Some(tx) = &active_tx {
            //     let tx_clone = tx.clone();

            //     tokio::spawn(async move {
            //         if let Err(e) = tx_clone.send(SyncRequest { crdt_bytes }).await {
            //             eprintln!("Failed to send request to handler: {}", e);
            //         }
            //     });
            // }
        }
        Command::Add { name, element } => {
            let mut state_map = state_map.lock().unwrap();

            match state_map.get_mut(&name) {
                Some(crdt) => {
                    if let Err(e) = crdt.try_add(element) {
                        eprintln!("{}", e);
                    }
                }
                None => eprintln!("Structure with name: {} doesn't extist", name),
            }

            //TODO: Work on broadcasting the set
        }
        Command::Remove { name, element } => {
            let mut state_map = state_map.lock().unwrap();

            match state_map.get_mut(&name) {
                Some(crdt) => {
                    if let Err(e) = crdt.try_remove(element) {
                        eprintln!("{}", e);
                    }
                }
                None => eprintln!("Structure with name: {} doesn't extist", name),
            }
            //TODO: Work on broadcasting the set
        }
        Command::Value { name } => {
            let state_map = state_map.lock().unwrap();

            match state_map.get(&name) {
                Some(crdt) => {
                    crdt.value();
                }
                None => eprintln!("Structure with name: {} doesn't extist", name),
            }
        }

        Command::Vars => {
            let state_map = state_map.lock().unwrap();

            for (name, crdt) in state_map.iter() {
                println!("{}: {}", name, crdt);
            }
        }
        Command::Connect => {
            // if active_connection_handle.is_some() {
            //     return;
            // }

            // let (tx, connection_handle) = establish_connection(state.clone());
            // active_tx = Some(tx);
            // active_connection_handle = Some(connection_handle);

            // let state = state.lock().unwrap();

            // let crdt_bytes = match bincode::serialize(&state.crdt) {
            //     Ok(crdt_bytes) => crdt_bytes,
            //     Err(e) => {
            //         eprintln!("Failed to serialize CRDT: {}", e);
            //         return;
            //     }
            // };

            // if let Some(tx) = &active_tx {
            //     let tx_clone = tx.clone();

            //     tokio::spawn(async move {
            //         if let Err(e) = tx_clone.send(SyncRequest { crdt_bytes }).await {
            //             eprintln!("Failed to send request to handler: {}", e);
            //         }
            //     });
            // }
        }
        Command::Disconnect => {
            // if let Some(handle) = active_connection_handle.take() {
            //     handle.abort();
            //     active_tx = None;
            // }
        }
        Command::Quit => return,
    });

    Ok(())
}
