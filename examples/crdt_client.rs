use bincode;
use clap::Parser;
use clap_repl::ClapEditor;
use clap_repl::reedline::DefaultPrompt;
use crdt::crdt::Crdt;
use crdt::crdt::g_counter::{GCounter, GCounterReplica};
use std::{
    error::Error,
    // TODO: Difference between tokio Mutex and std Mutex
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::transport::Channel;

use crate::pb::{SyncRequest, g_counter_service_client::GCounterServiceClient};

pub mod pb {
    tonic::include_proto!("crdt.v1");
}

#[derive(Debug, Parser)]
#[command(name = "")]
enum Command {
    Value,
    Inc,
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
    let state = Arc::new(Mutex::new(GCounterReplica::new()));

    let (tx, connection_handle) = establish_connection(state.clone());
    let mut active_tx: Option<mpsc::Sender<SyncRequest>> = Some(tx);
    let mut active_connection_handle: Option<JoinHandle<()>> = Some(connection_handle);

    let prompt = DefaultPrompt {
        ..DefaultPrompt::default()
    };

    let rl = ClapEditor::<Command>::builder()
        .with_prompt(Box::new(prompt))
        .build();

    rl.repl(|command| match command {
        Command::Inc => {
            let mut state = state.lock().unwrap();

            state.inc(1);

            let crdt_bytes = match bincode::serialize(&state.crdt) {
                Ok(crdt_bytes) => crdt_bytes,
                Err(e) => {
                    eprintln!("Failed to serialize CRDT: {}", e);
                    return;
                }
            };

            // TODO: Why there is &
            if let Some(tx) = &active_tx {
                let tx_clone = tx.clone();

                tokio::spawn(async move {
                    if let Err(e) = tx_clone.send(SyncRequest { crdt_bytes }).await {
                        eprintln!("Failed to send request to handler: {}", e);
                    }
                });
            }
        }
        Command::Value => {
            let state = state.lock().unwrap();
            println!("{}", state.value());
        }
        Command::Connect => {
            if active_connection_handle.is_some() {
                return;
            }

            let (tx, connection_handle) = establish_connection(state.clone());
            active_tx = Some(tx);
            active_connection_handle = Some(connection_handle);

            let state = state.lock().unwrap();

            let crdt_bytes = match bincode::serialize(&state.crdt) {
                Ok(crdt_bytes) => crdt_bytes,
                Err(e) => {
                    eprintln!("Failed to serialize CRDT: {}", e);
                    return;
                }
            };

            if let Some(tx) = &active_tx {
                let tx_clone = tx.clone();

                tokio::spawn(async move {
                    if let Err(e) = tx_clone.send(SyncRequest { crdt_bytes }).await {
                        eprintln!("Failed to send request to handler: {}", e);
                    }
                });
            }
        }
        Command::Disconnect => {
            if let Some(handle) = active_connection_handle.take() {
                handle.abort();
                active_tx = None;
            }
        }
        Command::Quit => return,
    });

    Ok(())
}
