use crate::pb::{SyncRequest, g_counter_service_client::GCounterServiceClient};
use bincode;
use clap::{Parser, ValueEnum};
use clap_repl::reedline::{DefaultPrompt, Reedline, Signal};
use crdt::crdt::Crdt;
use crdt::crdt::g_counter::{GCounter, GCounterReplica};
use crdt::crdt::lww_set::{LWWBias, LWWSetReplica};
use std::collections::HashMap;
use std::fmt::Display;
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

pub mod pb {
    tonic::include_proto!("crdt.v1");
}

#[derive(Debug, Clone, ValueEnum)]
pub enum CrdtType {
    GCounter,
    LWWSet,
}

#[derive(Debug, Parser)]
#[command(no_binary_name = true)]
pub enum GlobalCommand {
    New { name: String, crdt_type: CrdtType },
    Vars,
    Connect,
    Disconnect,
    Quit,
}

#[derive(Debug, Parser)]
#[command(no_binary_name = true)]
pub enum GCounterCmd {
    Inc,
    Value,
}

#[derive(Debug, Parser)]
#[command(no_binary_name = true)]
pub enum LWWSetCmd {
    Add { element: String },
    Remove { element: String },
    Value,
}

// Used on the client side to handle repl commands
// TODO: Figure out if we can use just one enum instead of two similar
pub enum CrdtState {
    GCounter(GCounterReplica),
    LWWSet(LWWSetReplica<String>),
}

impl Display for CrdtState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CrdtState::GCounter(_) => write!(f, "GCounter"),
            CrdtState::LWWSet(_) => write!(f, "LWWSet"),
        }
    }
}

impl CrdtState {
    pub fn execute(&mut self, args: &[&str]) {
        match self {
            CrdtState::GCounter(counter) => match GCounterCmd::try_parse_from(args) {
                Ok(GCounterCmd::Inc) => {
                    counter.inc(1);
                }
                Ok(GCounterCmd::Value) => println!("Counter Value: {}", counter.value()),
                Err(e) => {
                    let _ = e.print();
                }
            },
            CrdtState::LWWSet(set) => match LWWSetCmd::try_parse_from(args) {
                Ok(LWWSetCmd::Add { element }) => {
                    set.add(element);
                }
                Ok(LWWSetCmd::Remove { element }) => {
                    set.remove(element);
                }
                Ok(LWWSetCmd::Value) => println!("Set Members: {:#?}", set.members()),
                Err(e) => {
                    let _ = e.print();
                }
            },
        }
    }
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

    let mut line_editor = Reedline::create();
    let prompt = DefaultPrompt::default();

    loop {
        let sig = line_editor.read_line(&prompt);

        match sig {
            Ok(Signal::Success(buffer)) => {
                let tokens: Vec<&str> = buffer.split_whitespace().collect();
                if tokens.is_empty() {
                    continue;
                }

                // First we assume the command is a global command.

                if let Ok(global_cmd) = GlobalCommand::try_parse_from(&tokens) {
                    match global_cmd {
                        GlobalCommand::New { name, crdt_type } => {
                            let mut map = state_map.lock().unwrap();

                            if map.contains_key(&name) {
                                eprintln!("name is {} already taken", name);
                                continue;
                            }

                            let new_crdt = match crdt_type {
                                CrdtType::GCounter => CrdtState::GCounter(GCounterReplica::new()),
                                CrdtType::LWWSet => {
                                    CrdtState::LWWSet(LWWSetReplica::<String>::new(
                                        Arc::new(HLC::default()),
                                        LWWBias::Add,
                                    ))
                                }
                            };

                            map.insert(name, new_crdt);
                        }
                        GlobalCommand::Vars => {
                            let map = state_map.lock().unwrap();

                            for (name, crdt) in map.iter() {
                                println!("{}: {}", name, crdt);
                            }
                        }
                        GlobalCommand::Connect => {
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
                            //         if let Err(e) = tx_clone.send(SyncRequest { crdt_bytes }).await
                            //         {
                            //             eprintln!("Failed to send request to handler: {}", e);
                            //         }
                            //     });
                            // }
                        }
                        GlobalCommand::Disconnect => {
                            // if let Some(handle) = active_connection_handle.take() {
                            //     handle.abort();
                            //     active_tx = None;
                            // }
                        }
                        GlobalCommand::Quit => break,
                    }

                    continue;
                }

                // If it's not, we check if there is a variable with such a name.

                let first_word = tokens[0];
                let mut map = state_map.lock().unwrap();

                if let Some(crdt) = map.get_mut(first_word) {
                    let args = &tokens[1..];
                    crdt.execute(args);
                    break;
                }

                // If nothing worked, show an error.

                eprintln!("unknown command or variable")
            }
            Ok(Signal::CtrlD) | Ok(Signal::CtrlC) => {
                break;
            }
            Err(err) => {
                eprintln!("error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}
