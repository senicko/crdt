use crate::pb::{SyncStreamRequest, crdt_service_client::CrdtServiceClient};
use bincode;
use clap::{Parser, ValueEnum};
use clap_repl::reedline::{DefaultPrompt, Reedline, Signal};
use crdt::crdt::Crdt;
use crdt::crdt::g_counter::{GCounter, GCounterReplica};
use crdt::crdt::lww_set::{LWWBias, LWWSet, LWWSetReplica};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::{
    error::Error,
    sync::{Arc, Mutex}, // TODO: Difference between tokio Mutex and std Mutex
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

// Used on the client side to handle repl commands
// TODO: Figure out if we can use just one enum instead of two similar
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

impl AnyReplica {
    pub fn execute(&mut self, args: &[&str]) {
        match self {
            AnyReplica::GCounter(counter) => match GCounterCmd::try_parse_from(args) {
                Ok(GCounterCmd::Inc) => {
                    counter.inc(1);
                }
                Ok(GCounterCmd::Value) => println!("Counter Value: {}", counter.value()),
                Err(e) => {
                    let _ = e.print();
                }
            },
            AnyReplica::LWWSet(set) => match LWWSetCmd::try_parse_from(args) {
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
    mut client: CrdtServiceClient<Channel>,
    state: Arc<Mutex<HashMap<String, AnyReplica>>>,
    rx: Receiver<SyncStreamRequest>,
    hlc: Arc<HLC>,
) {
    let req_stream = ReceiverStream::new(rx);
    let response = client.sync_stream(req_stream).await.unwrap();
    let mut resp_stream = response.into_inner();

    // Server sync
    while let Some(received) = resp_stream.next().await {
        let received = received.unwrap();
        let var_name = received.var;

        match bincode::deserialize::<AnyCrdt>(&received.crdt_bytes) {
            Ok(deserialized) => {
                let mut state_map = state.lock().unwrap();

                match deserialized {
                    AnyCrdt::GCounter(remote_crdt) => {
                        match state_map.get_mut(&var_name) {
                            Some(AnyReplica::GCounter(local_var)) => {
                                local_var.merge(&remote_crdt);
                            }
                            Some(_) => eprintln!("Type mismatch for {}", var_name),
                            None => {
                                // Variable doesn't exist locally, create and insert it
                                let mut new_var = GCounterReplica::new();
                                new_var.merge(&remote_crdt);
                                state_map.insert(var_name, AnyReplica::GCounter(new_var));
                            }
                        }
                    }
                    AnyCrdt::LWWSet(remote_crdt) => {
                        match state_map.get_mut(&var_name) {
                            Some(AnyReplica::LWWSet(local_var)) => {
                                local_var.merge(&remote_crdt);
                            }
                            Some(_) => eprintln!("Type mismatch for {}", var_name),
                            None => {
                                // Variable doesn't exist locally, create and insert it
                                let mut new_var =
                                    LWWSetReplica::<String>::new(hlc.clone(), LWWBias::Add);
                                new_var.merge(&remote_crdt);
                                state_map.insert(var_name, AnyReplica::LWWSet(new_var));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to deserialize: {}", e);
            }
        }
    }
}

async fn establish_connection(
    state: Arc<Mutex<HashMap<String, AnyReplica>>>,
    hlc: Arc<HLC>,
) -> Result<
    (
        CrdtServiceClient<Channel>,
        Sender<SyncStreamRequest>,
        JoinHandle<()>,
    ),
    tonic::transport::Error,
> {
    let client = CrdtServiceClient::connect("http://[::1]:50051").await?;
    let (tx, rx) = mpsc::channel::<SyncStreamRequest>(16);
    let state_copy = state.clone();
    let stream_client = client.clone();

    let handle = tokio::spawn(async move {
        handle_connection(stream_client, state_copy, rx, hlc).await;
    });

    Ok((client, tx, handle))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let state_map = Arc::new(Mutex::new(HashMap::<String, AnyReplica>::new()));
    let hlc = Arc::new(HLC::default());

    // Track the active gRPC client so we can make Unary calls from the REPL loop
    let mut active_client: Option<CrdtServiceClient<Channel>> = None;
    let mut active_tx: Option<mpsc::Sender<SyncStreamRequest>> = None;
    let mut active_connection_handle: Option<JoinHandle<()>> = None;

    if let Ok((client, tx, connection_handle)) =
        establish_connection(state_map.clone(), hlc.clone()).await
    {
        active_client = Some(client);
        active_tx = Some(tx);
        active_connection_handle = Some(connection_handle);
    } else {
        eprintln!("Initial connection failed. You can connect later using the 'Connect' command.");
    }

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

                if let Ok(global_cmd) = GlobalCommand::try_parse_from(&tokens) {
                    match global_cmd {
                        GlobalCommand::New { name, crdt_type } => {
                            let mut state_map = state_map.lock().unwrap();

                            if state_map.contains_key(&name) {
                                eprintln!("name is {} already taken", name);
                                continue;
                            }

                            let new_crdt = match crdt_type {
                                CrdtType::GCounter => AnyReplica::GCounter(GCounterReplica::new()),
                                CrdtType::LWWSet => AnyReplica::LWWSet(
                                    LWWSetReplica::<String>::new(hlc.clone(), LWWBias::Add),
                                ),
                            };

                            // Serialize the new CRDT to send to the server
                            let crdt_bytes = match bincode::serialize(&new_crdt.as_crdt_ref()) {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    eprintln!("Failed to serialize new CRDT: {}", e);
                                    continue;
                                }
                            };

                            state_map.insert(name.clone(), new_crdt);

                            // Explicitly drop the lock before making network calls
                            drop(state_map);

                            // Propagate to the server if we are connected
                            if let Some(client) = &active_client {
                                let mut client_clone = client.clone();
                                let var_name = name.clone();

                                tokio::spawn(async move {
                                    let req = crate::pb::CreateVariableRequest {
                                        var: var_name,
                                        crdt_bytes,
                                    };

                                    if let Err(e) = client_clone.create_variable(req).await {
                                        eprintln!(
                                            "Failed to propagate new variable to server: {}",
                                            e
                                        );
                                    }
                                });
                            }
                        }
                        GlobalCommand::Vars => {
                            let map = state_map.lock().unwrap();
                            for (name, crdt) in map.iter() {
                                println!("{}: {}", name, crdt);
                            }
                        }
                        GlobalCommand::Connect => {
                            if active_connection_handle.is_some() {
                                continue;
                            }

                            // Match the updated async signature
                            match establish_connection(state_map.clone(), hlc.clone()).await {
                                Ok((client, tx, connection_handle)) => {
                                    active_client = Some(client);
                                    active_tx = Some(tx);
                                    active_connection_handle = Some(connection_handle);

                                    let state_map = state_map.lock().unwrap();

                                    for (var, replica) in state_map.iter() {
                                        let crdt_bytes =
                                            match bincode::serialize(&replica.as_crdt_ref()) {
                                                Ok(crdt_bytes) => crdt_bytes,
                                                Err(e) => {
                                                    eprintln!("Failed to serialize CRDT: {}", e);
                                                    continue;
                                                }
                                            };

                                        if let Some(tx) = &active_tx {
                                            let tx_clone = tx.clone();
                                            let var = var.clone();

                                            tokio::spawn(async move {
                                                if let Err(e) = tx_clone
                                                    .send(SyncStreamRequest { var, crdt_bytes })
                                                    .await
                                                {
                                                    eprintln!(
                                                        "Failed to send request to handler: {}",
                                                        e
                                                    );
                                                }
                                            });
                                        }
                                    }
                                }
                                Err(e) => eprintln!("Connection failed: {}", e),
                            }
                        }
                        GlobalCommand::Disconnect => {
                            if let Some(handle) = active_connection_handle.take() {
                                handle.abort();
                                active_tx = None;
                                active_client = None; // clear the client reference as well
                            }
                        }
                        GlobalCommand::Quit => break,
                    }

                    continue;
                } // If it's not, we check if there is a variable with such a name.

                let first_word = tokens[0];
                let mut map = state_map.lock().unwrap();

                if let Some(replica) = map.get_mut(first_word) {
                    let args = &tokens[1..];
                    replica.execute(args);

                    if let Some(tx) = &active_tx {
                        if let Ok(crdt_bytes) = bincode::serialize(&replica.as_crdt_ref()) {
                            let tx_clone = tx.clone();
                            let var_name = first_word.to_string();

                            tokio::spawn(async move {
                                if let Err(e) = tx_clone
                                    .send(SyncStreamRequest {
                                        var: var_name,
                                        crdt_bytes,
                                    })
                                    .await
                                {
                                    eprintln!("Failed to sync local change to server: {}", e);
                                }
                            });
                        }
                    }

                    continue;
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
