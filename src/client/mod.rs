use crate::crdt::{
    CrdtKind,
    enums::{AnyCrdt, AnyReplica},
    g_counter::GCounterReplica,
    lww_set::{LWWBias, LWWSetReplica},
};
use crate::pb::{
    CreateVariableRequest, SyncStreamRequest, SyncStreamResponse,
    crdt_service_client::CrdtServiceClient,
};
use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::transport::Channel;
use uhlc::HLC;

#[derive(Debug)]
pub enum StoreError {
    NotFound,
    TypeMismatch,
    AlreadyExists,
    Disconnected,
    Rpc(tonic::Status),
}

impl From<tonic::Status> for StoreError {
    fn from(s: tonic::Status) -> Self {
        StoreError::Rpc(s)
    }
}


enum SyncCommand {
    // Forward serialized CRDT bytes to the server outbound stream.
    Push { name: String, crdt_bytes: Vec<u8> },
    // Register a new variable with the server via the unary CreateVariable RPC.
    Create {
        name: String,
        crdt_bytes: Vec<u8>,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
}

type State = Arc<Mutex<HashMap<String, AnyReplica>>>;
type SyncTx = Arc<Mutex<Option<mpsc::Sender<SyncCommand>>>>;


pub enum SharedVariable {
    Counter(SharedCounter),
    Set(SharedSet),
}

#[derive(Clone)]
pub struct SharedCounter {
    name: String,
    state: State,
    sync_tx: SyncTx,
}

impl SharedCounter {
    pub fn inc(&self, delta: u64) {
        let crdt_bytes = {
            let mut state = self.state.lock().unwrap();
            if let Some(AnyReplica::GCounter(replica)) = state.get_mut(&self.name) {
                replica.inc(delta);
            }
            state
                .get(&self.name)
                .and_then(|r| bincode::serialize(&r.as_crdt_ref()).ok())
        };

        if let Some(bytes) = crdt_bytes {
            if let Some(tx) = self.sync_tx.lock().unwrap().as_ref() {
                let _ = tx.try_send(SyncCommand::Push {
                    name: self.name.clone(),
                    crdt_bytes: bytes,
                });
            }
        }
    }

    pub fn value(&self) -> u64 {
        let state = self.state.lock().unwrap();
        if let Some(AnyReplica::GCounter(r)) = state.get(&self.name) {
            r.value()
        } else {
            0
        }
    }
}

#[derive(Clone)]
pub struct SharedSet {
    name: String,
    state: State,
    sync_tx: SyncTx,
}

impl SharedSet {
    pub fn add(&self, element: String) {
        let crdt_bytes = {
            let mut state = self.state.lock().unwrap();
            if let Some(AnyReplica::LWWSet(replica)) = state.get_mut(&self.name) {
                replica.add(element);
            }
            state
                .get(&self.name)
                .and_then(|r| bincode::serialize(&r.as_crdt_ref()).ok())
        };

        if let Some(bytes) = crdt_bytes {
            if let Some(tx) = self.sync_tx.lock().unwrap().as_ref() {
                let _ = tx.try_send(SyncCommand::Push {
                    name: self.name.clone(),
                    crdt_bytes: bytes,
                });
            }
        }
    }

    pub fn remove(&self, element: String) {
        let crdt_bytes = {
            let mut state = self.state.lock().unwrap();
            if let Some(AnyReplica::LWWSet(replica)) = state.get_mut(&self.name) {
                replica.remove(element);
            }
            state
                .get(&self.name)
                .and_then(|r| bincode::serialize(&r.as_crdt_ref()).ok())
        };

        if let Some(bytes) = crdt_bytes {
            if let Some(tx) = self.sync_tx.lock().unwrap().as_ref() {
                let _ = tx.try_send(SyncCommand::Push {
                    name: self.name.clone(),
                    crdt_bytes: bytes,
                });
            }
        }
    }

    pub fn members(&self) -> HashSet<String> {
        let state = self.state.lock().unwrap();
        if let Some(AnyReplica::LWWSet(r)) = state.get(&self.name) {
            r.members()
        } else {
            HashSet::new()
        }
    }
}

#[derive(Clone)]
pub struct RemoteStore {
    state: State,
    sync_tx: SyncTx,
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    hlc: Arc<HLC>,
}

impl Default for RemoteStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RemoteStore {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(HashMap::new())),
            sync_tx: Arc::new(Mutex::new(None)),
            handle: Arc::new(Mutex::new(None)),
            hlc: Arc::new(HLC::default()),
        }
    }

    pub async fn connect(&self, addr: &str) -> Result<(), tonic::transport::Error> {
        self.disconnect();

        let client = CrdtServiceClient::connect(addr.to_string()).await?;
        let (new_tx, sync_rx) = mpsc::channel::<SyncCommand>(32);

        *self.sync_tx.lock().unwrap() = Some(new_tx.clone());

        let state = Arc::clone(&self.state);
        let hlc = Arc::clone(&self.hlc);
        let handle = tokio::spawn(run_actor(client, sync_rx, state, hlc));
        *self.handle.lock().unwrap() = Some(handle);

        let to_flush: Vec<(String, Vec<u8>)> = {
            let state = self.state.lock().unwrap();
            state
                .iter()
                .filter_map(|(name, replica)| {
                    bincode::serialize(&replica.as_crdt_ref())
                        .ok()
                        .map(|bytes| (name.clone(), bytes))
                })
                .collect()
        };

        for (name, crdt_bytes) in to_flush {
            let _ = new_tx.try_send(SyncCommand::Push { name, crdt_bytes });
        }

        Ok(())
    }

    pub fn disconnect(&self) {
        if let Some(handle) = self.handle.lock().unwrap().take() {
            handle.abort();
        }
        *self.sync_tx.lock().unwrap() = None;
    }

    pub fn is_connected(&self) -> bool {
        self.handle
            .lock()
            .unwrap()
            .as_ref()
            .map(|h| !h.is_finished())
            .unwrap_or(false)
    }

    pub async fn create(&self, name: &str, kind: CrdtKind) -> Result<SharedVariable, StoreError> {
        {
            let state = self.state.lock().unwrap();
            if state.contains_key(name) {
                return Err(StoreError::AlreadyExists);
            }
        }

        let new_replica = match kind {
            CrdtKind::GCounter => AnyReplica::GCounter(GCounterReplica::new()),
            CrdtKind::LWWSet => {
                AnyReplica::LWWSet(LWWSetReplica::<String>::new(self.hlc.clone(), LWWBias::Add))
            }
        };

        let crdt_bytes = bincode::serialize(&new_replica.as_crdt_ref())
            .map_err(|_| StoreError::TypeMismatch)?;

        let sync_tx = self.sync_tx.lock().unwrap().clone();
        if let Some(tx) = sync_tx {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(SyncCommand::Create {
                name: name.to_string(),
                crdt_bytes,
                reply: reply_tx,
            })
            .await
            .map_err(|_| StoreError::Disconnected)?;

            reply_rx.await.map_err(|_| StoreError::Disconnected)??;
        }

        self.state
            .lock()
            .unwrap()
            .insert(name.to_string(), new_replica);

        Ok(self.get(name).unwrap())
    }

    pub fn get(&self, name: &str) -> Option<SharedVariable> {
        let kind = {
            let state = self.state.lock().unwrap();
            match state.get(name)? {
                AnyReplica::GCounter(_) => CrdtKind::GCounter,
                AnyReplica::LWWSet(_) => CrdtKind::LWWSet,
            }
        };

        let state = Arc::clone(&self.state);
        let sync_tx = Arc::clone(&self.sync_tx);

        Some(match kind {
            CrdtKind::GCounter => SharedVariable::Counter(SharedCounter {
                name: name.to_string(),
                state,
                sync_tx,
            }),
            CrdtKind::LWWSet => SharedVariable::Set(SharedSet {
                name: name.to_string(),
                state,
                sync_tx,
            }),
        })
    }

    pub fn list(&self) -> Vec<(String, CrdtKind)> {
        let state = self.state.lock().unwrap();
        state
            .iter()
            .map(|(k, v)| {
                let kind = match v {
                    AnyReplica::GCounter(_) => CrdtKind::GCounter,
                    AnyReplica::LWWSet(_) => CrdtKind::LWWSet,
                };
                (k.clone(), kind)
            })
            .collect()
    }
}


async fn run_actor(
    mut grpc_client: CrdtServiceClient<Channel>,
    mut sync_rx: mpsc::Receiver<SyncCommand>,
    state: State,
    hlc: Arc<HLC>,
) {
    let (outbound_tx, outbound_rx) = mpsc::channel::<SyncStreamRequest>(16);
    let outbound_stream = ReceiverStream::new(outbound_rx);

    let response = match grpc_client.sync_stream(outbound_stream).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to open sync stream: {}", e);
            return;
        }
    };
    let mut inbound = response.into_inner();

    loop {
        tokio::select! {
            cmd = sync_rx.recv() => match cmd {
                Some(SyncCommand::Push { name, crdt_bytes }) => {
                    let _ = outbound_tx
                        .send(SyncStreamRequest { var: name, crdt_bytes })
                        .await;
                }
                Some(SyncCommand::Create { name, crdt_bytes, reply }) => {
                    let req = CreateVariableRequest { var: name, crdt_bytes };
                    let result = grpc_client
                        .create_variable(req)
                        .await
                        .map(|_| ())
                        .map_err(StoreError::Rpc);
                    let _ = reply.send(result);
                }
                None => break,
            },
            msg = inbound.next() => match msg {
                Some(Ok(received)) => merge_received(received, &state, &hlc),
                Some(Err(e)) => {
                    eprintln!("Inbound stream error: {}", e);
                    break;
                }
                None => break,
            },
        }
    }
}

/// Deserializes a server push and merges it into the shared local state.
fn merge_received(received: SyncStreamResponse, state: &State, hlc: &Arc<HLC>) {
    if let Ok(deserialized) = bincode::deserialize::<AnyCrdt>(&received.crdt_bytes) {
        let mut state = state.lock().unwrap();
        match state.entry(received.var.clone()) {
            Entry::Occupied(mut o) => {
                if let Err(crdt) = o.get_mut().merge(deserialized) {
                    eprintln!(
                        "Type mismatch for {}, replacing with server state",
                        received.var
                    );
                    o.insert(AnyReplica::from_crdt(crdt, hlc.clone()));
                }
            }
            Entry::Vacant(v) => {
                v.insert(AnyReplica::from_crdt(deserialized, hlc.clone()));
            }
        }
    }
}
