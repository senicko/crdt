use crate::crdt::{
    CrdtKind,
    enums::{AnyCrdt, AnyReplica},
    g_counter::GCounterReplica,
    lww_set::{LWWBias, LWWSetReplica},
    or_set::ORSetReplica,
    rga::RGAReplica,
};
use crate::pb::{CreateVariableRequest, SyncStreamRequest, crdt_service_client::CrdtServiceClient};
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
    Push {
        name: String,
        crdt: AnyCrdt,
    },
    Create {
        name: String,
        crdt: AnyCrdt,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
}

type State = Arc<Mutex<HashMap<String, AnyReplica>>>;
type SyncTx = Arc<Mutex<Option<mpsc::Sender<SyncCommand>>>>;

pub enum SharedVariable {
    Counter(SharedCounter),
    Set(SharedSet),
    Array(SharedArray),
}

#[derive(Clone)]
pub struct SharedCounter {
    name: String,
    state: State,
    sync_tx: SyncTx,
}

impl SharedCounter {
    pub fn inc(&self, delta: u64) {
        let crdt = {
            let mut state = self.state.lock().unwrap();

            if let Some(AnyReplica::GCounter(replica)) = state.get_mut(&self.name) {
                replica.inc(delta);
            }

            state.get(&self.name).map(|r| r.as_any_crdt())
        };

        if let Some(crdt) = crdt {
            if let Some(sync_tx) = self.sync_tx.lock().unwrap().as_ref() {
                let _ = sync_tx.try_send(SyncCommand::Push {
                    name: self.name.clone(),
                    crdt,
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
        let crdt = {
            let mut state = self.state.lock().unwrap();

            match state.get_mut(&self.name) {
                Some(AnyReplica::LWWSet(replica)) => replica.add(element),
                Some(AnyReplica::ORSet(replica)) => replica.add(element),
                _ => {}
            };

            state.get(&self.name).map(|r| r.as_any_crdt())
        };

        if let Some(crdt) = crdt {
            if let Some(sync_tx) = self.sync_tx.lock().unwrap().as_ref() {
                let _ = sync_tx.try_send(SyncCommand::Push {
                    name: self.name.clone(),
                    crdt,
                });
            }
        }
    }

    pub fn remove(&self, element: String) {
        let crdt = {
            let mut state = self.state.lock().unwrap();

            match state.get_mut(&self.name) {
                Some(AnyReplica::LWWSet(replica)) => replica.remove(&element),
                Some(AnyReplica::ORSet(replica)) => replica.remove(&element),
                _ => {}
            };

            state.get(&self.name).map(|r| r.as_any_crdt())
        };

        if let Some(crdt) = crdt {
            if let Some(sync_tx) = self.sync_tx.lock().unwrap().as_ref() {
                let _ = sync_tx.try_send(SyncCommand::Push {
                    name: self.name.clone(),
                    crdt,
                });
            }
        }
    }

    pub fn members(&self) -> HashSet<String> {
        let state = self.state.lock().unwrap();

        match state.get(&self.name) {
            Some(AnyReplica::LWWSet(replica)) => replica.members(),
            Some(AnyReplica::ORSet(replica)) => replica.members(),
            _ => HashSet::<String>::new(),
        }
    }
}

#[derive(Clone)]
pub struct SharedArray {
    name: String,
    state: State,
    sync_tx: SyncTx,
}

impl SharedArray {
    pub fn insert(&self, after_id: Option<String>, value: String) -> Option<String> {
        let (crdt, id) = {
            let mut state = self.state.lock().unwrap();

            let id = if let Some(AnyReplica::RGA(replica)) = state.get_mut(&self.name) {
                Some(replica.insert(after_id, value))
            } else {
                None
            };

            let crdt = state.get(&self.name).map(|r| r.as_any_crdt());
            (crdt, id)
        };

        if let Some(crdt) = crdt {
            if let Some(sync_tx) = self.sync_tx.lock().unwrap().as_ref() {
                let _ = sync_tx.try_send(SyncCommand::Push {
                    name: self.name.clone(),
                    crdt,
                });
            }
        }
        id
    }

    pub fn remove(&self, id: String) {
        let crdt = {
            let mut state = self.state.lock().unwrap();

            if let Some(AnyReplica::RGA(replica)) = state.get_mut(&self.name) {
                replica.remove(&id);
            }

            state.get(&self.name).map(|r| r.as_any_crdt())
        };

        if let Some(crdt) = crdt {
            if let Some(sync_tx) = self.sync_tx.lock().unwrap().as_ref() {
                let _ = sync_tx.try_send(SyncCommand::Push {
                    name: self.name.clone(),
                    crdt,
                });
            }
        }
    }

    pub fn to_vec(&self) -> Vec<(String, String)> {
        let state = self.state.lock().unwrap();

        if let Some(AnyReplica::RGA(r)) = state.get(&self.name) {
            r.to_vec()
        } else {
            Vec::new()
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

        let handle = tokio::spawn(run_grpc_actor(client, sync_rx, self.clone()));
        *self.handle.lock().unwrap() = Some(handle);

        let to_flush: Vec<(String, AnyCrdt)> = {
            let state = self.state.lock().unwrap();

            state
                .iter()
                .map(|(name, replica)| (name.clone(), replica.as_any_crdt()))
                .collect()
        };

        for (name, crdt) in to_flush {
            let _ = new_tx.try_send(SyncCommand::Push { name, crdt });
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
            CrdtKind::ORSet => AnyReplica::ORSet(ORSetReplica::<String>::new()),
            CrdtKind::RGA => AnyReplica::RGA(RGAReplica::<String>::new(self.hlc.clone())),
        };

        let crdt = new_replica.as_any_crdt();
        let sync_tx = self.sync_tx.lock().unwrap().clone();

        if let Some(tx) = sync_tx {
            let (reply_tx, reply_rx) = oneshot::channel();

            tx.send(SyncCommand::Create {
                name: name.to_string(),
                crdt,
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
                AnyReplica::ORSet(_) => CrdtKind::ORSet,
                AnyReplica::RGA(_) => CrdtKind::RGA,
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
            CrdtKind::ORSet => SharedVariable::Set(SharedSet {
                name: name.to_string(),
                state,
                sync_tx,
            }),
            CrdtKind::RGA => SharedVariable::Array(SharedArray {
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
                    AnyReplica::ORSet(_) => CrdtKind::ORSet,
                    AnyReplica::RGA(_) => CrdtKind::RGA,
                };
                (k.clone(), kind)
            })
            .collect()
    }

    fn merge_received(&self, var_name: String, crdt: AnyCrdt) {
        let mut state = self.state.lock().unwrap();

        match state.entry(var_name.clone()) {
            Entry::Occupied(mut o) => {
                if let Err(crdt) = o.get_mut().merge(crdt) {
                    eprintln!(
                        "Type mismatch for {}, replacing with server state",
                        var_name
                    );
                    o.insert(AnyReplica::from_crdt(crdt, self.hlc.clone()));
                }
            }
            Entry::Vacant(v) => {
                v.insert(AnyReplica::from_crdt(crdt, self.hlc.clone()));
            }
        }
    }
}

async fn run_grpc_actor(
    mut grpc_client: CrdtServiceClient<Channel>,
    mut sync_rx: mpsc::Receiver<SyncCommand>,
    remote_store: RemoteStore,
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
                Some(SyncCommand::Push { name, crdt }) => {
                    if let Ok(crdt_bytes) = bincode::serialize(&crdt) {
                        let _ = outbound_tx
                            .send(SyncStreamRequest { var: name, crdt_bytes })
                            .await;
                    }
                }
                Some(SyncCommand::Create { name, crdt, reply }) => {
                    if let Ok(crdt_bytes) = bincode::serialize(&crdt) {
                        let req = CreateVariableRequest { var: name, crdt_bytes };

                        let result = grpc_client
                            .create_variable(req)
                            .await
                            .map(|_| ())
                            .map_err(StoreError::Rpc);

                        let _ = reply.send(result);
                    } else {
                        let _ = reply.send(Err(StoreError::TypeMismatch));
                    }
                }
                None => break,
            },
            msg = inbound.next() => match msg {
                Some(Ok(received)) => {
                    if let Ok(deserialized) = bincode::deserialize::<AnyCrdt>(&received.crdt_bytes) {
                        remote_store.merge_received(received.var, deserialized);
                    }
                }
                Some(Err(e)) => {
                    eprintln!("Inbound stream error: {}", e);
                    break;
                }
                None => break,
            },
        }
    }
}
