use crate::crdt::{
    Crdt, CrdtKind,
    enums::{AnyCrdt, AnyReplica},
    g_counter::GCounterReplica,
    lww_set::{LWWBias, LWWSetReplica},
};
use crate::pb::{CreateVariableRequest, SyncStreamRequest, crdt_service_client::CrdtServiceClient};
use std::collections::hash_map::Entry;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::transport::Channel;
use uhlc::HLC;

#[derive(Debug)]
pub enum StoreError {
    NotFound,
    TypeMismatch,
    AlreadyExists,
    Rpc(tonic::Status),
}

impl From<tonic::Status> for StoreError {
    fn from(s: tonic::Status) -> Self {
        StoreError::Rpc(s)
    }
}

pub(crate) struct StoreInner {
    pub replicas: HashMap<String, AnyReplica>,
    pub tx: Option<mpsc::Sender<SyncStreamRequest>>,
    pub connection_handle: Option<JoinHandle<()>>,
    pub client: Option<CrdtServiceClient<Channel>>,
}

#[derive(Clone)]
pub struct RemoteStore {
    inner: Arc<Mutex<StoreInner>>,
    hlc: Arc<HLC>,
}

pub enum SharedVariable {
    Counter(SharedCounter),
    Set(SharedSet),
}

#[derive(Clone)]
pub struct SharedCounter {
    name: String,
    store: RemoteStore,
}

impl SharedCounter {
    pub fn inc(&self, delta: u64) {
        let mut inner = self.store.inner.lock().unwrap();
        if let Some(AnyReplica::GCounter(replica)) = inner.replicas.get_mut(&self.name) {
            replica.inc(delta);
        }

        if let Some(any_replica) = inner.replicas.get(&self.name) {
            if let Some(tx) = &inner.tx {
                if let Ok(crdt_bytes) = bincode::serialize(&any_replica.as_crdt_ref()) {
                    let tx_clone = tx.clone();
                    let var_name = self.name.clone();
                    tokio::spawn(async move {
                        let _ = tx_clone
                            .send(SyncStreamRequest {
                                var: var_name,
                                crdt_bytes,
                            })
                            .await;
                    });
                }
            }
        }
    }

    pub fn value(&self) -> u64 {
        let inner = self.store.inner.lock().unwrap();
        if let Some(AnyReplica::GCounter(replica)) = inner.replicas.get(&self.name) {
            replica.value()
        } else {
            0
        }
    }
}

#[derive(Clone)]
pub struct SharedSet {
    name: String,
    store: RemoteStore,
}

impl SharedSet {
    pub fn add(&self, element: String) {
        let mut inner = self.store.inner.lock().unwrap();

        if let Some(AnyReplica::LWWSet(replica)) = inner.replicas.get_mut(&self.name) {
            replica.add(element);
        }

        if let Some(any_replica) = inner.replicas.get(&self.name) {
            if let Some(tx) = &inner.tx {
                if let Ok(crdt_bytes) = bincode::serialize(&any_replica.as_crdt_ref()) {
                    let tx_clone = tx.clone();
                    let var_name = self.name.clone();

                    tokio::spawn(async move {
                        let _ = tx_clone
                            .send(SyncStreamRequest {
                                var: var_name,
                                crdt_bytes,
                            })
                            .await;
                    });
                }
            }
        }
    }

    pub fn remove(&self, element: String) {
        let mut inner = self.store.inner.lock().unwrap();

        if let Some(AnyReplica::LWWSet(replica)) = inner.replicas.get_mut(&self.name) {
            replica.remove(element);
        }

        if let Some(any_replica) = inner.replicas.get(&self.name) {
            if let Some(tx) = &inner.tx {
                if let Ok(crdt_bytes) = bincode::serialize(&any_replica.as_crdt_ref()) {
                    let tx_clone = tx.clone();
                    let var_name = self.name.clone();

                    tokio::spawn(async move {
                        let _ = tx_clone
                            .send(SyncStreamRequest {
                                var: var_name,
                                crdt_bytes,
                            })
                            .await;
                    });
                }
            }
        }
    }

    pub fn members(&self) -> HashSet<String> {
        let inner = self.store.inner.lock().unwrap();

        if let Some(AnyReplica::LWWSet(replica)) = inner.replicas.get(&self.name) {
            replica.members()
        } else {
            HashSet::new()
        }
    }
}

impl Default for RemoteStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RemoteStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner {
                replicas: HashMap::new(),
                tx: None,
                connection_handle: None,
                client: None,
            })),
            hlc: Arc::new(HLC::default()),
        }
    }

    pub async fn connect(&self, addr: &str) -> Result<(), tonic::transport::Error> {
        let client = CrdtServiceClient::connect(addr.to_string()).await?;
        let (tx, rx) = mpsc::channel::<SyncStreamRequest>(16);

        let inner_arc = Arc::clone(&self.inner);
        let hlc_clone = Arc::clone(&self.hlc);

        let stream_client = client.clone();
        let handle = tokio::spawn(async move {
            Self::handle_connection(stream_client, inner_arc, rx, hlc_clone).await;
        });

        let mut inner = self.inner.lock().unwrap();
        inner.tx = Some(tx.clone());
        inner.client = Some(client);
        inner.connection_handle = Some(handle);

        // Sync existing variables
        for (var, replica) in inner.replicas.iter() {
            if let Ok(crdt_bytes) = bincode::serialize(&replica.as_crdt_ref()) {
                let tx_clone = tx.clone();
                let var_name = var.clone();

                tokio::spawn(async move {
                    let _ = tx_clone
                        .send(SyncStreamRequest {
                            var: var_name,
                            crdt_bytes,
                        })
                        .await;
                });
            }
        }

        Ok(())
    }

    pub fn disconnect(&self) {
        let mut inner = self.inner.lock().unwrap();

        if let Some(handle) = inner.connection_handle.take() {
            handle.abort();
        }

        inner.tx = None;
        inner.client = None;
    }

    pub fn is_connected(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.connection_handle.is_some()
    }

    pub async fn create(&self, name: &str, kind: CrdtKind) -> Result<SharedVariable, StoreError> {
        let new_crdt = match kind {
            CrdtKind::GCounter => AnyReplica::GCounter(GCounterReplica::new()),
            CrdtKind::LWWSet => {
                AnyReplica::LWWSet(LWWSetReplica::<String>::new(self.hlc.clone(), LWWBias::Add))
            }
        };

        let crdt_bytes = match bincode::serialize(&new_crdt.as_crdt_ref()) {
            Ok(bytes) => bytes,
            Err(_) => return Err(StoreError::TypeMismatch),
        };

        let mut inner = self.inner.lock().unwrap();

        if inner.replicas.contains_key(name) {
            return Err(StoreError::AlreadyExists);
        }

        inner.replicas.insert(name.to_string(), new_crdt);

        if let Some(client) = &inner.client {
            let mut client_clone = client.clone();
            let var_name = name.to_string();

            tokio::spawn(async move {
                let req = CreateVariableRequest {
                    var: var_name,
                    crdt_bytes,
                };

                let _ = client_clone.create_variable(req).await;
            });
        }

        // We need to drop here so that self.get works,
        // otherwise we end up with a deadlock
        drop(inner);

        Ok(self.get(name).unwrap())
    }

    pub fn get(&self, name: &str) -> Option<SharedVariable> {
        let inner = self.inner.lock().unwrap();

        if let Some(replica) = inner.replicas.get(name) {
            match replica {
                AnyReplica::GCounter(_) => Some(SharedVariable::Counter(SharedCounter {
                    name: name.to_string(),
                    // Cloning self is fine because we just store Arcs to other structs.
                    // Cloning Arcs is extremely cheap as they are just fancy pointers.
                    store: self.clone(),
                })),
                AnyReplica::LWWSet(_) => Some(SharedVariable::Set(SharedSet {
                    name: name.to_string(),
                    store: self.clone(),
                })),
            }
        } else {
            None
        }
    }

    pub fn list(&self) -> Vec<(String, CrdtKind)> {
        let inner = self.inner.lock().unwrap();

        inner
            .replicas
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

    async fn handle_connection(
        mut client: CrdtServiceClient<Channel>,
        state: Arc<Mutex<StoreInner>>,
        rx: mpsc::Receiver<SyncStreamRequest>,
        hlc: Arc<HLC>,
    ) {
        let req_stream = ReceiverStream::new(rx);
        let response = match client.sync_stream(req_stream).await {
            Ok(r) => r,
            Err(_) => return,
        };
        let mut resp_stream = response.into_inner();

        while let Some(Ok(received)) = resp_stream.next().await {
            let var_name = received.var;

            if let Ok(deserialized) = bincode::deserialize::<AnyCrdt>(&received.crdt_bytes) {
                let mut inner = state.lock().unwrap();

                match inner.replicas.entry(var_name.clone()) {
                    Entry::Occupied(mut o) => {
                        if let Err(crdt) = o.get_mut().merge(deserialized) {
                            eprintln!(
                                "Type mismatch for {}, replacing with server state",
                                var_name
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
    }
}
