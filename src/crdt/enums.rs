use crate::crdt::{g_counter::GCounter, lww_set::LWWSet};
use serde::{Deserialize, Serialize};

// Will be used to serialize/deserialize to correct crdt
#[derive(Debug, Serialize, Deserialize)]
pub enum CrdtBytesContainer {
    GCounter(GCounter),
    LWWSet(LWWSet<String>),
}
