use serde::{Deserialize, Serialize};

use crate::crdt::{g_counter::GCounter, lww_set::LWWSet};

pub mod g_counter;
pub mod lww_set;

#[derive(Debug, Serialize, Deserialize)]
pub enum CrdtBytesContainer {
    GCounter(GCounter),
    LWWSet(LWWSet<String>),
}

pub trait Crdt {
    type Struct;

    /// Synchronizes with the other CRDT state.
    fn merge(&mut self, other: &Self::Struct);
}
