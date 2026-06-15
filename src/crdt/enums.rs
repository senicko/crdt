use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::crdt::{
    g_counter::{GCounter, GCounterReplica},
    lww_set::{LWWSet, LWWSetReplica},
};

// Will be used to serialize/deserialize to correct crdt
#[derive(Debug, Serialize, Deserialize)]
pub enum CrdtBytesContainer {
    GCounter(GCounter),
    LWWSet(LWWSet<String>),
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
    pub fn try_inc(&mut self, delta: u64) -> Result<(), &'static str> {
        match self {
            CrdtState::GCounter(counter) => {
                counter.inc(delta);
                Ok(())
            }
            _ => Err("'Inc' operation is only supported by G-Counter"),
        }
    }

    pub fn try_add(&mut self, element: String) -> Result<(), &'static str> {
        match self {
            CrdtState::LWWSet(set) => {
                set.add(element);
                Ok(())
            }
            _ => Err("'Add' operation is only supported by LWW-Set"),
        }
    }

    pub fn try_remove(&mut self, element: String) -> Result<(), &'static str> {
        match self {
            CrdtState::LWWSet(set) => {
                set.remove(element);
                Ok(())
            }
            _ => Err("'Add' operation is only supported by LWW-Set"),
        }
    }

    pub fn value(&self) {
        match self {
            CrdtState::GCounter(counter) => println!("Counter Value: {}", counter.value()),
            CrdtState::LWWSet(set) => println!("Set Members: {:#?}", set.members()),
        }
    }
}
