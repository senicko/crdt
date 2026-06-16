use crate::crdt::{
    Crdt,
    g_counter::{GCounter, GCounterReplica},
    lww_set::{LWWSet, LWWSetReplica},
};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::Arc;
use uhlc::HLC;

// Will be used to serialize/deserialize to correct crdt
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum AnyCrdt {
    GCounter(GCounter),
    LWWSet(LWWSet<String>),
}

#[derive(Debug, Serialize)]
pub(crate) enum AnyCrdtRef<'a> {
    GCounter(&'a GCounter),
    LWWSet(&'a LWWSet<String>),
}

pub(crate) enum AnyReplica {
    GCounter(GCounterReplica),
    LWWSet(LWWSetReplica<String>),
}

impl AnyReplica {
    pub(crate) fn from_crdt(crdt: AnyCrdt, hlc: Arc<HLC>) -> Self {
        match crdt {
            AnyCrdt::GCounter(c) => AnyReplica::GCounter(GCounterReplica {
                crdt: c,
                ..Default::default()
            }),
            AnyCrdt::LWWSet(c) => AnyReplica::LWWSet(LWWSetReplica { hlc, crdt: c }),
        }
    }

    pub(crate) fn merge(&mut self, crdt: AnyCrdt) -> Result<(), AnyCrdt> {
        match (self, crdt) {
            (AnyReplica::GCounter(replica), AnyCrdt::GCounter(c)) => {
                replica.merge(&c);
                Ok(())
            }
            (AnyReplica::LWWSet(replica), AnyCrdt::LWWSet(c)) => {
                replica.merge(&c);
                Ok(())
            }
            (_, c) => Err(c),
        }
    }

    pub(crate) fn as_crdt_ref(&self) -> AnyCrdtRef<'_> {
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
