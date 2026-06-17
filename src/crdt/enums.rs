use crate::crdt::{
    Crdt,
    g_counter::{GCounter, GCounterReplica},
    lww_set::{LWWSet, LWWSetReplica},
    or_set::{ORSet, ORSetReplica},
    rga::{RGA, RGAReplica},
};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::Arc;
use uhlc::HLC;

// Will be used to serialize/deserialize to correct crdt
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum AnyCrdt {
    GCounter(GCounter),
    LWWSet(LWWSet<String>),
    ORSet(ORSet<String>),
    RGA(RGA<String>),
}

#[derive(Debug, Serialize)]
pub(crate) enum AnyCrdtRef<'a> {
    GCounter(&'a GCounter),
    LWWSet(&'a LWWSet<String>),
    ORSet(&'a ORSet<String>),
    RGA(&'a RGA<String>),
}

pub(crate) enum AnyReplica {
    GCounter(GCounterReplica),
    LWWSet(LWWSetReplica<String>),
    ORSet(ORSetReplica<String>),
    RGA(RGAReplica<String>),
}

impl AnyReplica {
    pub(crate) fn from_crdt(crdt: AnyCrdt, hlc: Arc<HLC>) -> Self {
        match crdt {
            AnyCrdt::GCounter(c) => AnyReplica::GCounter(GCounterReplica {
                crdt: c,
                ..Default::default()
            }),
            AnyCrdt::LWWSet(c) => AnyReplica::LWWSet(LWWSetReplica { hlc, crdt: c }),
            AnyCrdt::ORSet(c) => AnyReplica::ORSet(ORSetReplica {
                id: uuid::Uuid::new_v4().to_string(),
                counter: 0,
                crdt: c,
            }),
            AnyCrdt::RGA(c) => AnyReplica::RGA(RGAReplica {
                id: uuid::Uuid::new_v4().to_string(),
                hlc,
                crdt: c,
            }),
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
            (AnyReplica::ORSet(replica), AnyCrdt::ORSet(c)) => {
                replica.merge(&c);
                Ok(())
            }
            (AnyReplica::RGA(replica), AnyCrdt::RGA(c)) => {
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
            AnyReplica::RGA(replica) => AnyCrdtRef::RGA(&replica.crdt),
            AnyReplica::ORSet(replica) => AnyCrdtRef::ORSet(&replica.crdt),
        }
    }

    pub(crate) fn as_any_crdt(&self) -> AnyCrdt {
        match self {
            AnyReplica::GCounter(replica) => AnyCrdt::GCounter(replica.crdt.clone()),
            AnyReplica::LWWSet(replica) => AnyCrdt::LWWSet(replica.crdt.clone()),
            AnyReplica::ORSet(replica) => AnyCrdt::ORSet(replica.crdt.clone()),
            AnyReplica::RGA(replica) => AnyCrdt::RGA(replica.crdt.clone()),
        }
    }
}

impl Display for AnyReplica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyReplica::GCounter(_) => write!(f, "GCounter"),
            AnyReplica::LWWSet(_) => write!(f, "LWWSet"),
            AnyReplica::ORSet(_) => write!(f, "ORSet"),
            AnyReplica::RGA(_) => write!(f, "RGA"),
        }
    }
}
