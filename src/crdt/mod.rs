pub mod enums;
pub mod g_counter;
pub mod lww_set;
pub mod or_set;
pub mod rga;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrdtKind {
    GCounter,
    LWWSet,
    ORSet,
    RGA,
}

pub trait Crdt {
    type Struct;

    /// Synchronizes with the other CRDT state.
    fn merge(&mut self, other: &Self::Struct);
}
