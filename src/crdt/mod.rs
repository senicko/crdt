pub mod enums;
pub mod g_counter;
pub mod lww_set;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrdtKind {
    GCounter,
    LWWSet,
}

pub trait Crdt {
    type Struct;

    /// Synchronizes with the other CRDT state.
    fn merge(&mut self, other: &Self::Struct);
}
