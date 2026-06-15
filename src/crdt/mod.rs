pub mod enums;
pub mod g_counter;
pub mod lww_set;

pub trait Crdt {
    type Struct;

    /// Synchronizes with the other CRDT state.
    fn merge(&mut self, other: &Self::Struct);
}
