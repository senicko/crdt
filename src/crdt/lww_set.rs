use super::Crdt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::Arc,
};
use uhlc::{HLC, Timestamp};

#[derive(Debug, Serialize, Deserialize)]
pub enum LwwBias {
    Remove,
    Add,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LWWSet<T>
where
    T: Eq + Hash,
{
    bias: LwwBias,
    add_set: HashMap<T, Timestamp>,
    remove_set: HashMap<T, Timestamp>,
}

impl<T> LWWSet<T>
where
    T: Eq + Hash,
{
    pub fn new(bias: LwwBias) -> Self {
        LWWSet {
            bias,
            add_set: HashMap::new(),
            remove_set: HashMap::new(),
        }
    }

    pub fn add(&mut self, element: T, timestamp: Timestamp) {
        self.add_set.insert(element, timestamp);
    }

    pub fn remove(&mut self, element: T, timestamp: Timestamp) {
        self.remove_set.insert(element, timestamp);
    }

    pub fn is_member(&self, element: &T) -> bool {
        let add_ts = self.add_set.get(element);
        let remove_ts = self.remove_set.get(element);

        match (add_ts, remove_ts) {
            (Some(add_time), Some(remove_time)) => match self.bias {
                LwwBias::Add => add_time >= remove_time,
                LwwBias::Remove => add_time > remove_time,
            },
            (Some(_), None) => true,
            _ => false,
        }
    }
}

impl<T> LWWSet<T>
where
    T: Eq + Hash + Clone,
{
    pub fn members(&self) -> HashSet<T> {
        let mut members = HashSet::new();

        for element in self.add_set.keys() {
            if self.is_member(element) {
                members.insert(element.clone());
            }
        }

        members
    }
}

fn merge_with_timestamp<T>(target: &mut HashMap<T, Timestamp>, with: &HashMap<T, Timestamp>)
where
    T: Eq + Hash + Clone,
{
    for (k, other_timestamp) in with.iter() {
        target
            .entry(k.clone())
            .and_modify(|our_timestamp| {
                if *our_timestamp < *other_timestamp {
                    *our_timestamp = *other_timestamp;
                }
            })
            .or_insert(*other_timestamp);
    }
}

impl<T> Crdt for LWWSet<T>
where
    T: Eq + Hash + Clone,
{
    type Struct = LWWSet<T>;

    fn merge(&mut self, other: &Self::Struct) {
        merge_with_timestamp(&mut self.add_set, &other.add_set);
        merge_with_timestamp(&mut self.remove_set, &other.remove_set);
    }
}

pub struct LWWSetReplica<T>
where
    T: Eq + Hash + Clone,
{
    hlc: Arc<HLC>,
    pub lww_set: LWWSet<T>,
}

impl<T> LWWSetReplica<T>
where
    T: Eq + Hash + Clone,
{
    pub fn new(hlc: Arc<HLC>, bias: LwwBias) -> Self {
        LWWSetReplica {
            hlc,
            lww_set: LWWSet::new(bias),
        }
    }

    pub fn add(&mut self, element: T) {
        let ts = self.hlc.new_timestamp();
        self.lww_set.add(element, ts);
    }

    pub fn remove(&mut self, element: T) {
        let ts = self.hlc.new_timestamp();
        self.lww_set.remove(element, ts);
    }

    pub fn members(&self) -> HashSet<T> {
        self.lww_set.members()
    }
}

impl<T> Crdt for LWWSetReplica<T>
where
    T: Eq + Hash + Clone,
{
    type Struct = LWWSet<T>;

    fn merge(&mut self, other: &Self::Struct) {
        self.lww_set.merge(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uhlc::HLC;

    #[test]
    fn test_add_and_is_member() {
        let mut set = LWWSet::new(LwwBias::Add);
        let hlc = HLC::default();

        let ts = hlc.new_timestamp();
        set.add(1, ts);

        assert!(set.is_member(&1));
        assert!(!set.is_member(&2));

        let members = set.members();
        assert_eq!(members.len(), 1);
        assert!(members.contains(&1));
    }

    #[test]
    fn test_remove_higher_timestamp() {
        let mut set = LWWSet::new(LwwBias::Add);
        let hlc = HLC::default();

        let ts1 = hlc.new_timestamp();
        let ts2 = hlc.new_timestamp();

        set.add(1, ts1);
        set.remove(1, ts2);

        assert!(
            !set.is_member(&1),
            "Remove should win with a higher timestamp"
        );
    }

    #[test]
    fn test_add_higher_timestamp() {
        let mut set = LWWSet::new(LwwBias::Add);
        let hlc = HLC::default();

        let ts1 = hlc.new_timestamp();
        let ts2 = hlc.new_timestamp();

        set.remove(1, ts1);
        set.add(1, ts2);

        assert!(set.is_member(&1), "Add should win with a higher timestamp");
    }

    #[test]
    fn test_add_bias_resolution() {
        let mut set = LWWSet::new(LwwBias::Add);
        let hlc = HLC::default();

        let ts = hlc.new_timestamp();
        set.add(1, ts);
        set.remove(1, ts);

        assert!(
            set.is_member(&1),
            "Set is Add-biased; element should be a member on a tie"
        );
    }

    #[test]
    fn test_remove_bias_resolution() {
        let mut set = LWWSet::new(LwwBias::Remove);
        let hlc = HLC::default();

        let ts = hlc.new_timestamp();
        set.add(1, ts);
        set.remove(1, ts);

        assert!(
            !set.is_member(&1),
            "Set is Remove-biased; element should NOT be a member on a tie"
        );
    }

    #[test]
    fn test_merge_disjoint_sets() {
        let mut set1 = LWWSet::new(LwwBias::Add);
        let mut set2 = LWWSet::new(LwwBias::Add);
        let hlc = HLC::default();

        set1.add(1, hlc.new_timestamp());
        set2.add(2, hlc.new_timestamp());
        set1.merge(&set2);

        assert!(set1.is_member(&1));
        assert!(
            set1.is_member(&2),
            "Set 1 should have merged the disjoint element from Set 2"
        );
    }

    #[test]
    fn test_merge_conflict_resolution() {
        let mut set1 = LWWSet::new(LwwBias::Add);
        let mut set2 = LWWSet::new(LwwBias::Add);
        let hlc = HLC::default();

        let ts_early = hlc.new_timestamp();
        let ts_late = hlc.new_timestamp();

        set1.add(1, ts_early);
        set2.remove(1, ts_late);
        set1.merge(&set2);

        assert!(
            !set1.is_member(&1),
            "The later removal timestamp from Node 2 should win"
        );
    }

    #[test]
    fn test_replica_sync() {
        let hlc1 = Arc::new(HLC::default());
        let hlc2 = Arc::new(HLC::default());

        let mut replica1 = LWWSetReplica::new(hlc1, LwwBias::Remove);
        let mut replica2 = LWWSetReplica::new(hlc2, LwwBias::Remove);

        replica1.add(100);
        replica2.add(200);

        assert!(replica1.members().contains(&100));
        assert!(!replica1.members().contains(&200));

        replica1.merge(&replica2.lww_set);

        let merged_members = replica1.members();

        assert!(merged_members.contains(&100));
        assert!(
            merged_members.contains(&200),
            "Replica 1 should contain Replica 2's state after merge"
        );
    }
}
