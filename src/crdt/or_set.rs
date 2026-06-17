use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::crdt::Crdt;

#[derive(Eq, PartialEq, Hash, Clone, Debug, Serialize, Deserialize)]
pub struct Tag {
    uuid: String,
    counter: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ORSet<T>
where
    T: Clone + Eq + Hash,
{
    elements: HashMap<T, HashSet<Tag>>,
    removed: HashMap<T, HashSet<Tag>>,
}

impl<T> ORSet<T>
where
    T: Hash + Eq + Clone,
{
    pub fn new() -> Self {
        Self {
            elements: HashMap::<T, HashSet<Tag>>::new(),
            removed: HashMap::<T, HashSet<Tag>>::new(),
        }
    }

    pub fn add(&mut self, element: T, tag: Tag) {
        self.elements
            .entry(element)
            .or_insert_with(HashSet::new)
            .insert(tag);
    }

    pub fn remove(&mut self, element: &T) {
        if let Some(tags) = self.elements.get(element) {
            let tags_to_remove = tags.clone();

            self.removed
                .entry(element.clone())
                .or_insert_with(HashSet::new)
                .extend(tags_to_remove);
        }
    }

    pub fn is_member(&self, element: &T) -> bool {
        if let Some(elements) = self.elements.get(element) {
            if let Some(removed) = self.removed.get(element) {
                elements.difference(removed).count() > 0
            } else {
                !elements.is_empty()
            }
        } else {
            false
        }
    }

    pub fn members(&self) -> HashSet<T> {
        let mut members = HashSet::new();

        for element in self.elements.keys() {
            if self.is_member(element) {
                members.insert(element.clone());
            }
        }

        members
    }
}

impl<T> Crdt for ORSet<T>
where
    T: Hash + Eq + Clone,
{
    type Struct = ORSet<T>;
    fn merge(&mut self, other: &Self::Struct) {
        for (element, tags) in &other.elements {
            self.elements
                .entry(element.clone())
                .or_insert_with(HashSet::new)
                .extend(tags.clone());
        }

        for (element, tags) in &other.removed {
            self.removed
                .entry(element.clone())
                .or_insert_with(HashSet::new)
                .extend(tags.clone());
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ORSetReplica<T>
where
    T: Eq + Hash + Clone,
{
    pub id: String,
    pub counter: u64,
    pub crdt: ORSet<T>,
}

impl<T> ORSetReplica<T>
where
    T: Eq + Hash + Clone,
{
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            counter: 0,
            crdt: ORSet::new(),
        }
    }

    pub fn add(&mut self, element: T) {
        self.counter += 1;
        let tag = Tag {
            uuid: self.id.clone(),
            counter: self.counter,
        };
        self.crdt.add(element, tag);
    }

    pub fn remove(&mut self, element: &T) {
        self.crdt.remove(element);
    }

    pub fn members(&self) -> HashSet<T> {
        self.crdt.members()
    }
}

impl<T> Crdt for ORSetReplica<T>
where
    T: Hash + Eq + Clone,
{
    type Struct = ORSet<T>;
    fn merge(&mut self, other: &Self::Struct) {
        self.crdt.merge(other);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_add_and_remove() {
        let mut replica = ORSetReplica::new();
        let item = "apple".to_string();

        replica.add(item.clone());
        assert!(replica.members().contains(&item));

        replica.remove(&item);
        assert!(!replica.members().contains(&item));
    }

    #[test]
    fn test_readd_after_remove() {
        let mut replica = ORSetReplica::new();
        let item = "banana".to_string();

        replica.add(item.clone());
        replica.remove(&item);
        assert!(!replica.members().contains(&item));

        replica.add(item.clone());
        assert!(replica.members().contains(&item));
    }

    #[test]
    fn test_concurrent_add_wins() {
        let mut replica_a = ORSetReplica::new();
        let mut replica_b = ORSetReplica::new();
        let item = "cherry".to_string();

        replica_a.add(item.clone());
        replica_b.add(item.clone());

        replica_a.remove(&item);
        assert!(!replica_a.members().contains(&item));

        replica_a.merge(&replica_b.crdt);

        assert!(replica_a.members().contains(&item));
    }

    #[test]
    fn test_merge_commutativity() {
        let mut replica_a = ORSetReplica::new();
        let mut replica_b = ORSetReplica::new();
        let item1 = "mango".to_string();
        let item2 = "kiwi".to_string();

        replica_a.add(item1.clone());
        replica_b.add(item2.clone());

        let mut merged_a = replica_a.clone();
        merged_a.merge(&replica_b.crdt);

        let mut merged_b = replica_b.clone();
        merged_b.merge(&replica_a.crdt);

        assert_eq!(merged_a.members(), merged_b.members());
        assert!(merged_a.members().contains(&item1));
        assert!(merged_a.members().contains(&item2));
    }

    #[test]
    fn test_idempotent_merge() {
        let mut replica_a = ORSetReplica::new();
        let item = "grape".to_string();

        replica_a.add(item.clone());

        replica_a.merge(&replica_a.crdt.clone());

        assert!(replica_a.members().contains(&item));
        assert_eq!(replica_a.members().len(), 1);
    }
}
