use super::Crdt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uhlc::HLC;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RgaNode<T> {
    pub id: String,
    pub parent: Option<String>,
    pub value: T,
    pub is_deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RGA<T> {
    pub nodes: HashMap<String, RgaNode<T>>,
}

impl<T: Clone> Default for RGA<T> {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }
}

impl<T: Clone> RGA<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, parent: Option<String>, id: String, value: T) {
        if !self.nodes.contains_key(&id) {
            self.nodes.insert(
                id.clone(),
                RgaNode {
                    id,
                    parent,
                    value,
                    is_deleted: false,
                },
            );
        }
    }

    pub fn remove(&mut self, id: &str) {
        if let Some(node) = self.nodes.get_mut(id) {
            node.is_deleted = true;
        }
    }

    pub fn to_vec(&self) -> Vec<(String, T)> {
        let mut result = Vec::new();
        self.traverse(None, &mut result);
        result
    }

    fn traverse(&self, current: Option<String>, result: &mut Vec<(String, T)>) {
        if let Some(ref id) = current {
            if let Some(node) = self.nodes.get(id) {
                if !node.is_deleted {
                    result.push((node.id.clone(), node.value.clone()));
                }
            }
        }

        // NOTE: This is highly inefficient. We should be using some
        //       doubly linked list approach or a tree here.

        let mut children: Vec<_> = self
            .nodes
            .values()
            .filter(|n| n.parent == current)
            .collect();

        children.sort_by(|a, b| b.id.cmp(&a.id));

        for child in children {
            self.traverse(Some(child.id.clone()), result);
        }
    }
}

impl<T: Clone> Crdt for RGA<T> {
    type Struct = RGA<T>;

    fn merge(&mut self, other: &Self::Struct) {
        for (id, remote_node) in other.nodes.iter() {
            let local_node = self
                .nodes
                .entry(id.clone())
                .or_insert_with(|| remote_node.clone());
            if remote_node.is_deleted {
                local_node.is_deleted = true;
            }
        }
    }
}

use std::fmt;

pub struct RGAReplica<T> {
    pub id: String,
    pub hlc: Arc<HLC>,
    pub crdt: RGA<T>,
}

impl<T: fmt::Debug> fmt::Debug for RGAReplica<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RGAReplica")
            .field("id", &self.id)
            .field("crdt", &self.crdt)
            .finish()
    }
}

impl<T: Clone> RGAReplica<T> {
    pub fn new(hlc: Arc<HLC>) -> Self {
        Self {
            id: Uuid::new_v4().as_simple().to_string()[0..8].to_string(),
            hlc,
            crdt: RGA::new(),
        }
    }

    pub fn insert(&mut self, parent: Option<String>, value: T) -> String {
        let ts = self.hlc.new_timestamp();
        let time = ts.get_time().as_u64();
        // Generate a sortable ID using time and the short replica ID
        let id = format!("{:016x}-{}", time, self.id);
        self.crdt.insert(parent, id.clone(), value);
        id
    }

    pub fn remove(&mut self, id: &str) {
        self.crdt.remove(id);
    }

    pub fn to_vec(&self) -> Vec<(String, T)> {
        self.crdt.to_vec()
    }
}

impl<T: Clone> Crdt for RGAReplica<T> {
    type Struct = RGA<T>;

    fn merge(&mut self, other: &Self::Struct) {
        self.crdt.merge(other);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uhlc::HLC;

    #[test]
    fn test_rga_insert_and_to_vec() {
        let hlc = Arc::new(HLC::default());
        let mut replica = RGAReplica::new(hlc);

        let id1 = replica.insert(None, "A");
        let id2 = replica.insert(Some(id1.clone()), "B");
        let _id3 = replica.insert(Some(id2.clone()), "C");

        let vec = replica.to_vec();
        assert_eq!(vec.len(), 3);
        assert_eq!(vec[0].1, "A");
        assert_eq!(vec[1].1, "B");
        assert_eq!(vec[2].1, "C");
    }

    #[test]
    fn test_rga_concurrent_insert() {
        let hlc1 = Arc::new(HLC::default());
        let hlc2 = Arc::new(HLC::default());

        let mut replica1 = RGAReplica::new(hlc1);
        let mut replica2 = RGAReplica::new(hlc2);

        let id_root = replica1.insert(None, "Root");
        replica2.merge(&replica1.crdt);

        // Concurrent inserts after Root
        let _id1 = replica1.insert(Some(id_root.clone()), "A");
        let _id2 = replica2.insert(Some(id_root.clone()), "B");

        replica1.merge(&replica2.crdt);
        replica2.merge(&replica1.crdt);

        let vec1 = replica1.to_vec();
        let vec2 = replica2.to_vec();

        assert_eq!(vec1.len(), 3);
        assert_eq!(vec2.len(), 3);

        // They must converge to the same order
        assert_eq!(vec1[1].1, vec2[1].1);
        assert_eq!(vec1[2].1, vec2[2].1);
    }

    #[test]
    fn test_rga_remove() {
        let hlc = Arc::new(HLC::default());
        let mut replica = RGAReplica::new(hlc);

        let id1 = replica.insert(None, "A");
        let id2 = replica.insert(Some(id1.clone()), "B");
        let _id3 = replica.insert(Some(id2.clone()), "C");

        replica.remove(&id2);

        let vec = replica.to_vec();
        assert_eq!(vec.len(), 2);
        assert_eq!(vec[0].1, "A");
        assert_eq!(vec[1].1, "C");
    }
}
