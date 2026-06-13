use super::Crdt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GCounter {
    counters: HashMap<String, u64>,
}

impl GCounter {
    pub fn new() -> Self {
        GCounter {
            counters: HashMap::new(),
        }
    }

    pub fn inc(&mut self, node_id: &str, delta: u64) {
        let counter = self.counters.entry(node_id.to_string()).or_insert(0);
        *counter += delta;
    }

    pub fn value(&self) -> u64 {
        self.counters.values().sum()
    }
}

impl Crdt for GCounter {
    type Struct = GCounter;

    fn merge(&mut self, other: &Self::Struct) {
        for (node_id, remote_counter) in other.counters.iter() {
            let local_counter = self.counters.entry(node_id.to_string()).or_insert(0);
            *local_counter = std::cmp::max(*local_counter, *remote_counter);
        }
    }
}

#[derive(Debug)]
pub struct GCounterReplica {
    id: String,
    pub crdt: GCounter,
}

impl GCounterReplica {
    pub fn new() -> Self {
        GCounterReplica {
            id: Uuid::new_v4().to_string(),
            crdt: GCounter::new(),
        }
    }

    pub fn inc(&mut self, delta: u64) {
        self.crdt.inc(&self.id, delta);
    }

    pub fn value(&self) -> u64 {
        self.crdt.value()
    }
}

impl Crdt for GCounterReplica {
    type Struct = GCounter;

    fn merge(&mut self, other: &Self::Struct) {
        self.crdt.merge(other);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_raw_gcounter_sync() {
        let g1_node = Uuid::new_v4().to_string();
        let mut g1 = GCounter::new();

        let g2_node = Uuid::new_v4().to_string();
        let mut g2 = GCounter::new();

        let g1_update = 10;
        let g2_update = 12;

        g1.inc(&g1_node, g1_update);
        g2.inc(&g2_node, g2_update);

        g1.merge(&g2);
        g2.merge(&g1);

        assert_eq!(g1.value(), g1_update + g2_update);
        assert_eq!(g2.value(), g1_update + g2_update);
    }

    #[test]
    fn test_gcounter_replica_sync() {
        let mut g1 = GCounterReplica::new();
        let mut g2 = GCounterReplica::new();

        let g1_update = 10;
        let g2_update = 12;

        g1.inc(g1_update);
        g2.inc(g2_update);

        g1.merge(&g2.crdt);
        g2.merge(&g1.crdt);

        assert_eq!(g1.value(), g1_update + g2_update);
        assert_eq!(g2.value(), g1_update + g2_update);
    }
}
