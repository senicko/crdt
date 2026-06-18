# Conflict-free Replicated Data Types (CRDTs)

## Abstract
This project provides a implementation of various Conflict-free Replicated Data Types (CRDTs) in Rust. CRDTs are a family of data structures designed to achieve strong eventual consistency in distributed systems without requiring coordination (such as distributed locks or consensus protocols) between replicas. By mathematically ensuring that concurrent updates can be merged commutatively, associatively, and idempotently, CRDTs allow systems to remain highly available and partition-tolerant while guaranteeing that all replicas will eventually converge to the same state.

## Introduction
In distributed computing, the CAP theorem posits that a system can only guarantee two of the following three properties: Consistency, Availability, and Partition tolerance. Systems requiring high availability and offline capabilities often sacrifice strong consistency in favor of **eventual consistency**.

CRDTs provide a framework for achieving **Strong Eventual Consistency (SEC)**. SEC guarantees that any two nodes that have received the same set of updates will be in the exact same state, regardless of the order in which those updates were received. This project implements **State-based CRDTs (CvRDTs)**, where replicas disseminate their full (or delta) local states, and a deterministic `merge` function computes the lowest upper bound (join) in a join-semilattice.

## Implemented Data Types

### 1. G-Counter (Grow-Only Counter)
The G-Counter is a state-based CRDT that only allows increments. 
- **Structure**: It maintains a payload mapping from each node's unique identifier to its local counter value (`HashMap<String, u64>`).
- **Update**: A node only increments its own entry in the map.
- **Query**: The value of the counter is the sum of all values in the map.
- **Merge**: The merge function iterates over the keys and takes the `max` of the local and remote values for each node. This operation forms a monotonic join-semilattice.

### 2. LWW-Set (Last-Writer-Wins Element Set)
The LWW-Set resolves concurrent modifications by relying on timestamps. This implementation utilizes **Hybrid Logical Clocks (HLC)** to generate sortable, monotonic timestamps without relying purely on synchronized physical clocks.
- **Structure**: Maintains two sets: an Add-Set and a Remove-Set, each mapping an element to the timestamp of its operation.
- **Query**: An element is considered a member of the set if it is in the Add-Set, and its timestamp in the Add-Set is greater than its timestamp in the Remove-Set. 
- **Bias**: To handle exact timestamp ties, the implementation supports configurable biases:
  - `Add-Bias`: In the event of a tie, the addition prevails.
  - `Remove-Bias`: In the event of a tie, the removal prevails.
- **Merge**: Takes the element-wise maximum timestamp for both the Add-Set and the Remove-Set.

### 3. OR-Set (Observed-Remove Set)
The OR-Set allows elements to be added and removed repeatedly without the use of timestamps, resolving the commutativity issues of a standard set by ensuring every insertion is unique.
- **Structure**: Every time an element is added, a unique `Tag` (combining a UUID and a monotonic counter) is generated. The set maintains a mapping from elements to their active tags, and a separate tombstone set for removed tags.
- **Update**: Removing an element moves all currently *observed* tags for that element into the tombstone set.
- **Query**: An element is a member if the set of its active tags is not a subset of its removed tags (i.e., there is at least one tag that hasn't been tombstoned).
- **Merge**: Computes the set union for both the active elements map and the removed tombstones map.

### 4. RGA (Replicated Growable Array)
RGA is a Sequence CRDT used for collaborative text editing and ordered lists. It ensures that concurrent insertions at the same position are totally ordered.
- **Structure**: Elements are organized as a tree (often linearized into a linked list). Each node has a unique ID (generated from the HLC timestamp and replica ID to ensure total ordering) and points to its parent (the element it was inserted after).
- **Update**: Insertions create a new node referencing a parent. Deletions are handled via tombstones (`is_deleted` flag) rather than physical removal, preserving the topological structure for subsequent insertions.
- **Traversal**: The tree is traversed by visiting siblings ordered by their timestamps in descending order, ensuring a deterministic sequence across all replicas.
- **Merge**: Computes the union of the node sets, carrying over the tombstone flags if a node was deleted remotely.

## System Architecture & Demonstration

To demonstrate how CRDTs operate in a distributed environment, the project includes an interactive client-server architecture. It simulates a distributed system where multiple clients can independently mutate local replicas of CRDTs and synchronize their state over the network.

- **`crdt_server`**: Acts as a central node or peer that maintains CRDT states and facilitates synchronization.
- **`crdt_client`**: An interactive REPL that allows users to instantiate new CRDTs, perform local mutations, and explicitly trigger synchronizations.

This architecture clearly illustrates how replicas can diverge while disconnected, yet cleanly and deterministically converge to the exact same state upon merging.

## Running the Examples

1. **Start the Server:**
   ```bash
   cargo run --example crdt_server
   ```

2. **Start the Interactive Client (in a separate terminal):**
   ```bash
   cargo run --example crdt_client
   ```

### Client REPL Usage

Inside the client REPL, you can execute the following commands to observe CRDTs in action:

- **Create a new CRDT variable:**
  ```text
  New my_counter GCounter
  New my_set ORSet
  New my_array RGA
  ```
- **Interact with the CRDTs:**
  ```text
  my_counter Inc
  my_counter Value

  my_set Add "Apple"
  my_set Remove "Apple"
  my_set Value

  my_array Insert "Hello" head
  my_array Value
  ```
- **Network commands:**
  ```text
  Status      # Check connection to the server
  Disconnect  # Simulate network partition
  Connect     # Reconnect and synchronize states
  ```
