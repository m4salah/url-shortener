use std::{
    collections::BTreeMap,
    hash::{DefaultHasher, Hash, Hasher},
};

/// A struct representing the consistent hash ring with virtual nodes
pub struct HashRing<T: Clone> {
    ring: BTreeMap<u64, T>, // Hash ring, where key is the hash and value is the shard ID
    virtual_nodes: usize,   // Number of virtual nodes per physical shard
}

impl<T> HashRing<T>
where
    T: Clone,
{
    /// Create a new consistent hash ring with the given shards and number of virtual nodes per shard
    pub fn new(virtual_nodes: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            virtual_nodes,
        }
    }

    pub fn add(&mut self, shard: u64, value: T) {
        for i in 0..self.virtual_nodes {
            // Create a virtual node by appending the index to the shard name
            let virtual_shard = format!("{}-VN{}", shard, i);
            let hash = Self::hash(&virtual_shard);
            println!("Adding shard: {}:{}", virtual_shard, hash);
            self.ring.insert(hash, value.clone());
        }
    }

    /// Get the physical shard corresponding to the given key (short ID)
    pub fn get_shard(&self, key: &str) -> T {
        let hash = Self::hash(key);
        println!("Retrieving pool from shard: {}", hash);
        // Find the first shard hash >= hashed key
        let shard = self
            .ring
            .range(hash..)
            .next()
            .unwrap_or(self.ring.iter().next().unwrap()) // Wrap around if necessary
            .1;
        shard.to_owned()
    }

    /// Hashes a shard (or virtual node) and returns the hash as u64
    fn hash(shard: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        shard.hash(&mut hasher);
        hasher.finish()
    }
}
