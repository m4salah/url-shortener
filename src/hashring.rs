use std::{
    collections::BTreeMap,
    hash::{DefaultHasher, Hash, Hasher},
};

use sha2::{Digest, Sha256};

/// A struct representing the consistent hash ring with virtual nodes
pub struct HashRing<T: Clone> {
    ring: BTreeMap<u64, T>, // Hash ring, where key is the hash and value is the shard ID
    virtual_nodes: usize,   // Number of virtual nodes per physical shard
}

impl<T: Clone> HashRing<T> {
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
            let hash = hash_shard(&virtual_shard);
            println!("Adding shard: {}:{}", virtual_shard, hash);
            self.ring.insert(hash, value.clone());
        }
    }

    /// Get the physical shard corresponding to the given key (short ID)
    pub fn get_shard(&self, key: &str) -> T {
        let hash = hash_str(key);
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
}

/// Hashes a shard (or virtual node) and returns the hash as u64
fn hash_shard(shard: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    shard.hash(&mut hasher);
    hasher.finish()
}

/// Hashes a string (typically the short ID) and returns the hash as u64
fn hash_str(s: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(s);
    let result = hasher.finalize();
    u64::from_be_bytes(result[0..8].try_into().unwrap()) // Return the first 8 bytes as u64
}
