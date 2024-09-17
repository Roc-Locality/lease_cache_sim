#![allow(dead_code)]
#![allow(clippy::needless_return)]
use abstract_cache::AccessResult;
use abstract_cache::CacheSim;
use abstract_cache::ObjIdTraits;
use rand::seq::SliceRandom;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::hash::Hash;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct TaggedObjectId<Tag: ObjIdTraits, Obj: ObjIdTraits>(pub Tag, pub Obj);
impl<Tag: ObjIdTraits, Obj: ObjIdTraits> Display for TaggedObjectId<Tag, Obj> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.0, self.1)
    }
}

impl<Tag: ObjIdTraits, Obj: ObjIdTraits> ObjIdTraits for TaggedObjectId<Tag, Obj> {}

/// A `LeaseCache` is a cache that associates objects with expiration times.
///
/// The cache maintains two main data structures:
/// - `expiring_map`: A `HashMap` that maps expiration times to sets of objects.
/// - `content_map`: A `HashMap` that maps objects to their expiration times.
///
/// The cache supports the following operations:
/// - `insert`: Adds an object to the cache with a specified lease (expiration time).
/// - `update`: Updates the lease of an existing object or inserts a new object if it doesn't exist.
/// - `contains`: Checks if an object is in the cache.
/// - `time_until_eviction`: Returns the time until an object is evicted from the cache.
/// - `remove`: Removes an object from the cache.
/// - `advance_time`: Advances the current time and evicts expired objects.
/// - `force_evict`: Randomly evicts an object from the cache.
///
/// The cache can also be configured with a maximum capacity, and it will evict objects to maintain this capacity.
///
/// # Type Parameters
/// - `Obj`: The type of objects stored in the cache. Must implement `ObjIdTraits`.
///
/// # Examples
///
/// ```
/// use lease_cache_sim::LeaseCache;
/// let mut lease_cache = LeaseCache::<usize>::new();
/// lease_cache.insert(1, 10);
/// assert!(lease_cache.contains(&1));
/// ```
#[derive(Clone)]
pub struct LeaseCache<Obj: ObjIdTraits> {
    //map from ref to (short_lease, long_lease, short_lease_prob)
    // pub(crate) lease_table: HashMap<Tag, (usize, usize, f64)>,
    expiring_map: HashMap<usize, HashSet<Obj>>,
    current_time: usize,
    content_map: HashMap<Obj, usize>, //map from ObjId to index in expiring_vec
    capacity: Option<usize>,
    // pub(crate) curr_expiring_index: usize,
    // pub(crate) cache_consumption: usize,
}
impl<Obj: ObjIdTraits> LeaseCache<Obj> {
    pub fn new() -> Self {
        LeaseCache {
            expiring_map: HashMap::new(),
            current_time: 0,
            content_map: HashMap::new(),
            capacity: None,
        }
    }

    pub fn insert(&mut self, obj_id: Obj, lease: usize) {
        let expiration = self.current_time + lease;
        self.expiring_map
            .entry(expiration)
            .or_default()
            .insert(obj_id.clone());
        self.content_map.insert(obj_id, expiration);
    }

    pub fn update(&mut self, obj_id: &Obj, lease: usize) -> AccessResult {
        self.advance_time();
        match self.content_map.get(obj_id) {
            Some(&old_expiration) => {
                self.remove_from_expiring_map(old_expiration, obj_id);
                if lease > 0 {
                    self.insert(obj_id.clone(), lease);
                } else {
                    self.content_map.remove(obj_id);
                }

                AccessResult::Hit
            }
            None => {
                if lease > 0 {
                    self.insert(obj_id.clone(), lease);
                }
                AccessResult::Miss
            }
        }
    }

    pub fn remove(&mut self, obj_id: &Obj) {
        if let Some(&expiration) = self.content_map.get(obj_id) {
            self.remove_from_expiring_map(expiration, obj_id);
            self.content_map.remove(obj_id);
        }
    }

    pub fn advance_time(&mut self) -> HashSet<Obj> {
        self.current_time += 1;
        if let Some(expiring_objects) = self.expiring_map.remove(&self.current_time) {
            for obj_id in &expiring_objects {
                self.content_map.remove(obj_id);
            }
            expiring_objects
        } else {
            HashSet::new()
        }
    }

    pub fn force_evict(&mut self) -> Obj {
        let keys: Vec<Obj> = self.content_map.keys().cloned().collect();
        if let Some(obj_id) = keys.choose(&mut rand::thread_rng()) {
            let expiration = *self.content_map.get(obj_id).unwrap();
            self.remove_from_expiring_map(expiration, obj_id);
            self.content_map.remove(obj_id);
            obj_id.clone()
        } else {
            panic!("Cache is empty; cannot evict.");
        }
    }

    pub fn get_cache_consumption(&self) -> usize {
        self.content_map.len()
    }

    //Helper Methods
    fn remove_from_expiring_map(&mut self, expiration: usize, obj_id: &Obj) {
        if let Some(set) = self.expiring_map.get_mut(&expiration) {
            set.remove(obj_id);
            if set.is_empty() {
                self.expiring_map.remove(&expiration);
            }
        }
    }

    // Utility functions
    pub fn contains(&self, obj_id: &Obj) -> bool {
        self.content_map.contains_key(obj_id)
    }

    pub fn time_until_eviction(&self, obj_id: &Obj) -> Option<usize> {
        self.content_map
            .get(obj_id)
            .map(|&expiration| expiration.saturating_sub(self.current_time))
    }
}

impl<Obj: ObjIdTraits> Default for LeaseCache<Obj> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Obj: ObjIdTraits> CacheSim<TaggedObjectId<usize, Obj>> for LeaseCache<Obj> {
    /// returns (total_access_count, miss_count)
    /// input is an iterator of TaggedObjectId<Lease, ObjId>
    fn cache_access(&mut self, access: TaggedObjectId<usize, Obj>) -> AccessResult {
        let TaggedObjectId(lease, obj_id) = access;
        let result = self.update(&obj_id, lease);
        if let Some(max_capacity) = self.capacity {
            while self.content_map.len() > max_capacity {
                self.force_evict();
            }
        }
        return result;
    }

    fn set_capacity(&mut self, capacity: usize) -> &mut Self {
        self.capacity = Some(capacity);
        self
    }
}

#[cfg(test)]

mod test {
    use super::*;

    #[test]
    fn test_time_till_eviction() {
        let mut lease_cache = LeaseCache::<usize>::new();
        lease_cache.insert(1, 1);
        lease_cache.insert(2, 2);
        lease_cache.insert(3, 3);
        assert_eq!(lease_cache.time_until_eviction(&1), Some(1));
        assert_eq!(lease_cache.time_until_eviction(&2), Some(2));
        assert_eq!(lease_cache.time_until_eviction(&3), Some(3));
        lease_cache.advance_time();

        println!("{:?}", lease_cache.time_until_eviction(&1));
        assert_eq!(lease_cache.time_until_eviction(&2), Some(1));
        assert_eq!(lease_cache.time_until_eviction(&3), Some(2));
    }

    #[test]
    fn test_lease_zero() {
        let mut lease_cache = LeaseCache::<usize>::new();
        lease_cache.update(&1, 2);
        lease_cache.update(&2, 0);
        assert_eq!(lease_cache.time_until_eviction(&1), Some(1));
        assert_eq!(lease_cache.time_until_eviction(&2), None);
        assert!(!lease_cache.content_map.contains_key(&2));
    }

    #[test]
    fn test_lease_cache_insert() {
        let mut lease_cache = LeaseCache::<usize>::new();
        lease_cache.insert(1, 1);
        lease_cache.insert(2, 2);
        lease_cache.insert(3, 3);
        assert!(lease_cache.content_map.contains_key(&1));
        assert!(lease_cache.content_map.contains_key(&2));
        assert!(lease_cache.content_map.contains_key(&3));
        let abs_index = lease_cache.content_map.get(&1).unwrap();
        assert!(lease_cache
            .expiring_map
            .get(abs_index)
            .unwrap()
            .contains(&1));
    }

    #[test]
    fn test_lease_cache_update() {
        let mut lease_cache = LeaseCache::<usize>::new();
        // Update the lease cache with obj_id 1 and index 1
        lease_cache.update(&1, 1);
        // Get the absolute index of obj_id 1
        let abs_index: usize = *lease_cache.content_map.get(&1).unwrap();
        assert!(lease_cache
            .expiring_map
            .get(&abs_index)
            .unwrap()
            .contains(&1));
        // Update the lease cache with obj_id 1 and new index 4
        lease_cache.update(&1, 4);
        assert_eq!(lease_cache.content_map.len(), 1);
        // Old index should no longer contain obj_id 1
        assert!(lease_cache
            .expiring_map
            .get(&abs_index)
            .unwrap_or(&HashSet::new())
            .is_empty());
        // New index should contain obj_id 1
        let abs_index_new = *lease_cache.content_map.get(&1).unwrap();
        assert!(lease_cache
            .expiring_map
            .get(&abs_index_new)
            .unwrap()
            .contains(&1));
    }

    #[test]
    fn test_lease_cache_dump_expiring() {
        let mut lease_cache = LeaseCache::<usize>::new();
        //test to make sure expiring objects are dumped correctly,
        //this means that each time we dump we see the objects that the correct
        //objects are expiring and the expiring index is incremented by one
        lease_cache.insert(1, 1);
        lease_cache.insert(2, 2);
        lease_cache.insert(3, 3);
        let mut expiring = lease_cache.advance_time();
        let mut expected = HashSet::new();
        expected.insert(1);
        assert_eq!(expiring, expected);
        expiring = lease_cache.advance_time();
        expected.insert(2);
        expected.remove(&1);
        assert_eq!(expiring, expected);
        assert!(!lease_cache.content_map.contains_key(&1));
        expiring = lease_cache.advance_time();
        expected.insert(3);
        expected.remove(&2);
        assert_eq!(expiring, expected);
        assert!(!lease_cache.content_map.contains_key(&2));
        assert_eq!(lease_cache.content_map.len(), 0);
        assert!(lease_cache.content_map.is_empty())

        //TODO: test that expiring index is incremented correctly at the boundry
    }

    fn test_lease_cache_force_evict() {
        let epsilon = 0.1;
        let num_iters = 100;
        //we want to test that each object in the cache has an equal chance of being evicted
        let mut num_obj1_evicted = 0;
        let mut num_obj2_evicted = 0;
        let mut num_obj3_evicted = 0;
        for _ in 0..num_iters {
            let mut lease_cache = LeaseCache::<usize>::new();
            lease_cache.insert(1, 100000);
            lease_cache.insert(2, 100000);
            lease_cache.insert(3, 9);
            let evicted_obj = lease_cache.force_evict();
            match evicted_obj {
                1 => num_obj1_evicted += 1,
                2 => num_obj2_evicted += 1,
                3 => num_obj3_evicted += 1,
                _ => panic!("Invalid object evicted"),
            }
            // if i % 10 == 1 {
            //     println!("{} ", i)
            // }
        }
        //check that each object was evicted is within a small epsilon
        let check_obj1 =
            ((num_obj1_evicted as f64 / num_iters as f64) - (1.0 / 3.0)).abs() < epsilon;
        let check_obj2 =
            ((num_obj2_evicted as f64 / num_iters as f64) - (1.0 / 3.0)).abs() < epsilon;
        let check_obj3 =
            ((num_obj3_evicted as f64 / num_iters as f64) - (1.0 / 3.0)).abs() < epsilon;
        println!(
            "eviction count: {} {} {}",
            num_obj1_evicted, num_obj2_evicted, num_obj3_evicted
        );
        println!(
            "eviction ratio: {} {} {}",
            num_obj1_evicted as f64 / num_iters as f64,
            num_obj2_evicted as f64 / num_iters as f64,
            num_obj3_evicted as f64 / num_iters as f64
        );
        assert!(check_obj1 && check_obj2 && check_obj3);
    }

    #[test]
    fn test_lease_cache_force_evict_string() {
        let epsilon = 0.1;
        let num_iters = 1000;
        //we want to test that each object in the cache has an equal chance of being evicted
        let mut eviction_counts = HashMap::new();
        let obj_1 = "x1";
        let obj_2 = "x2";
        let obj_3 = "x3";
        for _ in 0..num_iters {
            let mut lease_cache = LeaseCache::<String>::new();
            lease_cache.insert(obj_1.to_string(), 100000);
            lease_cache.insert(obj_2.to_string(), 100000);
            lease_cache.insert(obj_3.to_string(), 9);
            let evicted_obj = lease_cache.force_evict();
            *eviction_counts.entry(evicted_obj).or_insert(0) += 1;
        }
        // Check that each object was evicted within a small epsilon
        let expected_ratio = 1.0 / 3.0;
        for count in eviction_counts.values() {
            let ratio = *count as f64 / num_iters as f64;
            println!("Eviction ratio: {}", ratio);
            assert!(
                (ratio - expected_ratio).abs() < epsilon,
                "Eviction ratio {} differs from expected {}",
                ratio,
                expected_ratio
            );
        }
    }

    #[test]
    fn test_remove_from_cache() {
        let mut lease_cache = LeaseCache::<usize>::new();
        lease_cache.insert(1, 1);
        lease_cache.insert(2, 2);
        lease_cache.insert(3, 3);
        lease_cache.remove(&1);
        assert!(!lease_cache.content_map.contains_key(&1));
        assert_eq!(lease_cache.content_map.len(), 2);
        lease_cache.remove(&2);
        assert!(!lease_cache.content_map.contains_key(&2));
        assert_eq!(lease_cache.content_map.len(), 1);
        lease_cache.remove(&3);
        assert!(!lease_cache.content_map.contains_key(&3));
        assert_eq!(lease_cache.content_map.len(), 0);
    }
}
