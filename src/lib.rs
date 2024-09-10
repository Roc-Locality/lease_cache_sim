#![allow(dead_code)]
#![allow(clippy::needless_return)]
use abstract_cache::AccessResult;
use abstract_cache::CacheSim;
use abstract_cache::ObjIdTraits;
use rand::seq::IteratorRandom;
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

pub const MAX_EXPIRING_VEC_SIZE: usize = 10000000;
#[derive(Clone)]
pub struct LeaseCache<Obj: ObjIdTraits> {
    //map from ref to (short_lease, long_lease, short_lease_prob)
    // pub(crate) lease_table: HashMap<Tag, (usize, usize, f64)>,
    pub(crate) expiring_vec: Vec<HashSet<Obj>>,
    pub(crate) curr_expiring_index: usize,
    //map from ObjId to index in expiring_vec
    pub(crate) content_map: HashMap<Obj, usize>,
    pub(crate) cache_consumption: usize,
    pub(crate) cache_size: Option<usize>,
}
impl<Obj: ObjIdTraits> LeaseCache<Obj> {
    pub fn new() -> Self {
        LeaseCache {
            expiring_vec: vec![HashSet::new(); MAX_EXPIRING_VEC_SIZE],
            curr_expiring_index: 0,
            content_map: HashMap::new(),
            cache_consumption: 0,
            cache_size: None,
        }
    }

    pub fn insert(&mut self, obj_id: Obj, lease: usize) {
        let absolute_index = (self.curr_expiring_index + lease) % MAX_EXPIRING_VEC_SIZE;
        self.expiring_vec[absolute_index].insert(obj_id.clone());
        self.content_map.insert(obj_id, absolute_index);
    }

    pub fn update(&mut self, obj_id: &Obj, lease: usize) -> AccessResult {
        self.dump_expiring();
        let old_index = self.content_map.get(obj_id);
        match old_index {
            None => match lease {
                0 => AccessResult::Miss,
                _ => {
                    self.insert(obj_id.clone(), lease);
                    self.cache_consumption += 1;
                    AccessResult::Miss
                }
            },
            Some(old_index) => {
                self.expiring_vec[*old_index]
                    .remove(obj_id)
                    .then_some(())
                    .unwrap();
                if lease != 0 {
                    self.insert(obj_id.clone(), lease);
                }
                AccessResult::Hit
            }
        }
    }

    pub fn contains(&self, obj_id: &Obj) -> bool {
        self.content_map.contains_key(obj_id)
    }

    pub fn get_time_till_eviction(&self, obj_id: &Obj) -> Option<usize> {
        let index = self.content_map.get(obj_id);
        match index {
            None => {None},
            Some(index) => {
                let curr_index = self.curr_expiring_index;
                if *index > curr_index {
                    return Some(*index - curr_index);
                }
                return Some(MAX_EXPIRING_VEC_SIZE - curr_index + *index);
            }
        }
    }

    pub fn get_cache_consumption(&self) -> usize {
        self.cache_consumption
    }

    pub fn remove_from_cache(&mut self, obj_id: &Obj) {
        let index = self.content_map.get(obj_id).unwrap();
        self.expiring_vec[*index].remove(obj_id);
        self.content_map.remove(obj_id);
        self.cache_consumption -= 1;
    }

    pub fn dump_expiring(&mut self) -> HashSet<Obj> {
        self.curr_expiring_index = (self.curr_expiring_index + 1) % MAX_EXPIRING_VEC_SIZE;
        let expiring = self.expiring_vec[self.curr_expiring_index].clone();
        let expiring_copy = expiring.clone();
        //decrement the cache consumption when expiring
        self.cache_consumption -= expiring.len();
        //removing expiring from content map
        expiring.iter().for_each(|obj_id| {
            self.content_map.remove(obj_id);
        });
        self.expiring_vec[self.curr_expiring_index].clear();
        // self.curr_expiring_index = (self.curr_expiring_index + 1) % MAX_EXPIRING_VEC_SIZE;

        return expiring_copy;
    }

    pub fn remove_random_element<K, V>(map: &mut HashMap<K, V>) -> Option<(K, V)>
    where
        K: std::hash::Hash + Eq + Clone,
        V: Clone,
    {
        if let Some((key, val)) = map.clone().iter().choose(&mut rand::thread_rng()) {
            map.remove(key);
            return Some((key.clone(), val.clone()));
        }
        None
    }

    pub fn force_evict(&mut self) -> Obj {
        // println!("content map before {:?}", self.content_map);

        let (obj_id, absolute_index) =
            LeaseCache::<Obj>::remove_random_element(&mut self.content_map).unwrap();

        self.expiring_vec[absolute_index]
            .remove(&obj_id.clone())
            .then_some(())
            .unwrap();
        obj_id
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
    fn cache_access(&mut self, access: TaggedObjectId<usize, Obj>) -> abstract_cache::AccessResult {
        let TaggedObjectId(lease, obj_id) = access;
        let cache_result = self.update(&obj_id, lease);
        if self.cache_consumption > self.cache_size.unwrap() {
            self.force_evict();
            self.cache_consumption -= 1;
        }
        return cache_result;
    }

    fn set_capacity(&mut self, cache_size: usize) -> &mut Self {
        self.cache_size = Some(cache_size);
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
        lease_cache.cache_consumption = 3;
        assert_eq!(lease_cache.get_time_till_eviction(&1), Some(1));
        assert_eq!(lease_cache.get_time_till_eviction(&2), Some(2));
        assert_eq!(lease_cache.get_time_till_eviction(&3), Some(3));
        lease_cache.dump_expiring();

        println!("{:?}", lease_cache.get_time_till_eviction(&1));
        assert_eq!(lease_cache.get_time_till_eviction(&2), Some(1));
        assert_eq!(lease_cache.get_time_till_eviction(&3), Some(2));
    }

    #[test]
    fn test_lease_zero() {
        let mut lease_cache = LeaseCache::<usize>::new();
        lease_cache.update(&1, 2);
        lease_cache.update(&2, 0);
        assert_eq!(lease_cache.get_time_till_eviction(&1), Some(1));
        assert_eq!(lease_cache.get_time_till_eviction(&2), None);
        assert!(!lease_cache.content_map.contains_key(&2));
    }

    #[test]
    fn test_lease_cache_new() {
        let lease_cache = LeaseCache::<usize>::new();
        assert_eq!(lease_cache.expiring_vec.len(), self::MAX_EXPIRING_VEC_SIZE);
        assert_eq!(lease_cache.curr_expiring_index, 0);
        assert_eq!(lease_cache.content_map.len(), 0);
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
        let mut abs_index = lease_cache.content_map.get(&1).unwrap();
        assert_eq!(lease_cache.expiring_vec[*abs_index].contains(&1), true);
        abs_index = lease_cache.content_map.get(&2).unwrap();
        assert_eq!(lease_cache.expiring_vec[*abs_index].contains(&2), true);
        abs_index = lease_cache.content_map.get(&3).unwrap();
        assert_eq!(lease_cache.expiring_vec[*abs_index].contains(&3), true);
    }

    #[test]
    fn test_lease_cache_update() {
        let mut lease_cache = LeaseCache::<usize>::new();
        // Update the lease cache with obj_id 1 and index 1
        lease_cache.update(&1, 1);
        // Get the absolute index of obj_id 1 and release the immutable borrow
        let abs_index: usize = *lease_cache.content_map.get(&1).unwrap();
        assert!(lease_cache.expiring_vec[abs_index].contains(&1));
        // Update the lease cache with obj_id 1 and new index 4
        lease_cache.update(&1, 4);
        assert!(lease_cache.content_map.keys().len() == 1);
        // Get the old absolute index and assert it no longer contains obj_id 1
        // let abs_index_old = abs_index; // Reuse the old index
        println!(
            "results {}",
            lease_cache.expiring_vec[abs_index].contains(&1)
        );
        assert!(!lease_cache.expiring_vec[abs_index].contains(&1));
        assert!(lease_cache.content_map.keys().len() == 1);
        // Get the new absolute index and assert it contains obj_id 1
        let abs_index_new = *lease_cache.content_map.get(&1).unwrap();
        assert!(lease_cache.expiring_vec[abs_index_new].contains(&1));
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
        lease_cache.cache_consumption = 3;
        let mut expiring = lease_cache.dump_expiring();
        let mut expected = HashSet::new();
        expected.insert(1);
        assert_eq!(expiring, expected);
        expiring = lease_cache.dump_expiring();
        expected.insert(2);
        expected.remove(&1);
        assert_eq!(expiring, expected);
        assert!(!lease_cache.content_map.contains_key(&1));
        expiring = lease_cache.dump_expiring();
        expected.insert(3);
        expected.remove(&2);
        assert_eq!(expiring, expected);
        assert!(!lease_cache.content_map.contains_key(&2));
        assert_eq!(lease_cache.cache_consumption, 0);
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
        let num_iters = 100;
        //we want to test that each object in the cache has an equal chance of being evicted
        let mut num_obj1_evicted = 0;
        let mut num_obj2_evicted = 0;
        let mut num_obj3_evicted = 0;
        let obj_1 = "x1";
        let obj_2 = "x2";
        let obj_3 = "x3";
        for i in 0..num_iters {
            let mut lease_cache = LeaseCache::<String>::new();
            lease_cache.insert(obj_1.to_string(), 100000);
            lease_cache.insert(obj_2.to_string(), 100000);
            lease_cache.insert(obj_3.to_string(), 9);
            let evicted_obj = lease_cache.force_evict();
            // println!("evicted: {evicted_obj}");
            match evicted_obj.as_str() {
                o if o == obj_1 => num_obj1_evicted += 1,
                o if o == obj_2 => num_obj2_evicted += 1,
                o if o == obj_3 => num_obj3_evicted += 1,
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
    fn test_remove_from_cache() {
        let mut lease_cache = LeaseCache::<usize>::new();
        lease_cache.insert(1, 1);
        lease_cache.insert(2, 2);
        lease_cache.insert(3, 3);
        lease_cache.cache_consumption = 3;
        lease_cache.remove_from_cache(&1);
        assert!(!lease_cache.content_map.contains_key(&1));
        assert_eq!(lease_cache.cache_consumption, 2);
        lease_cache.remove_from_cache(&2);
        assert!(!lease_cache.content_map.contains_key(&2));
        assert_eq!(lease_cache.cache_consumption, 1);
        lease_cache.remove_from_cache(&3);
        assert!(!lease_cache.content_map.contains_key(&3));
        assert_eq!(lease_cache.cache_consumption, 0);
    }
}
