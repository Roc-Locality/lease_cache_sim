#![allow(dead_code)]
use crate::lease_cache::TaggedObjectId;
use std::collections::HashMap;

pub fn lease_to_map(file_path_str: String) -> HashMap<u64, (usize, usize, f64)> {
    //map: reference -> (short_lease, long_lease, short_lease_prob)
    let mut map = HashMap::new();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(file_path_str)
        .unwrap();
    reader.records().for_each(|result| {
        let record = result.unwrap();
        let reference: u64 = record[1].trim().parse().unwrap();
        let short_lease = usize::from_str_radix(record[2].trim(), 16).unwrap();
        let long_lease = usize::from_str_radix(record[3].trim(), 16).unwrap();
        // println!("short_lease: {}, long_lease: {}", short_lease, long_lease);
        let short_lease_prob = record[4].trim().parse::<f64>().unwrap();
        map.insert(reference, (short_lease, long_lease, short_lease_prob));
    });
    map
}

pub fn trace_to_vec_u64(file_path_str: String) -> Vec<TaggedObjectId<u64, u64>> {
    let mut vec = Vec::new();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(file_path_str)
        .unwrap();
    reader.records().for_each(|result| {
        let record = result.unwrap();
        let reference: u64 = u64::from_str_radix(record[0].trim(), 16).unwrap();
        let address: u64 = u64::from_str_radix(record[2].trim(), 16).unwrap();
        vec.push(TaggedObjectId(reference, address));
    });
    vec
}
