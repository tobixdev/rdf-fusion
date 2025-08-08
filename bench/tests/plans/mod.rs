//! Contains tests that assert that the query plans for the benchmark queries are correct.
//!
//! This should be used for the following purposes:
//! - To ensure that the query plans do not change unexpectedly.
//! - Given a new optimization, verify that the "end to end" query plans are indeed changed.

use regex::Regex;
use std::collections::HashMap;
use std::fmt::Write;

mod bsbm_business_intelligence;
mod bsbm_explore;
mod wind_farm;

fn canonicalize_uuids(s: &str) -> String {
    // This is a bit hacky. Oxigraph does not print leading zeroes, and therefore we must replace
    // also shorter uuids. We assume that more than 12 leading zeroes are very unlikely for random
    // uuids and that, on the other hand, 20 characters long hex numbers are also unlikely in LPs.
    let uuid_re = Regex::new(r"\b[0-9a-fA-F]{20,32}\b").unwrap();

    let mut uuid_map = HashMap::new();
    let mut counter = 1u128;

    let result = uuid_re.replace_all(s, |caps: &regex::Captures| {
        let uuid = caps.get(0).unwrap().as_str();

        // Map to a deterministic fake UUID if not already mapped
        let entry = uuid_map.entry(uuid.to_string()).or_insert_with(|| {
            let mut fake_uuid = String::new();
            write!(&mut fake_uuid, "{:032x}", counter).unwrap();
            counter += 1;
            fake_uuid
        });

        entry.clone()
    });

    result.into_owned()
}
