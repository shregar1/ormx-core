use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;

pub const DEFAULT_SHARD: &str = "default";

pub fn route_hash(value: &str, shards: &[String]) -> String {
    if shards.is_empty() {
        return DEFAULT_SHARD.to_string();
    }
    let hash = md5::compute(value);
    shards[(hash[0] as usize) % shards.len()].clone()
}

pub fn route_geo(value: &str, geo_map: &HashMap<String, String>) -> String {
    geo_map
        .get(value)
        .cloned()
        .unwrap_or_else(|| DEFAULT_SHARD.to_string())
}

pub fn route_list(value: &str, list_map: &HashMap<String, String>) -> String {
    list_map
        .get(value)
        .cloned()
        .unwrap_or_else(|| DEFAULT_SHARD.to_string())
}

pub fn route_range_unsorted(value: &str, range_map: &HashMap<i64, String>) -> String {
    let val: i64 = value.parse().unwrap_or(0);
    let mut sorted_keys: Vec<i64> = range_map.keys().copied().collect();
    sorted_keys.sort();
    for k in sorted_keys {
        if val <= k {
            return range_map.get(&k).unwrap().clone();
        }
    }
    DEFAULT_SHARD.to_string()
}

pub fn route_range_sorted(
    value: &str,
    sorted_keys: &[i64],
    range_map: &HashMap<i64, String>,
) -> String {
    let val: i64 = value.parse().unwrap_or(0);
    let idx = sorted_keys.partition_point(|&k| k < val);
    if idx >= sorted_keys.len() {
        DEFAULT_SHARD.to_string()
    } else {
        range_map.get(&sorted_keys[idx]).unwrap().clone()
    }
}

pub fn rows_to_json_string(rows: Vec<Vec<(String, JsonValue)>>) -> String {
    let results: Vec<JsonValue> = rows
        .into_iter()
        .map(|cols| {
            let mut map = Map::new();
            for (name, val) in cols {
                map.insert(name, val);
            }
            JsonValue::Object(map)
        })
        .collect();
    serde_json::to_string(&results).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn shards3() -> Vec<String> {
        vec!["a".into(), "b".into(), "c".into()]
    }

    #[test]
    fn hash_empty_shards_returns_default() {
        assert_eq!(route_hash("anything", &[]), "default");
    }

    #[test]
    fn hash_picks_deterministically_from_shards() {
        let shards = shards3();
        let first = route_hash("user-42", &shards);
        let second = route_hash("user-42", &shards);
        assert_eq!(first, second);
        assert!(shards.contains(&first));
    }

    #[test]
    fn geo_hits_map_entry() {
        let mut m = HashMap::new();
        m.insert("us-east".to_string(), "shard-a".to_string());
        assert_eq!(route_geo("us-east", &m), "shard-a");
    }

    #[test]
    fn geo_misses_fall_back_to_default() {
        let m = HashMap::new();
        assert_eq!(route_geo("nowhere", &m), "default");
    }

    #[test]
    fn list_hits_and_misses() {
        let mut m = HashMap::new();
        m.insert("vip".to_string(), "premium".to_string());
        assert_eq!(route_list("vip", &m), "premium");
        assert_eq!(route_list("free", &m), "default");
    }

    #[test]
    fn range_unsorted_finds_lowest_upper_bound() {
        let mut m = HashMap::new();
        m.insert(100, "shard-low".to_string());
        m.insert(200, "shard-mid".to_string());
        m.insert(300, "shard-high".to_string());

        assert_eq!(route_range_unsorted("50", &m), "shard-low");
        assert_eq!(route_range_unsorted("150", &m), "shard-mid");
        assert_eq!(route_range_unsorted("250", &m), "shard-high");
        assert_eq!(route_range_unsorted("999", &m), "default");
    }

    #[test]
    fn range_sorted_matches_unsorted_behavior() {
        let mut m = HashMap::new();
        m.insert(100, "shard-low".to_string());
        m.insert(200, "shard-mid".to_string());
        m.insert(300, "shard-high".to_string());
        let mut keys: Vec<i64> = m.keys().copied().collect();
        keys.sort();

        for v in ["0", "50", "100", "101", "200", "250", "300", "301", "9999"] {
            assert_eq!(
                route_range_sorted(v, &keys, &m),
                route_range_unsorted(v, &m),
                "mismatch at value {}",
                v
            );
        }
    }

    #[test]
    fn range_unparseable_value_treated_as_zero() {
        let mut m = HashMap::new();
        m.insert(10, "shard-zero".to_string());
        assert_eq!(route_range_unsorted("not-a-number", &m), "shard-zero");
    }

    #[test]
    fn json_encode_empty_rows_is_empty_array() {
        assert_eq!(rows_to_json_string(vec![]), "[]");
    }

    #[test]
    fn json_encode_preserves_columns_per_row() {
        let rows = vec![
            vec![("id".into(), json!(1)), ("name".into(), json!("alice"))],
            vec![("id".into(), json!(2)), ("name".into(), json!("bob"))],
        ];
        let out = rows_to_json_string(rows);
        let parsed: JsonValue = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed[0]["id"], json!(1));
        assert_eq!(parsed[1]["name"], json!("bob"));
    }
}
