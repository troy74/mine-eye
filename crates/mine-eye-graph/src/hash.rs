use mine_eye_types::NodeConfig;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub fn hash_node_config(config: &NodeConfig) -> String {
    let v = serde_json::to_value(config).unwrap_or(serde_json::Value::Null);
    let canonical = canonicalize_json(v);
    let json = serde_json::to_string(&canonical).unwrap_or_default();
    let mut h = Sha256::new();
    h.update(json.as_bytes());
    hex::encode(h.finalize())
}

fn canonicalize_json(v: serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(map) => {
            let mut out: BTreeMap<String, serde_json::Value> = BTreeMap::new();
            for (k, val) in map {
                out.insert(k, canonicalize_json(val));
            }
            serde_json::to_value(out).unwrap_or(serde_json::Value::Null)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(canonicalize_json).collect())
        }
        other => other,
    }
}
