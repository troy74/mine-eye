use mine_eye_types::NodeConfig;
use sha2::{Digest, Sha256};

pub fn hash_node_config(config: &NodeConfig) -> String {
    let json = serde_json::to_string(config).unwrap_or_default();
    let mut h = Sha256::new();
    h.update(json.as_bytes());
    hex::encode(h.finalize())
}
