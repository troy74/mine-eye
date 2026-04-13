use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use mine_eye_types::SemanticPortType;
use serde::{Deserialize, Serialize};

pub(crate) const BUNDLED_NODE_REGISTRY_JSON: &str = include_str!("node-registry.json");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryRoot {
    pub version: u32,
    #[serde(default)]
    pub nodes: Vec<RegistryNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryNode {
    pub kind: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub framework_group: Option<String>,
    #[serde(default)]
    pub submenu: Option<String>,
    #[serde(default)]
    pub plugin_source: Option<String>,
    #[serde(default)]
    pub menu: Option<serde_json::Value>,
    #[serde(default)]
    pub policy: Option<serde_json::Value>,
    #[serde(default)]
    pub interaction: Option<serde_json::Value>,
    #[serde(default)]
    pub ports: RegistryPorts,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RegistryPorts {
    #[serde(default)]
    pub inputs: Vec<RegistryPort>,
    #[serde(default)]
    pub outputs: Vec<RegistryPort>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryPort {
    pub id: String,
    #[serde(default)]
    pub label: Option<String>,
    pub semantic: String,
    #[serde(default)]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct ResolvedPort<'a> {
    pub port: &'a RegistryPort,
}

#[derive(Debug, Clone)]
pub struct NodeRegistry {
    root: RegistryRoot,
    by_kind: HashMap<String, RegistryNode>,
}

static NODE_REGISTRY: OnceLock<Result<NodeRegistry, String>> = OnceLock::new();

impl NodeRegistry {
    pub fn global() -> Result<&'static NodeRegistry, String> {
        NODE_REGISTRY
            .get_or_init(load_registry)
            .as_ref()
            .map_err(|e| e.clone())
    }

    pub fn root(&self) -> &RegistryRoot {
        &self.root
    }

    pub fn kind(&self, kind: &str) -> Option<&RegistryNode> {
        self.by_kind.get(kind)
    }

    pub fn ports_for_kind(&self, kind: &str) -> Option<&RegistryPorts> {
        self.by_kind.get(kind).map(|n| &n.ports)
    }

    pub fn resolve_output_port<'a>(
        &'a self,
        kind: &str,
        port_id: &str,
    ) -> Result<ResolvedPort<'a>, String> {
        let ports = self
            .ports_for_kind(kind)
            .ok_or_else(|| format!("node kind '{}' missing from registry", kind))?;
        resolve_port(&ports.outputs, port_id)
            .ok_or_else(|| format!("from_port '{}' not found on kind '{}'", port_id, kind))
    }

    pub fn resolve_input_port<'a>(
        &'a self,
        kind: &str,
        port_id: &str,
    ) -> Result<ResolvedPort<'a>, String> {
        let ports = self
            .ports_for_kind(kind)
            .ok_or_else(|| format!("node kind '{}' missing from registry", kind))?;
        resolve_port(&ports.inputs, port_id)
            .ok_or_else(|| format!("to_port '{}' not found on kind '{}'", port_id, kind))
    }

    pub fn resolve_edge_semantic(
        &self,
        from_kind: &str,
        from_port: &str,
        to_kind: &str,
        to_port: &str,
    ) -> Result<SemanticPortType, String> {
        let out_port = self.resolve_output_port(from_kind, from_port)?;
        let in_port = self.resolve_input_port(to_kind, to_port)?;
        let out_sem: SemanticPortType = out_port.port.semantic.parse()?;
        let in_sem: SemanticPortType = in_port.port.semantic.parse()?;
        out_sem
            .compatibility_to(in_sem)
            .map(|_| out_sem)
            .ok_or_else(|| {
                format!(
                    "wire incompatibility: {}.{} ({}) cannot connect to {}.{} ({})",
                    from_kind,
                    from_port,
                    out_sem.as_str(),
                    to_kind,
                    to_port,
                    in_sem.as_str()
                )
            })
    }
}

fn load_registry() -> Result<NodeRegistry, String> {
    let mut root: RegistryRoot = serde_json::from_str(BUNDLED_NODE_REGISTRY_JSON)
        .map_err(|e| format!("bundled node registry parse failed: {}", e))?;
    for path in registry_overlay_paths()? {
        let text = fs::read_to_string(&path).map_err(|e| {
            format!(
                "failed reading registry overlay '{}': {}",
                path.display(),
                e
            )
        })?;
        let overlay: RegistryRoot = serde_json::from_str(&text).map_err(|e| {
            format!(
                "registry overlay parse failed for '{}': {}",
                path.display(),
                e
            )
        })?;
        merge_registry(&mut root, overlay)?;
    }
    validate_registry(&root)?;
    let by_kind = root
        .nodes
        .iter()
        .cloned()
        .map(|n| (n.kind.clone(), n))
        .collect();
    Ok(NodeRegistry { root, by_kind })
}

fn registry_overlay_paths() -> Result<Vec<PathBuf>, String> {
    let mut out = Vec::new();
    if let Ok(file) = env::var("MINE_EYE_NODE_REGISTRY_OVERLAY") {
        let trimmed = file.trim();
        if !trimmed.is_empty() {
            out.push(PathBuf::from(trimmed));
        }
    }
    if let Ok(dir) = env::var("MINE_EYE_NODE_REGISTRY_DIR") {
        let trimmed = dir.trim();
        if !trimmed.is_empty() {
            let mut entries = fs::read_dir(trimmed)
                .map_err(|e| format!("failed reading registry dir '{}': {}", trimmed, e))?
                .filter_map(|entry| entry.ok().map(|e| e.path()))
                .filter(|path| is_registry_fragment(path))
                .collect::<Vec<_>>();
            entries.sort();
            out.extend(entries);
        }
    }
    Ok(out)
}

fn is_registry_fragment(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("json"))
        .unwrap_or(false)
}

fn merge_registry(base: &mut RegistryRoot, overlay: RegistryRoot) -> Result<(), String> {
    if overlay.version != base.version {
        return Err(format!(
            "registry overlay version {} does not match base version {}",
            overlay.version, base.version
        ));
    }
    let mut index_by_kind = base
        .nodes
        .iter()
        .enumerate()
        .map(|(idx, node)| (node.kind.clone(), idx))
        .collect::<HashMap<_, _>>();
    for node in overlay.nodes {
        if let Some(idx) = index_by_kind.get(&node.kind).copied() {
            base.nodes[idx] = node;
        } else {
            index_by_kind.insert(node.kind.clone(), base.nodes.len());
            base.nodes.push(node);
        }
    }
    Ok(())
}

fn validate_registry(root: &RegistryRoot) -> Result<(), String> {
    let mut seen_kinds = HashMap::<&str, usize>::new();
    for (idx, node) in root.nodes.iter().enumerate() {
        if node.kind.trim().is_empty() {
            return Err(format!("registry node at index {} is missing kind", idx));
        }
        if seen_kinds.insert(node.kind.as_str(), idx).is_some() {
            return Err(format!("duplicate node kind '{}' in registry", node.kind));
        }
        validate_ports(&node.kind, "input", &node.ports.inputs)?;
        validate_ports(&node.kind, "output", &node.ports.outputs)?;
    }
    Ok(())
}

fn validate_ports(kind: &str, direction: &str, ports: &[RegistryPort]) -> Result<(), String> {
    let mut seen_ids = HashMap::<&str, usize>::new();
    for (idx, port) in ports.iter().enumerate() {
        if port.id.trim().is_empty() {
            return Err(format!(
                "registry {} port {} on '{}' is missing id",
                direction, idx, kind
            ));
        }
        if seen_ids.insert(port.id.as_str(), idx).is_some() {
            return Err(format!(
                "duplicate {} port '{}' declared on kind '{}'",
                direction, port.id, kind
            ));
        }
        let sem: SemanticPortType = port.semantic.parse().map_err(|e: String| {
            format!(
                "invalid semantic '{}' on {} port '{}.{}': {}",
                port.semantic, direction, kind, port.id, e
            )
        })?;
        if sem == SemanticPortType::Any && direction == "output" {
            return Err(format!(
                "output port '{}.{}' cannot declare semantic 'any'",
                kind, port.id
            ));
        }
    }
    Ok(())
}

fn resolve_port<'a>(ports: &'a [RegistryPort], requested: &str) -> Option<ResolvedPort<'a>> {
    if let Some(port) = ports.iter().find(|p| p.id == requested) {
        return Some(ResolvedPort { port });
    }

    ports.iter().find_map(|port| {
        port_pattern_matches(port.id.as_str(), requested).then_some(ResolvedPort { port })
    })
}

fn port_pattern_matches(declared: &str, requested: &str) -> bool {
    if let Some(rest) = requested.strip_prefix(declared) {
        return rest.starts_with('_') && rest[1..].chars().all(|c| c.is_ascii_digit());
    }

    let Some((decl_prefix, decl_num)) = split_numeric_port_suffix(declared) else {
        return false;
    };
    let Some((req_prefix, req_num)) = split_numeric_port_suffix(requested) else {
        return false;
    };
    decl_prefix == req_prefix && decl_num >= 1 && req_num >= 1
}

fn split_numeric_port_suffix(value: &str) -> Option<(&str, u32)> {
    let (prefix, digits) = value.rsplit_once('_')?;
    let num = digits.parse::<u32>().ok()?;
    Some((prefix, num))
}
