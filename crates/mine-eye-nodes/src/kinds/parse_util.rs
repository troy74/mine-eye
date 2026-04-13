//! Shared numeric-parsing and statistical utilities used across node kinds.
//!
//! These were previously duplicated in `heatmap_raster_tile_cache` and
//! `resource_model`.  A single canonical copy lives here; both modules now
//! import from `super::parse_util`.

/// Parse a `serde_json::Value` as a finite `f64`.
///
/// Accepts JSON numbers and decimal strings (comma or period separator).
/// Returns `None` for non-finite results, nulls, booleans, and objects.
pub(crate) fn parse_numeric_value(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64().filter(|x| x.is_finite()),
        serde_json::Value::String(s) => s
            .trim()
            .replace(',', ".")
            .parse::<f64>()
            .ok()
            .filter(|x| x.is_finite()),
        _ => None,
    }
}

/// Case-insensitive key lookup returning a parsed `f64` value.
///
/// First tries an exact-match lookup (fast path), then falls back to a
/// case-insensitive linear scan.  Returns `None` if the key is absent or
/// the value does not parse as a finite float.
pub(crate) fn lookup_numeric_ci(
    obj: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<f64> {
    if let Some(v) = obj.get(key) {
        return parse_numeric_value(v);
    }
    let lk = key.to_ascii_lowercase();
    obj.iter()
        .find(|(k, _)| k.to_ascii_lowercase() == lk)
        .and_then(|(_, v)| parse_numeric_value(v))
}

/// Return the `pct`-th percentile of `values` (0–100 scale).
///
/// Sorts a copy of the slice; the original is left unchanged.
/// Returns `None` for an empty slice.
pub(crate) fn percentile_value(values: &[f64], pct: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p = pct.clamp(0.0, 100.0) / 100.0;
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted.get(idx).copied()
}
