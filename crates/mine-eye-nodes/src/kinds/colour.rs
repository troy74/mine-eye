//! Shared colour-ramp utilities used across node kinds.
//!
//! A single canonical `interpolate_palette` lives here; both
//! `heatmap_raster_tile_cache` and `resource_model` (if it ever renders
//! colour) import from `super::colour`.

/// Interpolate a named colour palette at position `t ∈ [0, 1]`.
///
/// Supported names (case-insensitive): `"inferno"`, `"viridis"`,
/// `"terrain"`.  All other names fall back to the default blue→red ramp.
///
/// Returns `[r, g, b]` as `u8` values.
pub(crate) fn interpolate_palette(palette: &str, t: f64) -> [u8; 3] {
    let t = t.clamp(0.0, 1.0);
    let stops: &[(f64, [u8; 3])] = match palette.to_ascii_lowercase().as_str() {
        "inferno" => &[
            (0.0, [0, 0, 4]),
            (0.2, [43, 10, 90]),
            (0.45, [120, 28, 109]),
            (0.7, [209, 58, 47]),
            (1.0, [255, 59, 47]),
        ],
        "viridis" => &[
            (0.0, [68, 1, 84]),
            (0.25, [59, 82, 139]),
            (0.5, [33, 144, 140]),
            (0.75, [93, 200, 99]),
            (1.0, [253, 231, 37]),
        ],
        "terrain" => &[
            (0.0, [43, 131, 186]),
            (0.35, [171, 221, 164]),
            (0.6, [102, 189, 99]),
            (0.8, [253, 174, 97]),
            (1.0, [215, 25, 28]),
        ],
        _ => &[
            (0.0, [44, 123, 182]),
            (0.25, [0, 166, 202]),
            (0.5, [0, 204, 106]),
            (0.75, [249, 208, 87]),
            (1.0, [215, 25, 28]),
        ],
    };
    for i in 1..stops.len() {
        let (ta, ca) = stops[i - 1];
        let (tb, cb) = stops[i];
        if t <= tb {
            let r = ((t - ta) / (tb - ta).max(1e-9)).clamp(0.0, 1.0);
            let lerp = |a: u8, b: u8| -> u8 {
                (a as f64 + (b as f64 - a as f64) * r)
                    .round()
                    .clamp(0.0, 255.0) as u8
            };
            return [lerp(ca[0], cb[0]), lerp(ca[1], cb[1]), lerp(ca[2], cb[2])];
        }
    }
    stops.last().map(|(_, c)| *c).unwrap_or([0, 0, 0])
}
