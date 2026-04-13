//! Magnetic depth / Euler deconvolution node.
//!
//! Accepts a `magnetic_grid.json` artifact (schema `grid.magnetic.v1`) and
//! produces a sparse 3-D voxel model of estimated source depths by running
//! Euler deconvolution (Reid et al. 1990) over a sliding window on the
//! gridded anomaly and its derivatives.
//!
//! ## Algorithm summary
//!
//! For each sliding window of `window_size × window_size` grid cells:
//!
//! 1. **Build the over-determined system**: the Euler equation for a potential
//!    field source at (x₀, y₀, z₀) under structural index N is:
//!
//!    `(xᵢ − x₀)·∂M/∂x + (yᵢ − y₀)·∂M/∂y + h·∂M/∂z + N·Mᵢ = N·B`
//!
//!    where h = z_flight − z₀ (source depth below observation level, positive),
//!    B is a slowly varying background, and the unknowns are [x₀, y₀, h, B].
//!
//! 2. **Solve** the 4-unknown least-squares system via normal equations with
//!    lightweight Tikhonov regularisation.
//!
//! 3. **Multi-N** (default): try N ∈ {0, 1, 2, 3} and keep the solution with
//!    the lowest normalised RMS residual.  **Single-pass** mode: use a fixed N
//!    configured by the user.
//!
//! 4. **Filter** solutions outside depth bounds or with poor fit.
//!
//! 5. Emit surviving solutions as `block_voxels` — the same schema used by the
//!    block grade model — so the 3-D viewer renders them immediately.
//!
//! ## Notes on ∂M/∂z
//!
//! For a 2-D airborne grid at constant observation altitude, the true first
//! vertical derivative is computed rigorously only via FFT-based methods.  We
//! use the spatial approximation `∂M/∂z ≈ FVD × resolution_m × 0.5`, where
//! FVD = ∂²M/∂z² is already available from the magnetic model node.  This
//! slightly underestimates depth but is geologically useful for exploratory
//! interpretation.

use mine_eye_types::{JobEnvelope, JobResult, JobStatus};
use rayon::prelude::*;
use serde_json::{json, Value};

use super::parse_util::parse_numeric_value;
use crate::executor::ExecutionContext;
use crate::NodeError;

// ── Parameter parsing ─────────────────────────────────────────────────────────

struct DepthParams {
    /// Euler window size in grid cells (must be odd; auto-rounded up).
    window_cells: usize,
    /// Step between window centres in cells.
    step_cells: usize,
    /// Typical flight height above terrain (metres). Used to scale the
    /// ∂M/∂z approximation from FVD.
    flight_height_m: f64,
    /// Only accept solutions shallower than this.
    max_depth_m: f64,
    /// Reject very shallow (noise) solutions.
    min_depth_m: f64,
    /// Source x₀,y₀ must lie within this factor × window_width_m of the
    /// window centre; otherwise the solution is geometrically unconstrained.
    max_offset_factor: f64,
    /// If `Some(n)`, fix the structural index and skip the multi-N loop.
    /// If `None`, try N ∈ {0, 1, 2, 3} and keep the best fit.
    structural_index: Option<f64>,
    /// Confidence threshold below which solutions are discarded (0–1).
    min_confidence: f64,
    /// Voxel half-size scale factor relative to depth.
    /// block_size = depth_m * voxel_scale_factor  (min: grid resolution).
    voxel_scale: f64,
}

impl DepthParams {
    fn from_job(job: &JobEnvelope) -> Self {
        let p = |ptr: &str, def: f64| -> f64 {
            job.output_spec
                .pointer(ptr)
                .and_then(parse_numeric_value)
                .unwrap_or(def)
        };
        let ps = |ptr: &str, def: &str| -> String {
            job.output_spec
                .pointer(ptr)
                .and_then(|v| v.as_str())
                .unwrap_or(def)
                .to_string()
        };

        let raw_win = p("/window_size", 7.0) as usize;
        let window_cells = if raw_win % 2 == 0 {
            raw_win + 1
        } else {
            raw_win
        }
        .max(3);

        let si_mode = ps("/structural_index_mode", "multi");
        let structural_index = if si_mode == "multi" {
            None
        } else {
            let n = p("/structural_index", 1.0).clamp(0.0, 3.0).round();
            Some(n)
        };

        Self {
            window_cells,
            step_cells: (p("/step_cells", 2.0) as usize).max(1),
            flight_height_m: p("/flight_height_m", 60.0).max(5.0),
            max_depth_m: p("/max_depth_m", 1500.0).max(10.0),
            min_depth_m: p("/min_depth_m", 10.0).max(1.0),
            max_offset_factor: p("/max_offset_factor", 2.5).max(0.5),
            structural_index,
            min_confidence: p("/min_confidence", 0.05).clamp(0.0, 0.99),
            voxel_scale: p("/voxel_scale", 0.15).max(0.01),
        }
    }
}

// ── Magnetic grid ─────────────────────────────────────────────────────────────

#[allow(dead_code)]
struct MagGrid {
    nx: usize,
    ny: usize,
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
    resolution_m: f64,
    /// Anomaly (nT).  Indexed [iy * nx + ix].
    m: Vec<Option<f64>>,
    /// Second vertical derivative (= −∂²M/∂x² − ∂²M/∂y²), nT/m².
    fvd: Vec<Option<f64>>,
}

impl MagGrid {
    fn idx(&self, ix: usize, iy: usize) -> usize {
        iy * self.nx + ix
    }
    fn x_of(&self, ix: usize) -> f64 {
        self.xmin + (ix as f64 + 0.5) * self.resolution_m
    }
    fn y_of(&self, iy: usize) -> f64 {
        self.ymin + (iy as f64 + 0.5) * self.resolution_m
    }
}

fn parse_mag_grid(root: &Value) -> Option<MagGrid> {
    // Accept both the magnetic grid schema and any artifact that carries the
    // recognised grid structure.
    let g = root.get("grid").or_else(|| root.get("surface_grid"))?;
    let go = g.as_object()?;
    let nx = go
        .get("nx")
        .and_then(parse_numeric_value)
        .map(|v| v as usize)?;
    let ny = go
        .get("ny")
        .and_then(parse_numeric_value)
        .map(|v| v as usize)?;
    if nx < 4 || ny < 4 {
        return None;
    }

    let xmin = go.get("xmin").and_then(parse_numeric_value)?;
    let xmax = go.get("xmax").and_then(parse_numeric_value)?;
    let ymin = go.get("ymin").and_then(parse_numeric_value)?;
    let ymax = go.get("ymax").and_then(parse_numeric_value)?;
    let resolution_m = go
        .get("resolution_m")
        .and_then(parse_numeric_value)
        .unwrap_or(((xmax - xmin) / nx as f64).max((ymax - ymin) / ny as f64));

    let mut m_grid = vec![None::<f64>; nx * ny];
    let mut fvd_grid = vec![None::<f64>; nx * ny];

    // Build a lookup from (ix, iy) via x/y coordinates.
    let dx = (xmax - xmin) / nx as f64;
    let dy = (ymax - ymin) / ny as f64;

    let coord_to_idx = |x: f64, y: f64| -> Option<(usize, usize)> {
        let ix = ((x - xmin) / dx).floor() as isize;
        let iy = ((y - ymin) / dy).floor() as isize;
        if ix < 0 || iy < 0 || ix >= nx as isize || iy >= ny as isize {
            return None;
        }
        Some((ix as usize, iy as usize))
    };

    if let Some(rows) = root.get("rows").and_then(|v| v.as_array()) {
        for row in rows {
            let ro = match row.as_object() {
                Some(o) => o,
                None => continue,
            };
            let x = ro.get("x").and_then(parse_numeric_value);
            let y = ro.get("y").and_then(parse_numeric_value);
            let mv = ro.get("M").and_then(parse_numeric_value);
            let fv = ro.get("fvd").and_then(parse_numeric_value);
            if let (Some(x), Some(y)) = (x, y) {
                if let Some((ix, iy)) = coord_to_idx(x, y) {
                    let idx = iy * nx + ix;
                    if let Some(v) = mv {
                        m_grid[idx] = Some(v);
                    }
                    if let Some(v) = fv {
                        fvd_grid[idx] = Some(v);
                    }
                }
            }
        }
    }

    // Alternatively the artifact may embed flat value arrays (tile-cache style).
    if let Some(vals) = g
        .as_object()
        .and_then(|o| o.get("values"))
        .and_then(|v| v.as_array())
    {
        for (idx, v) in vals.iter().enumerate() {
            if idx < m_grid.len() {
                m_grid[idx] = parse_numeric_value(v);
            }
        }
    }

    let filled = m_grid.iter().filter(|v| v.is_some()).count();
    if filled < 16 {
        return None;
    }

    Some(MagGrid {
        nx,
        ny,
        xmin,
        xmax,
        ymin,
        ymax,
        resolution_m,
        m: m_grid,
        fvd: fvd_grid,
    })
}

// ── Derivative grids ──────────────────────────────────────────────────────────

struct Derivatives {
    dmdx: Vec<Option<f64>>,
    dmdy: Vec<Option<f64>>,
    /// First vertical derivative (approximated from FVD).
    dmdz: Vec<Option<f64>>,
}

fn compute_derivatives(grid: &MagGrid, flight_height_m: f64) -> Derivatives {
    let n = grid.nx * grid.ny;
    let mut dmdx = vec![None; n];
    let mut dmdy = vec![None; n];
    let mut dmdz = vec![None; n];

    let dx = grid.resolution_m;
    let dy = grid.resolution_m;

    for iy in 0..grid.ny {
        for ix in 0..grid.nx {
            let idx = grid.idx(ix, iy);

            // ∂M/∂x — central difference (fall back to forward/backward at edges)
            if ix > 0 && ix + 1 < grid.nx {
                if let (Some(ml), Some(mr)) =
                    (grid.m[grid.idx(ix - 1, iy)], grid.m[grid.idx(ix + 1, iy)])
                {
                    dmdx[idx] = Some((mr - ml) / (2.0 * dx));
                }
            } else if ix == 0 {
                if let (Some(mc), Some(mr)) = (grid.m[idx], grid.m[grid.idx(ix + 1, iy)]) {
                    dmdx[idx] = Some((mr - mc) / dx);
                }
            } else {
                if let (Some(ml), Some(mc)) = (grid.m[grid.idx(ix - 1, iy)], grid.m[idx]) {
                    dmdx[idx] = Some((mc - ml) / dx);
                }
            }

            // ∂M/∂y — central difference
            if iy > 0 && iy + 1 < grid.ny {
                if let (Some(mb), Some(mt)) =
                    (grid.m[grid.idx(ix, iy - 1)], grid.m[grid.idx(ix, iy + 1)])
                {
                    dmdy[idx] = Some((mt - mb) / (2.0 * dy));
                }
            } else if iy == 0 {
                if let (Some(mc), Some(mt)) = (grid.m[idx], grid.m[grid.idx(ix, iy + 1)]) {
                    dmdy[idx] = Some((mt - mc) / dy);
                }
            } else {
                if let (Some(mb), Some(mc)) = (grid.m[grid.idx(ix, iy - 1)], grid.m[idx]) {
                    dmdy[idx] = Some((mc - mb) / dy);
                }
            }

            // ∂M/∂z ≈ FVD × flight_height × 0.5
            //
            // Derivation: for a source at depth d below the observation level,
            // M ∝ 1/d^(N+1) and ∂²M/∂z² ∝ 1/d^(N+3).  Integrating once over
            // an effective scale of (flight_height / 2) gives a first-order
            // approximation of ∂M/∂z.  The sign is negative because the field
            // decreases with increasing observation height above a buried source.
            if let Some(fv) = grid.fvd[idx] {
                dmdz[idx] = Some(-fv * flight_height_m * 0.5);
            } else {
                // FVD missing: estimate from M curvature if possible.
                // ∂²M/∂z² = -(∂²M/∂x² + ∂²M/∂y²) (Laplace equation).
                let d2dx2 = if ix > 0 && ix + 1 < grid.nx {
                    if let (Some(ml), Some(mc), Some(mr)) = (
                        grid.m[grid.idx(ix - 1, iy)],
                        grid.m[idx],
                        grid.m[grid.idx(ix + 1, iy)],
                    ) {
                        Some((mr - 2.0 * mc + ml) / (dx * dx))
                    } else {
                        None
                    }
                } else {
                    None
                };
                let d2dy2 = if iy > 0 && iy + 1 < grid.ny {
                    if let (Some(mb), Some(mc), Some(mt)) = (
                        grid.m[grid.idx(ix, iy - 1)],
                        grid.m[idx],
                        grid.m[grid.idx(ix, iy + 1)],
                    ) {
                        Some((mt - 2.0 * mc + mb) / (dy * dy))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let (Some(a), Some(b)) = (d2dx2, d2dy2) {
                    let fvd_est = -(a + b);
                    dmdz[idx] = Some(-fvd_est * flight_height_m * 0.5);
                }
            }
        }
    }

    Derivatives { dmdx, dmdy, dmdz }
}

// ── 4×4 Gaussian elimination ──────────────────────────────────────────────────

/// Solve the 4×4 augmented system [A | b] using partial-pivot Gaussian
/// elimination.  Returns `None` if the matrix is singular.
fn gauss4(mut m: [[f64; 5]; 4]) -> Option<[f64; 4]> {
    for col in 0..4 {
        // Partial pivot
        let mut best_row = col;
        let mut best_val = m[col][col].abs();
        for row in (col + 1)..4 {
            if m[row][col].abs() > best_val {
                best_val = m[row][col].abs();
                best_row = row;
            }
        }
        if best_val < 1e-14 {
            return None;
        }
        m.swap(col, best_row);

        let pivot = m[col][col];
        for row in (col + 1)..4 {
            let f = m[row][col] / pivot;
            for k in col..5 {
                m[row][k] -= f * m[col][k];
            }
        }
    }
    // Back-substitution
    let mut x = [0.0_f64; 4];
    for i in (0..4).rev() {
        x[i] = m[i][4];
        for j in (i + 1)..4 {
            x[i] -= m[i][j] * x[j];
        }
        x[i] /= m[i][i];
    }
    Some(x)
}

// ── Per-window Euler solve ────────────────────────────────────────────────────

/// One candidate solution from a single (window, N) pair.
#[derive(Clone)]
#[allow(dead_code)]
struct EulerSolution {
    /// Estimated source easting (projected CRS).
    x: f64,
    /// Estimated source northing.
    y: f64,
    /// Source depth below flight level (positive metres).
    depth_m: f64,
    /// Structural index of best-fit N.
    n_index: f64,
    /// Background field estimate (nT).
    background: f64,
    /// Normalised RMS residual (lower is better; 0 = perfect).
    norm_residual: f64,
    /// Mean |M| in window (nT) — proxy for anomaly amplitude.
    mean_anomaly_nt: f64,
    /// Window centre easting.
    win_cx: f64,
    /// Window centre northing.
    win_cy: f64,
}

/// Data passed for one grid cell inside a window.
type WinPoint = (f64, f64, f64, f64, f64, f64); // (x, y, M, dMdx, dMdy, dMdz)

/// Attempt to solve the Euler system for a single structural index `n`.
///
/// The observation equation for point `(xᵢ, yᵢ)` at flight altitude
/// (z ≈ 0 in local coordinates) with source depth h > 0:
///
///   x₀·∂M/∂x  +  y₀·∂M/∂y  −  h·∂M/∂z  +  N·B  =  xᵢ·∂M/∂x + yᵢ·∂M/∂y + N·Mᵢ
///
/// Unknowns: [x₀, y₀, h, B] — solved via normal equations with Tikhonov
/// regularisation on the spatial unknowns.
fn try_solve_n(pts: &[WinPoint], n: f64) -> Option<(f64, f64, f64, f64, f64)> {
    let k = pts.len();
    if k < 5 {
        return None;
    }

    // Build A^T A (4×4) and A^T b (4×1).
    let mut ata = [[0.0_f64; 4]; 4];
    let mut atb = [0.0_f64; 4];

    for &(xi, yi, mi, dx, dy, dz) in pts {
        let row: [f64; 4] = [dx, dy, -dz, n];
        let rhs = xi * dx + yi * dy + n * mi;
        for j in 0..4 {
            atb[j] += row[j] * rhs;
            for l in 0..4 {
                ata[j][l] += row[j] * row[l];
            }
        }
    }

    // Tikhonov: add λ·I to the spatial unknowns (not to background B).
    // λ = 1e-6 × max diagonal element prevents numerical rank deficiency
    // when the window has low horizontal gradient variation.
    let diag_max = (0..3).map(|i| ata[i][i]).fold(0.0_f64, f64::max);
    let lambda = diag_max * 1e-6;
    for i in 0..3 {
        ata[i][i] += lambda;
    }

    // Build augmented matrix [A^T A | A^T b].
    let mut aug = [[0.0_f64; 5]; 4];
    for i in 0..4 {
        for j in 0..4 {
            aug[i][j] = ata[i][j];
        }
        aug[i][4] = atb[i];
    }

    let sol = gauss4(aug)?;
    let (x0, y0, h, b) = (sol[0], sol[1], sol[2], sol[3]);

    // Compute normalised RMS residual.
    let mut ss = 0.0_f64;
    let mut mean_m = 0.0_f64;
    for &(xi, yi, mi, dx, dy, dz) in pts {
        let eq = x0 * dx + y0 * dy - h * dz + n * b - (xi * dx + yi * dy + n * mi);
        ss += eq * eq;
        mean_m += mi.abs();
    }
    mean_m /= k as f64;
    let rms_norm = if mean_m > 1e-9 {
        (ss / k as f64).sqrt() / mean_m
    } else {
        f64::INFINITY
    };

    Some((x0, y0, h, b, rms_norm))
}

/// Collect window points and attempt Euler solve for each requested N.
/// Returns the best-fit solution (lowest normalised residual).
fn solve_window(
    grid: &MagGrid,
    deriv: &Derivatives,
    params: &DepthParams,
    ic: usize, // window centre column
    ir: usize, // window centre row
) -> Option<EulerSolution> {
    let half = params.window_cells / 2;
    let ix_lo = ic.saturating_sub(half);
    let ix_hi = (ic + half + 1).min(grid.nx);
    let iy_lo = ir.saturating_sub(half);
    let iy_hi = (ir + half + 1).min(grid.ny);

    let mut pts: Vec<WinPoint> = Vec::with_capacity(params.window_cells * params.window_cells);

    for iy in iy_lo..iy_hi {
        for ix in ix_lo..ix_hi {
            let idx = grid.idx(ix, iy);
            if let (Some(m), Some(dx), Some(dy), Some(dz)) = (
                grid.m[idx],
                deriv.dmdx[idx],
                deriv.dmdy[idx],
                deriv.dmdz[idx],
            ) {
                // Skip cells where all derivatives vanish (uninformative).
                if dx.abs() < 1e-9 && dy.abs() < 1e-9 && dz.abs() < 1e-9 {
                    continue;
                }
                pts.push((grid.x_of(ix), grid.y_of(iy), m, dx, dy, dz));
            }
        }
    }

    // Require at least 8 valid points for a stable 4-unknown solve.
    if pts.len() < 8 {
        return None;
    }

    let ns: &[f64] = match params.structural_index {
        Some(n) => match n as u8 {
            0 => &[0.0],
            1 => &[1.0],
            2 => &[2.0],
            _ => &[3.0],
        },
        None => &[0.0, 1.0, 2.0, 3.0],
    };

    let win_cx = grid.x_of(ic);
    let win_cy = grid.y_of(ir);
    let win_half_m = half as f64 * grid.resolution_m;

    let mean_anomaly_nt =
        pts.iter().map(|(_, _, m, _, _, _)| m.abs()).sum::<f64>() / pts.len() as f64;

    let mut best: Option<EulerSolution> = None;

    for &n in ns {
        let Some((x0, y0, h, bg, res)) = try_solve_n(&pts, n) else {
            continue;
        };

        // Depth must be positive and within bounds.
        if h < params.min_depth_m || h > params.max_depth_m {
            continue;
        }

        // Source location must not be wildly offset from the window centre.
        let dx_off = (x0 - win_cx).abs();
        let dy_off = (y0 - win_cy).abs();
        if dx_off > params.max_offset_factor * win_half_m * 2.0 {
            continue;
        }
        if dy_off > params.max_offset_factor * win_half_m * 2.0 {
            continue;
        }

        let candidate = EulerSolution {
            x: x0,
            y: y0,
            depth_m: h,
            n_index: n,
            background: bg,
            norm_residual: res,
            mean_anomaly_nt,
            win_cx,
            win_cy,
        };

        if best
            .as_ref()
            .map_or(true, |b: &EulerSolution| res < b.norm_residual)
        {
            best = Some(candidate);
        }
    }

    best
}

// ── Confidence score ──────────────────────────────────────────────────────────

/// Map normalised residual to [0, 1] confidence.
/// norm_residual = 0 → confidence = 1; norm_residual → ∞ → confidence → 0.
fn confidence(norm_residual: f64) -> f64 {
    1.0 / (1.0 + norm_residual * 2.0)
}

// ── Main entry point ──────────────────────────────────────────────────────────

pub async fn run_magnetic_depth_model(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let params = DepthParams::from_job(job);

    // ── Find and parse the magnetic grid artifact ─────────────────────────────
    let mut grid_opt: Option<MagGrid> = None;
    let mut source_epsg: Option<i32> = None;

    for ar in &job.input_artifact_refs {
        let root = super::runtime::read_json_artifact(ctx, &ar.key).await?;
        // Accept schema_id "grid.magnetic.v1" or any artifact that has a
        // "grid" object with the expected fields.
        let is_mag_grid = root
            .get("schema_id")
            .and_then(|v| v.as_str())
            .map(|s| s.starts_with("grid.magnetic"))
            .unwrap_or(false)
            || root.get("grid").is_some();
        if is_mag_grid {
            if let Some(g) = parse_mag_grid(&root) {
                if source_epsg.is_none() {
                    source_epsg = root
                        .pointer("/crs/epsg")
                        .and_then(parse_numeric_value)
                        .map(|v| v as i32);
                }
                grid_opt = Some(g);
                break;
            }
        }
    }

    let grid = grid_opt.ok_or_else(|| {
        NodeError::InvalidConfig(
            "magnetic_depth_model: no valid magnetic grid artifact found upstream. \
             Connect the 'mag_grid' output of a magnetic_model node."
                .into(),
        )
    })?;

    let epsg = source_epsg.unwrap_or(32754); // Default: WGS84 UTM zone 54S (common AU)

    // ── Compute derivative grids ──────────────────────────────────────────────
    let deriv = compute_derivatives(&grid, params.flight_height_m);

    // ── Sliding-window Euler (parallelised) ───────────────────────────────────
    let step = params.step_cells;
    let half = params.window_cells / 2;

    // Collect all window centre coordinates upfront for parallel iteration.
    let centres: Vec<(usize, usize)> = {
        let mut v = Vec::new();
        let mut ir = half;
        while ir + half < grid.ny {
            let mut ic = half;
            while ic + half < grid.nx {
                v.push((ic, ir));
                ic += step;
            }
            ir += step;
        }
        v
    };

    let raw_solutions: Vec<EulerSolution> = centres
        .into_par_iter()
        .filter_map(|(ic, ir)| solve_window(&grid, &deriv, &params, ic, ir))
        .collect();

    // ── Filter by confidence ──────────────────────────────────────────────────
    let solutions: Vec<&EulerSolution> = raw_solutions
        .iter()
        .filter(|s| confidence(s.norm_residual) >= params.min_confidence)
        .collect();

    if solutions.is_empty() {
        return Err(NodeError::InvalidConfig(
            "magnetic_depth_model: Euler deconvolution produced no valid solutions. \
             Try reducing min_confidence, widening depth bounds, or increasing window_size."
                .into(),
        ));
    }

    // ── Compute normalisation ranges for susceptibility proxy ─────────────────
    let depth_max = solutions.iter().map(|s| s.depth_m).fold(0.0_f64, f64::max);
    let depth_min = solutions
        .iter()
        .map(|s| s.depth_m)
        .fold(f64::INFINITY, f64::min);
    let ampl_max = solutions
        .iter()
        .map(|s| s.mean_anomaly_nt)
        .fold(0.0_f64, f64::max);
    let n_sols = solutions.len();

    // Susceptibility proxy = amplitude × depth^(N+1), normalised to [0, 1].
    // For N=1 (dyke): ∝ M·h²; for N=3 (sphere): ∝ M·h⁴.
    // We normalise by the 95th-percentile value to avoid outlier saturation.
    let mut raw_proxy: Vec<f64> = solutions
        .iter()
        .map(|s| s.mean_anomaly_nt * s.depth_m.powf(s.n_index + 1.0))
        .collect();
    raw_proxy.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p95_idx = ((raw_proxy.len() as f64 * 0.95) as usize).min(raw_proxy.len() - 1);
    let proxy_scale = raw_proxy[p95_idx].max(1e-9);

    // ── Build block voxel JSON ────────────────────────────────────────────────
    let blocks: Vec<Value> = solutions
        .iter()
        .zip(raw_proxy.iter())
        .map(|(s, &proxy_raw)| {
            let conf = confidence(s.norm_residual);
            let z_src = -(s.depth_m); // negative = below surface (z = 0 = flight level)

            // Voxel edge length scales with depth so deep sources get larger
            // blocks (reflecting increased positional uncertainty).
            let bs = (s.depth_m * params.voxel_scale)
                .max(grid.resolution_m)
                .min(s.depth_m * 0.5);

            let susceptibility_proxy = (proxy_raw / proxy_scale).clamp(0.0, 1.0);

            json!({
                "x":  s.x,
                "y":  s.y,
                "z":  z_src,
                "dx": bs,
                "dy": bs,
                "dz": bs,
                "above_cutoff": true,
                "attributes": {
                    "susceptibility_proxy": susceptibility_proxy,
                    "depth_m":              s.depth_m,
                    "structural_index":     s.n_index,
                    "confidence":           conf,
                    "anomaly_nt":           s.mean_anomaly_nt,
                    "background_nt":        s.background,
                    "norm_residual":        s.norm_residual,
                }
            })
        })
        .collect();

    // ── Assemble output artifact ──────────────────────────────────────────────
    let out = json!({
        "schema_id": "block_grade_model_voxels.v1",
        "type":      "block_voxels",
        "display_contract": {
            "renderer":          "block_voxels",
            "display_pointer":   "scene3d.block_voxels",
            "measure_candidates": [
                "susceptibility_proxy",
                "depth_m",
                "confidence",
                "anomaly_nt",
                "structural_index",
            ],
            "editable": ["visible", "opacity", "palette", "measure", "cutoff"],
            "default_measure": "susceptibility_proxy"
        },
        "crs": { "epsg": epsg, "wkt": null },
        "summary": {
            "solutions":     n_sols,
            "depth_min_m":   depth_min,
            "depth_max_m":   depth_max,
            "anomaly_max_nt": ampl_max,
            "grid_nx":       grid.nx,
            "grid_ny":       grid.ny,
            "resolution_m":  grid.resolution_m,
            "window_cells":  params.window_cells,
            "step_cells":    params.step_cells,
            "flight_height_m": params.flight_height_m,
            "structural_index_mode": match params.structural_index {
                None    => "multi".to_string(),
                Some(n) => format!("fixed(N={})", n as u8),
            },
            "min_confidence": params.min_confidence,
        },
        "blocks": blocks,
    });

    let key = format!(
        "graphs/{}/nodes/{}/magnetic_depth_model.json",
        job.graph_id, job.node_id
    );
    let bytes = serde_json::to_vec(&out)?;
    let artifact =
        super::runtime::write_artifact(ctx, &key, &bytes, Some("application/json")).await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}
