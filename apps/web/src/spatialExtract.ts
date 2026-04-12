/**
 * Pull 2D map geometry from node artifact JSON (collars, merged ingest, trajectory segments).
 */

export type MapPoint = { holeId: string; x: number; y: number; label?: string };

export type MapPolyline = { holeId: string; coords: [number, number][] };

function num(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() !== "") {
    const n = parseFloat(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

export function extractCollarsFromJson(text: string): MapPoint[] {
  let root: unknown;
  try {
    root = JSON.parse(text) as unknown;
  } catch {
    return [];
  }
  if (!root || typeof root !== "object") return [];
  const o = root as Record<string, unknown>;
  const arr = o.collars;
  if (!Array.isArray(arr)) return [];
  const out: MapPoint[] = [];
  for (const row of arr) {
    if (!row || typeof row !== "object") continue;
    const r = row as Record<string, unknown>;
    const x = num(r.x);
    const y = num(r.y);
    if (x === null || y === null) continue;
    const holeId = String(r.hole_id ?? r.id ?? "?");
    out.push({ holeId, x, y, label: holeId });
  }
  return out;
}

export function extractTrajectoryPolylines(text: string): MapPolyline[] {
  let root: unknown;
  try {
    root = JSON.parse(text) as unknown;
  } catch {
    return [];
  }
  if (!Array.isArray(root)) return [];
  const byHole = new Map<string, [number, number][]>();
  for (const seg of root) {
    if (!seg || typeof seg !== "object") continue;
    const s = seg as Record<string, unknown>;
    const holeId = String(s.hole_id ?? "?");
    const xf = num(s.x_from);
    const yf = num(s.y_from);
    const xt = num(s.x_to);
    const yt = num(s.y_to);
    if (xf === null || yf === null || xt === null || yt === null) continue;
    let chain = byHole.get(holeId);
    if (!chain) {
      chain = [];
      byHole.set(holeId, chain);
    }
    if (chain.length === 0) {
      chain.push([xf, yf]);
    }
    chain.push([xt, yt]);
  }
  return [...byHole.entries()].map(([holeId, coords]) => ({ holeId, coords }));
}

export function epsgFromCollarJson(text: string): number | null {
  try {
    const root = JSON.parse(text) as Record<string, unknown>;
    const tryArray = (arr: unknown): number | null => {
      if (!Array.isArray(arr) || !arr[0] || typeof arr[0] !== "object") return null;
      const c = (arr[0] as Record<string, unknown>).crs;
      if (!c || typeof c !== "object") return null;
      const e = (c as Record<string, unknown>).epsg;
      return typeof e === "number" ? e : null;
    };
    return tryArray(root.collars) ?? tryArray(root.points) ?? null;
  } catch {
    return null;
  }
}

/** Best-effort EPSG extraction from common top-level and row-level CRS carriers. */
export function epsgFromAnyJson(text: string): number | null {
  try {
    const root = JSON.parse(text) as Record<string, unknown>;
    const top = (obj: Record<string, unknown> | null | undefined): number | null => {
      if (!obj) return null;
      const e = obj.epsg;
      return typeof e === "number" && Number.isFinite(e) ? Math.trunc(e) : null;
    };
    const asObj = (v: unknown): Record<string, unknown> | null =>
      v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;

    const direct =
      top(asObj(root.crs)) ??
      top(asObj(root.source_crs)) ??
      top(asObj(root.parsed_crs)) ??
      top(asObj(root.output_crs));
    if (direct) return direct;

    const tryArray = (arr: unknown): number | null => {
      if (!Array.isArray(arr) || !arr[0] || typeof arr[0] !== "object") return null;
      const c = (arr[0] as Record<string, unknown>).crs;
      return top(asObj(c));
    };
    return tryArray(root.collars) ?? tryArray(root.points) ?? null;
  } catch {
    return null;
  }
}

/** Generic x,y points for plan-view (no collar-specific lookup — used for viewer inputs only). */
export type PlanViewPoint = { x: number; y: number; label: string };
export type MeasuredPlanPoint = {
  x: number;
  y: number;
  label: string;
  measures: Record<string, number>;
};

export type HeatmapConfigHint = {
  measure?: string;
  renderMeasure?: string;
  method?: string;
  scale?: string;
  clampLowPct?: number;
  clampHighPct?: number;
  idwPower?: number;
  smoothness?: number;
  palette?: string;
  opacity?: number;
  minVisibleRender?: number;
  maxVisibleRender?: number;
};

export type DisplayContractHint = {
  renderer?: string;
  editable?: string[];
};

export type HeatSurfaceGrid = {
  nx: number;
  ny: number;
  xmin: number;
  xmax: number;
  ymin: number;
  ymax: number;
  values: Array<number | null>;
};

export type GeoLineString = {
  coords: [number, number][];
  level?: number;
};

/**
 * Best-effort extraction of plan-view points from any upstream JSON artifact.
 * Does not assume collars; tries collars, trajectory segments, `points` arrays, and root arrays of {x,y}.
 */
export function extractPlanViewPointsFromJson(
  text: string,
  artifactLabel: string
): PlanViewPoint[] {
  const out: PlanViewPoint[] = [];
  const prefix = artifactLabel;

  for (const c of extractCollarsFromJson(text)) {
    out.push({ x: c.x, y: c.y, label: `${prefix} · ${c.holeId}` });
  }

  for (const pl of extractTrajectoryPolylines(text)) {
    pl.coords.forEach(([x, y], i) => {
      out.push({ x, y, label: `${prefix} · ${pl.holeId} · v${i}` });
    });
  }

  let root: unknown;
  try {
    root = JSON.parse(text) as unknown;
  } catch {
    return out;
  }

  if (root && typeof root === "object" && !Array.isArray(root)) {
    const pts = (root as Record<string, unknown>).points;
    if (Array.isArray(pts)) {
      pts.forEach((p, i) => {
        if (!p || typeof p !== "object") return;
        const r = p as Record<string, unknown>;
        const x = num(r.x);
        const y = num(r.y);
        if (x === null || y === null) return;
        out.push({
          x,
          y,
          label: `${prefix} · ${String(r.id ?? r.label ?? r.name ?? i)}`,
        });
      });
    }
    const assayPts = (root as Record<string, unknown>).assay_points;
    if (Array.isArray(assayPts)) {
      assayPts.forEach((p, i) => {
        if (!p || typeof p !== "object") return;
        const r = p as Record<string, unknown>;
        const x = num(r.x);
        const y = num(r.y);
        if (x === null || y === null) return;
        out.push({
          x,
          y,
          label: `${prefix} · assay · ${String(r.hole_id ?? r.id ?? i)}`,
        });
      });
    }
  }

  if (Array.isArray(root) && root.length > 0) {
    const first = root[0];
    if (first && typeof first === "object" && "x_from" in (first as object)) {
      /* trajectory segments — already handled */
    } else {
      root.forEach((p, i) => {
        if (!p || typeof p !== "object") return;
        const r = p as Record<string, unknown>;
        const x = num(r.x);
        const y = num(r.y);
        if (x === null || y === null) return;
        out.push({
          x,
          y,
          label: `${prefix} · ${String(r.id ?? r.hole_id ?? i)}`,
        });
      });
    }
  }

  return out;
}

/** Pull assay/sample points with numeric measure attributes for heatmap use. */
export function extractMeasuredPlanPointsFromJson(
  text: string,
  artifactLabel: string
): MeasuredPlanPoint[] {
  let root: unknown;
  try {
    root = JSON.parse(text) as unknown;
  } catch {
    return [];
  }
  if (!root || typeof root !== "object" || Array.isArray(root)) return [];
  const obj = root as Record<string, unknown>;
  const arr = obj.assay_points ?? obj.points;
  if (!Array.isArray(arr)) return [];

  const out: MeasuredPlanPoint[] = [];
  arr.forEach((row, i) => {
    if (!row || typeof row !== "object") return;
    const r = row as Record<string, unknown>;
    const x = num(r.x);
    const y = num(r.y);
    if (x === null || y === null) return;

    const rawAttrs =
      r.attributes && typeof r.attributes === "object" && !Array.isArray(r.attributes)
        ? (r.attributes as Record<string, unknown>)
        : {};
    const measures: Record<string, number> = {};
    for (const [k, v] of Object.entries(rawAttrs)) {
      const n = num(v);
      if (n !== null) measures[k] = n;
    }
    if (Object.keys(measures).length === 0) return;

    out.push({
      x,
      y,
      label: `${artifactLabel} · ${String(r.hole_id ?? r.id ?? i)}`,
      measures,
    });
  });
  return out;
}

export function extractHeatmapConfigFromJson(text: string): HeatmapConfigHint | null {
  let root: unknown;
  try {
    root = JSON.parse(text) as unknown;
  } catch {
    return null;
  }
  if (!root || typeof root !== "object" || Array.isArray(root)) return null;
  const obj = root as Record<string, unknown>;
  const cfgRaw = obj.heatmap_config;
  if (!cfgRaw || typeof cfgRaw !== "object" || Array.isArray(cfgRaw)) {
    return null;
  }
  const cfg = cfgRaw as Record<string, unknown>;
  const out: HeatmapConfigHint = {};
  if (typeof cfg.measure === "string") out.measure = cfg.measure;
  if (typeof cfg.render_measure === "string") out.renderMeasure = cfg.render_measure;
  if (typeof cfg.method === "string") out.method = cfg.method;
  if (typeof cfg.scale === "string") out.scale = cfg.scale;
  if (typeof cfg.palette === "string") out.palette = cfg.palette;
  if (typeof cfg.clamp_low_pct === "number") out.clampLowPct = cfg.clamp_low_pct;
  if (typeof cfg.clamp_high_pct === "number") out.clampHighPct = cfg.clamp_high_pct;
  if (typeof cfg.idw_power === "number") out.idwPower = cfg.idw_power;
  if (typeof cfg.smoothness === "number") out.smoothness = cfg.smoothness;
  if (typeof cfg.opacity === "number") out.opacity = cfg.opacity;
  if (typeof cfg.min_visible_render === "number") out.minVisibleRender = cfg.min_visible_render;
  if (typeof cfg.max_visible_render === "number") out.maxVisibleRender = cfg.max_visible_render;
  return out;
}

export function extractDisplayContractFromJson(text: string): DisplayContractHint | null {
  let root: unknown;
  try {
    root = JSON.parse(text) as unknown;
  } catch {
    return null;
  }
  if (!root || typeof root !== "object" || Array.isArray(root)) return null;
  const obj = root as Record<string, unknown>;
  const c = obj.display_contract;
  if (!c || typeof c !== "object" || Array.isArray(c)) return null;
  const cc = c as Record<string, unknown>;
  const out: DisplayContractHint = {};
  if (typeof cc.renderer === "string") out.renderer = cc.renderer;
  if (Array.isArray(cc.editable)) {
    out.editable = cc.editable
      .filter((x): x is string => typeof x === "string")
      .map((x) => x.trim())
      .filter((x) => x.length > 0);
  }
  return out;
}

export function extractHeatSurfaceGridFromJson(text: string): HeatSurfaceGrid | null {
  let root: unknown;
  try {
    root = JSON.parse(text) as unknown;
  } catch {
    return null;
  }
  if (!root || typeof root !== "object" || Array.isArray(root)) return null;
  const obj = root as Record<string, unknown>;
  const g = obj.surface_grid;
  if (!g || typeof g !== "object" || Array.isArray(g)) return null;
  const gg = g as Record<string, unknown>;
  const nx = typeof gg.nx === "number" ? Math.trunc(gg.nx) : 0;
  const ny = typeof gg.ny === "number" ? Math.trunc(gg.ny) : 0;
  const xmin = num(gg.xmin);
  const xmax = num(gg.xmax);
  const ymin = num(gg.ymin);
  const ymax = num(gg.ymax);
  const valuesRaw = gg.values;
  if (
    nx <= 1 ||
    ny <= 1 ||
    xmin === null ||
    xmax === null ||
    ymin === null ||
    ymax === null ||
    !Array.isArray(valuesRaw)
  ) {
    return null;
  }
  const values: Array<number | null> = valuesRaw.map((v) => {
    const n = num(v);
    return n === null ? null : n;
  });
  if (values.length !== nx * ny) return null;
  return { nx, ny, xmin, xmax, ymin, ymax, values };
}

export function extractHeatmapMeasureCandidatesFromJson(text: string): string[] {
  let root: unknown;
  try {
    root = JSON.parse(text) as unknown;
  } catch {
    return [];
  }
  if (!root || typeof root !== "object" || Array.isArray(root)) return [];
  const obj = root as Record<string, unknown>;
  const arr = obj.measure_candidates;
  if (!Array.isArray(arr)) return [];
  return arr
    .filter((x): x is string => typeof x === "string")
    .map((x) => x.trim())
    .filter((x) => x.length > 0);
}

export function extractLineFeaturesFromGeoJson(text: string): GeoLineString[] {
  let root: unknown;
  try {
    root = JSON.parse(text) as unknown;
  } catch {
    return [];
  }
  if (!root || typeof root !== "object") return [];
  const obj = root as Record<string, unknown>;
  const features = obj.features;
  if (!Array.isArray(features)) return [];
  const out: GeoLineString[] = [];
  for (const f of features) {
    if (!f || typeof f !== "object") continue;
    const ff = f as Record<string, unknown>;
    const g = ff.geometry;
    if (!g || typeof g !== "object") continue;
    const gg = g as Record<string, unknown>;
    if (gg.type !== "LineString") continue;
    const coordsRaw = gg.coordinates;
    if (!Array.isArray(coordsRaw)) continue;
    const coords: [number, number][] = [];
    for (const c of coordsRaw) {
      if (!Array.isArray(c) || c.length < 2) continue;
      const x = num(c[0]);
      const y = num(c[1]);
      if (x === null || y === null) continue;
      coords.push([x, y]);
    }
    if (coords.length < 2) continue;
    const level =
      ff.properties &&
      typeof ff.properties === "object" &&
      !Array.isArray(ff.properties)
        ? num((ff.properties as Record<string, unknown>).level)
        : null;
    out.push({ coords, level: level ?? undefined });
  }
  return out;
}
