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
    const arr = root.collars;
    if (!Array.isArray(arr) || !arr[0] || typeof arr[0] !== "object") return null;
    const c = (arr[0] as Record<string, unknown>).crs;
    if (!c || typeof c !== "object") return null;
    const e = (c as Record<string, unknown>).epsg;
    return typeof e === "number" ? e : null;
  } catch {
    return null;
  }
}

/** Generic x,y points for plan-view (no collar-specific lookup — used for viewer inputs only). */
export type PlanViewPoint = { x: number; y: number; label: string };

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
