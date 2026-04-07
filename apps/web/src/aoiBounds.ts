import { lonLatFromProjectedAsync } from "./spatialReproject";

export type AoiBbox = [number, number, number, number]; // [xmin,ymin,xmax,ymax]

function toFiniteBbox(values: unknown[]): AoiBbox | null {
  if (values.length < 4) return null;
  const n = values.slice(0, 4).map(Number);
  if (!n.every(Number.isFinite)) return null;
  const [x0, y0, x1, y1] = n as AoiBbox;
  const xmin = Math.min(x0, x1);
  const xmax = Math.max(x0, x1);
  const ymin = Math.min(y0, y1);
  const ymax = Math.max(y0, y1);
  return [xmin, ymin, xmax, ymax];
}

function bboxFromObject(v: Record<string, unknown>): AoiBbox | null {
  const xmin = Number(v.xmin ?? v.west);
  const ymin = Number(v.ymin ?? v.south);
  const xmax = Number(v.xmax ?? v.east);
  const ymax = Number(v.ymax ?? v.north);
  if (![xmin, ymin, xmax, ymax].every(Number.isFinite)) return null;
  return [Math.min(xmin, xmax), Math.min(ymin, ymax), Math.max(xmin, xmax), Math.max(ymin, ymax)];
}

function bboxFromGeometry(data: Record<string, unknown>): AoiBbox | null {
  const geometry = data.geometry;
  if (!geometry || typeof geometry !== "object" || Array.isArray(geometry)) return null;
  const g = geometry as Record<string, unknown>;
  const coords = g.coordinates;
  if (!Array.isArray(coords)) return null;
  const stack: unknown[] = [...coords];
  let xmin = Number.POSITIVE_INFINITY;
  let ymin = Number.POSITIVE_INFINITY;
  let xmax = Number.NEGATIVE_INFINITY;
  let ymax = Number.NEGATIVE_INFINITY;
  while (stack.length > 0) {
    const v = stack.pop();
    if (!Array.isArray(v)) continue;
    if (v.length >= 2 && typeof v[0] === "number" && typeof v[1] === "number") {
      const x = v[0];
      const y = v[1];
      xmin = Math.min(xmin, x);
      ymin = Math.min(ymin, y);
      xmax = Math.max(xmax, x);
      ymax = Math.max(ymax, y);
    } else {
      for (const child of v) stack.push(child);
    }
  }
  if (![xmin, ymin, xmax, ymax].every(Number.isFinite)) return null;
  return [xmin, ymin, xmax, ymax];
}

export function extractBboxAndEpsg(data: Record<string, unknown>): { bbox: AoiBbox; epsg: number } | null {
  let epsg = 4326;
  const crs = data.crs;
  if (crs && typeof crs === "object" && !Array.isArray(crs)) {
    const raw = (crs as Record<string, unknown>).epsg;
    if (typeof raw === "number" && Number.isFinite(raw)) epsg = Math.trunc(raw);
    if (typeof raw === "string" && /^\d+$/.test(raw)) epsg = parseInt(raw, 10);
  }

  for (const key of ["bbox", "extent", "bounds", "bounding_box", "envelope"]) {
    const v = data[key];
    if (Array.isArray(v)) {
      const fromArray = toFiniteBbox(v);
      if (fromArray) return { bbox: fromArray, epsg };
      continue;
    }
    if (v && typeof v === "object" && !Array.isArray(v)) {
      const fromObj = bboxFromObject(v as Record<string, unknown>);
      if (fromObj) return { bbox: fromObj, epsg };
    }
  }

  const fromGeometry = bboxFromGeometry(data);
  if (fromGeometry) return { bbox: fromGeometry, epsg };
  return null;
}

export async function toWgs84Bbox(bbox: AoiBbox, epsg: number): Promise<AoiBbox | null> {
  if (epsg === 4326) {
    const [xmin, ymin, xmax, ymax] = bbox;
    if (xmin < -180 || xmax > 180 || ymin < -90 || ymax > 90) return null;
    return bbox;
  }

  const [xmin, ymin, xmax, ymax] = bbox;
  const corners: Array<[number, number]> = [
    [xmin, ymin],
    [xmin, ymax],
    [xmax, ymin],
    [xmax, ymax],
  ];
  const ll = await Promise.all(corners.map(([x, y]) => lonLatFromProjectedAsync(epsg, x, y)));
  if (ll.some((p) => !p)) return null;
  const pts = ll as [number, number][];
  const lons = pts.map((p) => p[0]);
  const lats = pts.map((p) => p[1]);
  const west = Math.min(...lons);
  const east = Math.max(...lons);
  const south = Math.min(...lats);
  const north = Math.max(...lats);
  if (west < -180 || east > 180 || south < -90 || north > 90) return null;
  return [west, south, east, north];
}

