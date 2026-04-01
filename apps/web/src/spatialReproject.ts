/**
 * Reproject projected easting/northing to WGS84 lon/lat for Leaflet.
 * Uses proj4 with:
 * - built-in curated defs
 * - persisted user-fetched defs (localStorage)
 * - runtime EPSG lookup from epsg.io when unknown
 */
import proj4 from "proj4";

const WGS84 = "EPSG:4326";
const PROJ4_CACHE_KEY = "mineeye:proj4defs:v1";

/** proj4 definition strings (horizontal). */
const EPSG_DEFS: Record<number, string> = {
  4326: "+proj=longlat +datum=WGS84 +no_defs +type=crs",
  // GDA2020 / MGA (approximate towgs84; adequate for map dots)
  7855: "+proj=utm +zone=55 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs",
  7856: "+proj=utm +zone=56 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs",
  7850: "+proj=utm +zone=50 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs",
  28355: "+proj=utm +zone=55 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs",
  28356: "+proj=utm +zone=56 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs",
  28350: "+proj=utm +zone=50 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs",
};

function loadCachedDefs(): Record<string, string> {
  try {
    const raw = localStorage.getItem(PROJ4_CACHE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw) as Record<string, string>;
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function saveCachedDef(epsg: number, def: string) {
  const all = loadCachedDefs();
  all[String(epsg)] = def;
  localStorage.setItem(PROJ4_CACHE_KEY, JSON.stringify(all));
}

function ensureDef(epsg: number): boolean {
  if (epsg === 4326) return true;
  const def = EPSG_DEFS[epsg] ?? loadCachedDefs()[String(epsg)];
  if (!def) return false;
  const key = `EPSG:${epsg}`;
  try {
    if (!proj4.defs(key)) proj4.defs(key, def);
  } catch {
    return false;
  }
  return true;
}

async function fetchProj4Def(epsg: number): Promise<string | null> {
  try {
    const url = `https://epsg.io/${epsg}.proj4`;
    const r = await fetch(url, { cache: "force-cache" });
    if (!r.ok) return null;
    const text = (await r.text()).trim();
    if (!text || text.toLowerCase().includes("not found")) return null;
    return text;
  } catch {
    return null;
  }
}

async function ensureDefAsync(epsg: number): Promise<boolean> {
  if (ensureDef(epsg)) return true;
  if (epsg === 4326) return true;
  const fetched = await fetchProj4Def(epsg);
  if (!fetched) return false;
  try {
    const key = `EPSG:${epsg}`;
    proj4.defs(key, fetched);
    saveCachedDef(epsg, fetched);
    return true;
  } catch {
    return false;
  }
}

/** @returns [lon, lat] in degrees, or null if unsupported EPSG */
export function lonLatFromProjected(
  epsg: number,
  x: number,
  y: number
): [number, number] | null {
  if (epsg === 4326) return [x, y];
  if (!ensureDef(epsg)) return null;
  const key = `EPSG:${epsg}`;
  const out = proj4(key, WGS84, [x, y]);
  if (!Array.isArray(out) || out.length < 2) return null;
  const lon = out[0];
  const lat = out[1];
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return null;
  return [lon, lat];
}

/** Async version that can fetch missing proj4 defs on demand. */
export async function lonLatFromProjectedAsync(
  epsg: number,
  x: number,
  y: number
): Promise<[number, number] | null> {
  if (epsg === 4326) return [x, y];
  const ok = await ensureDefAsync(epsg);
  if (!ok) return null;
  const key = `EPSG:${epsg}`;
  const out = proj4(key, WGS84, [x, y]);
  if (!Array.isArray(out) || out.length < 2) return null;
  const lon = out[0];
  const lat = out[1];
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return null;
  return [lon, lat];
}
