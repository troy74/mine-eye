/**
 * Reproject projected easting/northing to WGS84 lon/lat for Leaflet.
 * Uses proj4 with a small set of defs; extend EPSG_DEFS as needed.
 */
import proj4 from "proj4";

const WGS84 = "EPSG:4326";

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

function ensureDef(epsg: number): boolean {
  if (epsg === 4326) return true;
  const def = EPSG_DEFS[epsg];
  if (!def) return false;
  const key = `EPSG:${epsg}`;
  try {
    if (!proj4.defs(key)) proj4.defs(key, def);
  } catch {
    return false;
  }
  return true;
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
