/**
 * Nominatim geocoding utilities (OpenStreetMap).
 * Free, no API key required. Rate limit: 1 req/sec.
 * https://nominatim.openstreetmap.org/
 */

export type NominatimResult = {
  place_id: number;
  display_name: string;
  /** [south, north, west, east] as strings */
  boundingbox: [string, string, string, string];
  lat: string;
  lon: string;
  type: string;
  class: string;
  importance: number;
};

/** Search for places by name. Returns up to `limit` results. */
export async function geocodePlace(
  query: string,
  limit = 8
): Promise<NominatimResult[]> {
  const url =
    `https://nominatim.openstreetmap.org/search` +
    `?q=${encodeURIComponent(query)}` +
    `&format=json` +
    `&addressdetails=0` +
    `&limit=${limit}`;

  const resp = await fetch(url, {
    headers: { "Accept-Language": "en", "User-Agent": "MineEye/1.0" },
  });
  if (!resp.ok) throw new Error(`Nominatim error ${resp.status}`);
  return (await resp.json()) as NominatimResult[];
}

/**
 * Convert a Nominatim bounding box [south, north, west, east]
 * to AOI bbox format [xmin, ymin, xmax, ymax] = [west, south, east, north].
 */
export function nominatimBboxToAoi(
  bb: [string, string, string, string]
): [number, number, number, number] {
  const south = parseFloat(bb[0]);
  const north = parseFloat(bb[1]);
  const west = parseFloat(bb[2]);
  const east = parseFloat(bb[3]);
  return [west, south, east, north];
}

/** Format an AOI bbox as a readable string */
export function formatAoiBbox(bbox: [number, number, number, number]): string {
  return bbox.map((v) => v.toFixed(6)).join(", ");
}
