export type EpsgSearchHit = {
  code: string;
  name: string;
};

function normHit(v: unknown): EpsgSearchHit | null {
  if (!v || typeof v !== "object") return null;
  const o = v as Record<string, unknown>;
  const codeRaw = o.code ?? o.srid ?? o.auth_srid;
  const nameRaw = o.name ?? o.title;
  const code = String(codeRaw ?? "").trim();
  const name = String(nameRaw ?? "").trim();
  if (!/^\d+$/.test(code) || name.length === 0) return null;
  return { code, name };
}

/** Search EPSG registry by code/name via epsg.io JSON API. */
export async function searchEpsg(query: string): Promise<EpsgSearchHit[]> {
  const q = query.trim();
  if (q.length === 0 || (!/^\d+$/.test(q) && q.length < 2)) return [];
  const url = `/api/epsg/search?q=${encodeURIComponent(q)}`;
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) {
    const msg = (await r.text().catch(() => "")).trim();
    throw new Error(
      msg.length > 0 ? `EPSG search failed: HTTP ${r.status} - ${msg}` : `EPSG search failed: HTTP ${r.status}`
    );
  }
  const text = await r.text();
  let raw: unknown;
  try {
    raw = JSON.parse(text) as unknown;
  } catch {
    throw new Error("EPSG search failed: invalid JSON from API");
  }
  const root = raw as Record<string, unknown>;
  const arr = Array.isArray(root.results)
    ? root.results
    : Array.isArray(raw)
      ? raw
      : [];
  const hits: EpsgSearchHit[] = [];
  for (const item of arr) {
    const h = normHit(item);
    if (h) hits.push(h);
  }
  const uniq = new Map<string, EpsgSearchHit>();
  for (const h of hits) {
    if (!uniq.has(h.code)) uniq.set(h.code, h);
  }
  return [...uniq.values()].slice(0, 30);
}
