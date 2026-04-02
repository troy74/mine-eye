import { useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import { ACQUISITION_EPSG_OPTIONS } from "./crsOptions";
import { searchEpsg, type EpsgSearchHit } from "./epsgSearch";

const CACHE_KEY = "mineeye:epsg_picker_cache:v1";

type CrsOption = {
  value: string;
  label: string;
  source: "project" | "common" | "workspace" | "cached" | "search" | "derived";
};

function loadCache(): EpsgSearchHit[] {
  try {
    const raw = localStorage.getItem(CACHE_KEY);
    if (!raw) return [];
    const arr = JSON.parse(raw) as unknown[];
    if (!Array.isArray(arr)) return [];
    return arr
      .filter((x): x is EpsgSearchHit => {
        if (!x || typeof x !== "object") return false;
        const o = x as Record<string, unknown>;
        return typeof o.code === "string" && typeof o.name === "string";
      })
      .slice(0, 80);
  } catch {
    return [];
  }
}

function saveCache(next: EpsgSearchHit[]) {
  try {
    localStorage.setItem(CACHE_KEY, JSON.stringify(next.slice(0, 80)));
  } catch {
    /* ignore */
  }
}

function mergeHits(base: EpsgSearchHit[], extra: EpsgSearchHit[]): EpsgSearchHit[] {
  const map = new Map<string, EpsgSearchHit>();
  for (const h of base) map.set(h.code, h);
  for (const h of extra) map.set(h.code, h);
  return [...map.values()];
}

function tokenize(q: string): string[] {
  return q
    .toLowerCase()
    .split(/[,\s/|]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function normalizeText(s: string): string {
  return s.toLowerCase().replace(/\s+/g, " ").trim();
}

function utmFallbackHits(query: string): EpsgSearchHit[] {
  const q = normalizeText(query);
  const toks = tokenize(query);
  const out: EpsgSearchHit[] = [];
  const add = (zone: number, hemi: "N" | "S") => {
    const code = hemi === "N" ? String(32600 + zone) : String(32700 + zone);
    out.push({ code, name: `WGS 84 / UTM zone ${zone}${hemi}` });
  };
  const m = /(?:^|\s)(\d{1,2})\s*([ns])(?:\b|$)/i.exec(q);
  if (m) {
    const zone = parseInt(m[1], 10);
    const hemi = m[2].toUpperCase() as "N" | "S";
    if (zone >= 1 && zone <= 60) add(zone, hemi);
    return out;
  }
  const hasUtm = toks.includes("utm");
  const zoneTok = toks.find((t) => /^\d{1,2}$/.test(t));
  const hemiTok = toks.find((t) => t === "n" || t === "s");
  if (hasUtm && zoneTok && hemiTok) {
    const zone = parseInt(zoneTok, 10);
    const hemi = hemiTok.toUpperCase() as "N" | "S";
    if (zone >= 1 && zone <= 60) add(zone, hemi);
    return out;
  }
  if (hasUtm) {
    for (let z = 1; z <= 60; z += 1) {
      add(z, "N");
      add(z, "S");
    }
  }
  return out;
}

type Props = {
  value: string;
  onChange: (next: string) => void;
  projectEpsg: number;
  workspaceUsedEpsgs?: number[];
  includeProject?: boolean;
  placeholder?: string;
};

export function CrsPicker({
  value,
  onChange,
  projectEpsg,
  workspaceUsedEpsgs = [],
  includeProject = true,
  placeholder = "Search EPSG code or name…",
}: Props) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [cached, setCached] = useState<EpsgSearchHit[]>(() => loadCache());
  const [remote, setRemote] = useState<EpsgSearchHit[]>([]);
  const [searchBusy, setSearchBusy] = useState(false);

  const common = useMemo(() => {
    return ACQUISITION_EPSG_OPTIONS.filter((o) => /^\d+$/.test(o.value)).map((o) => ({
      code: o.value,
      name: o.label.replace(/^EPSG:\d+\s*/, "").trim(),
    }));
  }, []);

  useEffect(() => {
    const q = query.trim();
    if (q.length === 0 || (!/^\d+$/.test(q) && q.length < 2)) {
      setRemote([]);
      setSearchBusy(false);
      return;
    }
    let cancelled = false;
    const tid = window.setTimeout(() => {
      void (async () => {
        setSearchBusy(true);
        try {
          let hits = await searchEpsg(q);
          // UTM shorthand helper: "30N" => "UTM zone 30N"
          if (hits.length === 0) {
            const m = /^(\d{1,2})\s*([ns])$/i.exec(q);
            if (m) {
              hits = await searchEpsg(`utm zone ${m[1]}${m[2].toUpperCase()}`);
            }
          }
          if (hits.length === 0) {
            hits = utmFallbackHits(q);
          }
          if (cancelled) return;
          setRemote(hits);
          const merged = mergeHits(cached, hits);
          setCached(merged);
          saveCache(merged);
        } catch {
          if (!cancelled) setRemote([]);
        } finally {
          if (!cancelled) setSearchBusy(false);
        }
      })();
    }, 220);
    return () => {
      cancelled = true;
      window.clearTimeout(tid);
    };
  }, [cached, query]);

  useEffect(() => {
    if (!open) return;
    const onDocDown = (ev: MouseEvent) => {
      const el = rootRef.current;
      const t = ev.target as Node | null;
      if (!el || !t || el.contains(t)) return;
      setOpen(false);
      setQuery("");
    };
    document.addEventListener("mousedown", onDocDown);
    return () => document.removeEventListener("mousedown", onDocDown);
  }, [open]);

  const options = useMemo(() => {
    const out: CrsOption[] = [];
    if (includeProject && Number.isFinite(projectEpsg) && projectEpsg > 0) {
      out.push({
        value: "project",
        label: `Project CRS (EPSG:${projectEpsg})`,
        source: "project",
      });
    }
    for (const epsg of workspaceUsedEpsgs) {
      if (!Number.isFinite(epsg) || epsg <= 0 || Math.trunc(epsg) === Math.trunc(projectEpsg)) {
        continue;
      }
      out.push({
        value: String(Math.trunc(epsg)),
        label: `EPSG:${Math.trunc(epsg)} - used in workspace`,
        source: "workspace",
      });
    }
    const add = (h: EpsgSearchHit, source: CrsOption["source"]) => {
      out.push({ value: h.code, label: `EPSG:${h.code} - ${h.name}`, source });
    };
    for (const h of common) add(h, "common");
    for (const h of cached) add(h, "cached");
    for (const h of remote) add(h, "search");
    for (const h of utmFallbackHits(query)) add(h, "derived");
    const uniq = new Map<string, CrsOption>();
    for (const o of out) {
      if (!uniq.has(o.value)) uniq.set(o.value, o);
    }
    const queryRaw = query.trim();
    if (/^\d+$/.test(queryRaw) && !uniq.has(queryRaw)) {
      uniq.set(queryRaw, {
        value: queryRaw,
        label: `EPSG:${queryRaw} - use this code`,
        source: "search",
      });
    }
    const all = [...uniq.values()];
    const q = queryRaw.toLowerCase();
    if (!q) return all.slice(0, 60);
    const toks = tokenize(q);
    return all
      .filter((o) => {
        const hay = `${o.value} ${normalizeText(o.label)}`;
        return toks.every((t) => hay.includes(t));
      })
      .slice(0, 80);
  }, [cached, common, includeProject, projectEpsg, query, remote, workspaceUsedEpsgs]);

  const quickOptions = useMemo(() => {
    const out: CrsOption[] = [];
    for (const o of options) {
      if (o.source === "project" || o.source === "workspace" || o.source === "cached") {
        out.push(o);
      }
      if (out.length >= 8) break;
    }
    if (out.length === 0) {
      return options.slice(0, 5);
    }
    return out;
  }, [options]);

  const results = useMemo(() => {
    const q = query.trim();
    if (!q) return options.slice(0, 20);
    return options;
  }, [options, query]);

  const selectedLabel = useMemo(() => {
    if (value === "project") return `Project CRS (EPSG:${projectEpsg})`;
    const fromOptions = options.find((o) => o.value === value);
    if (fromOptions) return fromOptions.label;
    const fromCache = cached.find((h) => h.code === value);
    if (fromCache) return `EPSG:${fromCache.code} - ${fromCache.name}`;
    return value ? `EPSG:${value}` : "";
  }, [cached, options, projectEpsg, value]);

  return (
    <div ref={rootRef} style={{ position: "relative" }}>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        style={selectedButtonStyle}
        title={selectedLabel}
      >
        <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {selectedLabel || "Select CRS…"}
        </span>
        <span style={{ opacity: 0.7, marginLeft: 8 }}>▾</span>
      </button>
      {open && (
        <div
          style={menuStyle}
          onMouseDown={(e) => {
            // Prevent outside blur handlers from closing while interacting inside.
            e.stopPropagation();
          }}
        >
          <div style={searchWrapStyle}>
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Escape") {
                  setOpen(false);
                  setQuery("");
                }
              }}
              autoFocus
              placeholder={placeholder}
              style={searchInputStyle}
            />
            {query.length > 0 && (
              <button
                type="button"
                style={clearBtnStyle}
                onClick={() => setQuery("")}
                title="Clear search"
              >
                Clear
              </button>
            )}
          </div>
          {quickOptions.length > 0 && (
            <div style={quickSectionStyle}>
              <div style={sectionLabelStyle}>Quick picks</div>
              <div style={quickGridStyle}>
                {quickOptions.map((o) => (
                  <button
                    key={`quick:${o.source}:${o.value}`}
                    type="button"
                    style={quickBtnStyle}
                    onClick={() => {
                      onChange(o.value);
                      setOpen(false);
                      setQuery("");
                    }}
                    title={o.label}
                  >
                    {o.value === "project" ? "Project CRS" : `EPSG:${o.value}`}
                  </button>
                ))}
              </div>
            </div>
          )}
          {searchBusy && <div style={busyStyle}>Searching full EPSG registry…</div>}
          <div style={resultsHeaderStyle}>
            {query.trim() ? "Search results" : "Suggested CRS"}
          </div>
          {results.length === 0 ? (
            <div style={emptyStyle}>
              No matches. Try terms like <code style={codeStyle}>UTM</code>,{" "}
              <code style={codeStyle}>zone 30N</code>, or an EPSG code.
            </div>
          ) : (
            results.map((o) => (
              <button
                key={`${o.source}:${o.value}`}
                type="button"
                onMouseDown={(e) => {
                  e.preventDefault();
                  onChange(o.value);
                  if (/^\d+$/.test(o.value)) {
                    const merged = mergeHits(cached, [
                      { code: o.value, name: o.label.replace(/^EPSG:\d+\s*-\s*/, "") },
                    ]);
                    setCached(merged);
                    saveCache(merged);
                  }
                  setOpen(false);
                  setQuery("");
                }}
                style={optionStyle}
              >
                {o.label}
              </button>
            ))
          )}
        </div>
      )}
    </div>
  );
}

const selectedButtonStyle: CSSProperties = {
  width: "100%",
  background: "#0f1419",
  color: "#e6edf3",
  border: "1px solid #30363d",
  borderRadius: 6,
  padding: "6px 8px",
  fontSize: 12,
  textAlign: "left",
  cursor: "pointer",
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
};

const menuStyle: CSSProperties = {
  position: "absolute",
  top: "calc(100% + 4px)",
  left: 0,
  right: 0,
  maxHeight: 300,
  overflow: "auto",
  background: "#0f1419",
  border: "1px solid #30363d",
  borderRadius: 6,
  zIndex: 30,
};

const searchWrapStyle: CSSProperties = {
  display: "flex",
  gap: 6,
  alignItems: "center",
  padding: "8px",
  borderBottom: "1px solid #30363d",
  position: "sticky",
  top: 0,
  background: "#0f1419",
  zIndex: 1,
};

const searchInputStyle: CSSProperties = {
  flex: 1,
  minWidth: 0,
  background: "#0b0f14",
  color: "#e6edf3",
  border: "1px solid #30363d",
  borderRadius: 6,
  padding: "6px 8px",
  fontSize: 12,
};

const clearBtnStyle: CSSProperties = {
  border: "1px solid #30363d",
  background: "#161b22",
  color: "#c9d1d9",
  borderRadius: 6,
  fontSize: 11,
  padding: "6px 8px",
  cursor: "pointer",
};

const quickSectionStyle: CSSProperties = {
  padding: "8px",
  borderBottom: "1px solid #30363d",
};

const sectionLabelStyle: CSSProperties = {
  fontSize: 10,
  opacity: 0.7,
  marginBottom: 6,
  textTransform: "uppercase",
  letterSpacing: "0.03em",
};

const quickGridStyle: CSSProperties = {
  display: "flex",
  flexWrap: "wrap",
  gap: 6,
};

const quickBtnStyle: CSSProperties = {
  border: "1px solid #30363d",
  background: "#161b22",
  color: "#e6edf3",
  borderRadius: 999,
  fontSize: 11,
  padding: "4px 8px",
  cursor: "pointer",
  maxWidth: "100%",
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
};

const optionStyle: CSSProperties = {
  display: "block",
  width: "100%",
  textAlign: "left",
  border: "none",
  background: "transparent",
  color: "#e6edf3",
  padding: "7px 8px",
  fontSize: 11,
  cursor: "pointer",
};

const emptyStyle: CSSProperties = {
  padding: "8px",
  fontSize: 11,
  opacity: 0.7,
};

const busyStyle: CSSProperties = {
  padding: "8px",
  fontSize: 11,
  opacity: 0.8,
  borderBottom: "1px solid #30363d",
};

const resultsHeaderStyle: CSSProperties = {
  fontSize: 10,
  opacity: 0.7,
  padding: "8px 8px 4px 8px",
  textTransform: "uppercase",
  letterSpacing: "0.03em",
};

const codeStyle: CSSProperties = {
  fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
  fontSize: 10,
};
