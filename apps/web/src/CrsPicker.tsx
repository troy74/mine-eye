import { useEffect, useMemo, useState, type CSSProperties } from "react";
import { ACQUISITION_EPSG_OPTIONS } from "./crsOptions";
import { searchEpsg, type EpsgSearchHit } from "./epsgSearch";

const CACHE_KEY = "mineeye:epsg_picker_cache:v1";

type CrsOption = {
  value: string;
  label: string;
  source: "project" | "common" | "workspace" | "cached" | "search";
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
          const hits = await searchEpsg(q);
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
    if (!q) return all.slice(0, 40);
    return all
      .filter((o) => o.value.toLowerCase().includes(q) || o.label.toLowerCase().includes(q))
      .slice(0, 40);
  }, [cached, common, includeProject, projectEpsg, query, remote, workspaceUsedEpsgs]);

  const selectedLabel = useMemo(() => {
    if (value === "project") return `Project CRS (EPSG:${projectEpsg})`;
    const fromOptions = options.find((o) => o.value === value);
    if (fromOptions) return fromOptions.label;
    const fromCache = cached.find((h) => h.code === value);
    if (fromCache) return `EPSG:${fromCache.code} - ${fromCache.name}`;
    return value ? `EPSG:${value}` : "";
  }, [cached, options, projectEpsg, value]);

  return (
    <div style={{ position: "relative" }}>
      <input
        value={query.length ? query : selectedLabel}
        onChange={(e) => {
          setQuery(e.target.value);
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => {
          window.setTimeout(() => {
            setOpen(false);
            setQuery("");
          }, 120);
        }}
        placeholder={placeholder}
        style={inputStyle}
      />
      {open && (
        <div style={menuStyle}>
          {searchBusy && <div style={busyStyle}>Searching full EPSG registry…</div>}
          {options.length === 0 ? (
            <div style={emptyStyle}>No matches yet. Type at least 2 chars.</div>
          ) : (
            options.map((o) => (
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

const inputStyle: CSSProperties = {
  width: "100%",
  background: "#0f1419",
  color: "#e6edf3",
  border: "1px solid #30363d",
  borderRadius: 6,
  padding: "6px 8px",
  fontSize: 12,
};

const menuStyle: CSSProperties = {
  position: "absolute",
  top: "calc(100% + 4px)",
  left: 0,
  right: 0,
  maxHeight: 180,
  overflow: "auto",
  background: "#0f1419",
  border: "1px solid #30363d",
  borderRadius: 6,
  zIndex: 30,
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
