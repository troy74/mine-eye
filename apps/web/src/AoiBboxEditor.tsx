/**
 * AoiBboxEditor — full-screen modal / panel for editing AOI bounding boxes on a
 * Leaflet satellite map. Supports:
 *  - Viewing/displaying the current bbox as a rectangle
 *  - Drawing a new rectangle with mouse drag
 *  - Nominatim place-name search to zoom to locations
 *  - Full CRS picker — bbox is stored in the selected CRS, map display is always WGS84
 *  - Save / Cancel
 *
 * Props:
 *   initialBbox      — current [xmin, ymin, xmax, ymax] (in initialBboxEpsg CRS)
 *   initialBboxEpsg  — EPSG of the initial bbox (default 4326)
 *   projectEpsg      — project CRS for the CRS picker quick-picks
 *   workspaceUsedEpsgs — other CRS codes used in the workspace
 *   onSave(bbox, epsg) — called with new bbox in the chosen CRS + the EPSG code
 *   onCancel()       — close without saving
 */

import L, { type LatLngBoundsLiteral } from "leaflet";
import "leaflet/dist/leaflet.css";
import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type CSSProperties,
} from "react";
import {
  formatAoiBbox,
  geocodePlace,
  nominatimBboxToAoi,
  type NominatimResult,
} from "./nominatim";
import { CrsPicker } from "./CrsPicker";
import {
  lonLatFromProjectedAsync,
  projectedFromLonLatAsync,
} from "./spatialReproject";

// ── types ────────────────────────────────────────────────────────────────────

type AoiBbox = [number, number, number, number]; // [xmin,ymin,xmax,ymax] = [W,S,E,N]

type Props = {
  /** Current bbox in initialBboxEpsg CRS (or null if not yet set). */
  initialBbox: AoiBbox | null;
  /** EPSG of the initialBbox values. Defaults to 4326 (WGS84). */
  initialBboxEpsg?: number;
  /** Optional centre point [lat, lon] when there is no bbox. */
  defaultCenter?: [number, number];
  /** Project CRS EPSG — shown as a quick pick in the CRS picker. */
  projectEpsg?: number;
  /** Other CRS codes used in the workspace. */
  workspaceUsedEpsgs?: number[];
  /** Called with the final bbox in the chosen CRS and the EPSG of that CRS. */
  onSave: (bbox: AoiBbox, epsg: number) => void;
  onCancel: () => void;
  mode?: "modal" | "panel";
  /** @deprecated No longer needed — conditional rendering handles visibility. */
  active?: boolean;
};

// ── helpers ──────────────────────────────────────────────────────────────────

function bboxToLatLngBounds(bbox: AoiBbox): LatLngBoundsLiteral {
  // [xmin,ymin,xmax,ymax] = [W,S,E,N]
  const [west, south, east, north] = bbox;
  return [
    [south, west],
    [north, east],
  ];
}

function latLngToBbox(sw: L.LatLng, ne: L.LatLng): AoiBbox {
  return [sw.lng, sw.lat, ne.lng, ne.lat];
}

function applyBboxToMap(
  map: L.Map | null,
  rect: L.Rectangle | null,
  bbox: AoiBbox
): L.Rectangle | null {
  if (!map) return rect;
  const bounds = bboxToLatLngBounds(bbox);
  if (rect) {
    rect.setBounds(bounds);
    map.fitBounds(bounds, { padding: [40, 40] });
    return rect;
  }
  const next = L.rectangle(bounds, RECT_STYLE).addTo(map);
  map.fitBounds(bounds, { padding: [40, 40] });
  return next;
}

// ── main component ───────────────────────────────────────────────────────────

export function AoiBboxEditor({
  initialBbox,
  initialBboxEpsg = 4326,
  defaultCenter,
  projectEpsg = 4326,
  workspaceUsedEpsgs = [],
  onSave,
  onCancel,
  mode = "modal",
}: Props) {
  const mapElRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<L.Map | null>(null);
  const rectRef = useRef<L.Rectangle | null>(null);
  const drawRectRef = useRef<L.Rectangle | null>(null);
  const drawStartRef = useRef<L.LatLng | null>(null);
  const isDrawingRef = useRef(false);

  // currentBbox is ALWAYS stored in WGS84 internally; bboxEpsg is the output CRS
  const [currentBbox, setCurrentBbox] = useState<AoiBbox | null>(
    initialBboxEpsg === 4326 ? initialBbox : null // non-4326 initial bbox converted async below
  );
  // Mirror as a ref so effects that only depend on [active] always see the latest value
  const currentBboxRef = useRef<AoiBbox | null>(
    initialBboxEpsg === 4326 ? initialBbox : null
  );
  // Keep ref in sync on every render so effects with narrow deps always see latest bbox
  currentBboxRef.current = currentBbox;

  // The CRS the saved bbox will be expressed in.
  // When there is no existing bbox the project CRS is the natural default;
  // when there is an existing bbox preserve the CRS it was already stored in.
  const [bboxEpsg, setBboxEpsg] = useState<string>(
    initialBbox != null ? String(initialBboxEpsg ?? 4326) : String(projectEpsg ?? 4326)
  );
  const [crsConvertError, setCrsConvertError] = useState<string | null>(null);
  const [drawMode, setDrawMode] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<NominatimResult[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);

  // Keep this stable across re-renders where parent recreates an equal bbox array.
  const initialBboxKey = initialBbox ? initialBbox.join(",") : "null";

  // ── sync initial bbox props into internal WGS84 state ───────────────────

  useEffect(() => {
    if (!initialBbox) {
      setCurrentBbox(null);
      if (rectRef.current) {
        rectRef.current.remove();
        rectRef.current = null;
      }
      return;
    }

    let cancelled = false;
    void (async () => {
      if (initialBboxEpsg === 4326) {
        if (cancelled) return;
        setCurrentBbox(initialBbox);
        rectRef.current = applyBboxToMap(mapRef.current, rectRef.current, initialBbox);
        return;
      }

      const [xmin, ymin, xmax, ymax] = initialBbox;
      const sw = await lonLatFromProjectedAsync(initialBboxEpsg, xmin, ymin);
      const ne = await lonLatFromProjectedAsync(initialBboxEpsg, xmax, ymax);
      if (cancelled) return;
      if (!sw || !ne) {
        setCrsConvertError(`Could not reproject initial bbox from EPSG:${initialBboxEpsg} — showing WGS84 fallback`);
        // Try treating as WGS84 anyway so user isn't stuck
        setCurrentBbox(initialBbox);
        rectRef.current = applyBboxToMap(mapRef.current, rectRef.current, initialBbox);
        return;
      }
      const wgs84Bbox: AoiBbox = [
        Math.min(sw[0], ne[0]),
        Math.min(sw[1], ne[1]),
        Math.max(sw[0], ne[0]),
        Math.max(sw[1], ne[1]),
      ];
      setCurrentBbox(wgs84Bbox);
      rectRef.current = applyBboxToMap(mapRef.current, rectRef.current, wgs84Bbox);
    })();
    return () => { cancelled = true; };
  }, [initialBboxKey, initialBboxEpsg]);

  // ── ResizeObserver: invalidate Leaflet size on container resize ─────────
  // Prevents patchy tile loading when the app sidebar collapses/expands.

  useEffect(() => {
    const el = mapElRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => {
      mapRef.current?.invalidateSize();
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // ── map initialisation ──────────────────────────────────────────────────

  useEffect(() => {
    if (!mapElRef.current || mapRef.current) return;

    const map = L.map(mapElRef.current, {
      center: [0, 0],
      zoom: 3,
      zoomControl: true,
    });

    L.tileLayer(
      "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
      {
        attribution:
          "&copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community",
        maxZoom: 19,
      }
    ).addTo(map);

    mapRef.current = map;

    // Show initial bbox (already WGS84 in currentBboxRef) or fly to defaultCenter.
    // Use the ref so we always show the correct WGS84 coordinates even when the
    // source CRS was non-WGS84 (the async conversion effect will update this later).
    const initBbox = currentBboxRef.current;
    if (initBbox) {
      const bounds = bboxToLatLngBounds(initBbox);
      const rect = L.rectangle(bounds, RECT_STYLE).addTo(map);
      rectRef.current = rect;
      map.fitBounds(bounds, { padding: [40, 40] });
    } else if (defaultCenter) {
      map.setView(defaultCenter, 10);
    }

    return () => {
      map.remove();
      mapRef.current = null;
      rectRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ── draw-mode mouse handlers ────────────────────────────────────────────

  const startDraw = useCallback(() => {
    if (!mapRef.current) return;
    // Use a non-null cast so TypeScript trusts the type inside closures
    const map = mapRef.current as L.Map;
    map.getContainer().style.cursor = "crosshair";
    isDrawingRef.current = false;

    function onMouseDown(e: L.LeafletMouseEvent) {
      isDrawingRef.current = true;
      drawStartRef.current = e.latlng;

      // Remove any previous draw preview
      if (drawRectRef.current) {
        drawRectRef.current.remove();
        drawRectRef.current = null;
      }
    }

    function onMouseMove(e: L.LeafletMouseEvent) {
      if (!isDrawingRef.current || !drawStartRef.current) return;
      const start = drawStartRef.current;
      const end = e.latlng;
      const bounds: LatLngBoundsLiteral = [
        [Math.min(start.lat, end.lat), Math.min(start.lng, end.lng)],
        [Math.max(start.lat, end.lat), Math.max(start.lng, end.lng)],
      ];
      if (drawRectRef.current) {
        drawRectRef.current.setBounds(bounds);
      } else {
        drawRectRef.current = L.rectangle(bounds, DRAW_RECT_STYLE).addTo(map);
      }
    }

    function onMouseUp(e: L.LeafletMouseEvent) {
      if (!isDrawingRef.current || !drawStartRef.current) return;
      isDrawingRef.current = false;

      const start = drawStartRef.current;
      const end = e.latlng;
      drawStartRef.current = null;

      // Minimum drag threshold (2px equivalent)
      if (
        Math.abs(start.lat - end.lat) < 0.0001 &&
        Math.abs(start.lng - end.lng) < 0.0001
      ) {
        if (drawRectRef.current) {
          drawRectRef.current.remove();
          drawRectRef.current = null;
        }
        return;
      }

      const sw = L.latLng(
        Math.min(start.lat, end.lat),
        Math.min(start.lng, end.lng)
      );
      const ne = L.latLng(
        Math.max(start.lat, end.lat),
        Math.max(start.lng, end.lng)
      );
      const newBbox = latLngToBbox(sw, ne);

      // Commit draw rect → permanent rect
      if (drawRectRef.current) {
        drawRectRef.current.remove();
        drawRectRef.current = null;
      }
      if (rectRef.current) {
        rectRef.current.remove();
      }
      const rect = L.rectangle(
        bboxToLatLngBounds(newBbox),
        RECT_STYLE
      ).addTo(map);
      rectRef.current = rect;

      setCurrentBbox(newBbox);
      // Exit draw mode
      cleanup();
      setDrawMode(false);
    }

    function cleanup() {
      map.off("mousedown", onMouseDown);
      map.off("mousemove", onMouseMove);
      map.off("mouseup", onMouseUp);
      map.dragging.enable();
      map.getContainer().style.cursor = "";
    }

    map.dragging.disable();
    map.on("mousedown", onMouseDown);
    map.on("mousemove", onMouseMove);
    map.on("mouseup", onMouseUp);

    // Return cleanup for external cancel
    return cleanup;
  }, []);

  // Toggle draw mode
  useEffect(() => {
    if (!drawMode) return;
    const cleanup = startDraw();
    return () => cleanup?.();
  }, [drawMode, startDraw]);

  // ── clear bbox ──────────────────────────────────────────────────────────

  function clearBbox() {
    if (rectRef.current) {
      rectRef.current.remove();
      rectRef.current = null;
    }
    if (drawRectRef.current) {
      drawRectRef.current.remove();
      drawRectRef.current = null;
    }
    setCurrentBbox(null);
    setDrawMode(false);
  }

  // ── search debounce ─────────────────────────────────────────────────────

  useEffect(() => {
    const q = searchQuery.trim();
    if (!q) { setSearchResults([]); return; }
    const t = setTimeout(() => { void runSearch(); }, 500);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchQuery]);

  // ── nominatim search ────────────────────────────────────────────────────

  async function runSearch() {
    const q = searchQuery.trim();
    if (!q) return;
    setSearchLoading(true);
    setSearchError(null);
    setSearchResults([]);
    try {
      const results = await geocodePlace(q, 6);
      setSearchResults(results);
      if (results.length === 0) setSearchError("No results found.");
    } catch (err) {
      setSearchError(String(err));
    } finally {
      setSearchLoading(false);
    }
  }

  function flyToResult(result: NominatimResult) {
    const map = mapRef.current;
    if (!map) return;
    const bbox = nominatimBboxToAoi(result.boundingbox);
    const bounds = bboxToLatLngBounds(bbox);
    map.fitBounds(bounds, { padding: [60, 60] });
    setSearchResults([]);
    setSearchQuery("");
  }

  // ── save ────────────────────────────────────────────────────────────────

  async function handleSave() {
    if (!currentBbox) return;
    const epsgNum = /^\d+$/.test(bboxEpsg) ? parseInt(bboxEpsg, 10) : 4326;
    if (epsgNum === 4326) {
      onSave(currentBbox, 4326);
      return;
    }
    // currentBbox is WGS84 internally — convert to the target CRS before saving
    const [xmin, ymin, xmax, ymax] = currentBbox;
    const sw = await projectedFromLonLatAsync(epsgNum, xmin, ymin);
    const ne = await projectedFromLonLatAsync(epsgNum, xmax, ymax);
    if (!sw || !ne) {
      // Fallback: save in WGS84 with a warning
      setCrsConvertError(`Could not convert to EPSG:${epsgNum} — saved as WGS84 (4326)`);
      onSave(currentBbox, 4326);
      return;
    }
    onSave([sw[0], sw[1], ne[0], ne[1]], epsgNum);
  }

  // ── render ──────────────────────────────────────────────────────────────

  const inner = (
    <div style={mode === "panel" ? S.panelRoot : S.modal}>
      {/* Header (modal mode only) */}
      {mode !== "panel" && (
        <div style={S.header}>
          <span style={S.headerTitle}>Edit Area of Interest</span>
          <button style={S.closeBtn} onClick={onCancel} title="Cancel">✕</button>
        </div>
      )}

      {/* ── Row 1: Place search ───────────────────────────────────────── */}
      <div style={S.toolbarRow}>
        <span style={S.toolbarLabel}>📍 Navigate</span>
        <div style={S.searchRow}>
          <input
            style={S.searchInput}
            placeholder="Search place name…"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") void runSearch();
              if (e.key === "Escape") { setSearchResults([]); setSearchQuery(""); }
            }}
          />
          {searchLoading && <span style={{ fontSize: 12, color: "#8b949e" }}>Searching…</span>}
          {/* Dropdown results */}
          {searchResults.length > 0 && (
            <div style={S.searchDropdown}>
              {searchResults.map((r) => (
                <button
                  key={r.place_id}
                  style={S.searchItem}
                  onClick={() => flyToResult(r)}
                >
                  <span style={S.searchItemType}>{r.type}</span>
                  {r.display_name}
                </button>
              ))}
            </div>
          )}
          {searchError && <span style={S.searchError}>{searchError}</span>}
        </div>
      </div>

      {/* ── Row 2: CRS ───────────────────────────────────────────────── */}
      <div style={{ ...S.toolbarRow, zIndex: 20, position: "relative" }}>
        <span style={S.toolbarLabel}>🗺 Output CRS</span>
        <div style={{ width: 280, flexShrink: 0 }}>
          <CrsPicker
            value={bboxEpsg}
            onChange={setBboxEpsg}
            projectEpsg={projectEpsg}
            workspaceUsedEpsgs={workspaceUsedEpsgs}
            includeProject={true}
            placeholder="Search EPSG code or name…"
          />
        </div>
        {bboxEpsg !== "4326" && bboxEpsg !== "" && (
          <span style={{ fontSize: 11, color: "#8b949e", marginLeft: 8 }}>
            Draw in WGS84 on map → saved as EPSG:{bboxEpsg}
          </span>
        )}
        {crsConvertError && (
          <span style={{ fontSize: 11, color: "#f85149", marginLeft: 8 }}>{crsConvertError}</span>
        )}
      </div>

      {/* ── Row 3: Draw controls ─────────────────────────────────────── */}
      <div style={S.toolbarRow}>
        <span style={S.toolbarLabel}>✏ Draw bbox</span>
        <button
          style={drawMode ? S.drawBtnActive : S.drawBtn}
          onClick={() => setDrawMode((d) => !d)}
          title={drawMode ? "Click to cancel drawing" : "Click then drag on map to draw a bounding box"}
        >
          {drawMode ? "⬛ Cancel draw" : "⬜ Start drawing"}
        </button>
        <button
          style={{ ...S.toolBtn, opacity: currentBbox ? 1 : 0.4 }}
          onClick={clearBbox}
          disabled={!currentBbox}
          title="Remove the current bounding box"
        >
          🗑 Clear bbox
        </button>
        {currentBbox && (
          <code style={S.inlineBbox}>{formatAoiBbox(currentBbox)}</code>
        )}
        {!currentBbox && (
          <span style={{ fontSize: 11, color: "#6e7681" }}>No bbox — draw or search</span>
        )}
      </div>

      {/* ── Map ──────────────────────────────────────────────────────── */}
      <div style={S.mapWrap}>
        <div ref={mapElRef} style={S.map} />
        {drawMode && (
          <div style={S.drawHint}>
            Click and drag to draw — release to confirm
          </div>
        )}
      </div>

      {/* ── Footer: cancel / save ─────────────────────────────────────── */}
      <div style={S.footer}>
        <div style={S.footerBtns}>
          <button style={S.cancelBtn} onClick={onCancel}>Cancel</button>
          <button
            style={{ ...S.saveBtn, ...(currentBbox ? {} : S.saveBtnDisabled) }}
            onClick={() => void handleSave()}
            disabled={!currentBbox}
          >
            Save bbox
          </button>
        </div>
      </div>
    </div>
  );

  if (mode === "panel") return inner;

  return <div style={S.overlay}>{inner}</div>;
}

// ── Leaflet layer styles ─────────────────────────────────────────────────────

const RECT_STYLE: L.PathOptions = {
  color: "#f7b731",
  weight: 2,
  fillColor: "#f7b731",
  fillOpacity: 0.12,
  dashArray: "6 4",
};

const DRAW_RECT_STYLE: L.PathOptions = {
  color: "#58a6ff",
  weight: 2,
  fillColor: "#58a6ff",
  fillOpacity: 0.1,
  dashArray: "4 3",
};

// ── CSS-in-JS styles ─────────────────────────────────────────────────────────

const S: Record<string, CSSProperties> = {
  panelRoot: {
    position: "absolute",
    inset: 0,
    display: "flex",
    flexDirection: "column",
    background: "#0d1117",
  },
  overlay: {
    position: "fixed",
    inset: 0,
    background: "rgba(0,0,0,0.72)",
    zIndex: 2000,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
  },
  modal: {
    display: "flex",
    flexDirection: "column",
    width: "min(96vw, 1040px)",
    height: "min(92vh, 700px)",
    background: "#0d1117",
    border: "1px solid #30363d",
    borderRadius: 12,
    overflow: "hidden",
    boxShadow: "0 24px 64px rgba(0,0,0,0.8)",
  },
  header: {
    display: "flex",
    alignItems: "center",
    padding: "10px 14px",
    borderBottom: "1px solid #21262d",
    flexShrink: 0,
    gap: 8,
  },
  headerTitle: {
    flex: 1,
    color: "#e6edf3",
    fontWeight: 700,
    fontSize: 14,
    letterSpacing: "-0.01em",
  },
  closeBtn: {
    background: "transparent",
    border: "none",
    color: "#8b949e",
    cursor: "pointer",
    fontSize: 16,
    lineHeight: 1,
    padding: "2px 6px",
    borderRadius: 4,
  },
  /** Shared style for all horizontal toolbar rows above the map */
  toolbarRow: {
    display: "flex",
    alignItems: "center",
    gap: 8,
    padding: "7px 14px",
    borderBottom: "1px solid #21262d",
    background: "#161b22",
    flexShrink: 0,
    flexWrap: "wrap",
  },
  toolbarLabel: {
    fontSize: 11,
    fontWeight: 600,
    color: "#8b949e",
    textTransform: "uppercase" as const,
    letterSpacing: "0.04em",
    flexShrink: 0,
    minWidth: 90,
  },
  searchRow: {
    position: "relative",
    display: "flex",
    alignItems: "center",
    gap: 6,
    flex: 1,
    minWidth: 200,
  },
  searchInput: {
    background: "#0d1117",
    border: "1px solid #30363d",
    borderRadius: 6,
    color: "#e6edf3",
    fontSize: 12,
    padding: "5px 10px",
    outline: "none",
    flex: 1,
    minWidth: 160,
    fontFamily: "inherit",
  },
  drawBtn: {
    display: "inline-flex",
    alignItems: "center",
    gap: 6,
    background: "#21262d",
    border: "2px solid #388bfd",
    borderRadius: 6,
    color: "#58a6ff",
    cursor: "pointer",
    fontSize: 12,
    fontWeight: 600,
    padding: "5px 14px",
    fontFamily: "inherit",
    whiteSpace: "nowrap",
  },
  drawBtnActive: {
    display: "inline-flex",
    alignItems: "center",
    gap: 6,
    background: "rgba(88,166,255,0.18)",
    border: "2px solid #58a6ff",
    borderRadius: 6,
    color: "#58a6ff",
    cursor: "pointer",
    fontSize: 12,
    fontWeight: 700,
    padding: "5px 14px",
    fontFamily: "inherit",
    whiteSpace: "nowrap",
    boxShadow: "0 0 8px rgba(88,166,255,0.3)",
  },
  inlineBbox: {
    fontSize: 11,
    color: "#f7b731",
    background: "rgba(247,183,49,0.08)",
    border: "1px solid rgba(247,183,49,0.2)",
    borderRadius: 4,
    padding: "2px 8px",
    fontFamily: "monospace",
    marginLeft: 4,
  },
  searchDropdown: {
    position: "absolute",
    top: "calc(100% + 4px)",
    left: 0,
    zIndex: 3000,
    background: "#161b22",
    border: "1px solid #30363d",
    borderRadius: 8,
    boxShadow: "0 8px 24px rgba(0,0,0,0.6)",
    width: 420,
    maxHeight: 260,
    overflowY: "auto",
  },
  searchItem: {
    display: "block",
    width: "100%",
    textAlign: "left",
    padding: "7px 12px",
    background: "transparent",
    border: "none",
    borderBottom: "1px solid #21262d",
    color: "#c9d1d9",
    fontSize: 12,
    cursor: "pointer",
    lineHeight: 1.4,
    fontFamily: "inherit",
  },
  searchItemType: {
    display: "inline-block",
    background: "#21262d",
    color: "#8b949e",
    fontSize: 10,
    padding: "1px 5px",
    borderRadius: 3,
    marginRight: 6,
    textTransform: "capitalize",
  },
  searchError: {
    position: "absolute",
    top: "calc(100% + 4px)",
    left: 0,
    color: "#f85149",
    fontSize: 11,
    background: "#161b22",
    border: "1px solid #f85149",
    borderRadius: 6,
    padding: "4px 10px",
    whiteSpace: "nowrap",
  },
  toolBtn: {
    background: "#21262d",
    border: "1px solid #30363d",
    borderRadius: 6,
    color: "#c9d1d9",
    cursor: "pointer",
    fontSize: 12,
    padding: "5px 10px",
    fontFamily: "inherit",
    whiteSpace: "nowrap",
  },
  toolBtnActive: {
    background: "rgba(88,166,255,0.15)",
    borderColor: "#58a6ff",
    color: "#58a6ff",
  },
  mapWrap: {
    position: "relative",
    flex: 1,
    overflow: "hidden",
  },
  map: {
    width: "100%",
    height: "100%",
  },
  drawHint: {
    position: "absolute",
    bottom: 12,
    left: "50%",
    transform: "translateX(-50%)",
    background: "rgba(0,0,0,0.75)",
    color: "#58a6ff",
    fontSize: 12,
    padding: "5px 14px",
    borderRadius: 20,
    border: "1px solid rgba(88,166,255,0.4)",
    pointerEvents: "none",
    whiteSpace: "nowrap",
  },
  footer: {
    display: "flex",
    alignItems: "center",
    justifyContent: "flex-end",
    gap: 8,
    padding: "8px 14px",
    borderTop: "1px solid #21262d",
    background: "#161b22",
    flexShrink: 0,
  },
  footerBtns: {
    display: "flex",
    gap: 8,
    flexShrink: 0,
  },
  cancelBtn: {
    background: "transparent",
    border: "1px solid #30363d",
    borderRadius: 6,
    color: "#8b949e",
    cursor: "pointer",
    fontSize: 12,
    padding: "6px 14px",
    fontFamily: "inherit",
  },
  saveBtn: {
    background: "#1f6feb",
    border: "1px solid rgba(88,166,255,0.4)",
    borderRadius: 6,
    color: "#ffffff",
    cursor: "pointer",
    fontSize: 12,
    fontWeight: 600,
    padding: "6px 16px",
    fontFamily: "inherit",
  },
  saveBtnDisabled: {
    background: "#21262d",
    borderColor: "#30363d",
    color: "#484f58",
    cursor: "default",
  },
};
