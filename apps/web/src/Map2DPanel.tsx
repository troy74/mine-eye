import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import {
  api,
  fetchGraph,
  patchNodeParams,
  type ApiEdge,
  type ArtifactEntry,
} from "./graphApi";
import { isPlanViewInputSemantic } from "./portTaxonomy";
import { lonLatFromProjectedAsync } from "./spatialReproject";
import {
  epsgFromCollarJson,
  extractMeasuredPlanPointsFromJson,
  extractPlanViewPointsFromJson,
} from "./spatialExtract";

type Props = {
  graphId: string | null;
  activeBranchId?: string | null;
  active?: boolean;
  edges: ApiEdge[];
  artifacts: ArtifactEntry[];
  viewerNodeId: string | null;
  onClearViewer: () => void;
};

type RenderPoint = {
  lat: number;
  lon: number;
  label: string;
  measures: Record<string, number>;
};

type SourceData = {
  id: string;
  label: string;
  points: RenderPoint[];
  measureNames: string[];
};

function cacheKeyForView(graphId: string | null, viewerNodeId: string | null): string {
  return `mineeye:map2d:source:${graphId ?? ""}:${viewerNodeId ?? ""}`;
}

type PointShape = "circle" | "square" | "diamond";
type ChannelMode = "fixed" | "measure";

type SourceLayerConfig = {
  id: string;
  sourceId: string;
  title: string;
  visible: boolean;
  opacity: number;
  expanded: boolean;
  points: {
    shape: PointShape;
    colorMode: ChannelMode;
    color: string;
    colorMeasure: string;
    sizeMode: ChannelMode;
    sizePx: number;
    sizeMeasure: string;
    sizeMinPx: number;
    sizeMaxPx: number;
  };
  heatmap: {
    enabled: boolean;
    measure: string;
    opacity: number;
    smoothness: number;
    power: number;
  };
};

function upstreamSourcesForViewer(
  edges: ApiEdge[],
  viewerId: string
): { fromNode: string }[] {
  return edges
    .filter((e) => e.to_node === viewerId && isPlanViewInputSemantic(e.semantic_type))
    .map((e) => ({ fromNode: e.from_node }));
}

function jsonArtifactsForNodes(
  graphId: string,
  artifacts: ArtifactEntry[],
  nodeIds: Set<string>
): ArtifactEntry[] {
  const graphPrefix = `graphs/${graphId}/`;
  return artifacts.filter(
    (a) =>
      nodeIds.has(a.node_id) &&
      a.key.includes(graphPrefix) &&
      a.key.toLowerCase().endsWith(".json")
  );
}

function defaultLayerForSource(s: SourceData): SourceLayerConfig {
  return {
    id: `src:${s.id}`,
    sourceId: s.id,
    title: s.label,
    visible: true,
    opacity: 1,
    expanded: false,
    points: {
      shape: "circle",
      colorMode: "fixed",
      color: "#38bdf8",
      colorMeasure: s.measureNames[0] ?? "",
      sizeMode: "fixed",
      sizePx: 6,
      sizeMeasure: s.measureNames[0] ?? "",
      sizeMinPx: 4,
      sizeMaxPx: 12,
    },
    heatmap: {
      enabled: false,
      measure: s.measureNames[0] ?? "",
      opacity: 0.52,
      smoothness: 256,
      power: 2,
    },
  };
}

function valueMinMax(values: number[]): { min: number; max: number } | null {
  if (values.length === 0) return null;
  return { min: Math.min(...values), max: Math.max(...values) };
}

function norm(v: number, min: number, max: number): number {
  if (!Number.isFinite(v) || max <= min) return 0.5;
  return Math.max(0, Math.min(1, (v - min) / (max - min)));
}

function rainbowColor(tRaw: number): string {
  const t = Math.max(0, Math.min(1, tRaw));
  const hue = (1 - t) * 240;
  return `hsl(${hue}, 95%, 50%)`;
}

function hslToRgb(color: string): { r: number; g: number; b: number } {
  const m = color.match(/^hsl\(([-\d.]+),\s*([-\d.]+)%?,\s*([-\d.]+)%?\)$/i);
  if (!m) return { r: 56, g: 189, b: 248 };
  const h = Number(m[1]);
  const s = Number(m[2]) / 100;
  const l = Number(m[3]) / 100;
  const c = (1 - Math.abs(2 * l - 1)) * s;
  const hp = (h / 60) % 6;
  const x = c * (1 - Math.abs((hp % 2) - 1));
  let r = 0;
  let g = 0;
  let b = 0;
  if (hp >= 0 && hp < 1) [r, g, b] = [c, x, 0];
  else if (hp < 2) [r, g, b] = [x, c, 0];
  else if (hp < 3) [r, g, b] = [0, c, x];
  else if (hp < 4) [r, g, b] = [0, x, c];
  else if (hp < 5) [r, g, b] = [x, 0, c];
  else [r, g, b] = [c, 0, x];
  const m0 = l - c / 2;
  return {
    r: Math.round((r + m0) * 255),
    g: Math.round((g + m0) * 255),
    b: Math.round((b + m0) * 255),
  };
}

export function Map2DPanel({
  graphId,
  activeBranchId,
  active = true,
  edges,
  artifacts,
  viewerNodeId,
  onClearViewer,
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const [status, setStatus] = useState("");
  const [sourceData, setSourceData] = useState<SourceData[]>([]);
  const [layers, setLayers] = useState<SourceLayerConfig[]>([]);
  const [panelCollapsed, setPanelCollapsed] = useState(false);
  const [draggingId, setDraggingId] = useState<string | null>(null);
  const lastViewContextRef = useRef<string>("");
  const userMovedMapRef = useRef(false);
  const lastArtifactSigRef = useRef<string>("");
  const [configHydrated, setConfigHydrated] = useState(false);
  const baseUiRef = useRef<Record<string, unknown>>({});
  const saveTimerRef = useRef<number | null>(null);
  const mountedRef = useRef(true);
  const loadTokenRef = useRef(0);
  const loadInFlightRef = useRef(false);
  const loadSigRef = useRef("");
  const autoFitContextRef = useRef<string>("");

  const sourceById = useMemo(() => new Map(sourceData.map((s) => [s.id, s])), [sourceData]);
  const inputLinks = useMemo(
    () => (viewerNodeId ? upstreamSourcesForViewer(edges, viewerNodeId) : []),
    [edges, viewerNodeId]
  );

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;
    const map = L.map(containerRef.current, {
      zoomControl: true,
      attributionControl: true,
    }).setView([20, 0], 2);
    L.tileLayer(
      "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
      {
        maxZoom: 19,
        attribution:
          '&copy; <a href="https://www.esri.com/">Esri</a> (World Imagery)',
      }
    ).addTo(map);
    const onMoveStart = () => {
      userMovedMapRef.current = true;
    };
    map.on("movestart", onMoveStart);
    map.on("zoomstart", onMoveStart);
    mapRef.current = map;
    return () => {
      map.off("movestart", onMoveStart);
      map.off("zoomstart", onMoveStart);
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    const ctx = `${graphId ?? ""}:${viewerNodeId ?? ""}`;
    if (lastViewContextRef.current !== ctx) {
      lastViewContextRef.current = ctx;
      userMovedMapRef.current = false;
      lastArtifactSigRef.current = "";
      autoFitContextRef.current = "";
      try {
        const raw = localStorage.getItem(cacheKeyForView(graphId, viewerNodeId));
        if (raw) {
          const parsed = JSON.parse(raw) as SourceData[];
          if (Array.isArray(parsed) && parsed.length > 0) {
            setSourceData(parsed);
          }
        }
      } catch {
        // ignore cache parse issues
      }
    }
  }, [graphId, viewerNodeId]);

  useEffect(() => {
    let cancelled = false;
    setConfigHydrated(false);
    baseUiRef.current = {};
    if (!graphId || !viewerNodeId) return;
    void (async () => {
      try {
        const g = await fetchGraph(graphId);
        if (cancelled) return;
        const viewer = g.nodes.find((n) => n.id === viewerNodeId);
        const uiRaw = viewer?.config?.params?.ui;
        const ui =
          uiRaw && typeof uiRaw === "object" && !Array.isArray(uiRaw)
            ? (uiRaw as Record<string, unknown>)
            : {};
        baseUiRef.current = ui;
        const mv = ui.map2d_view;
        if (mv && typeof mv === "object" && !Array.isArray(mv)) {
          const m = mv as Record<string, unknown>;
          const collapsed = Boolean(m.panel_collapsed);
          const lay = m.layers;
          if (Array.isArray(lay)) {
            setLayers(lay as SourceLayerConfig[]);
          }
          setPanelCollapsed(collapsed);
        }
      } catch (e) {
        console.warn("load map2d view config:", e);
      } finally {
        if (!cancelled) setConfigHydrated(true);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [graphId, viewerNodeId]);

  useEffect(() => {
    if (!graphId || !viewerNodeId || !configHydrated) return;
    if (saveTimerRef.current !== null) {
      window.clearTimeout(saveTimerRef.current);
    }
    saveTimerRef.current = window.setTimeout(() => {
      const ui = {
        ...baseUiRef.current,
        map2d_view: {
          version: 1,
          panel_collapsed: panelCollapsed,
          layers,
        },
      };
      void patchNodeParams(graphId, viewerNodeId, { ui }, { branchId: activeBranchId }).then(
        () => {
          baseUiRef.current = ui;
        },
        (e) => {
          console.warn("save map2d view config:", e);
        }
      );
    }, 600);
    return () => {
      if (saveTimerRef.current !== null) {
        window.clearTimeout(saveTimerRef.current);
        saveTimerRef.current = null;
      }
    };
  }, [graphId, viewerNodeId, activeBranchId, panelCollapsed, layers, configHydrated]);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  useEffect(() => {
    if (!active) return;
    const tid = window.setTimeout(() => {
      mapRef.current?.invalidateSize();
    }, 30);
    return () => window.clearTimeout(tid);
  }, [active]);

  useEffect(() => {
    if (!active) return;
    const map = mapRef.current;
    if (!map) return;
    if (!graphId || !viewerNodeId) return;
    if (sourceData.length === 0) return;
    if (userMovedMapRef.current) return;

    const viewCtx = `${graphId}:${viewerNodeId}`;
    if (autoFitContextRef.current === viewCtx) return;

    const fit: L.LatLngExpression[] = [];
    sourceData.forEach((src) =>
      src.points.forEach((p) => {
        fit.push([p.lat, p.lon]);
      })
    );
    if (fit.length === 0) return;

    map.fitBounds(L.latLngBounds(fit).pad(0.25));
    autoFitContextRef.current = viewCtx;
  }, [active, graphId, viewerNodeId, sourceData]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const viewCtx = `${graphId ?? ""}:${viewerNodeId ?? ""}`;
    const ctxChanged = lastViewContextRef.current !== viewCtx;
    if (ctxChanged) {
      lastViewContextRef.current = viewCtx;
      userMovedMapRef.current = false;
      lastArtifactSigRef.current = "";
    }

    if (!graphId || !viewerNodeId) {
      setStatus("Select a plan-view node and click Open 2D map.");
      return;
    }
    if (inputLinks.length === 0) {
      setStatus(
        sourceData.length > 0
          ? "No compatible inputs wired now; showing last loaded state."
          : "No compatible inputs wired into this 2D viewer."
      );
      return;
    }

    const upstreamIds = new Set(inputLinks.map((x) => x.fromNode));
    const arts = jsonArtifactsForNodes(graphId, artifacts, upstreamIds);
    const artSig = arts
      .map((a) => `${a.key}:${a.content_hash}`)
      .sort((a, b) => a.localeCompare(b))
      .join("|");
    if (arts.length === 0) {
      setStatus(
        sourceData.length > 0
          ? "No upstream artifacts currently listed; showing last loaded state."
          : "No upstream JSON artifacts yet. Run worker then refresh."
      );
      return;
    }
    if (!ctxChanged && artSig === lastArtifactSigRef.current) {
      return;
    }
    if (!ctxChanged && loadInFlightRef.current && loadSigRef.current === artSig) {
      return;
    }
    lastArtifactSigRef.current = artSig;
    loadSigRef.current = artSig;
    loadInFlightRef.current = true;
    const token = ++loadTokenRef.current;

    setStatus(sourceData.length > 0 ? "Refreshing upstream artifacts…" : "Loading upstream artifacts…");

    void (async () => {
      const all: SourceData[] = [];
      const fit: L.LatLngExpression[] = [];
      let total = 0;
      const notes: string[] = [];

      for (const art of arts) {
        if (token !== loadTokenRef.current || !mountedRef.current) return;
        const r = await fetch(api(art.url));
        if (!r.ok) {
          notes.push(`${art.key.split("/").pop()}: HTTP ${r.status}`);
          continue;
        }
        const text = await r.text();
        const basic = extractPlanViewPointsFromJson(text, art.key.split("/").pop() ?? art.key);
        const measured = extractMeasuredPlanPointsFromJson(
          text,
          art.key.split("/").pop() ?? art.key
        );
        if (basic.length === 0 && measured.length === 0) continue;

        const mByXY = new Map<string, Record<string, number>>();
        measured.forEach((m) => mByXY.set(`${m.x}|${m.y}`, m.measures));
        const epsg = epsgFromCollarJson(text);
        const points: RenderPoint[] = [];
        for (const p of basic) {
          let lon = p.x;
          let lat = p.y;
          if (epsg && epsg !== 4326) {
            const ll = await lonLatFromProjectedAsync(epsg, p.x, p.y);
            if (ll) {
              lon = ll[0];
              lat = ll[1];
            }
          }
          points.push({
            lat,
            lon,
            label: p.label,
            measures: mByXY.get(`${p.x}|${p.y}`) ?? {},
          });
          fit.push([lat, lon]);
        }
        const measureNames = [...new Set(points.flatMap((p) => Object.keys(p.measures)))].sort(
          (a, b) => a.localeCompare(b)
        );
        all.push({
          id: `${art.node_id}:${art.key}`,
          label: art.key.split("/").pop() ?? art.key,
          points,
          measureNames,
        });
        total += points.length;
      }

      if (token !== loadTokenRef.current || !mountedRef.current) return;
      setSourceData(all);
      if (all.length > 0) {
        try {
          localStorage.setItem(cacheKeyForView(graphId, viewerNodeId), JSON.stringify(all));
        } catch {
          // ignore localStorage quota failures
        }
      }
      if (all.length === 0) {
        setStatus("No drawable points parsed.");
        return;
      }
      setStatus(`${total} point(s) from ${all.length} input artifact(s). ${notes.join(" · ")}`);
      if (fit.length && (!userMovedMapRef.current || ctxChanged)) {
        map.fitBounds(L.latLngBounds(fit).pad(0.25));
      }
    })().catch((e) => {
      if (token === loadTokenRef.current && mountedRef.current) {
        setStatus(e instanceof Error ? e.message : String(e));
      }
    }).finally(() => {
      if (token === loadTokenRef.current) {
        loadInFlightRef.current = false;
      }
    });
  }, [graphId, viewerNodeId, artifacts, inputLinks, sourceData.length]);

  useEffect(() => {
    if (sourceData.length === 0) return;
    setLayers((prev) => {
      const sourceIds = new Set(sourceData.map((s) => s.id));
      const prevById = new Map(prev.map((l) => [l.id, l]));
      const desired = sourceData.map((s) => {
        const id = `src:${s.id}`;
        const ex = prevById.get(id);
        if (!ex) return defaultLayerForSource(s);
        return {
          ...ex,
          sourceId: s.id,
          points: {
            ...ex.points,
            colorMeasure:
              ex.points.colorMeasure && s.measureNames.includes(ex.points.colorMeasure)
                ? ex.points.colorMeasure
                : s.measureNames[0] ?? "",
            sizeMeasure:
              ex.points.sizeMeasure && s.measureNames.includes(ex.points.sizeMeasure)
                ? ex.points.sizeMeasure
                : s.measureNames[0] ?? "",
          },
          heatmap: {
            ...ex.heatmap,
            measure:
              ex.heatmap.measure && s.measureNames.includes(ex.heatmap.measure)
                ? ex.heatmap.measure
                : s.measureNames[0] ?? "",
            enabled: s.measureNames.length > 0 ? ex.heatmap.enabled : false,
          },
        };
      });
      const desiredById = new Map(desired.map((d) => [d.id, d]));
      const ordered: SourceLayerConfig[] = [];
      for (const l of prev) {
        const n = desiredById.get(l.id);
        if (n) {
          ordered.push(n);
          desiredById.delete(l.id);
        }
      }
      for (const n of desiredById.values()) ordered.push(n);
      return ordered.filter((l) => sourceIds.has(l.sourceId));
    });
  }, [sourceData]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    map.eachLayer((layer) => {
      if (layer instanceof L.TileLayer) return;
      map.removeLayer(layer);
    });

    layers.forEach((layer, z) => {
      if (!layer.visible) return;
      const src = sourceById.get(layer.sourceId);
      if (!src) return;
      const heatPane = `heat:${layer.id}`;
      const pointPane = `pt:${layer.id}`;
      if (!map.getPane(heatPane)) map.createPane(heatPane);
      if (!map.getPane(pointPane)) map.createPane(pointPane);
      const hPane = map.getPane(heatPane);
      const pPane = map.getPane(pointPane);
      if (hPane) hPane.style.zIndex = String(430 + z * 20);
      if (pPane) pPane.style.zIndex = String(431 + z * 20);

      if (layer.heatmap.enabled && layer.heatmap.measure) {
        const samples = src.points
          .map((p) => {
            const v = p.measures[layer.heatmap.measure];
            return Number.isFinite(v) ? { ...p, value: v as number } : null;
          })
          .filter((v): v is RenderPoint & { value: number } => v !== null);
        if (samples.length >= 3) {
          const mm = valueMinMax(samples.map((s) => s.value));
          if (mm) {
            const latMin = Math.min(...samples.map((s) => s.lat));
            const latMax = Math.max(...samples.map((s) => s.lat));
            const lonMin = Math.min(...samples.map((s) => s.lon));
            const lonMax = Math.max(...samples.map((s) => s.lon));
            if (latMax > latMin && lonMax > lonMin) {
              const n = Math.max(128, Math.min(512, Math.trunc(layer.heatmap.smoothness)));
              const power = Math.max(1, Math.min(4, layer.heatmap.power));
              const canvas = document.createElement("canvas");
              canvas.width = n;
              canvas.height = n;
              const ctx = canvas.getContext("2d");
              if (ctx) {
                const img = ctx.createImageData(n, n);
                const idw = (lat: number, lon: number): number => {
                  let num = 0;
                  let den = 0;
                  for (const s of samples) {
                    const dx = lon - s.lon;
                    const dy = lat - s.lat;
                    const d2 = dx * dx + dy * dy;
                    if (d2 < 1e-12) return s.value;
                    const w = 1 / Math.pow(d2, power * 0.5);
                    num += w * s.value;
                    den += w;
                  }
                  return den > 0 ? num / den : mm.min;
                };
                for (let yi = 0; yi < n; yi++) {
                  const lat = latMax - ((yi + 0.5) / n) * (latMax - latMin);
                  for (let xi = 0; xi < n; xi++) {
                    const lon = lonMin + ((xi + 0.5) / n) * (lonMax - lonMin);
                    const t = norm(idw(lat, lon), mm.min, mm.max);
                    const { r, g, b } = hslToRgb(rainbowColor(t));
                    const idx = (yi * n + xi) * 4;
                    img.data[idx] = r;
                    img.data[idx + 1] = g;
                    img.data[idx + 2] = b;
                    img.data[idx + 3] = 255;
                  }
                }
                ctx.putImageData(img, 0, 0);
                L.imageOverlay(
                  canvas.toDataURL("image/png"),
                  [
                    [latMin, lonMin],
                    [latMax, lonMax],
                  ],
                  { pane: heatPane, opacity: layer.heatmap.opacity * layer.opacity, interactive: false }
                ).addTo(map);
              }
            }
          }
        }
      }

      const colorVals =
        layer.points.colorMode === "measure" && layer.points.colorMeasure
          ? src.points
              .map((p) => p.measures[layer.points.colorMeasure])
              .filter((v): v is number => Number.isFinite(v))
          : [];
      const sizeVals =
        layer.points.sizeMode === "measure" && layer.points.sizeMeasure
          ? src.points
              .map((p) => p.measures[layer.points.sizeMeasure])
              .filter((v): v is number => Number.isFinite(v))
          : [];
      const cmm = valueMinMax(colorVals);
      const smm = valueMinMax(sizeVals);

      src.points.forEach((pt) => {
        const color =
          layer.points.colorMode === "measure" &&
          cmm &&
          Number.isFinite(pt.measures[layer.points.colorMeasure])
            ? rainbowColor(norm(pt.measures[layer.points.colorMeasure], cmm.min, cmm.max))
            : layer.points.color;
        const size =
          layer.points.sizeMode === "measure" &&
          smm &&
          Number.isFinite(pt.measures[layer.points.sizeMeasure])
            ? layer.points.sizeMinPx +
              (layer.points.sizeMaxPx - layer.points.sizeMinPx) *
                norm(pt.measures[layer.points.sizeMeasure], smm.min, smm.max)
            : layer.points.sizePx;

        if (layer.points.shape === "circle") {
          L.circleMarker([pt.lat, pt.lon], {
            pane: pointPane,
            radius: size,
            color,
            fillColor: color,
            fillOpacity: 0.92 * layer.opacity,
            opacity: layer.opacity,
            weight: 1.3,
          })
            .bindTooltip(pt.label)
            .addTo(map);
          return;
        }

        const rot = layer.points.shape === "diamond" ? "rotate(45deg)" : "none";
        const wh = Math.max(4, size * 2);
        L.marker([pt.lat, pt.lon], {
          pane: pointPane,
          icon: L.divIcon({
            className: "mineeye-pt",
            html: `<div style="width:${wh}px;height:${wh}px;background:${color};opacity:${Math.max(
              0.2,
              layer.opacity
            )};transform:${rot};border-radius:${layer.points.shape === "square" ? "2px" : "1px"};"></div>`,
            iconSize: [wh, wh],
          }),
        })
          .bindTooltip(pt.label)
          .addTo(map);
      });
    });
  }, [layers, sourceById]);

  const onDropLayer = (targetId: string) => {
    if (!draggingId || draggingId === targetId) return;
    setLayers((prev) => {
      const from = prev.findIndex((l) => l.id === draggingId);
      const to = prev.findIndex((l) => l.id === targetId);
      if (from < 0 || to < 0) return prev;
      const out = prev.slice();
      const [item] = out.splice(from, 1);
      out.splice(to, 0, item);
      return out;
    });
    setDraggingId(null);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", minHeight: 0 }}>
      <div
        style={{
          padding: "8px 12px",
          borderBottom: "1px solid #30363d",
          fontSize: 12,
          display: "flex",
          alignItems: "center",
          gap: 10,
          flexWrap: "wrap",
          background: "#161b22",
        }}
      >
        <strong style={{ color: "#e6edf3" }}>2D map</strong>
        {viewerNodeId && (
          <>
            <span style={{ opacity: 0.75 }}>
              Viewer <code style={{ fontSize: 11 }}>{viewerNodeId.slice(0, 8)}…</code>
            </span>
            <button type="button" onClick={onClearViewer} style={ghostBtn}>
              Clear viewer
            </button>
          </>
        )}
      </div>
      <div style={{ fontSize: 11, opacity: 0.7, padding: "6px 12px", lineHeight: 1.4 }}>
        {status}
      </div>
      <div style={{ position: "relative", flex: 1, minHeight: 200 }}>
        <div ref={containerRef} style={{ position: "absolute", inset: 0, zIndex: 0 }} />
        <aside style={{ ...panel, width: panelCollapsed ? 168 : 320 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <strong>Layers</strong>
            <button type="button" onClick={() => setPanelCollapsed((v) => !v)} style={ghostBtn}>
              {panelCollapsed ? "+" : "-"}
            </button>
          </div>
          <div style={{ fontSize: 10, opacity: 0.7, marginTop: 4 }}>Base: Esri satellite</div>
          {!panelCollapsed && (
            <div style={{ marginTop: 8 }}>
              {layers.map((layer, i) => {
                const src = sourceById.get(layer.sourceId);
                const measures = src?.measureNames ?? [];
                return (
                  <div
                    key={layer.id}
                    draggable
                    onDragStart={() => setDraggingId(layer.id)}
                    onDragOver={(e) => e.preventDefault()}
                    onDrop={() => onDropLayer(layer.id)}
                    style={card}
                  >
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      <span style={{ opacity: 0.55, cursor: "grab" }}>⋮⋮</span>
                      <input
                        type="checkbox"
                        checked={layer.visible}
                        onChange={(e) =>
                          setLayers((prev) =>
                            prev.map((l) =>
                              l.id === layer.id ? { ...l, visible: e.target.checked } : l
                            )
                          )
                        }
                      />
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div
                          style={{
                            fontSize: 12,
                            fontWeight: 600,
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {layer.title}
                        </div>
                        <div style={{ fontSize: 10, opacity: 0.65 }}>
                          z:{i + 1} · m:{measures.length}
                        </div>
                      </div>
                      <button
                        type="button"
                        style={ghostBtn}
                        onClick={() =>
                          setLayers((prev) =>
                            prev.map((l) =>
                              l.id === layer.id ? { ...l, expanded: !l.expanded } : l
                            )
                          )
                        }
                      >
                        {layer.expanded ? "Hide" : "Edit"}
                      </button>
                    </div>
                    {layer.expanded && (
                      <div style={editor}>
                        <label style={field}>
                          <span>Source</span>
                          <select
                            value={layer.sourceId}
                            onChange={(e) =>
                              setLayers((prev) =>
                                prev.map((l) =>
                                  l.id === layer.id ? { ...l, sourceId: e.target.value } : l
                                )
                              )
                            }
                            style={select}
                          >
                            {sourceData.map((s) => (
                              <option key={s.id} value={s.id}>
                                {s.label}
                              </option>
                            ))}
                          </select>
                        </label>
                        <label style={field}>
                          <span>Layer opacity</span>
                          <input
                            type="range"
                            min={0}
                            max={100}
                            value={Math.round(layer.opacity * 100)}
                            onChange={(e) =>
                              setLayers((prev) =>
                                prev.map((l) =>
                                  l.id === layer.id
                                    ? {
                                        ...l,
                                        opacity: Math.max(
                                          0,
                                          Math.min(1, Number(e.target.value) / 100)
                                        ),
                                      }
                                    : l
                                )
                              )
                            }
                          />
                        </label>

                        <div style={groupTitle}>Points</div>
                        <label style={field}>
                          <span>Shape</span>
                          <select
                            value={layer.points.shape}
                            onChange={(e) =>
                              setLayers((prev) =>
                                prev.map((l) =>
                                  l.id === layer.id
                                    ? { ...l, points: { ...l.points, shape: e.target.value as PointShape } }
                                    : l
                                )
                              )
                            }
                            style={select}
                          >
                            <option value="circle">Circle</option>
                            <option value="square">Square</option>
                            <option value="diamond">Diamond</option>
                          </select>
                        </label>
                        <div style={row2}>
                          <label style={fieldInline}>
                            <span>Color</span>
                            <select
                              value={layer.points.colorMode}
                              onChange={(e) =>
                                setLayers((prev) =>
                                  prev.map((l) =>
                                    l.id === layer.id
                                      ? { ...l, points: { ...l.points, colorMode: e.target.value as ChannelMode } }
                                      : l
                                  )
                                )
                              }
                              style={select}
                            >
                              <option value="fixed">Fixed</option>
                              <option value="measure">Measure</option>
                            </select>
                          </label>
                          {layer.points.colorMode === "fixed" ? (
                            <label style={fieldInline}>
                              <span>Pick</span>
                              <input
                                type="color"
                                value={layer.points.color}
                                onChange={(e) =>
                                  setLayers((prev) =>
                                    prev.map((l) =>
                                      l.id === layer.id
                                        ? { ...l, points: { ...l.points, color: e.target.value } }
                                        : l
                                    )
                                  )
                                }
                              />
                            </label>
                          ) : (
                            <label style={fieldInline}>
                              <span>Measure</span>
                              <select
                                value={layer.points.colorMeasure}
                                onChange={(e) =>
                                  setLayers((prev) =>
                                    prev.map((l) =>
                                      l.id === layer.id
                                        ? { ...l, points: { ...l.points, colorMeasure: e.target.value } }
                                        : l
                                    )
                                  )
                                }
                                style={select}
                              >
                                {measures.map((m) => (
                                  <option key={m} value={m}>
                                    {m}
                                  </option>
                                ))}
                              </select>
                            </label>
                          )}
                        </div>

                        <div style={row2}>
                          <label style={fieldInline}>
                            <span>Size</span>
                            <select
                              value={layer.points.sizeMode}
                              onChange={(e) =>
                                setLayers((prev) =>
                                  prev.map((l) =>
                                    l.id === layer.id
                                      ? { ...l, points: { ...l.points, sizeMode: e.target.value as ChannelMode } }
                                      : l
                                  )
                                )
                              }
                              style={select}
                            >
                              <option value="fixed">Fixed</option>
                              <option value="measure">Measure</option>
                            </select>
                          </label>
                          {layer.points.sizeMode === "fixed" ? (
                            <label style={fieldInline}>
                              <span>Px</span>
                              <input
                                type="range"
                                min={2}
                                max={18}
                                value={layer.points.sizePx}
                                onChange={(e) =>
                                  setLayers((prev) =>
                                    prev.map((l) =>
                                      l.id === layer.id
                                        ? { ...l, points: { ...l.points, sizePx: Number(e.target.value) } }
                                        : l
                                    )
                                  )
                                }
                              />
                            </label>
                          ) : (
                            <label style={fieldInline}>
                              <span>Measure</span>
                              <select
                                value={layer.points.sizeMeasure}
                                onChange={(e) =>
                                  setLayers((prev) =>
                                    prev.map((l) =>
                                      l.id === layer.id
                                        ? { ...l, points: { ...l.points, sizeMeasure: e.target.value } }
                                        : l
                                    )
                                  )
                                }
                                style={select}
                              >
                                {measures.map((m) => (
                                  <option key={m} value={m}>
                                    {m}
                                  </option>
                                ))}
                              </select>
                            </label>
                          )}
                        </div>
                        {layer.points.sizeMode === "measure" && (
                          <label style={field}>
                            <span>
                              Size range {layer.points.sizeMinPx}-{layer.points.sizeMaxPx}px
                            </span>
                            <input
                              type="range"
                              min={2}
                              max={20}
                              value={layer.points.sizeMinPx}
                              onChange={(e) =>
                                setLayers((prev) =>
                                  prev.map((l) =>
                                    l.id === layer.id
                                      ? {
                                          ...l,
                                          points: {
                                            ...l.points,
                                            sizeMinPx: Math.min(
                                              Number(e.target.value),
                                              l.points.sizeMaxPx - 1
                                            ),
                                          },
                                        }
                                      : l
                                  )
                                )
                              }
                            />
                            <input
                              type="range"
                              min={3}
                              max={24}
                              value={layer.points.sizeMaxPx}
                              onChange={(e) =>
                                setLayers((prev) =>
                                  prev.map((l) =>
                                    l.id === layer.id
                                      ? {
                                          ...l,
                                          points: {
                                            ...l.points,
                                            sizeMaxPx: Math.max(
                                              Number(e.target.value),
                                              l.points.sizeMinPx + 1
                                            ),
                                          },
                                        }
                                      : l
                                  )
                                )
                              }
                            />
                          </label>
                        )}

                        <div style={groupTitle}>Heatmap</div>
                        <label style={fieldInline}>
                          <input
                            type="checkbox"
                            checked={layer.heatmap.enabled}
                            disabled={measures.length === 0}
                            onChange={(e) =>
                              setLayers((prev) =>
                                prev.map((l) =>
                                  l.id === layer.id
                                    ? { ...l, heatmap: { ...l.heatmap, enabled: e.target.checked } }
                                    : l
                                )
                              )
                            }
                          />
                          <span>Enable</span>
                        </label>
                        {layer.heatmap.enabled && (
                          <>
                            <label style={field}>
                              <span>Measure</span>
                              <select
                                value={layer.heatmap.measure}
                                onChange={(e) =>
                                  setLayers((prev) =>
                                    prev.map((l) =>
                                      l.id === layer.id
                                        ? { ...l, heatmap: { ...l.heatmap, measure: e.target.value } }
                                        : l
                                    )
                                  )
                                }
                                style={select}
                              >
                                {measures.map((m) => (
                                  <option key={m} value={m}>
                                    {m}
                                  </option>
                                ))}
                              </select>
                            </label>
                            <label style={field}>
                              <span>Heat opacity</span>
                              <input
                                type="range"
                                min={0}
                                max={100}
                                value={Math.round(layer.heatmap.opacity * 100)}
                                onChange={(e) =>
                                  setLayers((prev) =>
                                    prev.map((l) =>
                                      l.id === layer.id
                                        ? {
                                            ...l,
                                            heatmap: {
                                              ...l.heatmap,
                                              opacity: Math.max(
                                                0,
                                                Math.min(1, Number(e.target.value) / 100)
                                              ),
                                            },
                                          }
                                        : l
                                    )
                                  )
                                }
                              />
                            </label>
                            <label style={field}>
                              <span>Smoothness ({layer.heatmap.smoothness})</span>
                              <input
                                type="range"
                                min={128}
                                max={512}
                                step={32}
                                value={layer.heatmap.smoothness}
                                onChange={(e) =>
                                  setLayers((prev) =>
                                    prev.map((l) =>
                                      l.id === layer.id
                                        ? {
                                            ...l,
                                            heatmap: {
                                              ...l.heatmap,
                                              smoothness: Number(e.target.value),
                                            },
                                          }
                                        : l
                                    )
                                  )
                                }
                              />
                            </label>
                            <label style={field}>
                              <span>Blend power ({layer.heatmap.power.toFixed(1)})</span>
                              <input
                                type="range"
                                min={1}
                                max={4}
                                step={0.1}
                                value={layer.heatmap.power}
                                onChange={(e) =>
                                  setLayers((prev) =>
                                    prev.map((l) =>
                                      l.id === layer.id
                                        ? {
                                            ...l,
                                            heatmap: {
                                              ...l.heatmap,
                                              power: Number(e.target.value),
                                            },
                                          }
                                        : l
                                    )
                                  )
                                }
                              />
                            </label>
                          </>
                        )}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </aside>
      </div>
    </div>
  );
}

const panel: CSSProperties = {
  position: "absolute",
  top: 10,
  right: 10,
  zIndex: 900,
  maxHeight: "calc(100% - 20px)",
  overflow: "auto",
  background: "rgba(15,20,25,0.94)",
  border: "1px solid #30363d",
  borderRadius: 8,
  padding: 8,
  color: "#e6edf3",
  fontSize: 12,
  backdropFilter: "blur(4px)",
};

const card: CSSProperties = {
  border: "1px solid #30363d",
  borderRadius: 8,
  marginBottom: 6,
  padding: "6px 8px",
  background: "rgba(22,27,34,0.9)",
};

const editor: CSSProperties = {
  marginTop: 6,
  borderTop: "1px solid #30363d",
  paddingTop: 6,
};

const groupTitle: CSSProperties = {
  marginTop: 6,
  fontSize: 11,
  fontWeight: 700,
  opacity: 0.85,
};

const row2: CSSProperties = {
  display: "grid",
  gridTemplateColumns: "1fr 1fr",
  gap: 8,
};

const field: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  gap: 4,
  marginTop: 6,
  fontSize: 11,
};

const fieldInline: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  gap: 4,
  marginTop: 6,
  fontSize: 11,
};

const select: CSSProperties = {
  background: "#0f1419",
  color: "#e6edf3",
  border: "1px solid #30363d",
  borderRadius: 6,
  padding: "5px 7px",
  fontSize: 12,
};

const ghostBtn: CSSProperties = {
  background: "transparent",
  border: "1px solid #30363d",
  color: "#8b949e",
  borderRadius: 6,
  padding: "3px 8px",
  cursor: "pointer",
  fontSize: 11,
};
