import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import {
  api,
  fetchViewerManifest,
  fetchGraph,
  patchNodeParams,
  type ApiEdge,
  type ArtifactEntry,
} from "./graphApi";
import { isPlanViewInputSemantic } from "./portTaxonomy";
import { lonLatFromProjectedAsync } from "./spatialReproject";
import {
  extractDisplayContractFromJson,
  epsgFromCollarJson,
  extractHeatmapConfigFromJson,
  extractLineFeaturesFromGeoJson,
  extractHeatSurfaceGridFromJson,
  extractMeasuredPlanPointsFromJson,
  extractPlanViewPointsFromJson,
  type DisplayContractHint,
  type GeoLineString,
  type HeatmapConfigHint,
  type HeatSurfaceGrid,
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
  lines: GeoLineString[];
  measureNames: string[];
  heatmapHint?: HeatmapConfigHint | null;
  displayContract?: DisplayContractHint | null;
  surfaceGrid?: HeatSurfaceGrid | null;
};

function normalizeSourceData(raw: unknown): SourceData[] {
  if (!Array.isArray(raw)) return [];
  return raw
    .map((x) => {
      if (!x || typeof x !== "object") return null;
      const o = x as Record<string, unknown>;
      const id = typeof o.id === "string" ? o.id : "";
      const label = typeof o.label === "string" ? o.label : id;
      if (!id) return null;

      const pointsRaw = Array.isArray(o.points) ? o.points : [];
      const points: RenderPoint[] = pointsRaw
        .map((p) => {
          if (!p || typeof p !== "object") return null;
          const pp = p as Record<string, unknown>;
          const lat = typeof pp.lat === "number" ? pp.lat : NaN;
          const lon = typeof pp.lon === "number" ? pp.lon : NaN;
          if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
          const label = typeof pp.label === "string" ? pp.label : "";
          const measures =
            pp.measures && typeof pp.measures === "object" && !Array.isArray(pp.measures)
              ? (pp.measures as Record<string, number>)
              : {};
          return { lat, lon, label, measures };
        })
        .filter((p): p is RenderPoint => p !== null);

      const linesRaw = Array.isArray(o.lines) ? o.lines : [];
      const lines: GeoLineString[] = linesRaw
        .map((ln) => {
          if (!ln || typeof ln !== "object") return null;
          const ll = ln as Record<string, unknown>;
          const c = Array.isArray(ll.coords) ? ll.coords : [];
          const coords: [number, number][] = c
            .map((xy) => {
              if (!Array.isArray(xy) || xy.length < 2) return null;
              const x = typeof xy[0] === "number" ? xy[0] : NaN;
              const y = typeof xy[1] === "number" ? xy[1] : NaN;
              if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
              return [x, y] as [number, number];
            })
            .filter((xy): xy is [number, number] => xy !== null);
          if (coords.length < 2) return null;
          const level = typeof ll.level === "number" ? ll.level : undefined;
          return { coords, level };
        })
        .filter((ln): ln is GeoLineString => ln !== null);

      const measureNames = Array.isArray(o.measureNames)
        ? o.measureNames.filter((m): m is string => typeof m === "string")
        : [];
      const heatmapHint =
        o.heatmapHint && typeof o.heatmapHint === "object" && !Array.isArray(o.heatmapHint)
          ? (o.heatmapHint as HeatmapConfigHint)
          : null;
      const displayContract =
        o.displayContract &&
        typeof o.displayContract === "object" &&
        !Array.isArray(o.displayContract)
          ? (o.displayContract as DisplayContractHint)
          : null;
      const surfaceGrid =
        o.surfaceGrid && typeof o.surfaceGrid === "object" && !Array.isArray(o.surfaceGrid)
          ? (o.surfaceGrid as HeatSurfaceGrid)
          : null;

      return {
        id,
        label,
        points,
        lines,
        measureNames,
        heatmapHint,
        displayContract,
        surfaceGrid,
      };
    })
    .filter((s): s is SourceData => s !== null);
}

function cacheKeyForView(graphId: string | null, viewerNodeId: string | null): string {
  return `mineeye:map2d:source:${graphId ?? ""}:${viewerNodeId ?? ""}`;
}

type PointShape = "circle" | "square" | "diamond";
type ChannelMode = "fixed" | "measure";

type SourceLayerConfig = {
  id: string;
  sourceId: string;
  sourceHintSig?: string;
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

function hintSig(hint: HeatmapConfigHint | null | undefined): string {
  if (!hint) return "";
  return JSON.stringify({
    measure: hint.measure ?? "",
    renderMeasure: hint.renderMeasure ?? "",
    method: hint.method ?? "",
    scale: hint.scale ?? "",
    clampLowPct: hint.clampLowPct ?? null,
    clampHighPct: hint.clampHighPct ?? null,
    idwPower: hint.idwPower ?? null,
    smoothness: hint.smoothness ?? null,
    palette: hint.palette ?? "",
    opacity: hint.opacity ?? null,
    minVisibleRender: hint.minVisibleRender ?? null,
    maxVisibleRender: hint.maxVisibleRender ?? null,
  });
}

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
      (a.key.toLowerCase().endsWith(".json") || a.key.toLowerCase().endsWith(".geojson"))
  );
}

function defaultLayerForSource(s: SourceData): SourceLayerConfig {
  const hint = s.heatmapHint ?? {};
  const dc = s.displayContract ?? {};
  const lockedRenderer = dc.renderer === "heat_surface";
  const preferredMeasure = hint.renderMeasure ?? hint.measure ?? "";
  const hintMeasure =
    preferredMeasure && s.measureNames.includes(preferredMeasure)
      ? preferredMeasure
      : s.measureNames[0] ?? "";
  const hintMethod = String(hint.method ?? "idw").toLowerCase();
  const defaultSmoothness =
    typeof hint.smoothness === "number" && Number.isFinite(hint.smoothness)
      ? Math.max(128, Math.min(512, Math.trunc(hint.smoothness)))
      : hintMethod === "kriging"
        ? 384
        : 256;
  const defaultPower =
    typeof hint.idwPower === "number" && Number.isFinite(hint.idwPower)
      ? Math.max(1, Math.min(4, hint.idwPower))
      : hintMethod === "nearest"
        ? 1
        : 2;
  const defaultOpacity =
    typeof hint.opacity === "number" && Number.isFinite(hint.opacity)
      ? Math.max(0.1, Math.min(1, hint.opacity))
      : 0.52;
  return {
    id: `src:${s.id}`,
    sourceId: s.id,
    sourceHintSig: hintSig(s.heatmapHint),
    title: s.label,
    visible: true,
    opacity: 1,
    expanded: false,
    points: {
      shape: "circle",
      colorMode: "fixed",
      color: "#38bdf8",
      colorMeasure: hintMeasure,
      sizeMode: "fixed",
      sizePx: 6,
      sizeMeasure: hintMeasure,
      sizeMinPx: 4,
      sizeMaxPx: 12,
    },
    heatmap: {
      enabled: lockedRenderer ? true : hintMeasure.length > 0,
      measure: hintMeasure,
      opacity: defaultOpacity,
      smoothness: defaultSmoothness,
      power: defaultPower,
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

function hexToRgb(hex: string): { r: number; g: number; b: number } {
  const s = hex.replace("#", "");
  const n = s.length === 3 ? s.split("").map((c) => `${c}${c}`).join("") : s;
  const v = parseInt(n, 16);
  if (!Number.isFinite(v)) return { r: 56, g: 189, b: 248 };
  return { r: (v >> 16) & 255, g: (v >> 8) & 255, b: v & 255 };
}

function lerpRgb(a: { r: number; g: number; b: number }, b: { r: number; g: number; b: number }, t: number) {
  return {
    r: Math.round(a.r + (b.r - a.r) * t),
    g: Math.round(a.g + (b.g - a.g) * t),
    b: Math.round(a.b + (b.b - a.b) * t),
  };
}

function paletteStops(palette: string): Array<{ t: number; c: string }> {
  switch (palette.toLowerCase()) {
    case "inferno":
      return [
        { t: 0.0, c: "#000004" },
        { t: 0.2, c: "#2b0a5a" },
        { t: 0.45, c: "#781c6d" },
        { t: 0.7, c: "#d13a2f" },
        { t: 1.0, c: "#ff3b2f" },
      ];
    case "viridis":
      return [
        { t: 0.0, c: "#440154" },
        { t: 0.25, c: "#3b528b" },
        { t: 0.5, c: "#21908c" },
        { t: 0.75, c: "#5dc863" },
        { t: 1.0, c: "#fde725" },
      ];
    case "terrain":
      return [
        { t: 0.0, c: "#2b83ba" },
        { t: 0.35, c: "#abdda4" },
        { t: 0.6, c: "#66bd63" },
        { t: 0.8, c: "#fdae61" },
        { t: 1.0, c: "#d7191c" },
      ];
    default:
      return [
        { t: 0.0, c: "#2c7bb6" },
        { t: 0.25, c: "#00a6ca" },
        { t: 0.5, c: "#00cc6a" },
        { t: 0.75, c: "#f9d057" },
        { t: 1.0, c: "#d7191c" },
      ];
  }
}

function paletteRgb(palette: string, tRaw: number): { r: number; g: number; b: number } {
  const t = Math.max(0, Math.min(1, tRaw));
  const stops = paletteStops(palette);
  for (let i = 1; i < stops.length; i++) {
    const a = stops[i - 1];
    const b = stops[i];
    if (t <= b.t) {
      const tt = (t - a.t) / Math.max(1e-9, b.t - a.t);
      return lerpRgb(hexToRgb(a.c), hexToRgb(b.c), tt);
    }
  }
  return hexToRgb(stops[stops.length - 1].c);
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
  const [manifestArtifacts, setManifestArtifacts] = useState<ArtifactEntry[]>([]);
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
  const legend = useMemo(() => {
    const activeLayer = layers.find((l) => {
      if (!l.visible || !l.heatmap.enabled) return false;
      const s = sourceById.get(l.sourceId);
      return Boolean(s);
    });
    if (!activeLayer) return null;
    const s = sourceById.get(activeLayer.sourceId);
    if (!s) return null;
    const palette = s.heatmapHint?.palette ?? "rainbow";
    let min = 0;
    let max = 1;
    if (s.surfaceGrid) {
      const vals = s.surfaceGrid.values.filter(
        (v): v is number => typeof v === "number" && Number.isFinite(v)
      );
      const mm = valueMinMax(vals);
      if (mm) {
        min = mm.min;
        max = mm.max;
      }
    } else {
      const vals = s.points
        .map((p) => p.measures[activeLayer.heatmap.measure])
        .filter((v): v is number => Number.isFinite(v));
      const mm = valueMinMax(vals);
      if (mm) {
        min = mm.min;
        max = mm.max;
      }
    }
    return {
      measure: activeLayer.heatmap.measure,
      palette,
      min,
      max,
      clipMin: s.heatmapHint?.minVisibleRender,
      clipMax: s.heatmapHint?.maxVisibleRender,
    };
  }, [layers, sourceById]);
  const inputLinks = useMemo(
    () => (viewerNodeId ? upstreamSourcesForViewer(edges, viewerNodeId) : []),
    [edges, viewerNodeId]
  );

  useEffect(() => {
    if (!graphId || !viewerNodeId) {
      setManifestArtifacts([]);
      return;
    }
    let cancelled = false;
    void (async () => {
      try {
        const mf = await fetchViewerManifest(graphId, viewerNodeId);
        if (cancelled) return;
        const arts: ArtifactEntry[] = mf.layers.map((l) => ({
          node_id: l.source_node_id,
          key: l.artifact_key,
          url: l.artifact_url,
          content_hash: l.content_hash,
        }));
        setManifestArtifacts(arts);
      } catch {
        if (!cancelled) setManifestArtifacts([]);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [artifacts, graphId, viewerNodeId]);

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
          const parsed = normalizeSourceData(JSON.parse(raw) as unknown);
          if (parsed.length > 0) {
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
    const fallbackArts = jsonArtifactsForNodes(graphId, artifacts, upstreamIds);
    const arts = manifestArtifacts.length > 0 ? manifestArtifacts : fallbackArts;
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
        const heatmapHint = extractHeatmapConfigFromJson(text);
        const displayContract = extractDisplayContractFromJson(text);
        const surfaceGrid = extractHeatSurfaceGridFromJson(text);
        const lines = extractLineFeaturesFromGeoJson(text);
        if (basic.length === 0 && measured.length === 0 && lines.length === 0 && !surfaceGrid) {
          continue;
        }

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
          id: `${art.node_id}:${art.key}:${art.content_hash}`,
          label: art.key.split("/").pop() ?? art.key,
          points,
          lines,
          measureNames,
          heatmapHint,
          displayContract,
          surfaceGrid,
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
  }, [graphId, viewerNodeId, artifacts, manifestArtifacts, inputLinks, sourceData.length]);

  useEffect(() => {
    if (sourceData.length === 0) return;
    setLayers((prev) => {
      const sourceIds = new Set(sourceData.map((s) => s.id));
      const prevById = new Map(prev.map((l) => [l.id, l]));
      const desired = sourceData.map((s) => {
        const id = `src:${s.id}`;
        const defaultFromSource = defaultLayerForSource(s);
        const nextHintSig = hintSig(s.heatmapHint);
        const lockedRenderer = s.displayContract?.renderer === "heat_surface";
        const ex = prevById.get(id);
        if (!ex) return defaultFromSource;
        const hintChanged =
          s.heatmapHint &&
          (ex.sourceHintSig ?? "") !== nextHintSig;
        if (hintChanged) {
          return {
            ...ex,
            sourceId: s.id,
            sourceHintSig: nextHintSig,
            heatmap: defaultFromSource.heatmap,
            points: lockedRenderer ? defaultFromSource.points : ex.points,
          };
        }
        return {
          ...ex,
          sourceId: s.id,
          sourceHintSig: nextHintSig,
          points: {
            ...(lockedRenderer ? defaultFromSource.points : ex.points),
            colorMeasure:
              (lockedRenderer ? defaultFromSource.points.colorMeasure : ex.points.colorMeasure) &&
              s.measureNames.includes(
                lockedRenderer ? defaultFromSource.points.colorMeasure : ex.points.colorMeasure
              )
                ? (lockedRenderer
                    ? defaultFromSource.points.colorMeasure
                    : ex.points.colorMeasure)
                : s.measureNames[0] ?? "",
            sizeMeasure:
              (lockedRenderer ? defaultFromSource.points.sizeMeasure : ex.points.sizeMeasure) &&
              s.measureNames.includes(
                lockedRenderer ? defaultFromSource.points.sizeMeasure : ex.points.sizeMeasure
              )
                ? (lockedRenderer
                    ? defaultFromSource.points.sizeMeasure
                    : ex.points.sizeMeasure)
                : s.measureNames[0] ?? "",
          },
          heatmap: {
            ...(lockedRenderer ? defaultFromSource.heatmap : ex.heatmap),
            measure:
              (lockedRenderer ? defaultFromSource.heatmap.measure : ex.heatmap.measure) &&
              s.measureNames.includes(
                lockedRenderer ? defaultFromSource.heatmap.measure : ex.heatmap.measure
              )
                ? (lockedRenderer
                    ? defaultFromSource.heatmap.measure
                    : ex.heatmap.measure)
                : s.measureNames[0] ?? "",
            enabled: s.measureNames.length > 0 ? (lockedRenderer ? true : ex.heatmap.enabled) : false,
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
        const lockedRenderer = src.displayContract?.renderer === "heat_surface";
        const palette = src.heatmapHint?.palette ?? "rainbow";
        const clipMin = src.heatmapHint?.minVisibleRender;
        const clipMax = src.heatmapHint?.maxVisibleRender;
        if (lockedRenderer && src.surfaceGrid) {
          const g = src.surfaceGrid;
          const vals = g.values.filter((v): v is number => typeof v === "number" && Number.isFinite(v));
          const mm = valueMinMax(vals);
          if (mm) {
            const canvas = document.createElement("canvas");
            canvas.width = g.nx;
            canvas.height = g.ny;
            const ctx = canvas.getContext("2d");
            if (ctx) {
              const img = ctx.createImageData(g.nx, g.ny);
              for (let yi = 0; yi < g.ny; yi++) {
                for (let xi = 0; xi < g.nx; xi++) {
                  // Grid values are stored south->north; image rows are top->bottom, so flip Y.
                  const srcYi = g.ny - 1 - yi;
                  const v = g.values[srcYi * g.nx + xi];
                  const idx = (yi * g.nx + xi) * 4;
                  if (typeof v !== "number" || !Number.isFinite(v)) {
                    img.data[idx + 3] = 0;
                    continue;
                  }
                  if (typeof clipMin === "number" && v < clipMin) {
                    img.data[idx + 3] = 0;
                    continue;
                  }
                  if (typeof clipMax === "number" && v > clipMax) {
                    img.data[idx + 3] = 0;
                    continue;
                  }
                  const t = norm(v, mm.min, mm.max);
                  const { r, g: gg, b } = paletteRgb(palette, t);
                  img.data[idx] = r;
                  img.data[idx + 1] = gg;
                  img.data[idx + 2] = b;
                  img.data[idx + 3] = 255;
                }
              }
              ctx.putImageData(img, 0, 0);
              L.imageOverlay(
                canvas.toDataURL("image/png"),
                [
                  [g.ymin, g.xmin],
                  [g.ymax, g.xmax],
                ],
                {
                  pane: heatPane,
                  opacity: layer.heatmap.opacity * layer.opacity,
                  interactive: false,
                }
              ).addTo(map);
            }
          }
        } else {
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
                      const iv = idw(lat, lon);
                      if (typeof clipMin === "number" && iv < clipMin) {
                        const idx = (yi * n + xi) * 4;
                        img.data[idx + 3] = 0;
                        continue;
                      }
                      if (typeof clipMax === "number" && iv > clipMax) {
                        const idx = (yi * n + xi) * 4;
                        img.data[idx + 3] = 0;
                        continue;
                      }
                      const t = norm(iv, mm.min, mm.max);
                      const { r, g, b } = paletteRgb(palette, t);
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
                    {
                      pane: heatPane,
                      opacity: layer.heatmap.opacity * layer.opacity,
                      interactive: false,
                    }
                  ).addTo(map);
                }
              }
            }
          }
        }
      }

      const contourLines = src.lines ?? [];
      if (contourLines.length > 0) {
        contourLines.forEach((ln) => {
          const latLngs: [number, number][] = ln.coords.map(([x, y]) => [y, x]);
          if (latLngs.length < 2) return;
          L.polyline(latLngs, {
            pane: pointPane,
            color: "#f8fafc",
            opacity: Math.max(0.2, layer.opacity),
            weight: 1.2,
          })
            .bindTooltip(
              ln.level != null ? `Contour ${ln.level.toFixed(3)}` : "Contour",
              { sticky: true }
            )
            .addTo(map);
        });
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
        {legend && (
          <div
            style={{
              position: "absolute",
              left: 12,
              bottom: 12,
              zIndex: 850,
              background: "rgba(15,20,25,0.9)",
              border: "1px solid #30363d",
              borderRadius: 8,
              padding: "8px 10px",
              width: 220,
              fontSize: 10,
            }}
          >
            <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 4 }}>
              {legend.measure || "heatmap"}
            </div>
            <div
              style={{
                height: 10,
                borderRadius: 999,
                border: "1px solid #30363d",
                background: `linear-gradient(to right, ${paletteStops(legend.palette)
                  .map((s) => s.c)
                  .join(", ")})`,
              }}
            />
            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
              <span>{legend.min.toFixed(3)}</span>
              <span>{legend.max.toFixed(3)}</span>
            </div>
            {(typeof legend.clipMin === "number" || typeof legend.clipMax === "number") && (
              <div style={{ marginTop: 4, opacity: 0.8 }}>
                clip: {typeof legend.clipMin === "number" ? legend.clipMin.toFixed(3) : "—"} to{" "}
                {typeof legend.clipMax === "number" ? legend.clipMax.toFixed(3) : "—"}
              </div>
            )}
          </div>
        )}
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
                const lockedRenderer = src?.displayContract?.renderer === "heat_surface";
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
                            disabled={lockedRenderer}
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
                            disabled={lockedRenderer}
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
                                disabled={lockedRenderer}
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
                                disabled={lockedRenderer}
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
                              disabled={lockedRenderer}
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
                                disabled={lockedRenderer}
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
                                disabled={lockedRenderer}
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
                              disabled={lockedRenderer}
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
                              disabled={lockedRenderer}
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
                            disabled={measures.length === 0 || lockedRenderer}
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
                                disabled={lockedRenderer}
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
                            {!lockedRenderer && (
                              <>
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
                            {lockedRenderer && (
                              <div style={{ fontSize: 10, opacity: 0.65 }}>
                                Heat surface is node-driven; tune interpolation in the heatmap node.
                              </div>
                            )}
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
