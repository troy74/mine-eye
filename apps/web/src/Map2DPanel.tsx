import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  api,
  fetchViewerManifest,
  fetchGraph,
  patchNodeParams,
  type ApiEdge,
  type ArtifactEntry,
  type ViewerManifestLayer,
} from "./graphApi";
import { isPlanViewInputSemantic } from "./portTaxonomy";
import { edgeColorForApiEdge } from "./portTypes";
import { lonLatFromProjectedAsync } from "./spatialReproject";
import {
  createBoundedImageLayer,
  createGlobalTileLayer,
  createLocalExtentTileLayer,
  type LeafletRasterPane,
} from "./leafletRasterLayers";
import {
  parseRasterOverlayContract,
  rasterBoundsToLatLng,
  rasterContractPriority,
  resolveRasterOverlaySource,
  type RasterOverlayContract,
} from "./rasterOverlay";
import {
  extractDisplayContractFromJson,
  epsgFromAnyJson,
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
  artifactKey?: string;
  contentHash?: string;
  sourceType?: "magnetic" | "generic";
  points: RenderPoint[];
  lines: GeoLineString[];
  measureNames: string[];
  heatmapHint?: HeatmapConfigHint | null;
  displayContract?: DisplayContractHint | null;
  surfaceGrid?: HeatSurfaceGrid | null;
};

type LayerStackLayer = {
  layer_id?: string;
  kind?: string;
  source_artifact_ref?: { key?: string; content_hash?: string };
  priority?: number;
  visibility_default?: boolean;
};

type LayerStackContract = {
  schema_id: "scene3d.layer_stack.v1";
  layers?: LayerStackLayer[];
};

function normalizeSourceData(raw: unknown): SourceData[] {
  if (!Array.isArray(raw)) return [];
  return raw
    .map((x) => {
      if (!x || typeof x !== "object") return null;
      const o = x as Record<string, unknown>;
      const id = typeof o.id === "string" ? o.id : "";
      const label = typeof o.label === "string" ? o.label : id;
      const artifactKey = typeof o.artifactKey === "string" ? o.artifactKey : undefined;
      const contentHash = typeof o.contentHash === "string" ? o.contentHash : undefined;
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
        artifactKey,
        contentHash,
        sourceType: sourceTypeForArtifact(artifactKey ?? id, measureNames, heatmapHint),
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

function parseLayerStackContract(v: unknown): LayerStackContract | null {
  if (!v || typeof v !== "object" || Array.isArray(v)) return null;
  const obj = v as Record<string, unknown>;
  if (obj.schema_id !== "scene3d.layer_stack.v1") return null;
  return obj as unknown as LayerStackContract;
}

type PointShape = "circle" | "square" | "diamond";
type ChannelMode = "fixed" | "measure" | "categorical";
type ValueTransform = "linear" | "log10" | "ln";

type SourceLayerConfig = {
  id: string;
  sourceId: string;
  sourceHintSig?: string;
  title: string;
  visible: boolean;
  opacity: number;
  expanded: boolean;
  points: {
    enabled: boolean;
    shape: PointShape;
    colorMode: ChannelMode;
    color: string;
    colorMeasure: string;
    colorTransform: ValueTransform;
    colorPalette: string;
    sizeMode: ChannelMode;
    sizePx: number;
    sizeMeasure: string;
    sizeTransform: ValueTransform;
    sizeMinPx: number;
    sizeMaxPx: number;
  };
  heatmap: {
    enabled: boolean;
    measure: string;
    transform: ValueTransform;
    palette: string;
    opacity: number;
    smoothness: number;
    power: number;
  };
};

type LayerOrderMode = "contract" | "override";

function sourceTypeForArtifact(
  artifactKey: string,
  measureNames: string[],
  heatmapHint?: HeatmapConfigHint | null
): "magnetic" | "generic" {
  const key = artifactKey.toLowerCase();
  if (key.includes("magnetic_")) return "magnetic";
  if (measureNames.includes("M")) return "magnetic";
  const hm = String(heatmapHint?.measure ?? "").toLowerCase();
  if (hm === "m" || hm.includes("mag")) return "magnetic";
  return "generic";
}

function allowedHeatmapMeasuresForSource(s: SourceData | undefined): string[] {
  if (!s) return [];
  if (s.sourceType !== "magnetic") return s.measureNames;
  const allow = ["M", "TMF", "fvd", "grad_mag", "tilt"];
  return allow.filter((m) => s.measureNames.includes(m));
}

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

function isJsonLikeArtifact(a: ArtifactEntry): boolean {
  const k = a.key.toLowerCase();
  return k.endsWith(".json") || k.endsWith(".geojson");
}

function baseArtifactKeyForPreview(key: string): string {
  return key
    .replace(/\.preview(?=\.[^./]+$)/i, "")
    .replace(/\.sample(?=\.[^./]+$)/i, "");
}

function preferPreviewArtifacts(arts: ArtifactEntry[]): ArtifactEntry[] {
  const byBase = new Map<string, ArtifactEntry>();
  for (const art of arts) {
    const base = baseArtifactKeyForPreview(art.key);
    const prev = byBase.get(base);
    if (!prev) {
      byBase.set(base, art);
      continue;
    }
    const aScore =
      /\.preview\./i.test(art.key) ? 3 : art.key.toLowerCase().endsWith(".json") ? 2 : 1;
    const pScore =
      /\.preview\./i.test(prev.key) ? 3 : prev.key.toLowerCase().endsWith(".json") ? 2 : 1;
    if (aScore > pScore) byBase.set(base, art);
  }
  return [...byBase.values()];
}

function filterArtifactsForPlanView(arts: ArtifactEntry[]): ArtifactEntry[] {
  const contractNodeIds = new Set(
    arts
      .filter((a) => {
        const k = a.key.toLowerCase();
        return (
          k.endsWith("heatmap_imagery_drape.json") ||
          k.endsWith("imagery_drape.json") ||
          k.endsWith("raster_tile_manifest.json")
        );
      })
      .map((a) => a.node_id)
  );
  if (contractNodeIds.size === 0) return arts;
  return arts.filter((a) => {
    if (!contractNodeIds.has(a.node_id)) return true;
    const k = a.key.toLowerCase();
    return (
      k.endsWith("heatmap_imagery_drape.json") ||
      k.endsWith("imagery_drape.json") ||
      k.endsWith("raster_tile_manifest.json") ||
      k.endsWith("layer_stack.json")
    );
  });
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
  const allowedHeatMeasures = allowedHeatmapMeasuresForSource(s);
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
      enabled: s.sourceType === "magnetic" ? false : true,
      shape: "circle",
      colorMode: "fixed",
      color: "#38bdf8",
      colorMeasure: hintMeasure,
      colorTransform: "linear",
      colorPalette: "turbo",
      sizeMode: "fixed",
      sizePx: 6,
      sizeMeasure: hintMeasure,
      sizeTransform: "linear",
      sizeMinPx: 4,
      sizeMaxPx: 12,
    },
    heatmap: {
      enabled: lockedRenderer ? true : (s.sourceType === "magnetic" ? true : hintMeasure.length > 0),
      measure:
        hintMeasure && allowedHeatMeasures.includes(hintMeasure)
          ? hintMeasure
          : allowedHeatMeasures[0] ?? "",
      transform: "linear",
      palette: hint.palette ?? "rainbow",
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

function applyTransform(v: number, transform: ValueTransform): number | null {
  if (!Number.isFinite(v)) return null;
  if (transform === "linear") return v;
  if (v <= 0) return null;
  if (transform === "log10") return Math.log10(v);
  return Math.log(v);
}

function categoricalColor(value: number, palette: string): string {
  const bucket = Math.round(value * 1000) / 1000;
  const x = Math.abs(Math.floor(bucket * 104729));
  const t = (x % 997) / 996;
  const { r, g, b } = paletteRgb(palette, t);
  return `rgb(${r},${g},${b})`;
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

function fitMapToBounds(
  map: L.Map,
  bounds: L.LatLngBounds,
  options?: { padRatio?: number; maxZoom?: number }
) {
  const padRatio = options?.padRatio ?? 0;
  const padded = bounds.pad(padRatio);
  let zoom = map.getBoundsZoom(padded, false);
  if (typeof options?.maxZoom === "number" && Number.isFinite(options.maxZoom)) {
    zoom = Math.min(zoom, options.maxZoom);
  }
  map.setView(padded.getCenter(), zoom, { animate: false });
}

function ensurePane(map: L.Map, paneName: LeafletRasterPane, zIndex: number) {
  if (!map.getPane(paneName)) {
    map.createPane(paneName);
  }
  const pane = map.getPane(paneName);
  if (pane) {
    pane.style.zIndex = String(zIndex);
    pane.style.pointerEvents = "none";
  }
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
  const baseTileLayerRef = useRef<L.TileLayer | null>(null);
  const rasterBaseLayerRef = useRef<L.Layer | null>(null);
  const rasterAnalyticLayerGroupRef = useRef<L.LayerGroup | null>(null);
  const dynamicLayerGroupRef = useRef<L.LayerGroup | null>(null);
  const [status, setStatus] = useState("");
  const [imageryContract, setImageryContract] = useState<RasterOverlayContract | null>(null);
  const [layerStackContract, setLayerStackContract] = useState<LayerStackContract | null>(null);
  const [sourceData, setSourceData] = useState<SourceData[]>([]);
  const [manifestArtifacts, setManifestArtifacts] = useState<ArtifactEntry[]>([]);
  const [manifestLayers, setManifestLayers] = useState<ViewerManifestLayer[]>([]);
  const [layers, setLayers] = useState<SourceLayerConfig[]>([]);
  const [panelCollapsed, setPanelCollapsed] = useState(false);
  const [layerOrderMode, setLayerOrderMode] = useState<LayerOrderMode>("contract");
  const [baseVisible, setBaseVisible] = useState(true);
  const [baseOpacity, setBaseOpacity] = useState(1);
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

  // Map from upstream node_id → semantic edge colour, for layer dot colouring
  const nodeSemanticColorMap = useMemo(() => {
    if (!viewerNodeId) return new Map<string, string>();
    const map = new Map<string, string>();
    for (const e of edges) {
      if (e.to_node === viewerNodeId) {
        map.set(e.from_node, edgeColorForApiEdge(e));
      }
    }
    return map;
  }, [edges, viewerNodeId]);
  const legend = useMemo(() => {
    const activeLayer = layers.find((l) => {
      if (!l.visible || !l.heatmap.enabled) return false;
      const s = sourceById.get(l.sourceId);
      return Boolean(s);
    });
    if (!activeLayer) return null;
    const s = sourceById.get(activeLayer.sourceId);
    if (!s) return null;
    const palette = activeLayer.heatmap.palette || s.heatmapHint?.palette || "rainbow";
    let min = 0;
    let max = 1;
    if (s.surfaceGrid) {
      const vals = s.surfaceGrid.values
        .map((v) => (typeof v === "number" ? applyTransform(v, activeLayer.heatmap.transform) : null))
        .filter((v): v is number => v !== null && Number.isFinite(v));
      const mm = valueMinMax(vals);
      if (mm) {
        min = mm.min;
        max = mm.max;
      }
    } else {
      const vals = s.points
        .map((p) => p.measures[activeLayer.heatmap.measure])
        .map((v) => (typeof v === "number" ? applyTransform(v, activeLayer.heatmap.transform) : null))
        .filter((v): v is number => v !== null && Number.isFinite(v));
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
  const hasLayerStackContract = Boolean(layerStackContract?.layers?.length);
  const canDragLayers = !hasLayerStackContract || layerOrderMode === "override";

  useEffect(() => {
    if (!graphId || !viewerNodeId) {
      setManifestArtifacts([]);
      setManifestLayers([]);
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
        setManifestLayers(mf.layers);
      } catch {
        if (!cancelled) {
          setManifestArtifacts([]);
          setManifestLayers([]);
        }
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
      zoomAnimation: false,
      fadeAnimation: false,
      markerZoomAnimation: false,
    }).setView([20, 0], 2);
    const base = L.tileLayer(
      "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
      {
        maxZoom: 19,
        attribution:
          '&copy; <a href="https://www.esri.com/">Esri</a> (World Imagery)',
      }
    ).addTo(map);
    baseTileLayerRef.current = base;
    ensurePane(map, "mineeye-raster-base", 250);
    ensurePane(map, "mineeye-raster-analytic", 430);
    const dynamicGroup = L.layerGroup().addTo(map);
    dynamicLayerGroupRef.current = dynamicGroup;
    const analyticGroup = L.layerGroup().addTo(map);
    rasterAnalyticLayerGroupRef.current = analyticGroup;
    const onMoveStart = () => {
      userMovedMapRef.current = true;
    };
    map.on("movestart", onMoveStart);
    map.on("zoomstart", onMoveStart);
    mapRef.current = map;
    return () => {
      map.off("movestart", onMoveStart);
      map.off("zoomstart", onMoveStart);
      if (rasterBaseLayerRef.current && map.hasLayer(rasterBaseLayerRef.current)) {
        map.removeLayer(rasterBaseLayerRef.current);
      }
      rasterBaseLayerRef.current = null;
      if (rasterAnalyticLayerGroupRef.current && map.hasLayer(rasterAnalyticLayerGroupRef.current)) {
        map.removeLayer(rasterAnalyticLayerGroupRef.current);
      }
      rasterAnalyticLayerGroupRef.current = null;
      dynamicLayerGroupRef.current = null;
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    let cancelled = false;
    void (async () => {
      if (rasterBaseLayerRef.current && map.hasLayer(rasterBaseLayerRef.current)) {
        map.removeLayer(rasterBaseLayerRef.current);
      }
      rasterBaseLayerRef.current = null;
      const baseTile = baseTileLayerRef.current;
      if (baseTile && !map.hasLayer(baseTile)) {
        baseTile.addTo(map);
      }
      const source = await resolveRasterOverlaySource(imageryContract);
      if (!baseVisible || !source || !source.bounds) {
        return;
      }
      if (cancelled) {
        return;
      }
      let layer: L.Layer | null = null;
      if (source.mode === "single_image" && source.imageUrl) {
        layer = createBoundedImageLayer(api(source.imageUrl), source.bounds, {
          opacity: baseOpacity,
          pane: "mineeye-raster-base",
        });
      } else if (source.mode === "global_xyz_tiles" && source.tileUrlTemplate) {
        layer = createGlobalTileLayer(api(source.tileUrlTemplate), {
          minZoom: source.tileMinZoom,
          maxZoom: source.tileMaxZoom,
          opacity: baseOpacity,
          attribution: source.attribution,
          pane: "mineeye-raster-base",
        });
      } else if (source.tileUrlTemplate) {
        layer = createLocalExtentTileLayer({
          bounds: source.bounds,
          tileUrlTemplate: api(source.tileUrlTemplate),
          tileMinZoom: source.tileMinZoom,
          tileMaxZoom: source.tileMaxZoom,
          opacity: baseOpacity,
          pane: "mineeye-raster-base",
        });
      } else if (source.imageUrl) {
        layer = createBoundedImageLayer(api(source.imageUrl), source.bounds, {
          opacity: baseOpacity,
          pane: "mineeye-raster-base",
        });
      }
      if (layer) {
        layer.addTo(map);
        rasterBaseLayerRef.current = layer;
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [imageryContract, baseVisible, baseOpacity, graphId, viewerNodeId]);

  useEffect(() => {
    const ctx = `${graphId ?? ""}:${viewerNodeId ?? ""}`;
    if (lastViewContextRef.current !== ctx) {
      lastViewContextRef.current = ctx;
      userMovedMapRef.current = false;
      lastArtifactSigRef.current = "";
      autoFitContextRef.current = "";
      setSourceData([]);
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
          const savedBaseVisible =
            typeof m.base_visible === "boolean" ? m.base_visible : true;
          const savedBaseOpacity =
            typeof m.base_opacity === "number" && Number.isFinite(m.base_opacity)
              ? Math.max(0, Math.min(1, m.base_opacity))
              : 1;
          const orderMode =
            m.layer_order_mode === "override" || m.layer_order_mode === "contract"
              ? (m.layer_order_mode as LayerOrderMode)
              : "contract";
          const lay = m.layers;
          if (Array.isArray(lay)) {
            setLayers(lay as SourceLayerConfig[]);
          }
          setPanelCollapsed(collapsed);
          setBaseVisible(savedBaseVisible);
          setBaseOpacity(savedBaseOpacity);
          setLayerOrderMode(orderMode);
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
          base_visible: baseVisible,
          base_opacity: baseOpacity,
          layer_order_mode: layerOrderMode,
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
  }, [graphId, viewerNodeId, activeBranchId, panelCollapsed, baseVisible, baseOpacity, layerOrderMode, layers, configHydrated]);

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

  // Invalidate Leaflet size whenever the container element is resized (e.g. when
  // the left sidebar collapses/expands), preventing patchy tile loading.
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => {
      mapRef.current?.invalidateSize();
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

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

    fitMapToBounds(map, L.latLngBounds(fit), { padRatio: 0.25 });
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
      setImageryContract(null);
      setLayerStackContract(null);
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
    const manifestJsonArts = manifestArtifacts.filter(isJsonLikeArtifact);
    const artsRaw = manifestJsonArts.length > 0 ? manifestJsonArts : fallbackArts;
    const arts = filterArtifactsForPlanView(preferPreviewArtifacts(artsRaw));
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
      let foundImageryContract: RasterOverlayContract | null = null;
      let foundLayerStackContract: LayerStackContract | null = null;
      const manifestByArtifact = new Map<string, ViewerManifestLayer>();
      for (const ml of manifestLayers) {
        manifestByArtifact.set(`${ml.artifact_key}:${ml.content_hash}`, ml);
      }
      const fit: L.LatLngExpression[] = [];
      let total = 0;
      const notes: string[] = [];

      for (const art of arts) {
        if (token !== loadTokenRef.current || !mountedRef.current) return;
        const r = await fetch(api(art.url), { cache: "no-store" });
        if (!r.ok) {
          notes.push(`${art.key.split("/").pop()}: HTTP ${r.status}`);
          continue;
        }
        const text = await r.text();
        let parsedRoot: Record<string, unknown> | null = null;
        try {
          const raw = JSON.parse(text) as unknown;
          if (raw && typeof raw === "object" && !Array.isArray(raw)) {
            parsedRoot = raw as Record<string, unknown>;
          }
          const maybeImagery = parseRasterOverlayContract(raw);
          if (
            maybeImagery &&
            rasterContractPriority(maybeImagery) >
              rasterContractPriority(foundImageryContract)
          ) {
            foundImageryContract = maybeImagery;
          }
          const maybeLayerStack = parseLayerStackContract(raw);
          if (!foundLayerStackContract && maybeLayerStack) {
            foundLayerStackContract = maybeLayerStack;
          }
        } catch {
          // ignore parse errors
        }
        const manifestMeta = manifestByArtifact.get(`${art.key}:${art.content_hash}`);
        const mPres =
          manifestMeta?.presentation &&
          typeof manifestMeta.presentation === "object" &&
          !Array.isArray(manifestMeta.presentation)
            ? (manifestMeta.presentation as Record<string, unknown>)
            : null;
        const mHeatCfg =
          mPres?.heatmap_config &&
          typeof mPres.heatmap_config === "object" &&
          !Array.isArray(mPres.heatmap_config)
            ? (mPres.heatmap_config as Record<string, unknown>)
            : null;
        const manifestDisplayContract: DisplayContractHint | null = mPres
          ? {
              renderer:
                typeof mPres.renderer === "string" ? String(mPres.renderer) : undefined,
              editable: Array.isArray(mPres.editable)
                ? mPres.editable.filter((x): x is string => typeof x === "string")
                : undefined,
            }
          : null;
        const manifestHeatmapHint: HeatmapConfigHint | null = mHeatCfg
          ? {
              measure:
                typeof mHeatCfg.measure === "string" ? String(mHeatCfg.measure) : undefined,
              renderMeasure:
                typeof mHeatCfg.render_measure === "string"
                  ? String(mHeatCfg.render_measure)
                  : undefined,
              method:
                typeof mHeatCfg.method === "string" ? String(mHeatCfg.method) : undefined,
              scale:
                typeof mHeatCfg.scale === "string" ? String(mHeatCfg.scale) : undefined,
              palette:
                typeof mHeatCfg.palette === "string" ? String(mHeatCfg.palette) : undefined,
              clampLowPct:
                typeof mHeatCfg.clamp_low_pct === "number" ? mHeatCfg.clamp_low_pct : undefined,
              clampHighPct:
                typeof mHeatCfg.clamp_high_pct === "number"
                  ? mHeatCfg.clamp_high_pct
                  : undefined,
              idwPower:
                typeof mHeatCfg.idw_power === "number" ? mHeatCfg.idw_power : undefined,
              smoothness:
                typeof mHeatCfg.smoothness === "number" ? mHeatCfg.smoothness : undefined,
              opacity: typeof mHeatCfg.opacity === "number" ? mHeatCfg.opacity : undefined,
              minVisibleRender:
                typeof mHeatCfg.min_visible_render === "number"
                  ? mHeatCfg.min_visible_render
                  : undefined,
              maxVisibleRender:
                typeof mHeatCfg.max_visible_render === "number"
                  ? mHeatCfg.max_visible_render
                  : undefined,
            }
          : null;
        const basic = extractPlanViewPointsFromJson(text, art.key.split("/").pop() ?? art.key);
        const measured = extractMeasuredPlanPointsFromJson(
          text,
          art.key.split("/").pop() ?? art.key
        );
        const heatmapHint = manifestHeatmapHint ?? extractHeatmapConfigFromJson(text);
        const displayContract = manifestDisplayContract ?? extractDisplayContractFromJson(text);
        const surfaceGrid = extractHeatSurfaceGridFromJson(text);
        const lines = extractLineFeaturesFromGeoJson(text);
        if (basic.length === 0 && measured.length === 0 && lines.length === 0 && !surfaceGrid) {
          continue;
        }

        const mByXY = new Map<string, Record<string, number>>();
        measured.forEach((m) => mByXY.set(`${m.x}|${m.y}`, m.measures));
        const mByIndex = measured.map((m) => m.measures);
        const epsg = epsgFromAnyJson(text);
        const points: RenderPoint[] = [];
        for (let i = 0; i < basic.length; i++) {
          const p = basic[i];
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
            measures: mByXY.get(`${p.x}|${p.y}`) ?? mByIndex[i] ?? {},
          });
          fit.push([lat, lon]);
        }
        const candidateMeasures = Array.isArray(parsedRoot?.measure_candidates)
          ? (parsedRoot?.measure_candidates as unknown[])
              .filter((v): v is string => typeof v === "string" && v.trim().length > 0)
              .map((v) => v.trim())
          : [];
        const measureNames = [
          ...new Set([...points.flatMap((p) => Object.keys(p.measures)), ...candidateMeasures]),
        ].sort(
          (a, b) => a.localeCompare(b)
        );
        all.push({
          id: `${art.node_id}:${art.key}:${art.content_hash}`,
          label: art.key.split("/").pop() ?? art.key,
          artifactKey: art.key,
          contentHash: art.content_hash,
          sourceType: sourceTypeForArtifact(art.key, measureNames, heatmapHint),
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
      setImageryContract(foundImageryContract);
      setLayerStackContract(foundLayerStackContract);
      setSourceData(all);
      if (all.length === 0) {
        const imageryBounds = await rasterBoundsToLatLng(foundImageryContract);
        if (imageryBounds && (!userMovedMapRef.current || ctxChanged)) {
          fitMapToBounds(
            map,
            L.latLngBounds([
              [imageryBounds.south, imageryBounds.west],
              [imageryBounds.north, imageryBounds.east],
            ]),
            {
              padRatio: 0.12,
              maxZoom:
                typeof foundImageryContract?.tile_max_zoom === "number"
                  ? foundImageryContract.tile_max_zoom
                  : undefined,
            }
          );
        }
        const imgNote = foundImageryContract?.provider_label
          ? `${foundImageryContract.provider_label} contract loaded.`
          : "Raster contract loaded.";
        const lsNote = foundLayerStackContract ? " Layer stack contract loaded." : "";
        setStatus(`${imgNote}${lsNote}`);
        return;
      }
      const imgNote = foundImageryContract?.fingerprint
        ? ` · imagery ${foundImageryContract.fingerprint.slice(0, 10)}…`
        : foundImageryContract?.provider_label
          ? ` · ${foundImageryContract.provider_label}`
          : "";
      const lsNote = foundLayerStackContract ? " · layer-stack contract" : "";
      setStatus(`${total} point(s) from ${all.length} input artifact(s).${imgNote}${lsNote}${notes.length ? ` ${notes.join(" · ")}` : ""}`);
      if (fit.length && (!userMovedMapRef.current || ctxChanged)) {
        fitMapToBounds(map, L.latLngBounds(fit), { padRatio: 0.25 });
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
  }, [graphId, viewerNodeId, artifacts, manifestArtifacts, manifestLayers, inputLinks, sourceData.length]);

  useEffect(() => {
    if (sourceData.length === 0) {
      setLayers([]);
      return;
    }
    setLayers((prev) => {
      const contractBySource = new Map<
        string,
        { priority: number; visibilityDefault: boolean | null }
      >();
      if (layerStackContract?.layers?.length) {
        for (const l of layerStackContract.layers) {
          const k = l.source_artifact_ref?.key;
          if (!k) continue;
          const h = l.source_artifact_ref?.content_hash;
          const src = sourceData.find(
            (s) =>
              s.artifactKey === k &&
              (!h || !s.contentHash || s.contentHash === h)
          );
          if (!src) continue;
          contractBySource.set(src.id, {
            priority:
              typeof l.priority === "number" && Number.isFinite(l.priority)
                ? l.priority
                : 1000,
            visibilityDefault:
              typeof l.visibility_default === "boolean" ? l.visibility_default : null,
          });
        }
      }
      const sourceIds = new Set(sourceData.map((s) => s.id));
      const prevById = new Map(prev.map((l) => [l.id, l]));
        const desired = sourceData.map((s) => {
        const id = `src:${s.id}`;
        const defaultFromSource = defaultLayerForSource(s);
        const c = contractBySource.get(s.id);
        if (c && c.visibilityDefault !== null) {
          defaultFromSource.visible = c.visibilityDefault;
        }
        const nextHintSig = hintSig(s.heatmapHint);
        const lockedRenderer = s.displayContract?.renderer === "heat_surface";
        const allowedHeatMeasures = allowedHeatmapMeasuresForSource(s);
        const ex = prevById.get(id);
        if (!ex) return defaultFromSource;
        const hintChanged =
          s.heatmapHint &&
          (ex.sourceHintSig ?? "") !== nextHintSig;
        if (hintChanged) {
          const prevPoints =
            lockedRenderer || typeof ex.points.enabled !== "boolean"
              ? defaultFromSource.points
              : ex.points;
          return {
            ...ex,
            sourceId: s.id,
            sourceHintSig: nextHintSig,
            heatmap: defaultFromSource.heatmap,
            points: prevPoints,
          };
        }
        return {
          ...ex,
          sourceId: s.id,
          sourceHintSig: nextHintSig,
          points: {
            ...(lockedRenderer ? defaultFromSource.points : ex.points),
            enabled:
              lockedRenderer || typeof ex.points.enabled !== "boolean"
                ? defaultFromSource.points.enabled
                : ex.points.enabled,
            colorTransform:
              (lockedRenderer
                ? defaultFromSource.points.colorTransform
                : ex.points.colorTransform) === "log10" ||
              (lockedRenderer
                ? defaultFromSource.points.colorTransform
                : ex.points.colorTransform) === "ln"
                ? ((lockedRenderer
                    ? defaultFromSource.points.colorTransform
                    : ex.points.colorTransform) as ValueTransform)
                : "linear",
            colorPalette:
              typeof (lockedRenderer ? defaultFromSource.points.colorPalette : ex.points.colorPalette) ===
              "string"
                ? String(lockedRenderer ? defaultFromSource.points.colorPalette : ex.points.colorPalette)
                : "turbo",
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
            sizeTransform:
              (lockedRenderer
                ? defaultFromSource.points.sizeTransform
                : ex.points.sizeTransform) === "log10" ||
              (lockedRenderer
                ? defaultFromSource.points.sizeTransform
                : ex.points.sizeTransform) === "ln"
                ? ((lockedRenderer
                    ? defaultFromSource.points.sizeTransform
                    : ex.points.sizeTransform) as ValueTransform)
                : "linear",
          },
          heatmap: {
            ...(lockedRenderer ? defaultFromSource.heatmap : ex.heatmap),
            measure:
              (lockedRenderer ? defaultFromSource.heatmap.measure : ex.heatmap.measure) &&
              allowedHeatMeasures.includes(
                lockedRenderer ? defaultFromSource.heatmap.measure : ex.heatmap.measure
              )
                ? (lockedRenderer
                    ? defaultFromSource.heatmap.measure
                    : ex.heatmap.measure)
                : allowedHeatMeasures[0] ?? "",
            enabled:
              allowedHeatMeasures.length > 0 ? (lockedRenderer ? true : ex.heatmap.enabled) : false,
            transform:
              (lockedRenderer
                ? defaultFromSource.heatmap.transform
                : ex.heatmap.transform) === "log10" ||
              (lockedRenderer
                ? defaultFromSource.heatmap.transform
                : ex.heatmap.transform) === "ln"
                ? ((lockedRenderer
                    ? defaultFromSource.heatmap.transform
                    : ex.heatmap.transform) as ValueTransform)
                : "linear",
            palette:
              typeof (lockedRenderer ? defaultFromSource.heatmap.palette : ex.heatmap.palette) === "string"
                ? String(lockedRenderer ? defaultFromSource.heatmap.palette : ex.heatmap.palette)
                : "rainbow",
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
      const filtered = ordered.filter((l) => sourceIds.has(l.sourceId));
      if (contractBySource.size === 0 || layerOrderMode !== "contract") return filtered;
      return filtered
        .slice()
        .sort((a, b) => {
          const ap = contractBySource.get(a.sourceId)?.priority ?? Number.MAX_SAFE_INTEGER;
          const bp = contractBySource.get(b.sourceId)?.priority ?? Number.MAX_SAFE_INTEGER;
          return ap - bp || a.title.localeCompare(b.title);
        });
    });
  }, [layerOrderMode, layerStackContract, sourceData]);

  useEffect(() => {
    const map = mapRef.current;
    const dynamicGroup = dynamicLayerGroupRef.current;
    const analyticGroup = rasterAnalyticLayerGroupRef.current;
    if (!map || !dynamicGroup || !analyticGroup) return;
    dynamicGroup.clearLayers();
    analyticGroup.clearLayers();

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
        const palette = layer.heatmap.palette || src.heatmapHint?.palette || "rainbow";
        const clipMin = src.heatmapHint?.minVisibleRender;
        const clipMax = src.heatmapHint?.maxVisibleRender;
        if (lockedRenderer && src.surfaceGrid) {
          const g = src.surfaceGrid;
          const vals = g.values
            .map((v) => (typeof v === "number" ? applyTransform(v, layer.heatmap.transform) : null))
            .filter((v): v is number => v !== null && Number.isFinite(v));
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
                  const tv = applyTransform(v, layer.heatmap.transform);
                  if (tv === null) {
                    img.data[idx + 3] = 0;
                    continue;
                  }
                  const t = norm(tv, mm.min, mm.max);
                  const { r, g: gg, b } = paletteRgb(palette, t);
                  img.data[idx] = r;
                  img.data[idx + 1] = gg;
                  img.data[idx + 2] = b;
                  img.data[idx + 3] = 255;
                }
              }
              ctx.putImageData(img, 0, 0);
              createBoundedImageLayer(
                canvas.toDataURL("image/png"),
                { south: g.ymin, west: g.xmin, north: g.ymax, east: g.xmax },
                {
                  opacity: layer.heatmap.opacity * layer.opacity,
                  pane: "mineeye-raster-analytic",
                }
              ).addTo(analyticGroup);
            }
          }
        } else {
          const samples = src.points
            .map((p) => {
              const v = p.measures[layer.heatmap.measure];
              const tv = Number.isFinite(v) ? applyTransform(v as number, layer.heatmap.transform) : null;
              return tv !== null ? { ...p, value: tv } : null;
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
                  createBoundedImageLayer(
                    canvas.toDataURL("image/png"),
                    { south: latMin, west: lonMin, north: latMax, east: lonMax },
                    {
                      opacity: layer.heatmap.opacity * layer.opacity,
                      pane: "mineeye-raster-analytic",
                    }
                  ).addTo(analyticGroup);
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
            .addTo(dynamicGroup);
        });
      }

      const colorVals =
        layer.points.colorMode === "measure" && layer.points.colorMeasure
          ? src.points
              .map((p) => p.measures[layer.points.colorMeasure])
              .map((v) => (typeof v === "number" ? applyTransform(v, layer.points.colorTransform) : null))
              .filter((v): v is number => v !== null && Number.isFinite(v))
          : [];
      const sizeVals =
        layer.points.sizeMode === "measure" && layer.points.sizeMeasure
          ? src.points
              .map((p) => p.measures[layer.points.sizeMeasure])
              .map((v) => (typeof v === "number" ? applyTransform(v, layer.points.sizeTransform) : null))
              .filter((v): v is number => v !== null && Number.isFinite(v))
          : [];
      const cmm = valueMinMax(colorVals);
      const smm = valueMinMax(sizeVals);

      if (!layer.points.enabled) {
        return;
      }
      src.points.forEach((pt) => {
        const rawColorValue = pt.measures[layer.points.colorMeasure];
        const colorValue =
          typeof rawColorValue === "number"
            ? applyTransform(rawColorValue, layer.points.colorTransform)
            : null;
        const color =
          layer.points.colorMode === "measure" &&
          cmm &&
          colorValue !== null &&
          Number.isFinite(colorValue)
            ? (() => {
                const { r, g, b } = paletteRgb(
                  layer.points.colorPalette || "turbo",
                  norm(colorValue, cmm.min, cmm.max)
                );
                return `rgb(${r},${g},${b})`;
              })()
            : layer.points.colorMode === "categorical" &&
                typeof rawColorValue === "number" &&
                Number.isFinite(rawColorValue)
              ? categoricalColor(rawColorValue, layer.points.colorPalette)
              : layer.points.color;
        const rawSizeValue = pt.measures[layer.points.sizeMeasure];
        const sizeValue =
          typeof rawSizeValue === "number"
            ? applyTransform(rawSizeValue, layer.points.sizeTransform)
            : null;
        const size =
          layer.points.sizeMode === "measure" &&
          smm &&
          sizeValue !== null &&
          Number.isFinite(sizeValue)
            ? layer.points.sizeMinPx +
              (layer.points.sizeMaxPx - layer.points.sizeMinPx) *
                norm(sizeValue, smm.min, smm.max)
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
            .addTo(dynamicGroup);
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
          .addTo(dynamicGroup);
      });
    });
  }, [layers, sourceById]);

  const onDropLayer = (targetId: string) => {
    if (!canDragLayers) return;
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
            <button type="button" onClick={onClearViewer} className="me-btn">
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
        <aside style={{ position: "absolute", top: 12, right: 12, zIndex: 900, width: panelCollapsed ? 40 : 320, maxHeight: "calc(100% - 24px)", display: "flex", flexDirection: "column", background: "rgba(13,17,23,0.96)", border: "1px solid #30363d", borderRadius: 12, boxShadow: "0 4px 24px rgba(0,0,0,0.5)", overflow: "hidden" }}>
          {panelCollapsed ? (
            <button type="button" title="Expand panel" onClick={() => setPanelCollapsed(false)} style={{ flex: 1, background: "transparent", border: "none", color: "#6e7681", cursor: "pointer", fontSize: 16, padding: "8px 0" }}>›</button>
          ) : (
            <>
              <div className="me-panel-header">
                <span className="me-panel-header-title">Layers</span>
                <button type="button" title="Collapse" className="me-panel-collapse-btn" onClick={() => setPanelCollapsed(true)}>‹</button>
              </div>
              <div className="me-panel-body me-panel">
                <div className="me-section-note">
                  Base map: Esri satellite
                </div>
                {imageryContract ? (
                  <div className="me-layer-card">
                    <div className="me-layer-card-header">
                      <span className="me-layer-dot" style={{ background: "#facc15", opacity: baseVisible ? 1 : 0.25 }} />
                      <button
                        type="button"
                        className="me-layer-vis-btn"
                        title={baseVisible ? "Hide raster layer" : "Show raster layer"}
                        style={{ color: baseVisible ? "#e6edf3" : "#484f58" }}
                        onClick={() => setBaseVisible((v) => !v)}
                      >
                        {baseVisible ? "●" : "○"}
                      </button>
                      <div
                        style={{
                          flex: 1,
                          minWidth: 0,
                          fontSize: 12,
                          fontWeight: 600,
                          color: baseVisible ? "#e6edf3" : "#6e7681",
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {imageryContract.provider_label ?? "Raster drape"}
                      </div>
                    </div>
                    <div className="me-layer-card-body">
                      <div className="me-section-note">
                        {imageryContract.attribution ?? "Cached raster tile layer"}
                      </div>
                      <label>
                        <span>Raster opacity ({Math.round(baseOpacity * 100)}%)</span>
                        <input
                          type="range"
                          min={0}
                          max={100}
                          value={Math.round(baseOpacity * 100)}
                          onChange={(e) =>
                            setBaseOpacity(
                              Math.max(0, Math.min(1, Number(e.target.value) / 100))
                            )
                          }
                        />
                      </label>
                    </div>
                  </div>
                ) : null}
                {hasLayerStackContract ? (
                  <label>
                    Order
                    <select
                      value={layerOrderMode}
                      onChange={(e) => setLayerOrderMode(e.target.value as LayerOrderMode)}
                    >
                      <option value="contract">Contract</option>
                      <option value="override">Local override</option>
                    </select>
                    {layerOrderMode === "contract" ? (
                      <span className="me-section-note">(drag locked)</span>
                    ) : null}
                  </label>
                ) : null}
                <div style={{ display: "grid", gap: 6 }}>
              {layers.map((layer) => {
                const src = sourceById.get(layer.sourceId);
                const measures = src?.measureNames ?? [];
                const heatmapMeasures = allowedHeatmapMeasuresForSource(src);
                const lockedRenderer = src?.displayContract?.renderer === "heat_surface";
                // Derive dot colour: semantic type from edge takes priority, then content heuristic
                const srcNodeId = layer.sourceId.split(":")[0] ?? "";
                const dotColor = nodeSemanticColorMap.get(srcNodeId)
                  ?? (src?.surfaceGrid ? "#4ade80"
                    : (src?.lines?.length ?? 0) > 0 ? "#38bdf8"
                    : lockedRenderer ? "#facc15"
                    : src?.heatmapHint?.measure ? "#f97316"
                    : "#38bdf8");
                return (
                  <div
                    key={layer.id}
                    onDragOver={(e) => e.preventDefault()}
                    onDrop={() => onDropLayer(layer.id)}
                    className="me-layer-card"
                  >
                    <div className="me-layer-card-header">
                      <span
                        className="me-drag-handle"
                        style={{ opacity: canDragLayers ? 0.5 : 0.15, cursor: canDragLayers ? "grab" : "default" }}
                        draggable={canDragLayers}
                        onDragStart={(e) => {
                          if (!canDragLayers) return;
                          e.stopPropagation();
                          setDraggingId(layer.id);
                        }}
                        onDragEnd={() => setDraggingId(null)}
                      >
                        ⠿
                      </span>
                      <span className="me-layer-dot" style={{ background: dotColor, opacity: layer.visible ? 1 : 0.25 }} />
                      <button
                        type="button"
                        className="me-layer-vis-btn"
                        title={layer.visible ? "Hide layer" : "Show layer"}
                        style={{ color: layer.visible ? "#e6edf3" : "#484f58" }}
                        onClick={() =>
                          setLayers((prev) =>
                            prev.map((l) =>
                              l.id === layer.id ? { ...l, visible: !l.visible } : l
                            )
                          )
                        }
                      >
                        {layer.visible ? "●" : "○"}
                      </button>
                      <div style={{ flex: 1, minWidth: 0, fontSize: 12, fontWeight: 600, color: layer.visible ? "#e6edf3" : "#6e7681", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {layer.title}
                      </div>
                      {measures.length > 0 && (
                        <span className="me-section-note" style={{ flexShrink: 0 }}>m:{measures.length}</span>
                      )}
                      <button
                        type="button"
                        className="me-layer-expand-btn"
                        onClick={() =>
                          setLayers((prev) =>
                            prev.map((l) =>
                              l.id === layer.id ? { ...l, expanded: !l.expanded } : l
                            )
                          )
                        }
                      >
                        {layer.expanded ? "▾" : "›"}
                      </button>
                    </div>
                    {layer.expanded && (
                      <div className="me-layer-card-body">
                        <label>
                          Source
                          <select
                            value={layer.sourceId}
                            onChange={(e) =>
                              setLayers((prev) =>
                                prev.map((l) =>
                                  l.id === layer.id ? { ...l, sourceId: e.target.value } : l
                                )
                              )
                            }
                          >
                            {sourceData.map((s) => (
                              <option key={s.id} value={s.id}>
                                {s.label}
                              </option>
                            ))}
                          </select>
                        </label>
                        <label>
                          Layer opacity ({Math.round(layer.opacity * 100)}%)
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

                        <div className="me-style-subheader">Points</div>
                        <label>
                          <span>Enable points</span>
                          <input
                            type="checkbox"
                            checked={layer.points.enabled}
                            onChange={(e) =>
                              setLayers((prev) =>
                                prev.map((l) =>
                                  l.id === layer.id
                                    ? { ...l, points: { ...l.points, enabled: e.target.checked } }
                                    : l
                                )
                              )
                            }
                          />
                        </label>
                        <label>
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
                          >
                            <option value="circle">Circle</option>
                            <option value="square">Square</option>
                            <option value="diamond">Diamond</option>
                          </select>
                        </label>
                        <div className="me-col2">
                          <label>
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
                              >
                              <option value="fixed">Fixed</option>
                              <option value="measure">Measure</option>
                              <option value="categorical">Categorical</option>
                            </select>
                          </label>
                          {layer.points.colorMode === "fixed" ? (
                            <label>
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
                            <label>
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
                        {layer.points.colorMode !== "fixed" && (
                          <div className="me-col2">
                            {layer.points.colorMode === "measure" ? (
                              <label>
                                <span>Transform</span>
                                <select
                                  value={layer.points.colorTransform}
                                  disabled={lockedRenderer}
                                  onChange={(e) =>
                                    setLayers((prev) =>
                                      prev.map((l) =>
                                        l.id === layer.id
                                          ? {
                                              ...l,
                                              points: {
                                                ...l.points,
                                                colorTransform: e.target.value as ValueTransform,
                                              },
                                            }
                                          : l
                                      )
                                    )
                                  }
                                      >
                                  <option value="linear">Linear</option>
                                  <option value="log10">Log10</option>
                                  <option value="ln">Natural log</option>
                                </select>
                              </label>
                            ) : null}
                            <label>
                              <span>Palette</span>
                              <select
                                value={layer.points.colorPalette}
                                disabled={lockedRenderer}
                                onChange={(e) =>
                                  setLayers((prev) =>
                                    prev.map((l) =>
                                      l.id === layer.id
                                        ? {
                                            ...l,
                                            points: { ...l.points, colorPalette: e.target.value },
                                          }
                                        : l
                                    )
                                  )
                                }
                                  >
                                <option value="turbo">Turbo</option>
                                <option value="inferno">Inferno</option>
                                <option value="viridis">Viridis</option>
                                <option value="rainbow">Rainbow</option>
                              </select>
                            </label>
                          </div>
                        )}

                        <div className="me-col2">
                          <label>
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
                              >
                              <option value="fixed">Fixed</option>
                              <option value="measure">Measure</option>
                            </select>
                          </label>
                          {layer.points.sizeMode === "fixed" ? (
                            <label>
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
                            <label>
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
                          <label>
                            <span>Size transform</span>
                            <select
                              value={layer.points.sizeTransform}
                              disabled={lockedRenderer}
                              onChange={(e) =>
                                setLayers((prev) =>
                                  prev.map((l) =>
                                    l.id === layer.id
                                      ? {
                                          ...l,
                                          points: {
                                            ...l.points,
                                            sizeTransform: e.target.value as ValueTransform,
                                          },
                                        }
                                      : l
                                  )
                                )
                              }
                              >
                              <option value="linear">Linear</option>
                              <option value="log10">Log10</option>
                              <option value="ln">Natural log</option>
                            </select>
                          </label>
                        )}
                        {layer.points.sizeMode === "measure" && (
                          <label>
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

                        <div className="me-style-subheader">Heatmap</div>
                        <label>
                          <input
                            type="checkbox"
                            checked={layer.heatmap.enabled}
                            disabled={heatmapMeasures.length === 0 || lockedRenderer}
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
                            <label>
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
                                  >
                                {heatmapMeasures.map((m) => (
                                  <option key={m} value={m}>
                                    {m}
                                  </option>
                                ))}
                              </select>
                            </label>
                            {!lockedRenderer && (
                              <div className="me-col2">
                                <label>
                                  <span>Transform</span>
                                  <select
                                    value={layer.heatmap.transform}
                                    onChange={(e) =>
                                      setLayers((prev) =>
                                        prev.map((l) =>
                                          l.id === layer.id
                                            ? {
                                                ...l,
                                                heatmap: {
                                                  ...l.heatmap,
                                                  transform: e.target.value as ValueTransform,
                                                },
                                              }
                                            : l
                                        )
                                      )
                                    }
                                          >
                                    <option value="linear">Linear</option>
                                    <option value="log10">Log10</option>
                                    <option value="ln">Natural log</option>
                                  </select>
                                </label>
                                <label>
                                  <span>Palette</span>
                                  <select
                                    value={layer.heatmap.palette}
                                    onChange={(e) =>
                                      setLayers((prev) =>
                                        prev.map((l) =>
                                          l.id === layer.id
                                            ? {
                                                ...l,
                                                heatmap: { ...l.heatmap, palette: e.target.value },
                                              }
                                            : l
                                        )
                                      )
                                    }
                                          >
                                    <option value="rainbow">Rainbow</option>
                                    <option value="turbo">Turbo</option>
                                    <option value="inferno">Inferno</option>
                                    <option value="viridis">Viridis</option>
                                  </select>
                                </label>
                              </div>
                            )}
                            <label>
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
                                <label>
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
                                <label>
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
              </div>
            </>
          )}
        </aside>
      </div>
    </div>
  );
}
