import { useEffect, useMemo, useRef, useState } from "react";
import "cesium/Build/Cesium/Widgets/widgets.css";
import {
  fetchGraph,
  fetchViewerManifest,
  patchNodeParams,
  type ApiEdge,
  type ArtifactEntry,
  type ViewerManifestLayer,
} from "./graphApi";
import { isSceneViewInputSemantic } from "./portTaxonomy";
import { lonLatFromProjectedAsync } from "./spatialReproject";

type Props = {
  graphId: string | null;
  activeBranchId?: string | null;
  active?: boolean;
  edges: ApiEdge[];
  artifacts: ArtifactEntry[];
  viewerNodeId: string | null;
  onClearViewer?: () => void;
};

type Point3D = {
  x: number;
  y: number;
  z: number;
  measures?: Record<string, number>;
};

type Segment3D = {
  from: [number, number, number];
  to: [number, number, number];
  measures?: Record<string, number>;
};

type TerrainPoint = {
  x: number;
  y: number;
  z: number;
};

type SceneData = {
  traces: Segment3D[];
  drillSegments: Segment3D[];
  contourSegments: Segment3D[];
  assayPoints: Point3D[];
  terrainPoints: TerrainPoint[];
  measureCandidates: string[];
  totalArtifacts: number;
};

type SceneUiState = {
  showTraces: boolean;
  showSegments: boolean;
  showContours: boolean;
  showSamples: boolean;
  showTerrain: boolean;
  selectedMeasure: string;
  palette: "inferno" | "viridis" | "turbo" | "red_blue";
  clampLowPct: number;
  clampHighPct: number;
  radiusScale: number;
  traceWidth: number;
  segmentWidth: number;
  contourWidth: number;
  sampleSize: number;
  panelCollapsed: boolean;
};

const DEFAULT_UI: SceneUiState = {
  showTraces: true,
  showSegments: true,
  showContours: true,
  showSamples: true,
  showTerrain: false,
  selectedMeasure: "",
  palette: "inferno",
  clampLowPct: 2,
  clampHighPct: 98,
  radiusScale: 1,
  traceWidth: 2,
  segmentWidth: 4,
  contourWidth: 1,
  sampleSize: 7,
  panelCollapsed: false,
};

function n(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() !== "") {
    const x = Number(v);
    return Number.isFinite(x) ? x : null;
  }
  return null;
}

function upstreamSourcesForViewer(edges: ApiEdge[], viewerId: string): string[] {
  return edges
    .filter((e) => e.to_node === viewerId && isSceneViewInputSemantic(e.semantic_type))
    .map((e) => e.from_node);
}

function parseAssayMeasures(raw: unknown): Record<string, number> {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return {};
  const out: Record<string, number> = {};
  for (const [k, v] of Object.entries(raw as Record<string, unknown>)) {
    const x = n(v);
    if (x !== null) out[k] = x;
  }
  return out;
}

function parseSegmentMeasuresFromAssays(raw: unknown): Record<string, number> {
  if (!Array.isArray(raw)) return {};
  const sums = new Map<string, { sum: number; count: number }>();
  for (const row of raw) {
    if (!row || typeof row !== "object" || Array.isArray(row)) continue;
    const attrs = (row as Record<string, unknown>).attributes;
    const parsed = parseAssayMeasures(attrs);
    for (const [k, v] of Object.entries(parsed)) {
      const cur = sums.get(k) ?? { sum: 0, count: 0 };
      cur.sum += v;
      cur.count += 1;
      sums.set(k, cur);
    }
  }
  const out: Record<string, number> = {};
  for (const [k, v] of sums.entries()) {
    if (v.count > 0) out[k] = v.sum / v.count;
  }
  return out;
}

function parseSceneJson(
  text: string,
  manifestLayer: ViewerManifestLayer | undefined
): Omit<SceneData, "totalArtifacts"> {
  let root: unknown;
  try {
    root = JSON.parse(text) as unknown;
  } catch {
    return {
      traces: [],
      drillSegments: [],
      contourSegments: [],
      assayPoints: [],
      terrainPoints: [],
      measureCandidates: [],
    };
  }

  const traces: Segment3D[] = [];
  const drillSegments: Segment3D[] = [];
  const contourSegments: Segment3D[] = [];
  const assayPoints: Point3D[] = [];
  const terrainPoints: TerrainPoint[] = [];
  const measures = new Set<string>();

  const lowerKey = manifestLayer?.artifact_key.toLowerCase() ?? "";
  const sourceKind = manifestLayer?.source_node_kind ?? "";

  const maybePushSegment = (
    xf: unknown,
    yf: unknown,
    zf: unknown,
    xt: unknown,
    yt: unknown,
    zt: unknown,
    target: Segment3D[],
    segMeasures?: Record<string, number>
  ) => {
    const x0 = n(xf);
    const y0 = n(yf);
    const z0 = n(zf) ?? 0;
    const x1 = n(xt);
    const y1 = n(yt);
    const z1 = n(zt) ?? 0;
    if (x0 === null || y0 === null || x1 === null || y1 === null) return;
    target.push({
      from: [x0, y0, z0],
      to: [x1, y1, z1],
      measures: segMeasures,
    });
    if (segMeasures) {
      for (const k of Object.keys(segMeasures)) measures.add(k);
    }
  };

  const maybePushPoint = (xv: unknown, yv: unknown, zv: unknown, rawMeasures?: unknown) => {
    const x = n(xv);
    const y = n(yv);
    const z = n(zv) ?? 0;
    if (x === null || y === null) return;
    const parsedMeasures = parseAssayMeasures(rawMeasures);
    for (const k of Object.keys(parsedMeasures)) measures.add(k);
    assayPoints.push({
      x,
      y,
      z,
      measures: Object.keys(parsedMeasures).length ? parsedMeasures : undefined,
    });
  };

  if (Array.isArray(root)) {
    for (const row of root) {
      if (!row || typeof row !== "object" || Array.isArray(row)) continue;
      const r = row as Record<string, unknown>;
      if ("x_from" in r && "y_from" in r && "x_to" in r && "y_to" in r) {
        maybePushSegment(
          r.x_from,
          r.y_from,
          r.z_from,
          r.x_to,
          r.y_to,
          r.z_to,
          sourceKind === "desurvey_trajectory" ? traces : drillSegments
        );
      }
    }
  } else if (root && typeof root === "object") {
    const obj = root as Record<string, unknown>;

    const segs = obj.segments;
    if (Array.isArray(segs)) {
      for (const row of segs) {
        if (!row || typeof row !== "object" || Array.isArray(row)) continue;
        const r = row as Record<string, unknown>;
        const from_xyz = Array.isArray(r.from_xyz) ? r.from_xyz : null;
        const to_xyz = Array.isArray(r.to_xyz) ? r.to_xyz : null;
        const segMeasures = parseSegmentMeasuresFromAssays(r.assays);
        if (from_xyz && to_xyz && from_xyz.length >= 3 && to_xyz.length >= 3) {
          maybePushSegment(
            from_xyz[0],
            from_xyz[1],
            from_xyz[2],
            to_xyz[0],
            to_xyz[1],
            to_xyz[2],
            drillSegments,
            segMeasures
          );
        } else {
          maybePushSegment(
            r.x_from,
            r.y_from,
            r.z_from,
            r.x_to,
            r.y_to,
            r.z_to,
            drillSegments,
            segMeasures
          );
        }
      }
    }

    const assayPts = obj.assay_points;
    if (Array.isArray(assayPts)) {
      for (const row of assayPts) {
        if (!row || typeof row !== "object" || Array.isArray(row)) continue;
        const r = row as Record<string, unknown>;
        maybePushPoint(r.x, r.y, r.z, r.attributes);
      }
    }

    const pts = obj.points;
    if (Array.isArray(pts)) {
      for (const row of pts) {
        if (!row || typeof row !== "object" || Array.isArray(row)) continue;
        const r = row as Record<string, unknown>;
        maybePushPoint(r.x, r.y, r.z, r.attributes);
      }
    }

    const mcs = obj.measure_candidates;
    if (Array.isArray(mcs)) {
      for (const m of mcs) {
        if (typeof m === "string" && m.trim().length) measures.add(m.trim());
      }
    }

    const sg = obj.surface_grid;
    if (sg && typeof sg === "object" && !Array.isArray(sg)) {
      const sgo = sg as Record<string, unknown>;
      const nx = n(sgo.nx);
      const ny = n(sgo.ny);
      const xmin = n(sgo.xmin);
      const xmax = n(sgo.xmax);
      const ymin = n(sgo.ymin);
      const ymax = n(sgo.ymax);
      const vals = Array.isArray(sgo.values) ? sgo.values : [];
      if (
        nx !== null &&
        ny !== null &&
        xmin !== null &&
        xmax !== null &&
        ymin !== null &&
        ymax !== null &&
        vals.length === Math.trunc(nx) * Math.trunc(ny)
      ) {
        const nxi = Math.max(2, Math.trunc(nx));
        const nyi = Math.max(2, Math.trunc(ny));
        const stepX = (xmax - xmin) / (nxi - 1);
        const stepY = (ymax - ymin) / (nyi - 1);
        const stride = Math.max(1, Math.floor(Math.max(nxi, nyi) / 32));
        for (let iy = 0; iy < nyi; iy += stride) {
          for (let ix = 0; ix < nxi; ix += stride) {
            const idx = iy * nxi + ix;
            const zv = n(vals[idx]);
            if (zv === null) continue;
            terrainPoints.push({
              x: xmin + ix * stepX,
              y: ymin + iy * stepY,
              z: zv,
            });
          }
        }
      }
    }
  }

  if (lowerKey.includes("trajectory")) {
    for (const seg of drillSegments.splice(0)) traces.push(seg);
  }
  if (lowerKey.endsWith(".geojson")) {
    const obj = root as Record<string, unknown>;
    const features = Array.isArray(obj.features) ? obj.features : [];
    for (const f of features) {
      if (!f || typeof f !== "object" || Array.isArray(f)) continue;
      const ff = f as Record<string, unknown>;
      const g = ff.geometry;
      if (!g || typeof g !== "object" || Array.isArray(g)) continue;
      const gg = g as Record<string, unknown>;
      if (gg.type !== "LineString") continue;
      const coords = Array.isArray(gg.coordinates) ? gg.coordinates : [];
      if (coords.length < 2) continue;
      const p0 = coords[0];
      const p1 = coords[coords.length - 1];
      if (!Array.isArray(p0) || !Array.isArray(p1) || p0.length < 2 || p1.length < 2) continue;
      const z0 = p0.length >= 3 ? n(p0[2]) ?? 0 : 0;
      const z1 = p1.length >= 3 ? n(p1[2]) ?? z0 : z0;
      const x0 = n(p0[0]);
      const y0 = n(p0[1]);
      const x1 = n(p1[0]);
      const y1 = n(p1[1]);
      if (x0 === null || y0 === null || x1 === null || y1 === null) continue;
      contourSegments.push({
        from: [x0, y0, z0],
        to: [x1, y1, z1],
      });
    }
  }

  return {
    traces,
    drillSegments,
    contourSegments,
    assayPoints,
    terrainPoints,
    measureCandidates: [...measures].sort(),
  };
}

function percentile(sorted: number[], pct: number): number {
  if (sorted.length === 0) return 0;
  if (sorted.length === 1) return sorted[0];
  const p = Math.max(0, Math.min(100, pct));
  const idx = (p / 100) * (sorted.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  const t = idx - lo;
  return sorted[lo] * (1 - t) + sorted[hi] * t;
}

function samplePaletteRgb(t01: number, palette: SceneUiState["palette"]): [number, number, number] {
  const t = Math.max(0, Math.min(1, t01));
  if (palette === "red_blue") {
    const r = Math.round(255 * t);
    const b = Math.round(255 * (1 - t));
    return [r, 40, b];
  }
  if (palette === "inferno") {
    const stops: Array<[number, number, number]> = [
      [0, 0, 4],
      [51, 16, 88],
      [136, 34, 106],
      [203, 71, 119],
      [248, 149, 64],
      [252, 255, 164],
    ];
    const s = t * (stops.length - 1);
    const i = Math.floor(s);
    const j = Math.min(stops.length - 1, i + 1);
    const u = s - i;
    const a = stops[i];
    const b = stops[j];
    return [
      Math.round(a[0] * (1 - u) + b[0] * u),
      Math.round(a[1] * (1 - u) + b[1] * u),
      Math.round(a[2] * (1 - u) + b[2] * u),
    ];
  }
  if (palette === "viridis") {
    const stops: Array<[number, number, number]> = [
      [68, 1, 84],
      [59, 82, 139],
      [33, 145, 140],
      [94, 201, 97],
      [253, 231, 36],
    ];
    const s = t * (stops.length - 1);
    const i = Math.floor(s);
    const j = Math.min(stops.length - 1, i + 1);
    const u = s - i;
    const a = stops[i];
    const b = stops[j];
    return [
      Math.round(a[0] * (1 - u) + b[0] * u),
      Math.round(a[1] * (1 - u) + b[1] * u),
      Math.round(a[2] * (1 - u) + b[2] * u),
    ];
  }
  const stops: Array<[number, number, number]> = [
    [48, 18, 59],
    [50, 21, 110],
    [32, 73, 156],
    [18, 120, 142],
    [59, 173, 112],
    [171, 220, 50],
    [253, 231, 37],
  ];
  const s = t * (stops.length - 1);
  const i = Math.floor(s);
  const j = Math.min(stops.length - 1, i + 1);
  const u = s - i;
  const a = stops[i];
  const b = stops[j];
  return [
    Math.round(a[0] * (1 - u) + b[0] * u),
    Math.round(a[1] * (1 - u) + b[1] * u),
    Math.round(a[2] * (1 - u) + b[2] * u),
  ];
}

async function toLonLat(
  epsg: number,
  x: number,
  y: number
): Promise<[number, number] | null> {
  const ll = await lonLatFromProjectedAsync(epsg, x, y);
  if (!ll) return null;
  if (!Number.isFinite(ll[0]) || !Number.isFinite(ll[1])) return null;
  return ll;
}

export function Map3DPanel({
  graphId,
  activeBranchId,
  active = true,
  edges,
  artifacts,
  viewerNodeId,
  onClearViewer,
}: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const viewerRef = useRef<any>(null);
  const cesiumRef = useRef<any>(null);
  const [status, setStatus] = useState("Loading 3D viewer…");
  const [sceneData, setSceneData] = useState<SceneData>({
    traces: [],
    drillSegments: [],
    contourSegments: [],
    assayPoints: [],
    terrainPoints: [],
    measureCandidates: [],
    totalArtifacts: 0,
  });
  const [manifestArtifacts, setManifestArtifacts] = useState<ArtifactEntry[]>([]);
  const [manifestLayers, setManifestLayers] = useState<ViewerManifestLayer[]>([]);
  const [ui, setUi] = useState<SceneUiState>(DEFAULT_UI);
  const [configHydrated, setConfigHydrated] = useState(false);
  const [projectEpsg, setProjectEpsg] = useState(4326);
  const [cameraMoved, setCameraMoved] = useState(false);
  const savedRef = useRef<string>("");
  const saveTidRef = useRef<number | null>(null);
  const renderedEntityIdsRef = useRef<string[]>([]);
  const renderedHashRef = useRef<string>("");

  const inputLinks = useMemo(
    () => (viewerNodeId ? upstreamSourcesForViewer(edges, viewerNodeId) : []),
    [edges, viewerNodeId]
  );

  useEffect(() => {
    if (!graphId) return;
    let cancelled = false;
    void (async () => {
      try {
        const g = await fetchGraph(graphId);
        const epsg =
          g.project_crs &&
          typeof g.project_crs === "object" &&
          typeof g.project_crs.epsg === "number" &&
          Number.isFinite(g.project_crs.epsg)
            ? g.project_crs.epsg
            : 4326;
        if (!cancelled) setProjectEpsg(epsg);
      } catch {
        if (!cancelled) setProjectEpsg(4326);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [graphId]);

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
  }, [graphId, viewerNodeId, artifacts]);

  useEffect(() => {
    if (!graphId || !viewerNodeId) return;
    let cancelled = false;
    void (async () => {
      try {
        const g = await fetchGraph(graphId);
        if (cancelled) return;
        const viewer = g.nodes.find((n) => n.id === viewerNodeId);
        const uiRaw =
          viewer?.config?.params?.ui &&
          typeof viewer.config.params.ui === "object" &&
          !Array.isArray(viewer.config.params.ui)
            ? (viewer.config.params.ui as Record<string, unknown>)
            : {};
        setUi({
          showTraces: uiRaw.show_traces !== false,
          showSegments: uiRaw.show_segments !== false,
          showContours: uiRaw.show_contours !== false,
          showSamples: uiRaw.show_samples !== false,
          showTerrain: uiRaw.show_terrain === true,
          selectedMeasure:
            typeof uiRaw.selected_measure === "string" ? uiRaw.selected_measure : "",
          palette:
            uiRaw.palette === "viridis" ||
            uiRaw.palette === "turbo" ||
            uiRaw.palette === "red_blue" ||
            uiRaw.palette === "inferno"
              ? uiRaw.palette
              : "inferno",
          clampLowPct:
            typeof uiRaw.clamp_low_pct === "number" ? uiRaw.clamp_low_pct : DEFAULT_UI.clampLowPct,
          clampHighPct:
            typeof uiRaw.clamp_high_pct === "number"
              ? uiRaw.clamp_high_pct
              : DEFAULT_UI.clampHighPct,
          radiusScale:
            typeof uiRaw.radius_scale === "number" ? uiRaw.radius_scale : DEFAULT_UI.radiusScale,
          traceWidth:
            typeof uiRaw.trace_width === "number" ? uiRaw.trace_width : DEFAULT_UI.traceWidth,
          segmentWidth:
            typeof uiRaw.segment_width === "number"
              ? uiRaw.segment_width
              : DEFAULT_UI.segmentWidth,
          contourWidth:
            typeof uiRaw.contour_width === "number"
              ? uiRaw.contour_width
              : DEFAULT_UI.contourWidth,
          sampleSize:
            typeof uiRaw.sample_size === "number" ? uiRaw.sample_size : DEFAULT_UI.sampleSize,
          panelCollapsed: uiRaw.panel_collapsed === true,
        });
      } catch {
        setUi(DEFAULT_UI);
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
    const payload = JSON.stringify(ui);
    if (savedRef.current === payload) return;
    if (saveTidRef.current) window.clearTimeout(saveTidRef.current);
    saveTidRef.current = window.setTimeout(() => {
      const uiPatch: Record<string, unknown> = {
        show_traces: ui.showTraces,
        show_segments: ui.showSegments,
        show_contours: ui.showContours,
        show_samples: ui.showSamples,
        show_terrain: ui.showTerrain,
        selected_measure: ui.selectedMeasure,
        palette: ui.palette,
        clamp_low_pct: ui.clampLowPct,
        clamp_high_pct: ui.clampHighPct,
        radius_scale: ui.radiusScale,
        trace_width: ui.traceWidth,
        segment_width: ui.segmentWidth,
        contour_width: ui.contourWidth,
        sample_size: ui.sampleSize,
        panel_collapsed: ui.panelCollapsed,
      };
      void patchNodeParams(graphId, viewerNodeId, { ui: uiPatch }, { branchId: activeBranchId }).then(
        () => {
          savedRef.current = payload;
        },
        () => {
          /* ignore best-effort persistence */
        }
      );
    }, 350);
    return () => {
      if (saveTidRef.current) window.clearTimeout(saveTidRef.current);
    };
  }, [activeBranchId, configHydrated, graphId, ui, viewerNodeId]);

  useEffect(() => {
    if (!active) return;
    if (!containerRef.current) return;
    if (viewerRef.current) return;
    let cancelled = false;
    void (async () => {
      try {
        (window as unknown as { CESIUM_BASE_URL?: string }).CESIUM_BASE_URL =
          "https://cdn.jsdelivr.net/npm/cesium@latest/Build/Cesium";
        const Cesium = await import("cesium");
        if (cancelled || !containerRef.current) return;
        cesiumRef.current = Cesium;

        const imagery = await Cesium.ArcGisMapServerImageryProvider.fromUrl(
          "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer"
        );
        const terrain =
          typeof Cesium.createWorldTerrainAsync === "function"
            ? await Cesium.createWorldTerrainAsync()
            : Cesium.createWorldTerrain();

        const viewer = new Cesium.Viewer(containerRef.current, {
          animation: false,
          timeline: false,
          geocoder: false,
          homeButton: true,
          sceneModePicker: true,
          navigationHelpButton: false,
          baseLayerPicker: false,
          fullscreenButton: false,
          infoBox: false,
          selectionIndicator: false,
          shouldAnimate: false,
          terrainProvider: terrain,
          imageryProvider: imagery,
        });
        viewer.scene.globe.depthTestAgainstTerrain = true;
        viewer.scene.screenSpaceCameraController.enableCollisionDetection = true;
        viewer.camera.moveStart.addEventListener(() => setCameraMoved(true));
        viewerRef.current = viewer;
        setStatus("3D viewer ready.");
      } catch (e) {
        setStatus(`3D init failed: ${e instanceof Error ? e.message : String(e)}`);
      }
    })();
    return () => {
      cancelled = true;
      const v = viewerRef.current;
      if (v) {
        try {
          v.destroy();
        } catch {
          /* ignore */
        }
        viewerRef.current = null;
      }
    };
  }, [active]);

  useEffect(() => {
    if (!graphId || !viewerNodeId) {
      setSceneData({
        traces: [],
        drillSegments: [],
        contourSegments: [],
        assayPoints: [],
        terrainPoints: [],
        measureCandidates: [],
        totalArtifacts: 0,
      });
      setStatus("No 3D viewer node selected.");
      return;
    }
    let cancelled = false;
    void (async () => {
      const fromManifest = manifestArtifacts.length > 0 ? manifestArtifacts : [];
      const source = fromManifest.length
        ? fromManifest
        : artifacts.filter((a) => inputLinks.includes(a.node_id));
      if (source.length === 0) {
        setSceneData({
          traces: [],
          drillSegments: [],
          contourSegments: [],
          assayPoints: [],
          terrainPoints: [],
          measureCandidates: [],
          totalArtifacts: 0,
        });
        setStatus(
          inputLinks.length
            ? "No upstream 3D artifacts yet. Queue run, run worker, then refresh."
            : "No compatible inputs wired into this 3D viewer."
        );
        return;
      }

      const manifestByArtifact = new Map<string, ViewerManifestLayer>();
      for (const ml of manifestLayers) {
        manifestByArtifact.set(`${ml.artifact_key}:${ml.content_hash}`, ml);
      }

      const allTraces: Segment3D[] = [];
      const allSegs: Segment3D[] = [];
      const allContours: Segment3D[] = [];
      const allPoints: Point3D[] = [];
      const allTerrain: TerrainPoint[] = [];
      const allMeasures = new Set<string>();
      let loaded = 0;

      for (const art of source) {
        try {
          const r = await fetch(art.url);
          if (!r.ok) continue;
          const txt = await r.text();
          const parsed = parseSceneJson(
            txt,
            manifestByArtifact.get(`${art.key}:${art.content_hash}`)
          );
          loaded += 1;
          allTraces.push(...parsed.traces);
          allSegs.push(...parsed.drillSegments);
          allContours.push(...parsed.contourSegments);
          allPoints.push(...parsed.assayPoints);
          allTerrain.push(...parsed.terrainPoints);
          for (const m of parsed.measureCandidates) allMeasures.add(m);
        } catch {
          /* ignore bad artifact */
        }
      }
      if (cancelled) return;
      setSceneData({
        traces: allTraces,
        drillSegments: allSegs,
        contourSegments: allContours,
        assayPoints: allPoints,
        terrainPoints: allTerrain,
        measureCandidates: [...allMeasures].sort(),
        totalArtifacts: loaded,
      });
      setStatus(
        `${allTraces.length + allSegs.length + allContours.length} line segment(s), ${allPoints.length} point(s), ${allTerrain.length} terrain samples from ${loaded} artifact(s).`
      );
    })();
    return () => {
      cancelled = true;
    };
  }, [artifacts, graphId, inputLinks, manifestArtifacts, manifestLayers, viewerNodeId]);

  useEffect(() => {
    if (!ui.selectedMeasure && sceneData.measureCandidates.length > 0) {
      setUi((prev) => ({ ...prev, selectedMeasure: sceneData.measureCandidates[0] }));
    }
  }, [sceneData.measureCandidates, ui.selectedMeasure]);

  useEffect(() => {
    const viewer = viewerRef.current;
    const Cesium = cesiumRef.current;
    if (!viewer || !Cesium || !active) return;

    let cancelled = false;
    void (async () => {
      const prevIds = renderedEntityIdsRef.current.splice(0);
      for (const id of prevIds) {
        try {
          const ent = viewer.entities.getById(id);
          if (ent) viewer.entities.remove(ent);
        } catch {
          /* ignore */
        }
      }

      const allVals: number[] = [];
      const measure = ui.selectedMeasure.trim();
      if (measure) {
        for (const s of sceneData.drillSegments) {
          const v = s.measures?.[measure];
          if (typeof v === "number" && Number.isFinite(v)) allVals.push(v);
        }
        for (const p of sceneData.assayPoints) {
          const v = p.measures?.[measure];
          if (typeof v === "number" && Number.isFinite(v)) allVals.push(v);
        }
      }
      allVals.sort((a, b) => a - b);
      const lo = allVals.length ? percentile(allVals, ui.clampLowPct) : 0;
      const hi = allVals.length ? percentile(allVals, ui.clampHighPct) : 1;
      const den = hi - lo || 1;

      const toColor = (value: number | null, alpha = 1) => {
        if (value === null) return Cesium.Color.fromBytes(88, 166, 255, Math.round(alpha * 255));
        const t = Math.max(0, Math.min(1, (value - lo) / den));
        const [r, g, b] = samplePaletteRgb(t, ui.palette);
        return Cesium.Color.fromBytes(r, g, b, Math.round(alpha * 255));
      };

      const coordsForFit: Array<[number, number, number]> = [];
      let idx = 0;
      const widthScale = Math.max(0.1, ui.radiusScale);

      if (ui.showTraces) {
        for (const s of sceneData.traces) {
          const ll0 = await toLonLat(projectEpsg, s.from[0], s.from[1]);
          const ll1 = await toLonLat(projectEpsg, s.to[0], s.to[1]);
          if (cancelled || !ll0 || !ll1) continue;
          coordsForFit.push([ll0[0], ll0[1], s.from[2]]);
          coordsForFit.push([ll1[0], ll1[1], s.to[2]]);
          const id = `trace-${idx++}`;
          viewer.entities.add({
            id,
            polyline: {
              positions: Cesium.Cartesian3.fromDegreesArrayHeights([
                ll0[0],
                ll0[1],
                s.from[2],
                ll1[0],
                ll1[1],
                s.to[2],
              ]),
              width: Math.max(1, ui.traceWidth * widthScale),
              material: Cesium.Color.fromBytes(88, 166, 255, 230),
              clampToGround: false,
            },
          });
          renderedEntityIdsRef.current.push(id);
        }
      }

      if (ui.showSegments) {
        for (const s of sceneData.drillSegments) {
          const ll0 = await toLonLat(projectEpsg, s.from[0], s.from[1]);
          const ll1 = await toLonLat(projectEpsg, s.to[0], s.to[1]);
          if (cancelled || !ll0 || !ll1) continue;
          coordsForFit.push([ll0[0], ll0[1], s.from[2]]);
          coordsForFit.push([ll1[0], ll1[1], s.to[2]]);
          const mVal =
            measure && s.measures && typeof s.measures[measure] === "number"
              ? (s.measures[measure] as number)
              : null;
          const id = `seg-${idx++}`;
          viewer.entities.add({
            id,
            polyline: {
              positions: Cesium.Cartesian3.fromDegreesArrayHeights([
                ll0[0],
                ll0[1],
                s.from[2],
                ll1[0],
                ll1[1],
                s.to[2],
              ]),
              width: Math.max(1, ui.segmentWidth * widthScale),
              material: toColor(mVal, 0.95),
              clampToGround: false,
            },
          });
          renderedEntityIdsRef.current.push(id);
        }
      }

      if (ui.showContours) {
        for (const s of sceneData.contourSegments) {
          const ll0 = await toLonLat(projectEpsg, s.from[0], s.from[1]);
          const ll1 = await toLonLat(projectEpsg, s.to[0], s.to[1]);
          if (cancelled || !ll0 || !ll1) continue;
          coordsForFit.push([ll0[0], ll0[1], s.from[2]]);
          coordsForFit.push([ll1[0], ll1[1], s.to[2]]);
          const id = `ctr-${idx++}`;
          viewer.entities.add({
            id,
            polyline: {
              positions: Cesium.Cartesian3.fromDegreesArrayHeights([
                ll0[0],
                ll0[1],
                s.from[2],
                ll1[0],
                ll1[1],
                s.to[2],
              ]),
              width: Math.max(1, ui.contourWidth * widthScale),
              material: Cesium.Color.fromBytes(255, 196, 64, 210),
              clampToGround: false,
            },
          });
          renderedEntityIdsRef.current.push(id);
        }
      }

      if (ui.showSamples) {
        for (const p of sceneData.assayPoints) {
          const ll = await toLonLat(projectEpsg, p.x, p.y);
          if (cancelled || !ll) continue;
          coordsForFit.push([ll[0], ll[1], p.z]);
          const mVal =
            measure && p.measures && typeof p.measures[measure] === "number"
              ? (p.measures[measure] as number)
              : null;
          const id = `pt-${idx++}`;
          viewer.entities.add({
            id,
            position: Cesium.Cartesian3.fromDegrees(ll[0], ll[1], p.z),
            point: {
              pixelSize: Math.max(2, ui.sampleSize * widthScale),
              color: toColor(mVal, 0.9),
              outlineColor: Cesium.Color.BLACK.withAlpha(0.65),
              outlineWidth: 1,
              disableDepthTestDistance: Number.POSITIVE_INFINITY,
            },
          });
          renderedEntityIdsRef.current.push(id);
        }
      }

      if (ui.showTerrain) {
        for (const p of sceneData.terrainPoints) {
          const ll = await toLonLat(projectEpsg, p.x, p.y);
          if (cancelled || !ll) continue;
          coordsForFit.push([ll[0], ll[1], p.z]);
          const id = `ter-${idx++}`;
          viewer.entities.add({
            id,
            position: Cesium.Cartesian3.fromDegrees(ll[0], ll[1], p.z),
            point: {
              pixelSize: 2,
              color: Cesium.Color.fromBytes(222, 226, 230, 110),
              disableDepthTestDistance: Number.POSITIVE_INFINITY,
            },
          });
          renderedEntityIdsRef.current.push(id);
        }
      }

      const renderHash = JSON.stringify({
        a: sceneData.totalArtifacts,
        t: sceneData.traces.length,
        s: sceneData.drillSegments.length,
        c: sceneData.contourSegments.length,
        p: sceneData.assayPoints.length,
        g: sceneData.terrainPoints.length,
        m: measure,
      });
      const shouldFit = !cameraMoved && renderedHashRef.current !== renderHash && coordsForFit.length > 0;
      renderedHashRef.current = renderHash;
      if (shouldFit) {
        const lons = coordsForFit.map((c) => c[0]);
        const lats = coordsForFit.map((c) => c[1]);
        const west = Math.min(...lons);
        const east = Math.max(...lons);
        const south = Math.min(...lats);
        const north = Math.max(...lats);
        const rect = Cesium.Rectangle.fromDegrees(west, south, east, north);
        try {
          await viewer.camera.flyTo({
            destination: rect,
            duration: 0.8,
          });
        } catch {
          /* ignore */
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [active, cameraMoved, projectEpsg, sceneData, ui]);

  return (
    <div style={{ position: "relative", width: "100%", height: "100%", background: "#0f1419" }}>
      <div
        style={{
          position: "absolute",
          inset: 0,
          display: "flex",
          flexDirection: "column",
          minHeight: 0,
          minWidth: 0,
        }}
      >
        <div
          style={{
            padding: "8px 10px",
            borderBottom: "1px solid #30363d",
            background: "#161b22",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 10,
            fontSize: 13,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: 10, minWidth: 0 }}>
            <strong>3D scene</strong>
            {viewerNodeId && (
              <span style={{ opacity: 0.75 }}>
                Viewer <code style={{ fontSize: 11 }}>{viewerNodeId.slice(0, 8)}…</code>
              </span>
            )}
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <button type="button" onClick={() => setCameraMoved(false)}>
              Re-fit
            </button>
            {onClearViewer && (
              <button type="button" onClick={onClearViewer}>
                Clear viewer
              </button>
            )}
          </div>
        </div>
        <div style={{ padding: "6px 10px", fontSize: 12, opacity: 0.85, borderBottom: "1px solid #30363d" }}>
          {status}
        </div>
        <div style={{ position: "relative", flex: 1, minHeight: 0 }}>
          <div ref={containerRef} style={{ position: "absolute", inset: 0 }} />
          <aside
            style={{
              position: "absolute",
              top: 12,
              right: 12,
              width: ui.panelCollapsed ? 42 : 320,
              maxHeight: "calc(100% - 24px)",
              overflow: "auto",
              background: "rgba(15,20,25,0.92)",
              border: "1px solid #30363d",
              borderRadius: 10,
              padding: ui.panelCollapsed ? 6 : 10,
            }}
          >
            <button
              type="button"
              title={ui.panelCollapsed ? "Expand panel" : "Collapse panel"}
              onClick={() => setUi((prev) => ({ ...prev, panelCollapsed: !prev.panelCollapsed }))}
              style={{
                width: "100%",
                textAlign: ui.panelCollapsed ? "center" : "right",
                marginBottom: ui.panelCollapsed ? 0 : 8,
                background: "transparent",
                border: "none",
                color: "#8b949e",
                cursor: "pointer",
                fontSize: 16,
              }}
            >
              {ui.panelCollapsed ? "◀" : "▸"}
            </button>
            {!ui.panelCollapsed && (
              <div style={{ display: "grid", gap: 8, fontSize: 12 }}>
                <div style={{ fontWeight: 700 }}>Layers</div>
                <label><input type="checkbox" checked={ui.showTraces} onChange={(e) => setUi((p) => ({ ...p, showTraces: e.target.checked }))} /> Trajectories</label>
                <label><input type="checkbox" checked={ui.showSegments} onChange={(e) => setUi((p) => ({ ...p, showSegments: e.target.checked }))} /> Grade segments</label>
                <label><input type="checkbox" checked={ui.showContours} onChange={(e) => setUi((p) => ({ ...p, showContours: e.target.checked }))} /> Contours / iso lines</label>
                <label><input type="checkbox" checked={ui.showSamples} onChange={(e) => setUi((p) => ({ ...p, showSamples: e.target.checked }))} /> Assay points</label>
                <label><input type="checkbox" checked={ui.showTerrain} onChange={(e) => setUi((p) => ({ ...p, showTerrain: e.target.checked }))} /> Terrain points (from surface grids)</label>

                <div style={{ marginTop: 8, fontWeight: 700 }}>Measure</div>
                <select
                  value={ui.selectedMeasure}
                  onChange={(e) => setUi((p) => ({ ...p, selectedMeasure: e.target.value }))}
                >
                  {sceneData.measureCandidates.length === 0 ? (
                    <option value="">(none)</option>
                  ) : (
                    sceneData.measureCandidates.map((m) => (
                      <option key={m} value={m}>
                        {m}
                      </option>
                    ))
                  )}
                </select>

                <div style={{ marginTop: 8, fontWeight: 700 }}>Color ramp</div>
                <select
                  value={ui.palette}
                  onChange={(e) =>
                    setUi((p) => ({
                      ...p,
                      palette: e.target.value as SceneUiState["palette"],
                    }))
                  }
                >
                  <option value="inferno">Inferno</option>
                  <option value="viridis">Viridis</option>
                  <option value="turbo">Turbo</option>
                  <option value="red_blue">Red ↔ Blue</option>
                </select>

                <label>
                  Clamp low (%)
                  <input
                    type="number"
                    min={0}
                    max={100}
                    value={ui.clampLowPct}
                    onChange={(e) =>
                      setUi((p) => ({ ...p, clampLowPct: Math.max(0, Math.min(100, Number(e.target.value) || 0)) }))
                    }
                  />
                </label>
                <label>
                  Clamp high (%)
                  <input
                    type="number"
                    min={0}
                    max={100}
                    value={ui.clampHighPct}
                    onChange={(e) =>
                      setUi((p) => ({ ...p, clampHighPct: Math.max(0, Math.min(100, Number(e.target.value) || 100)) }))
                    }
                  />
                </label>

                <label>
                  Radius scale
                  <input
                    type="range"
                    min={0.25}
                    max={4}
                    step={0.05}
                    value={ui.radiusScale}
                    onChange={(e) => setUi((p) => ({ ...p, radiusScale: Number(e.target.value) || 1 }))}
                  />
                </label>
                <label>
                  Trace width
                  <input
                    type="range"
                    min={1}
                    max={8}
                    step={1}
                    value={ui.traceWidth}
                    onChange={(e) => setUi((p) => ({ ...p, traceWidth: Number(e.target.value) || 2 }))}
                  />
                </label>
                <label>
                  Segment width
                  <input
                    type="range"
                    min={1}
                    max={12}
                    step={1}
                    value={ui.segmentWidth}
                    onChange={(e) => setUi((p) => ({ ...p, segmentWidth: Number(e.target.value) || 4 }))}
                  />
                </label>
                <label>
                  Contour width
                  <input
                    type="range"
                    min={1}
                    max={8}
                    step={1}
                    value={ui.contourWidth}
                    onChange={(e) => setUi((p) => ({ ...p, contourWidth: Number(e.target.value) || 1 }))}
                  />
                </label>
                <label>
                  Sample size
                  <input
                    type="range"
                    min={2}
                    max={14}
                    step={1}
                    value={ui.sampleSize}
                    onChange={(e) => setUi((p) => ({ ...p, sampleSize: Number(e.target.value) || 7 }))}
                  />
                </label>
              </div>
            )}
          </aside>
        </div>
      </div>
    </div>
  );
}
