import { useEffect, useMemo, useRef, useState } from "react";
import { Canvas } from "@react-three/fiber";
import { TrackballControls } from "@react-three/drei";
import * as THREE from "three";
import {
  api,
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
  hasExplicitZ?: boolean;
  epsg?: number;
  measures?: Record<string, number>;
};

type Segment3D = {
  from: [number, number, number];
  to: [number, number, number];
  epsg?: number;
  radiusM?: number;
  measures?: Record<string, number>;
  hasExplicitZ?: boolean;
  contourLevel?: number;
};

type TerrainPoint = {
  x: number;
  y: number;
  z: number;
  epsg?: number;
};

type TerrainGrid = {
  nx: number;
  ny: number;
  xmin: number;
  xmax: number;
  ymin: number;
  ymax: number;
  values: number[];
  epsg?: number;
};

type TerrainGridPick = {
  grid: TerrainGrid;
  rank: number;
  cells: number;
};

type SceneData = {
  traces: Segment3D[];
  drillSegments: Segment3D[];
  contourSegments: Segment3D[];
  assayPoints: Point3D[];
  terrainPoints: TerrainPoint[];
  terrainGrid: TerrainGrid | null;
  measureCandidates: string[];
  totalArtifacts: number;
};

type ImageryDrapeContract = {
  schema_id: "scene3d.imagery_drape.v1" | "scene3d.tilebroker_response.v1";
  provider_id?: string;
  provider_label?: string;
  attribution?: string;
  image_url?: string;
  image_url_candidates?: string[];
  tile_url_template?: string;
  bounds?: { xmin: number; xmax: number; ymin: number; ymax: number };
  z_mode?: "drape_on_surface" | "flat";
  quality_flags?: string[];
  fingerprint?: string;
};

type LayerStackLayer = {
  kind?: string;
  priority?: number;
  visibility_default?: boolean;
};

type LayerStackContract = {
  schema_id: "scene3d.layer_stack.v1";
  layers?: LayerStackLayer[];
};

type SceneUiState = {
  showTraces: boolean;
  showSegments: boolean;
  showContours: boolean;
  showSamples: boolean;
  showTerrain: boolean;
  showDrape: boolean;
  showBalloons: boolean;
  imageryProvider: ImageryProviderId;
  selectedMeasure: string;
  palette: "inferno" | "viridis" | "turbo" | "red_blue";
  clampLowPct: number;
  clampHighPct: number;
  radiusScale: number;
  traceWidth: number;
  segmentWidth: number;
  sampleSize: number;
  terrainOpacity: number;
  drapeOpacity: number;
  contourColor: string;
  contourOpacity: number;
  contourWidth: number;
  contourIntervalStep: number;
  traceColor: string;
  balloonThresholdPct: number;
  balloonScale: number;
  balloonOpacity: number;
  layerOrder: LayerKey[];
  layerOrderMode: "contract" | "override";
  invertDepth: boolean;
  panelCollapsed: boolean;
};

const LAYER_KEYS = [
  "esri_drape",
  "terrain_points",
  "contours",
  "trajectories",
  "grade_segments",
  "assay_points",
  "high_grade_balloons",
] as const;

type LayerKey = (typeof LAYER_KEYS)[number];

type ImageryProviderId =
  | "esri_world_imagery"
  | "esri_world_topo"
  | "esri_natgeo"
  | "usgs_imagery";

const IMAGERY_PROVIDERS: Record<
  ImageryProviderId,
  { label: string; attribution: string; exportUrl: string; format: "jpg" | "png" }
> = {
  esri_world_imagery: {
    label: "Esri World Imagery",
    attribution: "Esri, Maxar, Earthstar Geographics, and the GIS User Community",
    exportUrl:
      "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/export",
    format: "jpg",
  },
  esri_world_topo: {
    label: "Esri World Topo",
    attribution: "Esri, HERE, Garmin, FAO, NOAA, USGS",
    exportUrl:
      "https://services.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/export",
    format: "jpg",
  },
  esri_natgeo: {
    label: "Esri NatGeo World",
    attribution: "Esri, National Geographic, Garmin, HERE, UNEP-WCMC, USGS, NASA",
    exportUrl:
      "https://services.arcgisonline.com/ArcGIS/rest/services/NatGeo_World_Map/MapServer/export",
    format: "jpg",
  },
  usgs_imagery: {
    label: "USGS Imagery",
    attribution: "USGS National Map",
    exportUrl:
      "https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryOnly/MapServer/export",
    format: "jpg",
  },
};

const DEFAULT_UI: SceneUiState = {
  showTraces: true,
  showSegments: true,
  showContours: true,
  showSamples: true,
  showTerrain: false,
  showDrape: true,
  showBalloons: false,
  imageryProvider: "esri_world_imagery",
  selectedMeasure: "",
  palette: "inferno",
  clampLowPct: 2,
  clampHighPct: 98,
  radiusScale: 1.35,
  traceWidth: 2,
  segmentWidth: 6,
  sampleSize: 7,
  terrainOpacity: 0.9,
  drapeOpacity: 0.95,
  contourColor: "#ffc440",
  contourOpacity: 0.9,
  contourWidth: 1.5,
  contourIntervalStep: 1,
  traceColor: "#58a6ff",
  balloonThresholdPct: 92,
  balloonScale: 1.1,
  balloonOpacity: 0.42,
  layerOrder: [...LAYER_KEYS],
  layerOrderMode: "contract",
  invertDepth: false,
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
      terrainGrid: null,
      measureCandidates: [],
    };
  }

  const traces: Segment3D[] = [];
  const drillSegments: Segment3D[] = [];
  const contourSegments: Segment3D[] = [];
  const assayPoints: Point3D[] = [];
  const terrainPoints: TerrainPoint[] = [];
  let terrainGrid: TerrainGrid | null = null;
  const measures = new Set<string>();

  const lowerKey = manifestLayer?.artifact_key.toLowerCase() ?? "";
  const sourceKind = manifestLayer?.source_node_kind ?? "";
  const sourceKindLower = sourceKind.toLowerCase();
  const displayPointer =
    manifestLayer?.presentation &&
    typeof manifestLayer.presentation === "object" &&
    !Array.isArray(manifestLayer.presentation) &&
    typeof manifestLayer.presentation.display_pointer === "string"
      ? manifestLayer.presentation.display_pointer.toLowerCase()
      : "";
  const treatSurfaceGridAsTerrain =
    displayPointer === "scene3d.terrain" ||
    sourceKindLower === "dem_fetch" ||
    sourceKindLower === "terrain_adjust" ||
    sourceKindLower === "dem_integrate" ||
    sourceKindLower === "xyz_to_surface" ||
    lowerKey.endsWith("/dem_surface.json") ||
    lowerKey.endsWith("/terrain_adjusted.json") ||
    lowerKey.endsWith("/xyz_surface.json");
  let artifactEpsg: number | undefined;

  if (!Array.isArray(root) && root && typeof root === "object") {
    const crs = (root as Record<string, unknown>).crs;
    if (crs && typeof crs === "object" && !Array.isArray(crs)) {
      const e = n((crs as Record<string, unknown>).epsg);
      if (e !== null) artifactEpsg = Math.trunc(e);
    }
  }

  const maybePushSegment = (
    xf: unknown,
    yf: unknown,
    zf: unknown,
    xt: unknown,
    yt: unknown,
    zt: unknown,
    target: Segment3D[],
    segMeasures?: Record<string, number>,
    epsg?: number,
    radiusM?: number
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
      epsg,
      radiusM,
      measures: segMeasures,
    });
    if (segMeasures) {
      for (const k of Object.keys(segMeasures)) measures.add(k);
    }
  };

  const maybePushPoint = (
    xv: unknown,
    yv: unknown,
    zv: unknown,
    rawMeasures?: unknown,
    epsg?: number
  ) => {
    const x = n(xv);
    const y = n(yv);
    const zRaw = n(zv);
    const z = zRaw ?? 0;
    if (x === null || y === null) return;
    const parsedMeasures = parseAssayMeasures(rawMeasures);
    for (const k of Object.keys(parsedMeasures)) measures.add(k);
    assayPoints.push({
      x,
      y,
      z,
      hasExplicitZ: zRaw !== null,
      epsg,
      measures: Object.keys(parsedMeasures).length ? parsedMeasures : undefined,
    });
  };

  if (Array.isArray(root)) {
    for (const row of root) {
      if (!row || typeof row !== "object" || Array.isArray(row)) continue;
      const r = row as Record<string, unknown>;
      if ("x_from" in r && "y_from" in r && "x_to" in r && "y_to" in r) {
        const target =
          displayPointer === "scene3d.trace_polyline"
            ? traces
            : displayPointer === "scene3d.contour_lines"
              ? contourSegments
              : sourceKind === "desurvey_trajectory"
                ? traces
                : drillSegments;
        maybePushSegment(
          r.x_from,
          r.y_from,
          r.z_from,
          r.x_to,
          r.y_to,
          r.z_to,
          target,
          undefined,
          artifactEpsg,
          n(r.radius_m) ?? undefined
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
        const rad = n(r.radius_m) ?? undefined;
        if (from_xyz && to_xyz && from_xyz.length >= 3 && to_xyz.length >= 3) {
          maybePushSegment(from_xyz[0], from_xyz[1], from_xyz[2], to_xyz[0], to_xyz[1], to_xyz[2], drillSegments, segMeasures, artifactEpsg, rad);
        } else {
          maybePushSegment(r.x_from, r.y_from, r.z_from, r.x_to, r.y_to, r.z_to, drillSegments, segMeasures, artifactEpsg, rad);
        }
      }
    }

    const assayPts = obj.assay_points;
    if (Array.isArray(assayPts)) {
      for (const row of assayPts) {
        if (!row || typeof row !== "object" || Array.isArray(row)) continue;
        const r = row as Record<string, unknown>;
        maybePushPoint(r.x, r.y, r.z, r.attributes, artifactEpsg);
      }
    }

    const pts = obj.points;
    if (Array.isArray(pts)) {
      for (const row of pts) {
        if (!row || typeof row !== "object" || Array.isArray(row)) continue;
        const r = row as Record<string, unknown>;
        maybePushPoint(r.x, r.y, r.z, r.attributes, artifactEpsg);
      }
    }

    const mcs = obj.measure_candidates;
    if (Array.isArray(mcs)) {
      for (const m of mcs) {
        if (typeof m === "string" && m.trim().length) measures.add(m.trim());
      }
    }

    const sg = obj.surface_grid;
    if (treatSurfaceGridAsTerrain && sg && typeof sg === "object" && !Array.isArray(sg)) {
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
        const stride = Math.max(1, Math.floor(Math.max(nxi, nyi) / 36));
        for (let iy = 0; iy < nyi; iy += stride) {
          for (let ix = 0; ix < nxi; ix += stride) {
            const idx = iy * nxi + ix;
            const zv = n(vals[idx]);
            if (zv === null) continue;
            terrainPoints.push({ x: xmin + ix * stepX, y: ymin + iy * stepY, z: zv, epsg: artifactEpsg });
          }
        }
        terrainGrid = {
          nx: nxi,
          ny: nyi,
          xmin,
          xmax,
          ymin,
          ymax,
          values: vals.map((v) => n(v) ?? 0),
          epsg: artifactEpsg,
        };
      }
    }
  }

  if (displayPointer === "scene3d.trace_polyline" || (displayPointer.length === 0 && lowerKey.includes("trajectory"))) {
    for (const seg of drillSegments.splice(0)) traces.push(seg);
  }

  if (displayPointer === "scene3d.contour_lines" || (displayPointer.length === 0 && lowerKey.endsWith(".geojson"))) {
    const obj = root as Record<string, unknown>;
    const features = Array.isArray(obj.features) ? obj.features : [];
    for (const f of features) {
      if (!f || typeof f !== "object" || Array.isArray(f)) continue;
      const props =
        (f as Record<string, unknown>).properties &&
        typeof (f as Record<string, unknown>).properties === "object" &&
        !Array.isArray((f as Record<string, unknown>).properties)
          ? ((f as Record<string, unknown>).properties as Record<string, unknown>)
          : null;
      const contourLevel =
        n(props?.elevation) ??
        n(props?.elev) ??
        n(props?.z) ??
        n(props?.contour) ??
        n(props?.height) ??
        n(props?.value);
      const g = (f as Record<string, unknown>).geometry;
      if (!g || typeof g !== "object" || Array.isArray(g)) continue;
      const gg = g as Record<string, unknown>;
      const asLineStrings: unknown[][] =
        gg.type === "LineString"
          ? [Array.isArray(gg.coordinates) ? gg.coordinates : []]
          : gg.type === "MultiLineString"
            ? (Array.isArray(gg.coordinates)
                ? gg.coordinates.filter((x): x is unknown[] => Array.isArray(x))
                : [])
            : [];
      for (const coords of asLineStrings) {
        if (coords.length < 2) continue;
        for (let i = 1; i < coords.length; i++) {
          const p0 = coords[i - 1];
          const p1 = coords[i];
          if (!Array.isArray(p0) || !Array.isArray(p1) || p0.length < 2 || p1.length < 2) continue;
          const x0 = n(p0[0]);
          const y0 = n(p0[1]);
          const x1 = n(p1[0]);
          const y1 = n(p1[1]);
          if (x0 === null || y0 === null || x1 === null || y1 === null) continue;
          const z0n = p0.length >= 3 ? n(p0[2]) : null;
          const z1n = p1.length >= 3 ? n(p1[2]) : null;
          contourSegments.push({
            from: [x0, y0, z0n ?? 0],
            to: [x1, y1, z1n ?? (z0n ?? 0)],
            epsg: artifactEpsg,
            hasExplicitZ: z0n !== null && z1n !== null,
            contourLevel: contourLevel ?? undefined,
          });
        }
      }
    }
  }

  return {
    traces,
    drillSegments,
    contourSegments,
    assayPoints,
    terrainPoints,
    terrainGrid,
    measureCandidates: [...measures].sort(),
  };
}

function terrainGridRank(manifestLayer: ViewerManifestLayer | undefined, artifactKey: string): number {
  const key = artifactKey.toLowerCase();
  const sourceKind = String(manifestLayer?.source_node_kind ?? "").toLowerCase();
  const displayPointer =
    manifestLayer?.presentation &&
    typeof manifestLayer.presentation === "object" &&
    !Array.isArray(manifestLayer.presentation) &&
    typeof manifestLayer.presentation.display_pointer === "string"
      ? String(manifestLayer.presentation.display_pointer).toLowerCase()
      : "";

  let rank = 0;
  if (displayPointer === "scene3d.terrain") rank += 1000;
  if (displayPointer === "scene3d.surface" || displayPointer === "scene3d.surface_mesh") rank += 800;
  if (sourceKind === "dem_fetch") rank += 900;
  if (sourceKind === "terrain_adjust") rank += 850;
  if (sourceKind === "xyz_to_surface") rank += 700;
  if (sourceKind === "surface_sample_ingest") rank -= 250;
  if (key.endsWith("/dem_surface.json")) rank += 700;
  if (key.endsWith("/xyz_surface.json")) rank += 350;
  if (key.endsWith("/surface_samples.json")) rank -= 200;
  return rank;
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

function normalizeLayerOrder(raw: unknown): LayerKey[] {
  const chosen = Array.isArray(raw)
    ? raw.filter(
        (v): v is LayerKey =>
          typeof v === "string" && (LAYER_KEYS as readonly string[]).includes(v)
      )
    : [];
  const out: LayerKey[] = [];
  for (const id of chosen) {
    if (!out.includes(id)) out.push(id);
  }
  for (const id of LAYER_KEYS) {
    if (!out.includes(id)) out.push(id);
  }
  return out;
}

function sampleTerrainZ(grid: TerrainGrid | null, x: number, y: number): number | null {
  if (!grid || grid.nx < 2 || grid.ny < 2 || grid.values.length !== grid.nx * grid.ny) return null;
  if (x < grid.xmin || x > grid.xmax || y < grid.ymin || y > grid.ymax) return null;
  const tx = (x - grid.xmin) / Math.max(1e-12, grid.xmax - grid.xmin);
  const ty = (y - grid.ymin) / Math.max(1e-12, grid.ymax - grid.ymin);
  const gx = tx * (grid.nx - 1);
  const gy = ty * (grid.ny - 1);
  const ix0 = Math.max(0, Math.min(grid.nx - 1, Math.floor(gx)));
  const iy0 = Math.max(0, Math.min(grid.ny - 1, Math.floor(gy)));
  const ix1 = Math.max(0, Math.min(grid.nx - 1, ix0 + 1));
  const iy1 = Math.max(0, Math.min(grid.ny - 1, iy0 + 1));
  const fx = Math.max(0, Math.min(1, gx - ix0));
  const fy = Math.max(0, Math.min(1, gy - iy0));
  const z00 = grid.values[iy0 * grid.nx + ix0];
  const z10 = grid.values[iy0 * grid.nx + ix1];
  const z01 = grid.values[iy1 * grid.nx + ix0];
  const z11 = grid.values[iy1 * grid.nx + ix1];
  const z0 = z00 * (1 - fx) + z10 * fx;
  const z1 = z01 * (1 - fx) + z11 * fx;
  return z0 * (1 - fy) + z1 * fy;
}

function resolvePointZ(point: Point3D, grid: TerrainGrid | null): number | null {
  if (point.hasExplicitZ !== false && Number.isFinite(point.z)) return point.z;
  const sampled = sampleTerrainZ(grid, point.x, point.y);
  if (typeof sampled === "number" && Number.isFinite(sampled)) return sampled;
  return null;
}

function parseImageryContract(v: unknown): ImageryDrapeContract | null {
  if (!v || typeof v !== "object" || Array.isArray(v)) return null;
  const obj = v as Record<string, unknown>;
  if (obj.schema_id !== "scene3d.imagery_drape.v1" && obj.schema_id !== "scene3d.tilebroker_response.v1") return null;
  return obj as unknown as ImageryDrapeContract;
}

function parseLayerStackContract(v: unknown): LayerStackContract | null {
  if (!v || typeof v !== "object" || Array.isArray(v)) return null;
  const obj = v as Record<string, unknown>;
  if (obj.schema_id !== "scene3d.layer_stack.v1") return null;
  return obj as unknown as LayerStackContract;
}

function mapLayerKindToKey(kind: string): LayerKey | null {
  if (kind === "imagery_drape") return "esri_drape";
  if (kind === "terrain") return "terrain_points";
  if (kind === "contours") return "contours";
  if (kind === "drill_segments") return "grade_segments";
  if (kind === "assay_points") return "assay_points";
  if (kind === "mesh") return "trajectories";
  if (kind === "volume") return "high_grade_balloons";
  return null;
}

function isLikelyPaidProvider(providerId: string): boolean {
  const k = providerId.trim().toLowerCase();
  if (!k) return false;
  if (k.startsWith("esri_") || k.startsWith("usgs_")) return false;
  return true;
}

function imageryUrlCandidates(url: string): string[] {
  const out: string[] = [url];
  if (url.includes("/World_Imagery/MapServer/export")) {
    if (url.includes("services.arcgisonline.com")) {
      out.push(url.replace("services.arcgisonline.com", "server.arcgisonline.com"));
    } else if (url.includes("server.arcgisonline.com")) {
      out.push(url.replace("server.arcgisonline.com", "services.arcgisonline.com"));
    }
  }
  return [...new Set(out)];
}

function lonLatToWebMercator(lonDeg: number, latDeg: number): [number, number] {
  const maxLat = 85.05112878;
  const lat = Math.max(-maxLat, Math.min(maxLat, latDeg));
  const x = (lonDeg * 20037508.34) / 180;
  const rad = (lat * Math.PI) / 180;
  const y = Math.log(Math.tan(Math.PI / 4 + rad / 2)) * 6378137;
  return [x, y];
}

function boundsMismatchContractVsTerrain(
  contractBounds: { xmin: number; xmax: number; ymin: number; ymax: number } | undefined,
  terrainBounds: { xmin: number; xmax: number; ymin: number; ymax: number }
): boolean {
  if (!contractBounds) return false;
  const vals = [
    contractBounds.xmin,
    contractBounds.xmax,
    contractBounds.ymin,
    contractBounds.ymax,
    terrainBounds.xmin,
    terrainBounds.xmax,
    terrainBounds.ymin,
    terrainBounds.ymax,
  ];
  if (!vals.every((v) => Number.isFinite(v))) return true;
  const cSpanX = Math.max(1e-9, Math.abs(contractBounds.xmax - contractBounds.xmin));
  const cSpanY = Math.max(1e-9, Math.abs(contractBounds.ymax - contractBounds.ymin));
  const tSpanX = Math.max(1e-9, Math.abs(terrainBounds.xmax - terrainBounds.xmin));
  const tSpanY = Math.max(1e-9, Math.abs(terrainBounds.ymax - terrainBounds.ymin));
  const spanRatioX = Math.max(cSpanX / tSpanX, tSpanX / cSpanX);
  const spanRatioY = Math.max(cSpanY / tSpanY, tSpanY / cSpanY);
  const cCx = (contractBounds.xmin + contractBounds.xmax) / 2;
  const cCy = (contractBounds.ymin + contractBounds.ymax) / 2;
  const tCx = (terrainBounds.xmin + terrainBounds.xmax) / 2;
  const tCy = (terrainBounds.ymin + terrainBounds.ymax) / 2;
  const centerDelta = Math.hypot(cCx - tCx, cCy - tCy);
  const terrainDiag = Math.hypot(tSpanX, tSpanY);
  return spanRatioX > 200 || spanRatioY > 200 || centerDelta > terrainDiag * 100;
}

function paletteColor(value: number | null, lo: number, hi: number, palette: SceneUiState["palette"]): string {
  if (value === null) return "#58a6ff";
  const den = hi - lo || 1;
  const t = Math.max(0, Math.min(1, (value - lo) / den));
  if (palette === "red_blue") {
    const r = Math.round(255 * t);
    const b = Math.round(255 * (1 - t));
    return `rgb(${r},40,${b})`;
  }
  const stops =
    palette === "viridis"
      ? [[68, 1, 84], [59, 82, 139], [33, 145, 140], [94, 201, 97], [253, 231, 36]]
      : palette === "inferno"
        ? [[0, 0, 4], [51, 16, 88], [136, 34, 106], [203, 71, 119], [248, 149, 64], [252, 255, 164]]
        : [[48, 18, 59], [50, 21, 110], [32, 73, 156], [18, 120, 142], [59, 173, 112], [171, 220, 50], [253, 231, 37]];
  const s = t * (stops.length - 1);
  const i = Math.floor(s);
  const j = Math.min(stops.length - 1, i + 1);
  const u = s - i;
  const a = stops[i];
  const b = stops[j];
  const r = Math.round(a[0] * (1 - u) + b[0] * u);
  const g = Math.round(a[1] * (1 - u) + b[1] * u);
  const bb = Math.round(a[2] * (1 - u) + b[2] * u);
  return `rgb(${r},${g},${bb})`;
}

function SegmentTube({
  from,
  to,
  radius,
  color,
  opacity = 1,
}: {
  from: THREE.Vector3;
  to: THREE.Vector3;
  radius: number;
  color: string;
  opacity?: number;
}) {
  const len = from.distanceTo(to);
  const mid = useMemo(() => from.clone().add(to).multiplyScalar(0.5), [from, to]);
  const quat = useMemo(() => {
    const dir = to.clone().sub(from).normalize();
    const q = new THREE.Quaternion();
    q.setFromUnitVectors(new THREE.Vector3(0, 1, 0), dir);
    return q;
  }, [from, to]);
  if (!Number.isFinite(len) || len <= 0) return null;
  return (
    <mesh position={[mid.x, mid.y, mid.z]} quaternion={quat}>
      <cylinderGeometry args={[radius, radius, len, 10, 1]} />
      <meshStandardMaterial
        color={color}
        metalness={0.05}
        roughness={0.8}
        transparent={opacity < 0.999}
        opacity={Math.max(0.02, Math.min(1, opacity))}
      />
    </mesh>
  );
}

function EsriDrape({
  urls,
  width,
  depth,
  y,
  geometry,
  opacity,
  onLoadFailure,
  onLoadSuccess,
}: {
  urls: string[];
  width: number;
  depth: number;
  y: number;
  geometry: THREE.BufferGeometry | null;
  opacity: number;
  onLoadFailure?: (attempted: string[]) => void;
  onLoadSuccess?: (resolvedUrl: string) => void;
}) {
  const [texture, setTexture] = useState<THREE.Texture | null>(null);
  useEffect(() => {
    const candidates = urls
      .map((u) => u.trim())
      .filter((u, i, arr) => u.length > 0 && arr.indexOf(u) === i);
    if (candidates.length === 0) {
      setTexture((prev) => {
        prev?.dispose();
        return null;
      });
      return;
    }
    let cancelled = false;

    const tryLoad = (idx: number) => {
      if (cancelled) return;
      if (idx >= candidates.length) {
        setTexture((prev) => {
          prev?.dispose();
          return null;
        });
        onLoadFailure?.(candidates);
        return;
      }
      const loader = new THREE.TextureLoader();
      loader.setCrossOrigin("anonymous");
      loader.load(
        candidates[idx],
        (tex) => {
          if (cancelled) {
            tex.dispose();
            return;
          }
          tex.colorSpace = THREE.SRGBColorSpace;
          setTexture((prev) => {
            prev?.dispose();
            return tex;
          });
          onLoadSuccess?.(candidates[idx]);
        },
        undefined,
        () => {
          if (!cancelled) tryLoad(idx + 1);
        }
      );
    };
    tryLoad(0);
    return () => {
      cancelled = true;
      setTexture((prev) => {
        prev?.dispose();
        return null;
      });
    };
  }, [onLoadFailure, onLoadSuccess, urls]);

  if (!texture) return null;
  if (geometry) {
    return (
      <mesh geometry={geometry}>
        <meshBasicMaterial
          map={texture}
          transparent
          opacity={Math.max(0, Math.min(1, opacity))}
          side={THREE.DoubleSide}
        />
      </mesh>
    );
  }
  if (width <= 0 || depth <= 0) return null;
  return (
    <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, y, 0]}>
      <planeGeometry args={[width, depth]} />
      <meshBasicMaterial
        map={texture}
        transparent
        opacity={Math.max(0, Math.min(1, opacity))}
        side={THREE.DoubleSide}
      />
    </mesh>
  );
}

export function Map3DThreePanel({ graphId, activeBranchId, active = true, edges, artifacts, viewerNodeId, onClearViewer }: Props) {
  const [status, setStatus] = useState("Loading 3D viewer…");
  const [projectEpsg, setProjectEpsg] = useState(4326);
  const [manifestArtifacts, setManifestArtifacts] = useState<ArtifactEntry[]>([]);
  const [manifestLayers, setManifestLayers] = useState<ViewerManifestLayer[]>([]);
  const [sceneData, setSceneData] = useState<SceneData>({
    traces: [],
    drillSegments: [],
    contourSegments: [],
    assayPoints: [],
    terrainPoints: [],
    terrainGrid: null,
    measureCandidates: [],
    totalArtifacts: 0,
  });
  const [ui, setUi] = useState<SceneUiState>(DEFAULT_UI);
  const [configHydrated, setConfigHydrated] = useState(false);
  const [cameraResetToken, setCameraResetToken] = useState(0);
  const [drapeUrls, setDrapeUrls] = useState<string[]>([]);
  const [drapeStatus, setDrapeStatus] = useState<string>("Drape off.");
  const [drapeLoadError, setDrapeLoadError] = useState<string | null>(null);
  const [contractImagery, setContractImagery] = useState<ImageryDrapeContract | null>(null);
  const [contractLayerStack, setContractLayerStack] = useState<LayerStackContract | null>(null);
  const [draggingLayer, setDraggingLayer] = useState<LayerKey | null>(null);
  const [expandedLayers, setExpandedLayers] = useState<Record<LayerKey, boolean>>({
    esri_drape: false,
    terrain_points: false,
    contours: false,
    trajectories: false,
    grade_segments: true,
    assay_points: false,
    high_grade_balloons: false,
  });
  const savedRef = useRef<string>("");
  const saveTidRef = useRef<number | null>(null);
  const autoRefitSigRef = useRef<string>("");

  const inputLinks = useMemo(() => (viewerNodeId ? upstreamSourcesForViewer(edges, viewerNodeId) : []), [edges, viewerNodeId]);

  useEffect(() => {
    if (!graphId) return;
    let cancelled = false;
    void (async () => {
      try {
        const g = await fetchGraph(graphId);
        const epsg = g.project_crs && typeof g.project_crs === "object" && typeof g.project_crs.epsg === "number" ? g.project_crs.epsg : 4326;
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
        setManifestArtifacts(mf.layers.map((l) => ({ node_id: l.source_node_id, key: l.artifact_key, url: l.artifact_url, content_hash: l.content_hash })));
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
    if (!graphId || !viewerNodeId) return;
    let cancelled = false;
    void (async () => {
      try {
        const g = await fetchGraph(graphId);
        if (cancelled) return;
        const viewer = g.nodes.find((x) => x.id === viewerNodeId);
        const uiRaw = viewer?.config?.params?.ui && typeof viewer.config.params.ui === "object" && !Array.isArray(viewer.config.params.ui) ? (viewer.config.params.ui as Record<string, unknown>) : {};
        setUi({
          showTraces: uiRaw.show_traces !== false,
          showSegments: uiRaw.show_segments !== false,
          showContours: uiRaw.show_contours !== false,
          showSamples: uiRaw.show_samples !== false,
          showTerrain: uiRaw.show_terrain === true,
          showDrape: uiRaw.show_drape !== false,
          showBalloons: uiRaw.show_balloons === true,
          imageryProvider:
            typeof uiRaw.imagery_provider === "string" &&
            uiRaw.imagery_provider in IMAGERY_PROVIDERS
              ? (uiRaw.imagery_provider as ImageryProviderId)
              : DEFAULT_UI.imageryProvider,
          selectedMeasure: typeof uiRaw.selected_measure === "string" ? uiRaw.selected_measure : "",
          palette: uiRaw.palette === "viridis" || uiRaw.palette === "turbo" || uiRaw.palette === "red_blue" || uiRaw.palette === "inferno" ? uiRaw.palette : "inferno",
          clampLowPct: typeof uiRaw.clamp_low_pct === "number" ? uiRaw.clamp_low_pct : DEFAULT_UI.clampLowPct,
          clampHighPct: typeof uiRaw.clamp_high_pct === "number" ? uiRaw.clamp_high_pct : DEFAULT_UI.clampHighPct,
          radiusScale: typeof uiRaw.radius_scale === "number" ? uiRaw.radius_scale : DEFAULT_UI.radiusScale,
          traceWidth: typeof uiRaw.trace_width === "number" ? uiRaw.trace_width : DEFAULT_UI.traceWidth,
          segmentWidth: typeof uiRaw.segment_width === "number" ? uiRaw.segment_width : DEFAULT_UI.segmentWidth,
          sampleSize: typeof uiRaw.sample_size === "number" ? uiRaw.sample_size : DEFAULT_UI.sampleSize,
          terrainOpacity: typeof uiRaw.terrain_opacity === "number" ? uiRaw.terrain_opacity : DEFAULT_UI.terrainOpacity,
          drapeOpacity: typeof uiRaw.drape_opacity === "number" ? uiRaw.drape_opacity : DEFAULT_UI.drapeOpacity,
          contourColor: typeof uiRaw.contour_color === "string" ? uiRaw.contour_color : DEFAULT_UI.contourColor,
          contourOpacity: typeof uiRaw.contour_opacity === "number" ? uiRaw.contour_opacity : DEFAULT_UI.contourOpacity,
          contourWidth: typeof uiRaw.contour_width === "number" ? uiRaw.contour_width : DEFAULT_UI.contourWidth,
          contourIntervalStep:
            typeof uiRaw.contour_interval_step === "number"
              ? uiRaw.contour_interval_step
              : DEFAULT_UI.contourIntervalStep,
          traceColor: typeof uiRaw.trace_color === "string" ? uiRaw.trace_color : DEFAULT_UI.traceColor,
          balloonThresholdPct:
            typeof uiRaw.balloon_threshold_pct === "number"
              ? uiRaw.balloon_threshold_pct
              : DEFAULT_UI.balloonThresholdPct,
          balloonScale:
            typeof uiRaw.balloon_scale === "number" ? uiRaw.balloon_scale : DEFAULT_UI.balloonScale,
          balloonOpacity:
            typeof uiRaw.balloon_opacity === "number"
              ? uiRaw.balloon_opacity
              : DEFAULT_UI.balloonOpacity,
          layerOrder: normalizeLayerOrder(uiRaw.layer_order),
          layerOrderMode:
            uiRaw.layer_order_mode === "override" || uiRaw.layer_order_mode === "contract"
              ? (uiRaw.layer_order_mode as "contract" | "override")
              : DEFAULT_UI.layerOrderMode,
          // Use a new explicit key so legacy persisted `invert_depth=true`
          // cannot silently keep scenes upside-down after orientation fixes.
          invertDepth: uiRaw.invert_vertical_axis === true,
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
      void patchNodeParams(
        graphId,
        viewerNodeId,
        {
          ui: {
            show_traces: ui.showTraces,
            show_segments: ui.showSegments,
            show_contours: ui.showContours,
            show_samples: ui.showSamples,
            show_terrain: ui.showTerrain,
            show_drape: ui.showDrape,
            show_balloons: ui.showBalloons,
            imagery_provider: ui.imageryProvider,
            selected_measure: ui.selectedMeasure,
            palette: ui.palette,
            clamp_low_pct: ui.clampLowPct,
            clamp_high_pct: ui.clampHighPct,
            radius_scale: ui.radiusScale,
            trace_width: ui.traceWidth,
            segment_width: ui.segmentWidth,
            sample_size: ui.sampleSize,
            terrain_opacity: ui.terrainOpacity,
            drape_opacity: ui.drapeOpacity,
            contour_color: ui.contourColor,
            contour_opacity: ui.contourOpacity,
            contour_width: ui.contourWidth,
            contour_interval_step: Math.max(1, Math.trunc(ui.contourIntervalStep)),
            trace_color: ui.traceColor,
            balloon_threshold_pct: ui.balloonThresholdPct,
            balloon_scale: ui.balloonScale,
            balloon_opacity: ui.balloonOpacity,
            layer_order: ui.layerOrder,
            layer_order_mode: ui.layerOrderMode,
            // Persist both during transition; loader reads invert_vertical_axis only.
            invert_vertical_axis: ui.invertDepth,
            invert_depth: ui.invertDepth,
            panel_collapsed: ui.panelCollapsed,
          },
        },
        { branchId: activeBranchId }
      ).then(
        () => {
          savedRef.current = payload;
        },
        () => {
          /* ignore */
        }
      );
    }, 350);
    return () => {
      if (saveTidRef.current) window.clearTimeout(saveTidRef.current);
    };
  }, [activeBranchId, configHydrated, graphId, ui, viewerNodeId]);

  useEffect(() => {
    if (!graphId || !viewerNodeId) return;
    let cancelled = false;
    void (async () => {
      const source = manifestArtifacts.length ? manifestArtifacts : artifacts.filter((a) => inputLinks.includes(a.node_id));
      if (source.length === 0) {
        setContractImagery(null);
        setContractLayerStack(null);
        setSceneData({
          traces: [],
          drillSegments: [],
          contourSegments: [],
          assayPoints: [],
          terrainPoints: [],
          terrainGrid: null,
          measureCandidates: [],
          totalArtifacts: 0,
        });
        setStatus(inputLinks.length ? "No upstream 3D artifacts yet. Queue run, run worker, then refresh." : "No compatible inputs wired into this 3D viewer.");
        return;
      }

      const manifestByArtifact = new Map<string, ViewerManifestLayer>();
      for (const ml of manifestLayers) manifestByArtifact.set(`${ml.artifact_key}:${ml.content_hash}`, ml);

      const allTraces: Segment3D[] = [];
      const allSegs: Segment3D[] = [];
      const allContours: Segment3D[] = [];
      const allPoints: Point3D[] = [];
      const allTerrain: TerrainPoint[] = [];
      let bestTerrainGrid: TerrainGrid | null = null;
      let bestTerrainPick: TerrainGridPick | null = null;
      let imageryContract: ImageryDrapeContract | null = null;
      let layerStackContract: LayerStackContract | null = null;
      const allMeasures = new Set<string>();
      let loaded = 0;

      for (const art of source) {
        try {
          const r = await fetch(api(art.url), { cache: "no-store" });
          if (!r.ok) continue;
          const txt = await r.text();
          try {
            const raw = JSON.parse(txt) as unknown;
            const maybeImagery = parseImageryContract(raw);
            if (!imageryContract && maybeImagery) imageryContract = maybeImagery;
            const maybeLayerStack = parseLayerStackContract(raw);
            if (!layerStackContract && maybeLayerStack) layerStackContract = maybeLayerStack;
          } catch {
            /* ignore non-json */
          }
          const parsed = parseSceneJson(txt, manifestByArtifact.get(`${art.key}:${art.content_hash}`));
          loaded += 1;
          allTraces.push(...parsed.traces);
          allSegs.push(...parsed.drillSegments);
          allContours.push(...parsed.contourSegments);
          allPoints.push(...parsed.assayPoints);
          allTerrain.push(...parsed.terrainPoints);
          if (parsed.terrainGrid) {
            const ml = manifestByArtifact.get(`${art.key}:${art.content_hash}`);
            const curCells = parsed.terrainGrid.nx * parsed.terrainGrid.ny;
            const curRank = terrainGridRank(ml, art.key);
            const cur: TerrainGridPick = { grid: parsed.terrainGrid, rank: curRank, cells: curCells };
            if (!bestTerrainPick) {
              bestTerrainPick = cur;
              bestTerrainGrid = cur.grid;
            } else if (
              cur.rank > bestTerrainPick.rank ||
              (cur.rank === bestTerrainPick.rank && cur.cells > bestTerrainPick.cells)
            ) {
              bestTerrainPick = cur;
              bestTerrainGrid = cur.grid;
            }
          }
          for (const m of parsed.measureCandidates) allMeasures.add(m);
        } catch {
          /* ignore */
        }
      }
      if (cancelled) return;
      setContractImagery(imageryContract);
      setContractLayerStack(layerStackContract);
      setSceneData({
        traces: allTraces,
        drillSegments: allSegs,
        contourSegments: allContours,
        assayPoints: allPoints,
        terrainPoints: allTerrain,
        terrainGrid: bestTerrainGrid,
        measureCandidates: [...allMeasures].sort(),
        totalArtifacts: loaded,
      });
      setStatus(`${allTraces.length + allSegs.length + allContours.length} line segment(s), ${allPoints.length} point(s), ${allTerrain.length} terrain samples from ${loaded} artifact(s).`);
    })();
    return () => {
      cancelled = true;
    };
  }, [artifacts, graphId, inputLinks, manifestArtifacts, manifestLayers, viewerNodeId]);

  useEffect(() => {
    if (!ui.selectedMeasure && sceneData.measureCandidates.length) {
      setUi((p) => ({ ...p, selectedMeasure: sceneData.measureCandidates[0] }));
    }
  }, [sceneData.measureCandidates, ui.selectedMeasure]);

  const world = useMemo(() => {
    const pts: Array<[number, number, number]> = [];
    for (const s of sceneData.traces) pts.push(s.from, s.to);
    for (const s of sceneData.drillSegments) pts.push(s.from, s.to);
    for (const s of sceneData.contourSegments) pts.push(s.from, s.to);
    for (const p of sceneData.assayPoints) {
      const z = resolvePointZ(p, sceneData.terrainGrid);
      if (typeof z === "number" && Number.isFinite(z)) pts.push([p.x, p.y, z]);
    }
    for (const p of sceneData.terrainPoints) pts.push([p.x, p.y, p.z]);
    if (sceneData.terrainGrid && sceneData.terrainGrid.values.length > 0) {
      const g = sceneData.terrainGrid;
      let gzMin = Number.POSITIVE_INFINITY;
      let gzMax = Number.NEGATIVE_INFINITY;
      for (const z of g.values) {
        if (typeof z === "number" && Number.isFinite(z)) {
          gzMin = Math.min(gzMin, z);
          gzMax = Math.max(gzMax, z);
        }
      }
      if (Number.isFinite(gzMin) && Number.isFinite(gzMax)) {
        pts.push([g.xmin, g.ymin, gzMin], [g.xmax, g.ymax, gzMax]);
      } else {
        pts.push([g.xmin, g.ymin, 0], [g.xmax, g.ymax, 0]);
      }
    }
    if (pts.length === 0) {
      return { center: [0, 0, 0] as [number, number, number], bounds: { xmin: 0, xmax: 1, ymin: 0, ymax: 1, zmin: 0, zmax: 1 } };
    }
    let xmin = Number.POSITIVE_INFINITY;
    let xmax = Number.NEGATIVE_INFINITY;
    let ymin = Number.POSITIVE_INFINITY;
    let ymax = Number.NEGATIVE_INFINITY;
    let zmin = Number.POSITIVE_INFINITY;
    let zmax = Number.NEGATIVE_INFINITY;
    for (const [x, y, z] of pts) {
      xmin = Math.min(xmin, x);
      xmax = Math.max(xmax, x);
      ymin = Math.min(ymin, y);
      ymax = Math.max(ymax, y);
      zmin = Math.min(zmin, z);
      zmax = Math.max(zmax, z);
    }
    return { center: [(xmin + xmax) / 2, (ymin + ymax) / 2, (zmin + zmax) / 2] as [number, number, number], bounds: { xmin, xmax, ymin, ymax, zmin, zmax } };
  }, [sceneData]);

  const sceneScale = useMemo(() => {
    const dx = Math.max(1e-9, world.bounds.xmax - world.bounds.xmin);
    const dy = Math.max(1e-9, world.bounds.ymax - world.bounds.ymin);
    const dz = Math.max(1e-9, world.bounds.zmax - world.bounds.zmin);
    return Math.max(dx, dy, dz, 1);
  }, [world.bounds]);

  const toLocal = useMemo(() => {
    const [cx, cy, cz] = world.center;
    return (x: number, y: number, z: number) =>
      new THREE.Vector3(x - cx, ui.invertDepth ? -(z - cz) : z - cz, y - cy);
  }, [ui.invertDepth, world.center]);

  const measureDomain = useMemo(() => {
    const key = ui.selectedMeasure.trim();
    if (!key) return { lo: 0, hi: 1 };
    const vals: number[] = [];
    for (const s of sceneData.drillSegments) {
      const v = s.measures?.[key];
      if (typeof v === "number" && Number.isFinite(v)) vals.push(v);
    }
    for (const p of sceneData.assayPoints) {
      const v = p.measures?.[key];
      if (typeof v === "number" && Number.isFinite(v)) vals.push(v);
    }
    vals.sort((a, b) => a - b);
    return { lo: vals.length ? percentile(vals, ui.clampLowPct) : 0, hi: vals.length ? percentile(vals, ui.clampHighPct) : 1 };
  }, [sceneData.assayPoints, sceneData.drillSegments, ui.clampHighPct, ui.clampLowPct, ui.selectedMeasure]);

  const drapeBounds = useMemo(() => {
    if (sceneData.terrainGrid) {
      return {
        xmin: sceneData.terrainGrid.xmin,
        xmax: sceneData.terrainGrid.xmax,
        ymin: sceneData.terrainGrid.ymin,
        ymax: sceneData.terrainGrid.ymax,
      };
    }
    return world.bounds;
  }, [sceneData.terrainGrid, world.bounds]);
  const currentImageryProvider = IMAGERY_PROVIDERS[ui.imageryProvider] ?? IMAGERY_PROVIDERS.esri_world_imagery;

  useEffect(() => {
    if (!ui.showDrape) {
      setDrapeUrls([]);
      setDrapeStatus("Drape off.");
      setDrapeLoadError(null);
      return;
    }
    const hasContractImage =
      (typeof contractImagery?.image_url === "string" &&
        contractImagery.image_url.trim().length > 0) ||
      (Array.isArray(contractImagery?.image_url_candidates) &&
        contractImagery.image_url_candidates.some((u) => typeof u === "string" && u.trim().length > 0));
    if (hasContractImage) {
      const quality = Array.isArray(contractImagery?.quality_flags)
        ? contractImagery?.quality_flags?.join(", ")
        : "";
      const terrainBoundsForCheck = sceneData.terrainGrid
        ? {
            xmin: sceneData.terrainGrid.xmin,
            xmax: sceneData.terrainGrid.xmax,
            ymin: sceneData.terrainGrid.ymin,
            ymax: sceneData.terrainGrid.ymax,
          }
        : drapeBounds;
      const badContractBounds = boundsMismatchContractVsTerrain(
        contractImagery?.bounds,
        terrainBoundsForCheck
      );
      if (!badContractBounds) {
        const fromContract = [
          ...(typeof contractImagery?.image_url === "string" ? [contractImagery.image_url] : []),
          ...(Array.isArray(contractImagery?.image_url_candidates) ? contractImagery.image_url_candidates : []),
        ]
          .map((u) => u.trim())
          .filter((u, i, arr) => u.length > 0 && arr.indexOf(u) === i)
          .flatMap((u) => imageryUrlCandidates(u))
          .filter((u, i, arr) => arr.indexOf(u) === i);
        setDrapeUrls(fromContract);
        setDrapeStatus(
          `${contractImagery?.provider_label ?? "Contract imagery"} drape active${
            quality ? ` (${quality})` : ""
          }.`
        );
        setDrapeLoadError(null);
        return;
      }
      setDrapeStatus(
        "Contract imagery bounds mismatch terrain; using runtime provider extent instead."
      );
    }
    if (!sceneData.terrainGrid) {
      setDrapeUrls([]);
      setDrapeStatus("Imagery drape flat fallback only (no terrain DEM input wired).");
      setDrapeLoadError(null);
      return;
    }
    const { xmin, xmax, ymin, ymax } = drapeBounds;
    const hasFiniteBounds = [xmin, xmax, ymin, ymax].every((v) => Number.isFinite(v));
    if (!hasFiniteBounds) {
      setDrapeUrls([]);
      setDrapeStatus("Imagery drape unavailable (invalid bounds).");
      setDrapeLoadError(null);
      return;
    }
    let cancelled = false;
    const debounceMs = isLikelyPaidProvider(ui.imageryProvider) ? 900 : 350;
    const tid = window.setTimeout(() => {
      void (async () => {
      const buildImageryUrls = (lonMin: number, latMin: number, lonMax: number, latMax: number) => {
        const padX = Math.max(0.0001, (lonMax - lonMin) * 0.03);
        const padY = Math.max(0.0001, (latMax - latMin) * 0.03);
        const lon0 = lonMin - padX;
        const lon1 = lonMax + padX;
        const lat0 = latMin - padY;
        const lat1 = latMax + padY;
        const spanLon = Math.max(1e-6, lon1 - lon0);
        const spanLat = Math.max(1e-6, lat1 - lat0);
        const aspect = Math.max(0.25, Math.min(4, spanLon / spanLat));
        const isArcGis = /arcgisonline\.com/i.test(currentImageryProvider.exportUrl);
        const sizeBases = [1024, 768, 512];
        const urls: string[] = [];
        const pushUrl = (bbox: string, sr: "3857" | "4326", base: number) => {
          let pxW = base;
          let pxH = base;
          if (aspect >= 1) {
            pxH = Math.max(256, Math.min(1400, Math.round(pxW / aspect)));
          } else {
            pxW = Math.max(256, Math.min(1400, Math.round(pxH * aspect)));
          }
          const url = new URL(currentImageryProvider.exportUrl);
          url.searchParams.set("bbox", bbox);
          url.searchParams.set("bboxSR", sr);
          url.searchParams.set("imageSR", sr);
          url.searchParams.set("size", `${pxW},${pxH}`);
          url.searchParams.set("format", currentImageryProvider.format);
          url.searchParams.set("transparent", "false");
          url.searchParams.set("f", "image");
          urls.push(...imageryUrlCandidates(url.toString()));
        };

        if (isArcGis) {
          const [x0, y0] = lonLatToWebMercator(lon0, lat0);
          const [x1, y1] = lonLatToWebMercator(lon1, lat1);
          const wmBbox = `${Math.min(x0, x1)},${Math.min(y0, y1)},${Math.max(x0, x1)},${Math.max(y0, y1)}`;
          for (const base of sizeBases) pushUrl(wmBbox, "3857", base);
        }
        const llBbox = `${lon0},${lat0},${lon1},${lat1}`;
        for (const base of sizeBases) pushUrl(llBbox, "4326", base);
        return [...new Set(urls)];
      };

      const rawLooksGeographic =
        xmin >= -180 && xmax <= 180 && ymin >= -90 && ymax <= 90;
      if (rawLooksGeographic) {
        if (!cancelled) {
          setDrapeUrls(buildImageryUrls(xmin, ymin, xmax, ymax));
          setDrapeLoadError(null);
          setDrapeStatus(
            projectEpsg === 4326
              ? `${currentImageryProvider.label} drape active.`
              : `${currentImageryProvider.label} drape active (fallback: raw XY treated as WGS84 despite EPSG:${projectEpsg}).`
          );
        }
        return;
      }

      const projectedCorners: Array<[number, number]> = [
        [xmin, ymin],
        [xmin, ymax],
        [xmax, ymin],
        [xmax, ymax],
      ];
      const ll: Array<[number, number]> = [];
      const convertCornerToWgs84 = async (x: number, y: number): Promise<[number, number] | null> => {
        if (projectEpsg === 4326) {
          if (x >= -180 && x <= 180 && y >= -90 && y <= 90) return [x, y];
          // Common mislabel case: coordinates are actually Web Mercator meters.
          const wm = await lonLatFromProjectedAsync(3857, x, y);
          if (wm && wm[0] >= -180 && wm[0] <= 180 && wm[1] >= -90 && wm[1] <= 90) return wm;
          return null;
        }
        return lonLatFromProjectedAsync(projectEpsg, x, y);
      };
      for (const [x, y] of projectedCorners) {
        const pt = await convertCornerToWgs84(x, y);
        if (!pt) {
          if (!cancelled) {
            setDrapeUrls([]);
            setDrapeStatus(
              projectEpsg === 4326
                ? "Imagery drape unavailable (EPSG:4326 is out-of-range for lon/lat; set correct project CRS)."
                : `Imagery drape unavailable (CRS reprojection failed for EPSG:${projectEpsg}).`
            );
            setDrapeLoadError(null);
          }
          return;
        }
        ll.push(pt);
      }
      const lons = ll.map((p) => p[0]);
      const lats = ll.map((p) => p[1]);
      const lonMin = Math.min(...lons);
      const lonMax = Math.max(...lons);
      const latMin = Math.min(...lats);
      const latMax = Math.max(...lats);
      if (lonMin < -180 || lonMax > 180 || latMin < -90 || latMax > 90) {
        if (!cancelled) {
          setDrapeUrls([]);
          setDrapeStatus(
            `Imagery drape unavailable (reprojected bounds outside WGS84 extent; EPSG:${projectEpsg}).`
          );
          setDrapeLoadError(null);
        }
        return;
      }
      if (!cancelled) {
        setDrapeUrls(buildImageryUrls(lonMin, latMin, lonMax, latMax));
        setDrapeLoadError(null);
        setDrapeStatus(`${currentImageryProvider.label} drape active.`);
      }
      })();
    }, debounceMs);
    return () => {
      cancelled = true;
      window.clearTimeout(tid);
    };
  }, [contractImagery, currentImageryProvider, drapeBounds, projectEpsg, sceneData.terrainGrid, ui.imageryProvider, ui.showDrape]);

  const filteredContourSegments = useMemo(() => {
    if (sceneData.contourSegments.length === 0) return [] as Segment3D[];
    const step = Math.max(1, Math.trunc(ui.contourIntervalStep));
    if (step <= 1) return sceneData.contourSegments;
    const levels = Array.from(
      new Set(
        sceneData.contourSegments
          .map((s) => (typeof s.contourLevel === "number" && Number.isFinite(s.contourLevel) ? s.contourLevel : null))
          .filter((v): v is number => v !== null)
      )
    ).sort((a, b) => a - b);
    if (levels.length < 2) return sceneData.contourSegments;
    const keep = new Set(levels.filter((_, idx) => idx % step === 0));
    return sceneData.contourSegments.filter(
      (s) => !(typeof s.contourLevel === "number" && Number.isFinite(s.contourLevel)) || keep.has(s.contourLevel)
    );
  }, [sceneData.contourSegments, ui.contourIntervalStep]);

  const balloonThresholdValue = useMemo(() => {
    const key = ui.selectedMeasure.trim();
    if (!key) return null;
    const vals = sceneData.assayPoints
      .map((p) => p.measures?.[key])
      .filter((v): v is number => typeof v === "number" && Number.isFinite(v))
      .sort((a, b) => a - b);
    if (vals.length === 0) return null;
    return percentile(vals, Math.max(0, Math.min(100, ui.balloonThresholdPct)));
  }, [sceneData.assayPoints, ui.balloonThresholdPct, ui.selectedMeasure]);

  const balloonPoints = useMemo(() => {
    const key = ui.selectedMeasure.trim();
    if (!key || balloonThresholdValue === null) return [] as Array<{ point: Point3D; value: number }>;
    return sceneData.assayPoints
      .map((p) => {
        const v = p.measures?.[key];
        return typeof v === "number" && Number.isFinite(v) ? { point: p, value: v } : null;
      })
      .filter((x): x is { point: Point3D; value: number } => x !== null && x.value >= balloonThresholdValue);
  }, [balloonThresholdValue, sceneData.assayPoints, ui.selectedMeasure]);

  const localDrillSegments = useMemo(
    () =>
      sceneData.drillSegments.map((s) => ({
        a: toLocal(s.from[0], s.from[1], s.from[2]),
        b: toLocal(s.to[0], s.to[1], s.to[2]),
      })),
    [sceneData.drillSegments, toLocal]
  );

  const drapeTerrainGeom = useMemo(() => {
    if (!sceneData.terrainGrid) return null;
    const g = sceneData.terrainGrid;
    if (g.nx < 2 || g.ny < 2 || g.values.length !== g.nx * g.ny) return null;

    const positions = new Float32Array(g.nx * g.ny * 3);
    const uvs = new Float32Array(g.nx * g.ny * 2);
    const stepX = (g.xmax - g.xmin) / (g.nx - 1);
    const stepY = (g.ymax - g.ymin) / (g.ny - 1);
    const zLift = Math.max(0.02, sceneScale * 0.0002);

    for (let iy = 0; iy < g.ny; iy++) {
      for (let ix = 0; ix < g.nx; ix++) {
        const idx = iy * g.nx + ix;
        const x = g.xmin + ix * stepX;
        const y = g.ymin + iy * stepY;
        const z = g.values[idx] + zLift;
        const p = toLocal(x, y, z);
        positions[idx * 3 + 0] = p.x;
        positions[idx * 3 + 1] = p.y;
        positions[idx * 3 + 2] = p.z;
        uvs[idx * 2 + 0] = ix / (g.nx - 1);
        uvs[idx * 2 + 1] = 1 - iy / (g.ny - 1);
      }
    }

    const triCount = (g.nx - 1) * (g.ny - 1) * 2;
    const indices = new Uint32Array(triCount * 3);
    let w = 0;
    for (let iy = 0; iy < g.ny - 1; iy++) {
      for (let ix = 0; ix < g.nx - 1; ix++) {
        const i00 = iy * g.nx + ix;
        const i10 = i00 + 1;
        const i01 = i00 + g.nx;
        const i11 = i01 + 1;
        indices[w++] = i00;
        indices[w++] = i01;
        indices[w++] = i10;
        indices[w++] = i10;
        indices[w++] = i01;
        indices[w++] = i11;
      }
    }

    const geom = new THREE.BufferGeometry();
    geom.setAttribute("position", new THREE.BufferAttribute(positions, 3));
    geom.setAttribute("uv", new THREE.BufferAttribute(uvs, 2));
    geom.setIndex(new THREE.BufferAttribute(indices, 1));
    geom.computeVertexNormals();
    return geom;
  }, [sceneData.terrainGrid, sceneScale, toLocal]);

  const autoRefitSig = useMemo(() => {
    const g = sceneData.terrainGrid;
    return JSON.stringify({
      viewerNodeId,
      totalArtifacts: sceneData.totalArtifacts,
      traces: sceneData.traces.length,
      segments: sceneData.drillSegments.length,
      points: sceneData.assayPoints.length + sceneData.terrainPoints.length,
      grid: g
        ? {
            nx: g.nx,
            ny: g.ny,
            xmin: g.xmin,
            xmax: g.xmax,
            ymin: g.ymin,
            ymax: g.ymax,
          }
        : null,
    });
  }, [sceneData, viewerNodeId]);

  useEffect(() => {
    if (!active) return;
    if (sceneData.totalArtifacts <= 0) return;
    if (autoRefitSigRef.current === autoRefitSig) return;
    autoRefitSigRef.current = autoRefitSig;
    setCameraResetToken((v) => v + 1);
  }, [active, autoRefitSig, sceneData.totalArtifacts]);

  const orderedLayerIds = useMemo(() => {
    const userOrder = normalizeLayerOrder(ui.layerOrder);
    if (ui.layerOrderMode !== "contract") return userOrder;
    const contractOrder = (contractLayerStack?.layers ?? [])
      .slice()
      .sort((a, b) => (a.priority ?? 0) - (b.priority ?? 0))
      .map((l) => mapLayerKindToKey(String(l.kind ?? "")))
      .filter((x): x is LayerKey => x !== null);
    if (contractOrder.length === 0) return userOrder;
    return normalizeLayerOrder([...contractOrder, ...userOrder]);
  }, [contractLayerStack, ui.layerOrder, ui.layerOrderMode]);

  const hasLayerStackContract = Boolean(contractLayerStack?.layers?.length);
  const canDragLayers = !hasLayerStackContract || ui.layerOrderMode === "override";

  const onDropLayer = (targetId: LayerKey) => {
    if (!canDragLayers) return;
    if (!draggingLayer || draggingLayer === targetId) return;
    setUi((prev) => {
      const order = normalizeLayerOrder(prev.layerOrder);
      const from = order.findIndex((x) => x === draggingLayer);
      const to = order.findIndex((x) => x === targetId);
      if (from < 0 || to < 0) return prev;
      const out = order.slice();
      const [item] = out.splice(from, 1);
      out.splice(to, 0, item);
      return { ...prev, layerOrder: out };
    });
    setDraggingLayer(null);
  };

  const layerLabel: Record<LayerKey, string> = {
    esri_drape: "Imagery drape",
    terrain_points: "Terrain points",
    contours: "Contours",
    trajectories: "Trajectories",
    grade_segments: "Grade segments",
    assay_points: "Assay points",
    high_grade_balloons: "High-grade balloons",
  };

  useEffect(() => {
    return () => {
      drapeTerrainGeom?.dispose();
    };
  }, [drapeTerrainGeom]);

  return (
    <div style={{ position: "relative", width: "100%", height: "100%", background: "#0f1419" }}>
      <div style={{ position: "absolute", inset: 0, display: "flex", flexDirection: "column", minHeight: 0, minWidth: 0 }}>
        <div style={{ padding: "8px 10px", borderBottom: "1px solid #30363d", background: "#121a27", display: "flex", alignItems: "center", justifyContent: "space-between", gap: 10, fontSize: 13 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, minWidth: 0 }}>
            <strong>3D scene (Three.js)</strong>
            {viewerNodeId && <span style={{ opacity: 0.75 }}>Viewer <code style={{ fontSize: 11 }}>{viewerNodeId.slice(0, 8)}…</code></span>}
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <button type="button" onClick={() => setCameraResetToken((v) => v + 1)}>Re-fit</button>
            {onClearViewer && <button type="button" onClick={onClearViewer}>Clear viewer</button>}
          </div>
        </div>
        <div style={{ padding: "6px 10px", fontSize: 12, opacity: 0.85, borderBottom: "1px solid #30363d" }}>
          {status}{" · "}{drapeLoadError ? `${drapeStatus} (${drapeLoadError})` : drapeStatus}
          <div style={{ fontSize: 10, opacity: 0.72, marginTop: 2 }}>
            Imagery attribution: {contractImagery?.attribution || currentImageryProvider.attribution}
          </div>
          {contractImagery?.fingerprint ? (
            <div style={{ fontSize: 10, opacity: 0.66, marginTop: 1 }}>
              Imagery fingerprint: {contractImagery.fingerprint.slice(0, 10)}…
            </div>
          ) : null}
        </div>

        <div style={{ position: "relative", flex: 1, minHeight: 0 }}>
          {active ? (
            <Canvas
              key={`${viewerNodeId ?? "none"}:${cameraResetToken}`}
              camera={{
                position: [sceneScale * 0.85, sceneScale * 1.15, sceneScale * 1.05],
                fov: 50,
                near: 0.01,
                far: Math.max(5000, sceneScale * 500),
              }}
              gl={{ logarithmicDepthBuffer: true }}
            >
              <color attach="background" args={["#0b1220"]} />
              <fog attach="fog" args={["#0b1220", sceneScale * 2, sceneScale * 40]} />
              <ambientLight intensity={0.92} />
              <hemisphereLight args={["#c4d8ff", "#1a2438", 0.55]} />
              <directionalLight position={[sceneScale, sceneScale * 0.8, sceneScale * 0.5]} intensity={0.42} />
              <directionalLight position={[-sceneScale * 0.7, sceneScale * 0.6, -sceneScale * 0.7]} intensity={0.24} />
              <gridHelper args={[sceneScale * 1.25, 30, "#30363d", "#21262d"]} position={[0, -sceneScale * 0.02, 0]} />

              {orderedLayerIds.map((layerId) => {
                if (layerId === "esri_drape") {
                  if (!ui.showDrape || drapeUrls.length === 0) return null;
                  return (
                    <EsriDrape
                      key="layer-esri-drape"
                      urls={drapeUrls}
                      width={Math.max(1, world.bounds.xmax - world.bounds.xmin)}
                      depth={Math.max(1, world.bounds.ymax - world.bounds.ymin)}
                      y={world.bounds.zmin - world.center[2] - sceneScale * 0.005}
                      geometry={drapeTerrainGeom}
                      opacity={ui.drapeOpacity}
                      onLoadFailure={(attempted) => {
                        const hostList = attempted
                          .map((u) => {
                            try {
                              return new URL(u).host;
                            } catch {
                              return "invalid-url";
                            }
                          })
                          .filter((h, i, arr) => h && arr.indexOf(h) === i)
                          .join(", ");
                        setDrapeLoadError(
                          `imagery load failed from ${hostList || "configured URL(s)"}`
                        );
                      }}
                      onLoadSuccess={() => {
                        setDrapeLoadError(null);
                      }}
                    />
                  );
                }
                if (layerId === "trajectories") {
                  if (!ui.showTraces) return null;
                  return (
                    <group key="layer-traces">
                      {sceneData.traces.map((s, i) => {
                        const a = toLocal(s.from[0], s.from[1], s.from[2]);
                        const b = toLocal(s.to[0], s.to[1], s.to[2]);
                        const r = Math.max(0.06, sceneScale * 0.0006 * ui.radiusScale * (ui.traceWidth / 2));
                        return <SegmentTube key={`trace-${i}`} from={a} to={b} radius={r} color={ui.traceColor} />;
                      })}
                    </group>
                  );
                }
                if (layerId === "grade_segments") {
                  if (!ui.showSegments) return null;
                  return (
                    <group key="layer-segments">
                      {sceneData.drillSegments.map((s, i) => {
                        const a = toLocal(s.from[0], s.from[1], s.from[2]);
                        const b = toLocal(s.to[0], s.to[1], s.to[2]);
                        const key = ui.selectedMeasure.trim();
                        const mv = key && s.measures && typeof s.measures[key] === "number" ? (s.measures[key] as number) : null;
                        const color = paletteColor(mv, measureDomain.lo, measureDomain.hi, ui.palette);
                        const rFromData = projectEpsg !== 4326 && typeof s.radiusM === "number" ? s.radiusM : 0;
                        const r = Math.max(0.08, sceneScale * 0.0012 * ui.radiusScale * (ui.segmentWidth / 4), rFromData * ui.radiusScale);
                        return <SegmentTube key={`seg-${i}`} from={a} to={b} radius={r} color={color} />;
                      })}
                    </group>
                  );
                }
                if (layerId === "contours") {
                  if (!ui.showContours || filteredContourSegments.length === 0) return null;
                  const r = Math.max(0.01, sceneScale * 0.00015 * Math.max(0.25, ui.contourWidth));
                  const zLift = Math.max(0.01, sceneScale * 0.00018);
                  return (
                    <group key="layer-contours">
                      {filteredContourSegments.map((s, i) => {
                        const az = s.hasExplicitZ
                          ? s.from[2]
                          : sampleTerrainZ(sceneData.terrainGrid, s.from[0], s.from[1]) ?? s.from[2];
                        const bz = s.hasExplicitZ
                          ? s.to[2]
                          : sampleTerrainZ(sceneData.terrainGrid, s.to[0], s.to[1]) ?? s.to[2];
                        const a = toLocal(s.from[0], s.from[1], az + zLift);
                        const b = toLocal(s.to[0], s.to[1], bz + zLift);
                        return (
                          <SegmentTube
                            key={`contour-${i}`}
                            from={a}
                            to={b}
                            radius={r}
                            color={ui.contourColor}
                            opacity={ui.contourOpacity}
                          />
                        );
                      })}
                    </group>
                  );
                }
                if (layerId === "assay_points") {
                  if (!ui.showSamples) return null;
                  return (
                    <group key="layer-assay-points">
                      {sceneData.assayPoints.map((p, i) => {
                        const pz = resolvePointZ(p, sceneData.terrainGrid);
                        if (pz === null) return null;
                        const lp = toLocal(p.x, p.y, pz);
                        const key = ui.selectedMeasure.trim();
                        const mv = key && p.measures && typeof p.measures[key] === "number" ? (p.measures[key] as number) : null;
                        const color = paletteColor(mv, measureDomain.lo, measureDomain.hi, ui.palette);
                        const rr = Math.max(0.09, sceneScale * 0.0009 * ui.radiusScale * (ui.sampleSize / 4));
                        return (
                          <mesh key={`pt-${i}`} position={[lp.x, lp.y, lp.z]}>
                            <sphereGeometry args={[rr, 10, 10]} />
                            <meshStandardMaterial color={color} />
                          </mesh>
                        );
                      })}
                    </group>
                  );
                }
                if (layerId === "terrain_points") {
                  if (!ui.showTerrain) return null;
                  return (
                    <group key="layer-terrain-points">
                      {sceneData.terrainPoints.map((p, i) => {
                        const lp = toLocal(p.x, p.y, p.z);
                        return (
                          <mesh key={`ter-${i}`} position={[lp.x, lp.y, lp.z]}>
                            <sphereGeometry args={[Math.max(0.03, sceneScale * 0.00035), 6, 6]} />
                            <meshBasicMaterial
                              color="#dde2e6"
                              transparent
                              opacity={Math.max(0.05, Math.min(1, ui.terrainOpacity * 0.45))}
                            />
                          </mesh>
                        );
                      })}
                    </group>
                  );
                }
                if (layerId === "high_grade_balloons") {
                  if (!ui.showBalloons || balloonPoints.length === 0) return null;
                  const baseR = Math.max(0.14, sceneScale * 0.0014 * Math.max(0.35, ui.balloonScale));
                  const maxAttachDist = Math.max(0.2, sceneScale * 0.04);
                  return (
                    <group key="layer-high-grade-balloons">
                      {balloonPoints.map(({ point, value }, i) => {
                        const pz = resolvePointZ(point, sceneData.terrainGrid);
                        if (pz === null) return null;
                        const lp = toLocal(point.x, point.y, pz);
                        const t =
                          measureDomain.hi > measureDomain.lo
                            ? Math.max(
                                0,
                                Math.min(1, (value - measureDomain.lo) / (measureDomain.hi - measureDomain.lo))
                              )
                            : 1;
                        const r = baseR * (0.85 + t * 2.2);
                        const tube = Math.max(r * 0.2, sceneScale * 0.0003);
                        const color = paletteColor(value, measureDomain.lo, measureDomain.hi, ui.palette);
                        let nearestDir: THREE.Vector3 | null = null;
                        let bestDistSq = Number.POSITIVE_INFINITY;
                        for (const seg of localDrillSegments) {
                          const ab = seg.b.clone().sub(seg.a);
                          const lenSq = Math.max(1e-12, ab.lengthSq());
                          const tProj = Math.max(
                            0,
                            Math.min(1, lp.clone().sub(seg.a).dot(ab) / lenSq)
                          );
                          const q = seg.a.clone().add(ab.multiplyScalar(tProj));
                          const dSq = q.distanceToSquared(lp);
                          if (dSq < bestDistSq) {
                            bestDistSq = dSq;
                            nearestDir = seg.b.clone().sub(seg.a).normalize();
                          }
                        }
                        const normal =
                          nearestDir && Math.sqrt(bestDistSq) <= maxAttachDist
                            ? nearestDir
                            : new THREE.Vector3(0, 1, 0);
                        const quat = new THREE.Quaternion().setFromUnitVectors(
                          new THREE.Vector3(0, 0, 1),
                          normal.clone().normalize()
                        );
                        return (
                          <mesh key={`balloon-${i}`} position={[lp.x, lp.y, lp.z]} quaternion={quat}>
                            <torusGeometry args={[r, tube, 14, 30]} />
                            <meshStandardMaterial
                              color={color}
                              transparent
                              opacity={Math.max(0.05, Math.min(1, ui.balloonOpacity))}
                              emissive={new THREE.Color(color)}
                              emissiveIntensity={0.14}
                            />
                          </mesh>
                        );
                      })}
                    </group>
                  );
                }
                return null;
              })}

              <TrackballControls
                makeDefault
                target={[0, 0, 0]}
                rotateSpeed={2.4}
                zoomSpeed={1.4}
                panSpeed={1.0}
                dynamicDampingFactor={0.16}
              />
            </Canvas>
          ) : null}

          <aside style={{ position: "absolute", top: 12, right: 12, width: ui.panelCollapsed ? 42 : 320, maxHeight: "calc(100% - 24px)", overflow: "auto", background: "rgba(15,20,25,0.92)", border: "1px solid #30363d", borderRadius: 10, padding: ui.panelCollapsed ? 6 : 10 }}>
            <button type="button" onClick={() => setUi((p) => ({ ...p, panelCollapsed: !p.panelCollapsed }))} style={{ width: "100%", textAlign: ui.panelCollapsed ? "center" : "right", marginBottom: ui.panelCollapsed ? 0 : 8, background: "transparent", border: "none", color: "#8b949e", cursor: "pointer", fontSize: 16 }}>{ui.panelCollapsed ? "◀" : "▸"}</button>
            {!ui.panelCollapsed && (
              <div style={{ display: "grid", gap: 8, fontSize: 12 }}>
                <details open>
                  <summary style={{ fontWeight: 700, cursor: "pointer" }}>Layers</summary>
                  <div style={{ display: "grid", gap: 6, marginTop: 6 }}>
                    {hasLayerStackContract ? (
                      <div style={{ fontSize: 11, opacity: 0.8 }}>
                        Layer order:
                        <select
                          value={ui.layerOrderMode}
                          onChange={(e) =>
                            setUi((p) => ({
                              ...p,
                              layerOrderMode: e.target.value as "contract" | "override",
                            }))
                          }
                          style={{ marginLeft: 6, fontSize: 11 }}
                        >
                          <option value="contract">Contract</option>
                          <option value="override">Local override</option>
                        </select>
                        {ui.layerOrderMode === "contract" ? (
                          <span style={{ marginLeft: 6, opacity: 0.7 }}>(drag locked)</span>
                        ) : null}
                      </div>
                    ) : null}
                    {orderedLayerIds.map((layerId, i) => {
                      const visible =
                        layerId === "esri_drape"
                          ? ui.showDrape
                          : layerId === "terrain_points"
                            ? ui.showTerrain
                            : layerId === "contours"
                              ? ui.showContours
                            : layerId === "trajectories"
                                ? ui.showTraces
                                : layerId === "grade_segments"
                                  ? ui.showSegments
                                  : layerId === "high_grade_balloons"
                                    ? ui.showBalloons
                                    : ui.showSamples;
                      return (
                        <div
                          key={`chip-${layerId}`}
                          draggable={canDragLayers}
                          onDragStart={() => {
                            if (!canDragLayers) return;
                            setDraggingLayer(layerId);
                          }}
                          onDragOver={(e) => e.preventDefault()}
                          onDrop={() => onDropLayer(layerId)}
                          style={{
                            border: "1px solid #30363d",
                            borderRadius: 8,
                            padding: "6px 8px",
                            background: "rgba(22,27,34,0.75)",
                          }}
                        >
                          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                            <span
                              style={{
                                opacity: canDragLayers ? 0.55 : 0.28,
                                cursor: canDragLayers ? "grab" : "not-allowed",
                              }}
                            >
                              ⋮⋮
                            </span>
                            <input
                              type="checkbox"
                              checked={visible}
                              onChange={(e) =>
                                setUi((p) =>
                                  layerId === "esri_drape"
                                    ? { ...p, showDrape: e.target.checked }
                                    : layerId === "terrain_points"
                                      ? { ...p, showTerrain: e.target.checked }
                                      : layerId === "contours"
                                        ? { ...p, showContours: e.target.checked }
                                        : layerId === "trajectories"
                                          ? { ...p, showTraces: e.target.checked }
                                    : layerId === "grade_segments"
                                            ? { ...p, showSegments: e.target.checked }
                                            : layerId === "high_grade_balloons"
                                              ? { ...p, showBalloons: e.target.checked }
                                            : { ...p, showSamples: e.target.checked }
                                )
                              }
                            />
                            <div style={{ flex: 1, minWidth: 0 }}>
                              <div style={{ fontSize: 12, fontWeight: 600 }}>{layerLabel[layerId]}</div>
                              <div style={{ fontSize: 10, opacity: 0.65 }}>z:{i + 1}</div>
                            </div>
                            <button
                              type="button"
                              onClick={() =>
                                setExpandedLayers((prev) => ({ ...prev, [layerId]: !prev[layerId] }))
                              }
                            >
                              {expandedLayers[layerId] ? "Hide" : "Edit"}
                            </button>
                          </div>
                          {expandedLayers[layerId] && (
                            <div style={{ marginTop: 6, display: "grid", gap: 6 }}>
                              {layerId === "esri_drape" ? (
                                <>
                                  <label>
                                    Imagery provider
                                    <select
                                      value={ui.imageryProvider}
                                      disabled={Boolean(contractImagery)}
                                      onChange={(e) =>
                                        setUi((p) => ({
                                          ...p,
                                          imageryProvider: e.target.value as ImageryProviderId,
                                        }))
                                      }
                                    >
                                      {Object.entries(IMAGERY_PROVIDERS).map(([id, p]) => (
                                        <option key={id} value={id}>
                                          {p.label}
                                        </option>
                                      ))}
                                    </select>
                                  </label>
                                  {contractImagery ? (
                                    <div style={{ fontSize: 10, opacity: 0.72 }}>
                                      Provider controlled by `imagery_provider` contract input.
                                    </div>
                                  ) : (
                                    <div style={{ fontSize: 10, opacity: 0.72 }}>
                                      Default is unpaid Esri imagery. Paid services should stay debounced.
                                    </div>
                                  )}
                                  <label>
                                    Drape alpha
                                    <input
                                      type="range"
                                      min={0}
                                      max={1}
                                      step={0.02}
                                      value={ui.drapeOpacity}
                                      onChange={(e) =>
                                        setUi((p) => ({ ...p, drapeOpacity: Number(e.target.value) || 0 }))
                                      }
                                    />
                                  </label>
                                  <div style={{ fontSize: 10, opacity: 0.72 }}>
                                    {IMAGERY_PROVIDERS[ui.imageryProvider]?.attribution}
                                  </div>
                                </>
                              ) : null}
                              {layerId === "terrain_points" ? (
                                <label>
                                  Terrain alpha
                                  <input
                                    type="range"
                                    min={0}
                                    max={1}
                                    step={0.02}
                                    value={ui.terrainOpacity}
                                    onChange={(e) =>
                                      setUi((p) => ({ ...p, terrainOpacity: Number(e.target.value) || 0 }))
                                    }
                                  />
                                </label>
                              ) : null}
                              {layerId === "contours" ? (
                                <>
                                  <label>
                                    Contour color
                                    <input
                                      type="color"
                                      value={ui.contourColor}
                                      onChange={(e) => setUi((p) => ({ ...p, contourColor: e.target.value }))}
                                    />
                                  </label>
                                  <label>
                                    Contour opacity
                                    <input
                                      type="range"
                                      min={0.05}
                                      max={1}
                                      step={0.02}
                                      value={ui.contourOpacity}
                                      onChange={(e) =>
                                        setUi((p) => ({ ...p, contourOpacity: Number(e.target.value) || 1 }))
                                      }
                                    />
                                  </label>
                                  <label>
                                    Contour width
                                    <input
                                      type="range"
                                      min={0.3}
                                      max={6}
                                      step={0.1}
                                      value={ui.contourWidth}
                                      onChange={(e) =>
                                        setUi((p) => ({ ...p, contourWidth: Number(e.target.value) || 1 }))
                                      }
                                    />
                                  </label>
                                  <label>
                                    Show every Nth contour
                                    <input
                                      type="number"
                                      min={1}
                                      max={50}
                                      step={1}
                                      value={Math.max(1, Math.trunc(ui.contourIntervalStep))}
                                      onChange={(e) =>
                                        setUi((p) => ({
                                          ...p,
                                          contourIntervalStep: Math.max(
                                            1,
                                            Math.min(50, Math.trunc(Number(e.target.value) || 1))
                                          ),
                                        }))
                                      }
                                    />
                                  </label>
                                </>
                              ) : null}
                              {layerId === "trajectories" ? (
                                <>
                                  <label>
                                    Trace color
                                    <input
                                      type="color"
                                      value={ui.traceColor}
                                      onChange={(e) => setUi((p) => ({ ...p, traceColor: e.target.value }))}
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
                                      onChange={(e) =>
                                        setUi((p) => ({ ...p, traceWidth: Number(e.target.value) || 2 }))
                                      }
                                    />
                                  </label>
                                </>
                              ) : null}
                              {layerId === "grade_segments" ? (
                                <label>
                                  Segment width
                                  <input
                                    type="range"
                                    min={1}
                                    max={12}
                                    step={1}
                                    value={ui.segmentWidth}
                                    onChange={(e) =>
                                      setUi((p) => ({ ...p, segmentWidth: Number(e.target.value) || 4 }))
                                    }
                                  />
                                </label>
                              ) : null}
                              {layerId === "assay_points" ? (
                                <label>
                                  Sample size
                                  <input
                                    type="range"
                                    min={2}
                                    max={14}
                                    step={1}
                                    value={ui.sampleSize}
                                    onChange={(e) =>
                                      setUi((p) => ({ ...p, sampleSize: Number(e.target.value) || 7 }))
                                    }
                                  />
                                </label>
                              ) : null}
                              {layerId === "high_grade_balloons" ? (
                                <>
                                  <label>
                                    Threshold percentile ({Math.round(ui.balloonThresholdPct)}%)
                                    <input
                                      type="range"
                                      min={50}
                                      max={99}
                                      step={1}
                                      value={ui.balloonThresholdPct}
                                      onChange={(e) =>
                                        setUi((p) => ({
                                          ...p,
                                          balloonThresholdPct: Number(e.target.value) || 92,
                                        }))
                                      }
                                    />
                                  </label>
                                  <label>
                                    Balloon scale
                                    <input
                                      type="range"
                                      min={0.4}
                                      max={4}
                                      step={0.1}
                                      value={ui.balloonScale}
                                      onChange={(e) =>
                                        setUi((p) => ({ ...p, balloonScale: Number(e.target.value) || 1 }))
                                      }
                                    />
                                  </label>
                                  <label>
                                    Balloon opacity
                                    <input
                                      type="range"
                                      min={0.05}
                                      max={1}
                                      step={0.02}
                                      value={ui.balloonOpacity}
                                      onChange={(e) =>
                                        setUi((p) => ({ ...p, balloonOpacity: Number(e.target.value) || 1 }))
                                      }
                                    />
                                  </label>
                                </>
                              ) : null}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                </details>

                <details open>
                  <summary style={{ fontWeight: 700, cursor: "pointer" }}>Measure</summary>
                  <div style={{ display: "grid", gap: 6, marginTop: 6 }}>
                    <select value={ui.selectedMeasure} onChange={(e) => setUi((p) => ({ ...p, selectedMeasure: e.target.value }))}>
                      {sceneData.measureCandidates.length === 0 ? <option value="">(none)</option> : sceneData.measureCandidates.map((m) => <option key={m} value={m}>{m}</option>)}
                    </select>
                    <select value={ui.palette} onChange={(e) => setUi((p) => ({ ...p, palette: e.target.value as SceneUiState["palette"] }))}>
                      <option value="inferno">Inferno</option>
                      <option value="viridis">Viridis</option>
                      <option value="turbo">Turbo</option>
                      <option value="red_blue">Red ↔ Blue</option>
                    </select>
                    <label>Clamp low (%)<input type="number" min={0} max={100} value={ui.clampLowPct} onChange={(e) => setUi((p) => ({ ...p, clampLowPct: Math.max(0, Math.min(100, Number(e.target.value) || 0)) }))} /></label>
                    <label>Clamp high (%)<input type="number" min={0} max={100} value={ui.clampHighPct} onChange={(e) => setUi((p) => ({ ...p, clampHighPct: Math.max(0, Math.min(100, Number(e.target.value) || 100)) }))} /></label>
                  </div>
                </details>

                <details open>
                  <summary style={{ fontWeight: 700, cursor: "pointer" }}>View</summary>
                  <div style={{ display: "grid", gap: 6, marginTop: 6 }}>
                    <div style={{ opacity: 0.7, fontSize: 11 }}>
                      Axis map: X=easting, Y=elevation (up), Z=northing.
                    </div>
                    <label><input type="checkbox" checked={ui.invertDepth} onChange={(e) => setUi((p) => ({ ...p, invertDepth: e.target.checked }))} /> Invert vertical axis (depth-down view)</label>
                    <label>Radius scale<input type="range" min={0.25} max={4} step={0.05} value={ui.radiusScale} onChange={(e) => setUi((p) => ({ ...p, radiusScale: Number(e.target.value) || 1 }))} /></label>
                  </div>
                </details>
              </div>
            )}
          </aside>
        </div>
      </div>
    </div>
  );
}
