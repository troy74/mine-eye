import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { Canvas, useThree } from "@react-three/fiber";
import { Text, TrackballControls } from "@react-three/drei";
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
import {
  imageryUrlCandidates,
  parseRasterOverlayContract,
  type RasterOverlayContract,
} from "./rasterOverlay";
import { lonLatFromProjectedAsync } from "./spatialReproject";
import { interpolatePaletteHex } from "./palettes";

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
  measures?: Record<string, number | string>;
  sourceLayerId?: string;   // e.g. "assay_points__abc123node"
};

type Segment3D = {
  from: [number, number, number];
  to: [number, number, number];
  epsg?: number;
  radiusM?: number;
  measures?: Record<string, number | string>;
  hasExplicitZ?: boolean;
  contourLevel?: number;
  sourceLayerId?: string;   // e.g. "grade_segments__abc123node"
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

type BlockVoxel = {
  x: number;
  y: number;
  z: number;
  dx: number;
  dy: number;
  dz: number;
  aboveCutoff?: boolean;
  belowCutoffOpacity?: number;
  cutoffGrade?: number;
  epsg?: number;
  measures?: Record<string, number | string>;
  sourceLayerId?: string;
};

// Pre-computed heatmap surface grid from the assay_heatmap pipeline node.
// Values are measure values (e.g. Au ppm), NOT elevation. Stored row-major
// south-to-north (row 0 = ymin, row ny-1 = ymax), same as TerrainGrid.
type HeatmapSurfaceGrid = {
  id: string;
  nodeId: string;
  nodeKind: string;
  label: string;
  grid: {
    nx: number;
    ny: number;
    xmin: number;
    xmax: number;
    ymin: number;
    ymax: number;
    values: Array<number | null>;
  };
};

type SectionPlaneGrid = {
  id: string;
  nodeId: string;
  nodeKind: string;
  label: string;
  centerX: number;
  centerY: number;
  azimuthDeg: number;
  sMin: number;
  sMax: number;
  zTop: number;
  zBottom: number;
  nx: number;
  nz: number;
  measureGrids: Record<string, Array<number | null>>;
};

type SceneData = {
  traces: Segment3D[];
  drillSegments: Segment3D[];
  contourSegments: Segment3D[];
  assayPoints: Point3D[];
  blockVoxels: BlockVoxel[];
  terrainPoints: TerrainPoint[];
  terrainGrids: Array<{
    id: string;
    label: string;
    rank: number;
    portOrder: number;
    grid: TerrainGrid;
  }>;
  terrainGrid: TerrainGrid | null;
  aoiBounds: { xmin: number; xmax: number; ymin: number; ymax: number } | null;
  totalArtifacts: number;
  sourceLayers: SourceLayer[];
  heatmapSurfaces: HeatmapSurfaceGrid[];
  sectionPlanes: SectionPlaneGrid[];
};

type SourceLayer = {
  id: string;         // "${baseType}__${nodeId}"  e.g. "assay_points__abc123"
  baseType: string;   // "assay_points" | "grade_segments" | "trajectories" | "block_voxels"
  nodeId: string;
  nodeKind: string;
  label: string;      // human-readable
  dotColor: string;
  measureCandidates: string[];
  editable: string[];
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

type ColorStop = { pos: number; color: string };

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
  measureColorMode: "continuous" | "categorical";
  measureTransform: "linear" | "log10" | "ln";
  categoricalColorMap: string;
  groundSurfaceKey: string;
  pointShape: "sphere" | "box" | "diamond";
  layerStyles: Record<string, LayerVizStyle>;
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
  // Scene globals
  ambientIntensity: number;
  fogEnabled: boolean;
  gridEnabled: boolean;
  bgPreset: "night" | "dusk" | "dawn" | "overcast";
};

type LayerVizStyle = {
  attributeKey: string;
  palette: SceneUiState["palette"];
  colorMode: SceneUiState["measureColorMode"];
  transform: SceneUiState["measureTransform"];
  clampLowPct: number;
  clampHighPct: number;
  categoricalColorMap: string;
  pointShape: "sphere" | "box" | "diamond" | "cone" | "disc" | "spike";
  sizeAttribute: string;
  sizeMin: number;
  sizeMax: number;
  sizeTransform: "linear" | "sqrt" | "log10";
  opacity: number;
  showLabels: boolean;
  labelAttribute: string;
  labelSize: number;
  colorStops: ColorStop[];
  rampNormMode: "pct" | "fixed";
  fixedMin: number;
  fixedMax: number;
  visible: boolean;
  displayMode: "points" | "heatmap";
  hmGridSize: number;   // IDW grid resolution 64-512
  hmPower: number;      // IDW distance exponent 1-4
};

const LAYER_KEYS = [
  "esri_drape",
  "terrain_points",
  "contours",
  "trajectories",
  "grade_segments",
  "block_voxels",
  "assay_points",
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
  measureColorMode: "continuous",
  measureTransform: "linear",
  categoricalColorMap: "{}",
  groundSurfaceKey: "",
  pointShape: "sphere",
  layerStyles: {},
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
  ambientIntensity: 0.92,
  fogEnabled: true,
  gridEnabled: true,
  bgPreset: "night",
};

const BG_PRESETS: Record<SceneUiState["bgPreset"], { sky: string; fog: string; grid1: string; grid2: string; hemi: [string, string] }> = {
  night:    { sky: "#0b1220", fog: "#0b1220", grid1: "#30363d", grid2: "#21262d", hemi: ["#c4d8ff", "#1a2438"] },
  dusk:     { sky: "#12081e", fog: "#12081e", grid1: "#3d2d4e", grid2: "#261a35", hemi: ["#e8c4ff", "#1a0d30"] },
  dawn:     { sky: "#1a0e08", fog: "#261408", grid1: "#4e3020", grid2: "#2e1a0e", hemi: ["#ffd8a8", "#1a0800"] },
  overcast: { sky: "#1c2228", fog: "#1c2228", grid1: "#3a4550", grid2: "#2a3540", hemi: ["#d8e8ff", "#253040"] },
};

const PALETTE_STOPS: Record<string, ColorStop[]> = {
  inferno: [
    { pos: 0.00, color: "#000004" }, { pos: 0.20, color: "#420a68" },
    { pos: 0.40, color: "#932667" }, { pos: 0.60, color: "#dd513a" },
    { pos: 0.80, color: "#fca50a" }, { pos: 1.00, color: "#fcffa4" },
  ],
  viridis: [
    { pos: 0.00, color: "#440154" }, { pos: 0.25, color: "#31688e" },
    { pos: 0.50, color: "#35b779" }, { pos: 1.00, color: "#fde725" },
  ],
  turbo: [
    { pos: 0.00, color: "#30123b" }, { pos: 0.14, color: "#4456c7" },
    { pos: 0.28, color: "#1b9ce2" }, { pos: 0.43, color: "#29e5a3" },
    { pos: 0.57, color: "#9ef551" }, { pos: 0.71, color: "#f9c632" },
    { pos: 0.85, color: "#e7630a" }, { pos: 1.00, color: "#b01b0c" },
  ],
  red_blue: [
    { pos: 0.00, color: "#3b82f6" }, { pos: 0.50, color: "#e8e8e8" }, { pos: 1.00, color: "#ef4444" },
  ],
  plasma: [
    { pos: 0.00, color: "#0d0887" }, { pos: 0.25, color: "#7e03a8" },
    { pos: 0.50, color: "#cc4778" }, { pos: 0.75, color: "#f89540" }, { pos: 1.00, color: "#f0f921" },
  ],
  cool: [
    { pos: 0.00, color: "#030d28" }, { pos: 0.33, color: "#0077b6" },
    { pos: 0.66, color: "#48cae4" }, { pos: 1.00, color: "#caf0f8" },
  ],
};

const DEFAULT_LAYER_STYLE: LayerVizStyle = {
  attributeKey: "",
  palette: "inferno",
  colorMode: "continuous",
  transform: "linear",
  clampLowPct: 2,
  clampHighPct: 98,
  categoricalColorMap: "{}",
  pointShape: "sphere",
  sizeAttribute: "",
  sizeMin: 1.0,
  sizeMax: 3.0,
  sizeTransform: "linear",
  opacity: 1.0,
  showLabels: false,
  labelAttribute: "",
  labelSize: 11,
  colorStops: PALETTE_STOPS.inferno,
  rampNormMode: "pct",
  fixedMin: 0,
  fixedMax: 1,
  visible: true,
  displayMode: "points",
  hmGridSize: 256,
  hmPower: 2,
};

type ContourJoinPoint = {
  x: number;
  y: number;
  z: number;
  hasExplicitZ: boolean;
};

type ContourPolyline = {
  level: number | null;
  sourceLayerId?: string;
  points: ContourJoinPoint[];
  closed: boolean;
};

// Bounded LRU cache for immutable (content-addressed) artifact text.
// Content-addressed URLs never change so we can keep them in memory
// indefinitely — but we cap at MAX_IMMUTABLE_CACHE entries to avoid
// unbounded memory growth on large scenes with many artifacts.
const MAX_IMMUTABLE_CACHE = 200;
class LruTextCache {
  private readonly map = new Map<string, string>();
  get(key: string): string | undefined {
    const v = this.map.get(key);
    if (v !== undefined) {
      // Refresh insertion order (LRU).
      this.map.delete(key);
      this.map.set(key, v);
    }
    return v;
  }
  set(key: string, value: string): void {
    if (this.map.has(key)) this.map.delete(key);
    this.map.set(key, value);
    if (this.map.size > MAX_IMMUTABLE_CACHE) {
      // Evict the oldest entry (first in insertion order).
      this.map.delete(this.map.keys().next().value!);
    }
  }
}
const immutableArtifactTextCache = new LruTextCache();
const immutableArtifactTextInflight = new Map<string, Promise<string>>();

// ── Drape texture cache ──────────────────────────────────────────────────────
//
// THREE.Texture objects are expensive to create: each one involves a full
// GPU upload.  When the Canvas remounts (e.g. on first load) or when the
// imagery URL is unchanged between renders, we should reuse the existing
// GPU texture rather than re-fetching the imagery tile from the network.
//
// This module-level cache persists across Canvas mounts/unmounts for the
// lifetime of the page.  Entries are only evicted when a new URL displaces
// the oldest entry (LRU, max 20 textures ≈ a handful of providers).
const MAX_DRAPE_TEXTURES = 20;
class DrapeTextureCache {
  private readonly map = new Map<string, THREE.Texture>();
  get(url: string): THREE.Texture | undefined {
    const t = this.map.get(url);
    if (t !== undefined) {
      this.map.delete(url);
      this.map.set(url, t);
    }
    return t;
  }
  set(url: string, tex: THREE.Texture): void {
    if (this.map.has(url)) this.map.delete(url);
    this.map.set(url, tex);
    if (this.map.size > MAX_DRAPE_TEXTURES) {
      const oldest = this.map.keys().next().value!;
      const evicted = this.map.get(oldest)!;
      this.map.delete(oldest);
      evicted.dispose();
    }
  }
  /** Mark a texture as still in use (called when the component mounts with a
   *  cached URL so the LRU entry is refreshed). */
  touch(url: string): void {
    const t = this.map.get(url);
    if (t) { this.map.delete(url); this.map.set(url, t); }
  }
}
const drapeTextureCache = new DrapeTextureCache();

// ── Imperative camera reset ──────────────────────────────────────────────────
//
// Previously the <Canvas> was given key={viewerNodeId + ":" + cameraResetToken}.
// Changing the key causes React to unmount and remount the entire Canvas,
// destroying the WebGL context and all GPU resources (textures, geometries).
// That forced the drape imagery to be re-fetched from the network on every
// scene load.
//
// We now keep the Canvas alive (key=viewerNodeId only) and reset the camera
// imperatively from within the scene via this child component.
function CameraAutoFit({
  resetToken,
  sceneScale,
}: {
  resetToken: number;
  sceneScale: number;
}) {
  const { camera, invalidate } = useThree();
  const lastToken = useRef(-1);
  useEffect(() => {
    if (lastToken.current === resetToken) return;
    lastToken.current = resetToken;
    camera.position.set(sceneScale * 0.85, sceneScale * 1.15, sceneScale * 1.05);
    camera.lookAt(0, 0, 0);
    camera.near = 0.01;
    camera.far = Math.max(5000, sceneScale * 500);
    camera.updateProjectionMatrix();
    invalidate();
  }, [camera, invalidate, resetToken, sceneScale]);
  return null;
}

function defaultLayerStyleForId(layerId: string): LayerVizStyle {
  const isContourLayer = layerId === "contours" || layerId.startsWith("contours__");
  const isBlockLayer = layerId === "block_voxels" || layerId.startsWith("block_voxels__");
  return {
    ...DEFAULT_LAYER_STYLE,
    ...(isContourLayer ? { showLabels: true, labelSize: 12 } : {}),
    ...(isBlockLayer
      ? {
          opacity: 0.95,
          palette: "turbo",
          colorStops: PALETTE_STOPS.turbo,
          clampLowPct: 5,
          clampHighPct: 95,
        }
      : {}),
  };
}

function contourPointDistanceSq(a: ContourJoinPoint, b: ContourJoinPoint): number {
  const dx = a.x - b.x;
  const dy = a.y - b.y;
  return dx * dx + dy * dy;
}

function contourPointLerp(a: ContourJoinPoint, b: ContourJoinPoint, t: number): ContourJoinPoint {
  return {
    x: a.x * (1 - t) + b.x * t,
    y: a.y * (1 - t) + b.y * t,
    z: a.z * (1 - t) + b.z * t,
    hasExplicitZ: a.hasExplicitZ && b.hasExplicitZ,
  };
}

function dedupeContourPoints(points: ContourJoinPoint[], tolSq: number): ContourJoinPoint[] {
  if (points.length <= 1) return points.slice();
  const out: ContourJoinPoint[] = [points[0]];
  for (let i = 1; i < points.length; i++) {
    if (contourPointDistanceSq(out[out.length - 1], points[i]) > tolSq) out.push(points[i]);
  }
  return out;
}

function contourJoinTolerance(segments: Segment3D[]): number {
  if (segments.length === 0) return 1e-6;
  const lengths = segments
    .map((s) => Math.hypot(s.to[0] - s.from[0], s.to[1] - s.from[1]))
    .filter((v) => Number.isFinite(v) && v > 0)
    .sort((a, b) => a - b);
  let xmin = Infinity;
  let xmax = -Infinity;
  let ymin = Infinity;
  let ymax = -Infinity;
  for (const s of segments) {
    xmin = Math.min(xmin, s.from[0], s.to[0]);
    xmax = Math.max(xmax, s.from[0], s.to[0]);
    ymin = Math.min(ymin, s.from[1], s.to[1]);
    ymax = Math.max(ymax, s.from[1], s.to[1]);
  }
  const span = Math.max(1e-6, xmax - xmin, ymax - ymin);
  const p10 = lengths.length > 0 ? percentile(lengths, 10) : 0;
  return Math.max(span * 1e-6, p10 * 0.12, 1e-6);
}

function stitchContourSegments(segments: Segment3D[]): ContourPolyline[] {
  if (segments.length === 0) return [];
  const grouped = new Map<string, Segment3D[]>();
  for (const seg of segments) {
    const levelKey =
      typeof seg.contourLevel === "number" && Number.isFinite(seg.contourLevel)
        ? seg.contourLevel.toFixed(6)
        : "na";
    const key = `${seg.sourceLayerId ?? ""}::${levelKey}`;
    const list = grouped.get(key);
    if (list) list.push(seg);
    else grouped.set(key, [seg]);
  }

  const out: ContourPolyline[] = [];
  for (const group of grouped.values()) {
    const tol = contourJoinTolerance(group);
    const tolSq = tol * tol;
    const entries = group.map((seg) => ({
      used: false,
      sourceLayerId: seg.sourceLayerId,
      level:
        typeof seg.contourLevel === "number" && Number.isFinite(seg.contourLevel)
          ? seg.contourLevel
          : null,
      a: {
        x: seg.from[0],
        y: seg.from[1],
        z: seg.from[2],
        hasExplicitZ: seg.hasExplicitZ !== false,
      },
      b: {
        x: seg.to[0],
        y: seg.to[1],
        z: seg.to[2],
        hasExplicitZ: seg.hasExplicitZ !== false,
      },
    }));

    const tryExtend = (poly: ContourJoinPoint[], atHead: boolean): boolean => {
      const target = atHead ? poly[0] : poly[poly.length - 1];
      let bestIdx = -1;
      let bestDist = Number.POSITIVE_INFINITY;
      let matchOn: "a" | "b" | null = null;
      for (let i = 0; i < entries.length; i++) {
        const entry = entries[i];
        if (entry.used) continue;
        const dA = contourPointDistanceSq(target, entry.a);
        if (dA <= tolSq && dA < bestDist) {
          bestIdx = i;
          bestDist = dA;
          matchOn = "a";
        }
        const dB = contourPointDistanceSq(target, entry.b);
        if (dB <= tolSq && dB < bestDist) {
          bestIdx = i;
          bestDist = dB;
          matchOn = "b";
        }
      }
      if (bestIdx < 0 || !matchOn) return false;
      const entry = entries[bestIdx];
      entry.used = true;
      const matched = matchOn === "a" ? entry.a : entry.b;
      const other = matchOn === "a" ? entry.b : entry.a;
      const snapped = contourPointLerp(target, matched, 0.5);
      if (atHead) {
        poly[0] = snapped;
        poly.unshift(other);
      } else {
        poly[poly.length - 1] = snapped;
        poly.push(other);
      }
      return true;
    };

    for (let i = 0; i < entries.length; i++) {
      if (entries[i].used) continue;
      entries[i].used = true;
      const poly: ContourJoinPoint[] = [entries[i].a, entries[i].b];
      while (true) {
        const tailExtended = tryExtend(poly, false);
        const headExtended = tryExtend(poly, true);
        if (!tailExtended && !headExtended) break;
      }
      let closed = false;
      let points = dedupeContourPoints(poly, tolSq);
      if (points.length > 2 && contourPointDistanceSq(points[0], points[points.length - 1]) <= tolSq) {
        points = points.slice(0, -1);
        closed = true;
      }
      if (points.length >= 2) {
        out.push({
          level: entries[i].level,
          sourceLayerId: entries[i].sourceLayerId,
          points,
          closed,
        });
      }
    }
  }
  return out;
}

function smoothContourPolyline(
  points: ContourJoinPoint[],
  closed: boolean,
  passes: number
): ContourJoinPoint[] {
  let current = points.slice();
  for (let pass = 0; pass < passes; pass++) {
    if (current.length < 3) break;
    if (closed) {
      const next: ContourJoinPoint[] = [];
      for (let i = 0; i < current.length; i++) {
        const a = current[i];
        const b = current[(i + 1) % current.length];
        next.push(contourPointLerp(a, b, 0.25), contourPointLerp(a, b, 0.75));
      }
      current = next;
    } else {
      const next: ContourJoinPoint[] = [current[0]];
      for (let i = 0; i < current.length - 1; i++) {
        const a = current[i];
        const b = current[i + 1];
        next.push(contourPointLerp(a, b, 0.25), contourPointLerp(a, b, 0.75));
      }
      next.push(current[current.length - 1]);
      current = next;
    }
  }
  return current;
}

function polylineLength(points: THREE.Vector3[]): number {
  let total = 0;
  for (let i = 1; i < points.length; i++) total += points[i - 1].distanceTo(points[i]);
  return total;
}

function samplePolylineAtDistance(
  points: THREE.Vector3[],
  distance: number
): { point: THREE.Vector3; tangent: THREE.Vector3 } | null {
  if (points.length < 2) return null;
  let remaining = Math.max(0, distance);
  for (let i = 1; i < points.length; i++) {
    const a = points[i - 1];
    const b = points[i];
    const segLen = a.distanceTo(b);
    if (segLen <= 1e-9) continue;
    if (remaining <= segLen || i === points.length - 1) {
      const t = Math.max(0, Math.min(1, remaining / segLen));
      return {
        point: a.clone().lerp(b, t),
        tangent: b.clone().sub(a).normalize(),
      };
    }
    remaining -= segLen;
  }
  return null;
}

function normalizeContourLabelAngle(angle: number): number {
  let out = angle;
  if (out > Math.PI / 2 || out < -Math.PI / 2) out += Math.PI;
  while (out <= -Math.PI) out += Math.PI * 2;
  while (out > Math.PI) out -= Math.PI * 2;
  return out;
}

function formatContourLabel(level: number): string {
  if (!Number.isFinite(level)) return "";
  const rounded = Math.round(level);
  return `${Object.is(rounded, -0) ? 0 : rounded} m`;
}

function sameStringArray(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

async function fetchArtifactTextCached(url: string): Promise<string> {
  const resolved = api(url);
  const immutableKey = /(?:\?|&)h=/.test(resolved) ? resolved : null;
  if (immutableKey) {
    const cached = immutableArtifactTextCache.get(immutableKey);
    if (cached !== undefined) return cached;
    const inFlight = immutableArtifactTextInflight.get(immutableKey);
    if (inFlight) return inFlight;
    const request = fetch(resolved, { cache: "force-cache" })
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const txt = await r.text();
        immutableArtifactTextCache.set(immutableKey, txt);
        return txt;
      })
      .finally(() => {
        immutableArtifactTextInflight.delete(immutableKey);
      });
    immutableArtifactTextInflight.set(immutableKey, request);
    return request;
  }
  // Non-content-addressed URL: use "default" so the browser can revalidate
  // via ETag/Last-Modified rather than bypassing the cache entirely.
  const r = await fetch(resolved, { cache: "default" });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.text();
}

const PALETTE_GRADIENT: Record<string, string> = {
  inferno: "linear-gradient(to right, #000004, #420a68, #932667, #dd513a, #fca50a, #fcffa4)",
  viridis: "linear-gradient(to right, #440154, #31688e, #35b779, #fde725)",
  turbo: "linear-gradient(to right, #30123b, #4456c7, #1b9ce2, #29e5a3, #9ef551, #f9c632, #e7630a, #b01b0c)",
  red_blue: "linear-gradient(to right, #3b82f6, #f8f8f8, #ef4444)",
};

const subHead: { fontSize: number; fontWeight: number; letterSpacing: string; textTransform: "uppercase"; color: string; marginTop: number } = {
  fontSize: 9.5,
  fontWeight: 700,
  letterSpacing: "0.07em",
  textTransform: "uppercase",
  color: "#484f58",
  marginTop: 2,
};

const ctlSelect: { width: string; background: string; border: string; borderRadius: number; color: string; fontSize: number; padding: string; fontFamily: string } = {
  width: "100%",
  background: "#0d1117",
  border: "1px solid #30363d",
  borderRadius: 5,
  color: "#c9d1d9",
  fontSize: 11,
  padding: "4px 6px",
  fontFamily: "inherit",
};

const pillBtn: { padding: string; borderRadius: number; cursor: string; fontSize: number; fontFamily: string; fontWeight: number } = {
  padding: "3px 6px",
  borderRadius: 5,
  cursor: "pointer",
  fontSize: 10,
  fontFamily: "inherit",
  fontWeight: 600,
};

const miniLabel: { fontSize: number; color: string; display: string; gap: number } = {
  fontSize: 10,
  color: "#6e7681",
  display: "grid",
  gap: 2,
};

const rangeVal: { fontSize: number; color: string; textAlign: "right" } = {
  fontSize: 9,
  color: "#484f58",
  textAlign: "right",
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

function parseAssayAttributes(raw: unknown): Record<string, number | string> {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return {};
  const out: Record<string, number | string> = {};
  for (const [k, v] of Object.entries(raw as Record<string, unknown>)) {
    const x = n(v);
    if (x !== null) {
      out[k] = x;
      continue;
    }
    if (typeof v === "string" && v.trim().length > 0) {
      out[k] = v.trim();
      continue;
    }
    if (typeof v === "boolean") {
      out[k] = v ? "true" : "false";
    }
  }
  return out;
}

function parseSegmentMeasuresFromAssays(raw: unknown): Record<string, number | string> {
  if (!Array.isArray(raw)) return {};
  const sums = new Map<string, { sum: number; count: number }>();
  const cats = new Map<string, Map<string, number>>();
  for (const row of raw) {
    if (!row || typeof row !== "object" || Array.isArray(row)) continue;
    const attrs = (row as Record<string, unknown>).attributes;
    const parsed = parseAssayAttributes(attrs);
    for (const [k, v] of Object.entries(parsed)) {
      if (typeof v === "number") {
        const cur = sums.get(k) ?? { sum: 0, count: 0 };
        cur.sum += v;
        cur.count += 1;
        sums.set(k, cur);
      } else {
        const byVal = cats.get(k) ?? new Map<string, number>();
        byVal.set(v, (byVal.get(v) ?? 0) + 1);
        cats.set(k, byVal);
      }
    }
  }
  const out: Record<string, number | string> = {};
  for (const [k, v] of sums.entries()) {
    if (v.count > 0) out[k] = v.sum / v.count;
  }
  for (const [k, byVal] of cats.entries()) {
    if (out[k] !== undefined) continue;
    let best: string | null = null;
    let bestCount = -1;
    for (const [val, c] of byVal.entries()) {
      if (c > bestCount) {
        best = val;
        bestCount = c;
      }
    }
    if (best !== null) out[k] = best;
  }
  return out;
}

function parseSceneJson(
  text: string,
  manifestLayer: ViewerManifestLayer | undefined
): Omit<SceneData, "totalArtifacts" | "sourceLayers" | "heatmapSurfaces" | "sectionPlanes"> {
  let root: unknown;
  try {
    root = JSON.parse(text) as unknown;
  } catch {
    return {
      traces: [],
      drillSegments: [],
      contourSegments: [],
      assayPoints: [],
      blockVoxels: [],
      terrainPoints: [],
      terrainGrids: [],
      terrainGrid: null,
      aoiBounds: null,
      measureCandidates: [],
      sectionPlanes: [],
    };
  }

  const traces: Segment3D[] = [];
  const drillSegments: Segment3D[] = [];
  const contourSegments: Segment3D[] = [];
  const assayPoints: Point3D[] = [];
  const blockVoxels: BlockVoxel[] = [];
  const terrainPoints: TerrainPoint[] = [];
  const sectionPlanes: SectionPlaneGrid[] = [];
  const terrainGrids: Array<{
    id: string;
    label: string;
    rank: number;
    portOrder: number;
    grid: TerrainGrid;
  }> = [];
  let terrainGrid: TerrainGrid | null = null;
  let aoiBounds: { xmin: number; xmax: number; ymin: number; ymax: number } | null = null;
  const inferredMeasures = new Set<string>();
  const declaredMeasures: string[] = [];

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
    const schemaId = (root as Record<string, unknown>).schema_id;
    if (schemaId === "spatial.aoi.v1") {
      const b = (root as Record<string, unknown>).bounds;
      if (b && typeof b === "object" && !Array.isArray(b)) {
        const xmin = n((b as Record<string, unknown>).xmin);
        const xmax = n((b as Record<string, unknown>).xmax);
        const ymin = n((b as Record<string, unknown>).ymin);
        const ymax = n((b as Record<string, unknown>).ymax);
        if (
          xmin !== null &&
          xmax !== null &&
          ymin !== null &&
          ymax !== null &&
          xmin < xmax &&
          ymin < ymax
        ) {
          aoiBounds = { xmin, xmax, ymin, ymax };
        }
      }
    }
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
    const z0Raw = n(zf);
    const z0 = z0Raw ?? 0;
    const x1 = n(xt);
    const y1 = n(yt);
    const z1Raw = n(zt);
    const z1 = z1Raw ?? 0;
    if (x0 === null || y0 === null || x1 === null || y1 === null) return;
    target.push({
      from: [x0, y0, z0],
      to: [x1, y1, z1],
      epsg,
      radiusM,
      measures: segMeasures,
      hasExplicitZ: z0Raw !== null && z1Raw !== null,
    });
    if (segMeasures) {
      for (const k of Object.keys(segMeasures)) inferredMeasures.add(k);
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
    const parsedMeasures = parseAssayAttributes(rawMeasures);
    for (const k of Object.keys(parsedMeasures)) inferredMeasures.add(k);
    assayPoints.push({
      x,
      y,
      z,
      hasExplicitZ: zRaw !== null,
      epsg,
      measures: Object.keys(parsedMeasures).length ? parsedMeasures : undefined,
    });
  };

  const maybePushBlock = (
    xv: unknown,
    yv: unknown,
    zv: unknown,
    dxv: unknown,
    dyv: unknown,
    dzv: unknown,
    rawMeasures?: unknown,
    aboveCutoff?: boolean,
    belowCutoffOpacity?: number,
    cutoffGrade?: number,
    epsg?: number
  ) => {
    const x = n(xv);
    const y = n(yv);
    const z = n(zv);
    const dx = n(dxv);
    const dy = n(dyv);
    const dz = n(dzv);
    if (x === null || y === null || z === null || dx === null || dy === null || dz === null) return;
    if (dx <= 0 || dy <= 0 || dz <= 0) return;
    const parsedMeasures = parseAssayAttributes(rawMeasures);
    for (const k of Object.keys(parsedMeasures)) inferredMeasures.add(k);
    blockVoxels.push({
      x,
      y,
      z,
      dx,
      dy,
      dz,
      aboveCutoff,
      belowCutoffOpacity,
      cutoffGrade,
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

    const blocks = obj.blocks;
    const styleDefaults =
      obj.style_defaults && typeof obj.style_defaults === "object" && !Array.isArray(obj.style_defaults)
        ? (obj.style_defaults as Record<string, unknown>)
        : null;
    const defaultBelowCutoffOpacity = styleDefaults
      ? n(styleDefaults.below_cutoff_opacity)
      : null;
    const defaultCutoffGrade = styleDefaults
      ? n(styleDefaults.cutoff_grade)
      : null;
    if (Array.isArray(blocks)) {
      for (const row of blocks) {
        if (!row || typeof row !== "object" || Array.isArray(row)) continue;
        const r = row as Record<string, unknown>;
        const attrs =
          r.attributes && typeof r.attributes === "object" && !Array.isArray(r.attributes)
            ? (r.attributes as Record<string, unknown>)
            : undefined;
        maybePushBlock(
          r.x,
          r.y,
          r.z,
          r.dx,
          r.dy,
          r.dz,
          attrs,
          typeof r.above_cutoff === "boolean" ? r.above_cutoff : undefined,
          n(r.below_cutoff_opacity) ?? defaultBelowCutoffOpacity ?? undefined,
          n(r.cutoff_grade) ?? defaultCutoffGrade ?? undefined,
          artifactEpsg
        );
      }
    }

    const mcs = obj.measure_candidates;
    if (Array.isArray(mcs)) {
      for (const m of mcs) {
        if (typeof m === "string" && m.trim().length && !declaredMeasures.includes(m.trim())) {
          declaredMeasures.push(m.trim());
        }
      }
    }

    const section = obj.section;
    if (section && typeof section === "object" && !Array.isArray(section)) {
      const sec = section as Record<string, unknown>;
      const centerX = n(sec.center_x);
      const centerY = n(sec.center_y);
      const azimuthDeg = n(sec.azimuth_deg);
      const sMin = n(sec.s_min);
      const sMax = n(sec.s_max);
      const zTop = n(sec.z_top);
      const zBottom = n(sec.z_bottom);
      const nx = n(sec.nx);
      const nz = n(sec.nz);
      const grids =
        sec.measure_grids && typeof sec.measure_grids === "object" && !Array.isArray(sec.measure_grids)
          ? (sec.measure_grids as Record<string, unknown>)
          : null;
      if (
        centerX !== null &&
        centerY !== null &&
        azimuthDeg !== null &&
        sMin !== null &&
        sMax !== null &&
        zTop !== null &&
        zBottom !== null &&
        nx !== null &&
        nz !== null &&
        grids
      ) {
        const measureGrids: Record<string, Array<number | null>> = {};
        for (const [k, v] of Object.entries(grids)) {
          if (!Array.isArray(v)) continue;
          measureGrids[k] = v.map((x) => (typeof x === "number" && Number.isFinite(x) ? x : null));
          inferredMeasures.add(k);
        }
        sectionPlanes.push({
          id: `${manifestLayer?.source_node_id ?? "section"}:${manifestLayer?.artifact_key ?? "section"}`,
          nodeId: String(manifestLayer?.source_node_id ?? "section"),
          nodeKind: String(manifestLayer?.source_node_kind ?? "unknown"),
          label: "Section plane",
          centerX,
          centerY,
          azimuthDeg,
          sMin,
          sMax,
          zTop,
          zBottom,
          nx: Math.max(2, Math.trunc(nx)),
          nz: Math.max(2, Math.trunc(nz)),
          measureGrids,
        });
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
        n(props?.level) ??
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
    blockVoxels,
    terrainPoints,
    terrainGrids,
    terrainGrid,
    aoiBounds,
    measureCandidates:
      declaredMeasures.length > 0
        ? declaredMeasures
        : [...inferredMeasures].sort(),
    sectionPlanes,
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

function portOrderFromToPort(toPort: string | undefined): number {
  if (!toPort) return Number.MAX_SAFE_INTEGER;
  const m = /^in_(\d+)$/.exec(String(toPort).trim());
  if (!m) return Number.MAX_SAFE_INTEGER;
  const v = Number(m[1]);
  return Number.isFinite(v) ? Math.max(1, Math.trunc(v)) : Number.MAX_SAFE_INTEGER;
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

function buildDrapedOverlayGeometry({
  xmin,
  xmax,
  ymin,
  ymax,
  segX,
  segY,
  groundTerrainGrid,
  toLocal,
  zLift,
}: {
  xmin: number;
  xmax: number;
  ymin: number;
  ymax: number;
  segX: number;
  segY: number;
  groundTerrainGrid: TerrainGrid | null;
  toLocal: (x: number, y: number, z: number) => THREE.Vector3;
  zLift: number;
}): THREE.BufferGeometry | null {
  if (!groundTerrainGrid) return null;
  if (!(xmax > xmin) || !(ymax > ymin)) return null;
  const sx = Math.max(1, Math.trunc(segX));
  const sy = Math.max(1, Math.trunc(segY));
  const positions = new Float32Array((sx + 1) * (sy + 1) * 3);
  const uvs = new Float32Array((sx + 1) * (sy + 1) * 2);

  for (let iy = 0; iy <= sy; iy++) {
    const worldY = ymin + (iy / sy) * (ymax - ymin);
    const sampleY = Math.max(groundTerrainGrid.ymin, Math.min(groundTerrainGrid.ymax, worldY));
    for (let ix = 0; ix <= sx; ix++) {
      const worldX = xmin + (ix / sx) * (xmax - xmin);
      const sampleX = Math.max(groundTerrainGrid.xmin, Math.min(groundTerrainGrid.xmax, worldX));
      const idx = iy * (sx + 1) + ix;
      const terrainZ = sampleTerrainZ(groundTerrainGrid, sampleX, sampleY) ?? 0;
      const p = toLocal(worldX, worldY, terrainZ + zLift);
      positions[idx * 3 + 0] = p.x;
      positions[idx * 3 + 1] = p.y;
      positions[idx * 3 + 2] = p.z;
      uvs[idx * 2 + 0] = ix / sx;
      uvs[idx * 2 + 1] = 1 - iy / sy;
    }
  }

  const triCount = sx * sy * 2;
  const indices = new Uint32Array(triCount * 3);
  let w = 0;
  for (let iy = 0; iy < sy; iy++) {
    for (let ix = 0; ix < sx; ix++) {
      const i00 = iy * (sx + 1) + ix;
      const i10 = i00 + 1;
      const i01 = i00 + (sx + 1);
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
}

function resolvePointZ(point: Point3D, grid: TerrainGrid | null): number | null {
  if (point.hasExplicitZ !== false && Number.isFinite(point.z)) return point.z;
  const sampled = sampleTerrainZ(grid, point.x, point.y);
  if (typeof sampled === "number" && Number.isFinite(sampled)) return sampled;
  return null;
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
  return null;
}

function isLikelyPaidProvider(providerId: string): boolean {
  const k = providerId.trim().toLowerCase();
  if (!k) return false;
  if (k.startsWith("esri_") || k.startsWith("usgs_")) return false;
  return true;
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
  if (palette === "inferno" || palette === "viridis") {
    return interpolatePaletteHex(palette, t);
  }
  // turbo — inline stops (not in shared palettes.ts)
  const stops = [[48, 18, 59], [50, 21, 110], [32, 73, 156], [18, 120, 142], [59, 173, 112], [171, 220, 50], [253, 231, 37]];
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

function applyMeasureTransform(
  value: number,
  mode: SceneUiState["measureTransform"]
): number | null {
  if (!Number.isFinite(value)) return null;
  if (mode === "linear") return value;
  if (value <= 0) return null;
  if (mode === "log10") return Math.log10(value);
  return Math.log(value);
}

function categoricalMeasureColor(
  rawValue: number | null,
  palette: SceneUiState["palette"]
): string {
  if (rawValue === null || !Number.isFinite(rawValue)) return "#58a6ff";
  const bucket = Math.round(rawValue * 1000) / 1000;
  const x = Math.abs(Math.floor(bucket * 104729));
  const t = (x % 997) / 996;
  return paletteColor(t, 0, 1, palette);
}

// ── Color stop helpers ────────────────────────────────────────────────────────

function hexToRgb(hex: string): [number, number, number] {
  const h = hex.replace("#", "").padEnd(6, "0");
  return [parseInt(h.slice(0,2),16)||0, parseInt(h.slice(2,4),16)||0, parseInt(h.slice(4,6),16)||0];
}

function rgbToHex(r: number, g: number, b: number): string {
  return "#" + [r,g,b].map(v => Math.round(Math.max(0,Math.min(255,v))).toString(16).padStart(2,"0")).join("");
}

function interpolateStopsColor(stops: ColorStop[], t: number): string {
  if (!stops.length) return "#888888";
  const s = [...stops].sort((a,b) => a.pos - b.pos);
  if (t <= s[0].pos) return s[0].color;
  if (t >= s[s.length-1].pos) return s[s.length-1].color;
  for (let i = 0; i < s.length-1; i++) {
    if (t >= s[i].pos && t <= s[i+1].pos) {
      const u = (t - s[i].pos) / ((s[i+1].pos - s[i].pos) || 1);
      const [r1,g1,b1] = hexToRgb(s[i].color);
      const [r2,g2,b2] = hexToRgb(s[i+1].color);
      return rgbToHex(r1+(r2-r1)*u, g1+(g2-g1)*u, b1+(b2-b1)*u);
    }
  }
  return s[s.length-1].color;
}

function stopsToGradientCss(stops: ColorStop[]): string {
  if (!stops.length) return "#888888";
  const s = [...stops].sort((a,b) => a.pos - b.pos);
  return `linear-gradient(to right, ${s.map(st=>`${st.color} ${(st.pos*100).toFixed(1)}%`).join(", ")})`;
}

function parseCategoricalColorMap(raw: string): Record<string, string> {
  if (!raw || raw.trim().length === 0) return {};
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
      if (typeof k !== "string") continue;
      if (typeof v === "string" && /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.test(v.trim())) {
        out[k] = v.trim();
      }
    }
    return out;
  } catch {
    return {};
  }
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
        roughness={0.75}
        transparent={opacity < 0.999}
        opacity={Math.max(0.02, Math.min(1, opacity))}
        polygonOffset={true}
        polygonOffsetFactor={-2}
        polygonOffsetUnits={-2}
      />
    </mesh>
  );
}

function BlockVoxelLayer3D({
  voxels,
  style,
  domain,
  toLocal,
  lift,
  colorForMeasure,
}: {
  voxels: BlockVoxel[];
  style: LayerVizStyle;
  domain: { lo: number; hi: number };
  toLocal: (x: number, y: number, z: number) => THREE.Vector3;
  lift: number;
  colorForMeasure: (raw: number | string | null, domain: { lo: number; hi: number }) => string;
}) {
  const mergedGeometry = useMemo(() => {
    const attrKey = style.attributeKey.trim();
    const base = new THREE.BoxGeometry(1, 1, 1);
    const posAttr = base.getAttribute("position");
    const idxAttr = base.getIndex();
    if (!idxAttr) return null;

    const positions: number[] = [];
    const colors: number[] = [];
    const indices: number[] = [];
    let vertOffset = 0;

    for (const v of voxels) {
      const raw = attrKey.length > 0 ? (v.measures?.[attrKey] ?? null) : null;
      const numericRaw = typeof raw === "number" && Number.isFinite(raw) ? raw : null;
      const passesCutoff =
        typeof v.aboveCutoff === "boolean"
          ? v.aboveCutoff
          : numericRaw === null || typeof v.cutoffGrade !== "number"
            ? true
            : numericRaw >= v.cutoffGrade;
      if (!passesCutoff) continue;

      const center = toLocal(v.x, v.y, v.z + lift);
      const sx = Math.max(0.1, v.dx);
      const sy = Math.max(0.1, v.dz);
      const sz = Math.max(0.1, v.dy);
      const c = new THREE.Color(colorForMeasure(raw, domain));

      for (let i = 0; i < posAttr.count; i++) {
        positions.push(
          center.x + posAttr.getX(i) * sx,
          center.y + posAttr.getY(i) * sy,
          center.z + posAttr.getZ(i) * sz
        );
        colors.push(c.r, c.g, c.b);
      }
      for (let i = 0; i < idxAttr.count; i++) {
        indices.push(idxAttr.getX(i) + vertOffset);
      }
      vertOffset += posAttr.count;
    }

    base.dispose();
    if (positions.length === 0) return null;

    const geom = new THREE.BufferGeometry();
    geom.setAttribute("position", new THREE.Float32BufferAttribute(positions, 3));
    geom.setAttribute("color", new THREE.Float32BufferAttribute(colors, 3));
    geom.setIndex(indices);
    geom.computeVertexNormals();
    return geom;
  }, [colorForMeasure, domain, lift, style.attributeKey, toLocal, voxels]);

  const edgesGeometry = useMemo(() => {
    if (!mergedGeometry) return null;
    return new THREE.EdgesGeometry(mergedGeometry);
  }, [mergedGeometry]);

  useEffect(() => {
    return () => {
      mergedGeometry?.dispose();
      edgesGeometry?.dispose();
    };
  }, [edgesGeometry, mergedGeometry]);

  if (!mergedGeometry) return null;

  return (
    <group>
      <mesh geometry={mergedGeometry} renderOrder={20}>
        <meshStandardMaterial
          vertexColors
          transparent
          opacity={Math.max(0.05, style.opacity)}
          depthWrite={false}
          depthTest
          roughness={0.5}
          metalness={0.0}
          emissive="#1f1f1f"
          emissiveIntensity={0.45}
          toneMapped={false}
        />
      </mesh>
      {edgesGeometry ? (
      <lineSegments geometry={edgesGeometry} renderOrder={21}>
        <lineBasicMaterial
          color="#dbe7ff"
          transparent
          opacity={Math.min(0.22, Math.max(0.08, style.opacity * 0.28))}
          toneMapped={false}
        />
      </lineSegments>
      ) : null}
    </group>
  );
}

function ContourLayer3D({
  layerId,
  segs,
  style,
  toLocal,
  sceneScale,
  lift,
  groundTerrainGrid,
  radius,
  opacity,
  fixedColor,
}: {
  layerId: string;
  segs: Segment3D[];
  style: LayerVizStyle;
  toLocal: (x: number, y: number, z: number) => THREE.Vector3;
  sceneScale: number;
  lift: number;
  groundTerrainGrid: TerrainGrid | null;
  radius: number;
  opacity: number;
  fixedColor?: string;
}) {
  const renderData = useMemo(() => {
    const stitched = stitchContourSegments(segs);
    if (stitched.length === 0) return { lines: [], labels: [] } as const;

    const contourLevels = stitched
      .map((poly) => poly.level)
      .filter((v): v is number => typeof v === "number" && Number.isFinite(v));
    const clMin = contourLevels.length ? Math.min(...contourLevels) : 0;
    const clMax = contourLevels.length ? Math.max(...contourLevels) : 1;
    const clRange = clMax - clMin || 1;
    const stopsToUse =
      style.colorStops?.length >= 2
        ? style.colorStops
        : PALETTE_STOPS[style.palette] ?? PALETTE_STOPS.inferno;
    const zLift = Math.max(0.01, sceneScale * 0.00018) + lift;
    const labelFontSize = Math.max(sceneScale * 0.004, sceneScale * 0.012 * (style.labelSize / 12));
    const labelHover = Math.max(radius * 2.8, labelFontSize * 0.32, sceneScale * 0.0003);

    const lines: Array<{ id: string; color: string; points: THREE.Vector3[] }> = [];
    const labels: Array<{
      id: string;
      text: string;
      color: string;
      position: THREE.Vector3;
      rotationY: number;
      fontSize: number;
    }> = [];

    stitched.forEach((poly, idx) => {
      const passes = poly.points.length >= 8 ? 2 : poly.points.length >= 4 ? 1 : 0;
      const smoothed = passes > 0 ? smoothContourPolyline(poly.points, poly.closed, passes) : poly.points;
      const localPoints = smoothed.map((p) => {
        const z = p.hasExplicitZ ? p.z : sampleTerrainZ(groundTerrainGrid, p.x, p.y) ?? p.z;
        return toLocal(p.x, p.y, z + zLift);
      });
      const dedupedLocal: THREE.Vector3[] = [];
      for (const pt of localPoints) {
        const prev = dedupedLocal[dedupedLocal.length - 1];
        if (!prev || prev.distanceToSquared(pt) > 1e-10) dedupedLocal.push(pt);
      }
      if (poly.closed && dedupedLocal.length > 2) {
        const first = dedupedLocal[0];
        const last = dedupedLocal[dedupedLocal.length - 1];
        if (first.distanceToSquared(last) > 1e-10) dedupedLocal.push(first.clone());
      }
      if (dedupedLocal.length < 2) return;

      const t =
        typeof poly.level === "number"
          ? Math.max(0, Math.min(1, (poly.level - clMin) / clRange))
          : 0.5;
      const lineColor = fixedColor ?? interpolateStopsColor(stopsToUse, t);
      lines.push({ id: `${layerId}-line-${idx}`, color: lineColor, points: dedupedLocal });

      if (!style.showLabels || typeof poly.level !== "number") return;
      const labelText = formatContourLabel(poly.level);
      if (!labelText) return;
      const totalLength = polylineLength(dedupedLocal);
      const estimatedWidth = Math.max(labelFontSize * 2.4, labelText.length * labelFontSize * 0.62);
      if (totalLength < estimatedWidth * 1.8) return;
      const idealSpacing = Math.max(estimatedWidth * 5, sceneScale * 0.18);
      const labelCount = Math.min(3, Math.max(1, Math.floor(totalLength / idealSpacing)));
      for (let labelIdx = 0; labelIdx < labelCount; labelIdx++) {
        const sample = samplePolylineAtDistance(
          dedupedLocal,
          (totalLength * (labelIdx + 1)) / (labelCount + 1)
        );
        if (!sample) continue;
        const tangent = sample.tangent.clone();
        tangent.y = 0;
        if (tangent.lengthSq() <= 1e-8) continue;
        tangent.normalize();
        const position = sample.point.clone();
        position.y += labelHover;
        labels.push({
          id: `${layerId}-label-${idx}-${labelIdx}`,
          text: labelText,
          color: lineColor,
          position,
          rotationY: normalizeContourLabelAngle(Math.atan2(tangent.z, tangent.x)),
          fontSize: labelFontSize,
        });
      }
    });

    return { lines, labels };
  }, [fixedColor, groundTerrainGrid, layerId, lift, opacity, radius, sceneScale, segs, style, toLocal]);

  return (
    <group key={`layer-contours-${layerId}`}>
      {renderData.lines.map((line) => (
        <group key={line.id}>
          {line.points.slice(1).map((to, i) => (
            <SegmentTube
              key={`${line.id}-seg-${i}`}
              from={line.points[i]}
              to={to}
              radius={radius}
              color={line.color}
              opacity={opacity}
            />
          ))}
        </group>
      ))}
      {renderData.labels.map((label) => (
        <group
          key={label.id}
          position={[label.position.x, label.position.y, label.position.z]}
          rotation={[0, label.rotationY, 0]}
        >
          <Text
            rotation={[-Math.PI / 2, 0, 0]}
            fontSize={label.fontSize}
            color={label.color}
            anchorX="center"
            anchorY="middle"
            outlineWidth={label.fontSize * 0.1}
            outlineColor="#0d1117"
          >
            {label.text}
          </Text>
        </group>
      ))}
    </group>
  );
}

// ── 3-D IDW terrain heatmap ───────────────────────────────────────────────────
// Computes an Inverse-Distance-Weighted grid from scattered assay points and
// renders it as a canvas-textured overlay draped onto the terrain mesh when
// available, otherwise falls back to a flat plane.
function HeatmapLayer3D({
  layerId,
  pts,
  style,
  domain,
  toLocal,
  sceneScale,
  lift,
  groundTerrainGrid,
}: {
  layerId: string;
  pts: Point3D[];
  style: LayerVizStyle;
  domain: { lo: number; hi: number };
  toLocal: (x: number, y: number, z: number) => THREE.Vector3;
  sceneScale: number;
  lift: number;
  groundTerrainGrid: TerrainGrid | null;
}) {
  const result = useMemo(() => {
    const key = style.attributeKey.trim();
    if (!key) return null;

    const samples: { x: number; y: number; value: number }[] = [];
    for (const p of pts) {
      const v = p.measures?.[key];
      if (typeof v === "number" && Number.isFinite(v)) samples.push({ x: p.x, y: p.y, value: v });
    }
    if (samples.length < 3) return null;

    const xMin = Math.min(...samples.map(s => s.x));
    const xMax = Math.max(...samples.map(s => s.x));
    const yMin = Math.min(...samples.map(s => s.y));
    const yMax = Math.max(...samples.map(s => s.y));
    if (xMax <= xMin || yMax <= yMin) return null;

    const n = Math.max(64, Math.min(512, Math.trunc(style.hmGridSize ?? 256)));
    const power = Math.max(1, Math.min(4, style.hmPower ?? 2));
    const vMin = domain.lo;
    const vMax = domain.hi;
    const vRange = vMax - vMin || 1;
    const stops = style.colorStops?.length >= 2 ? style.colorStops : PALETTE_STOPS[style.palette] ?? PALETTE_STOPS.inferno;

    const canvas = document.createElement("canvas");
    canvas.width = n;
    canvas.height = n;
    const ctx = canvas.getContext("2d");
    if (!ctx) return null;
    const img = ctx.createImageData(n, n);

    // Canvas row 0 = northMax (yi=0→yMax), last row = southMin (yi=n-1→yMin).
    // We set tex.flipY=false so THREE.js doesn't double-flip; with rotation=-90°X
    // UV.y=0 lands at Three.js +Z (north) which is correct for this orientation.
    for (let yi = 0; yi < n; yi++) {
      const worldY = yMax - ((yi + 0.5) / n) * (yMax - yMin);
      for (let xi = 0; xi < n; xi++) {
        const worldX = xMin + ((xi + 0.5) / n) * (xMax - xMin);
        let num = 0, den = 0;
        for (const s of samples) {
          const dx = worldX - s.x;
          const dy = worldY - s.y;
          const d2 = dx * dx + dy * dy;
          if (d2 < 1e-12) { num = s.value; den = 1; break; }
          const w = 1 / Math.pow(d2, power * 0.5);
          num += w * s.value;
          den += w;
        }
        const iv = den > 0 ? num / den : vMin;
        const t = Math.max(0, Math.min(1, (iv - vMin) / vRange));
        const hex = interpolateStopsColor(stops, t);
        const [r, g, b] = hexToRgb(hex);
        const idx = (yi * n + xi) * 4;
        img.data[idx] = r; img.data[idx + 1] = g; img.data[idx + 2] = b; img.data[idx + 3] = 255;
      }
    }
    ctx.putImageData(img, 0, 0);
    const tex = new THREE.CanvasTexture(canvas);
    tex.flipY = false; // canvas yi=0=north; without this Three.js would reverse it
    return { tex, xMin, xMax, yMin, yMax, gridSize: n };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pts, style.attributeKey, style.colorStops, style.palette, style.hmGridSize, style.hmPower, domain.lo, domain.hi]);

  useEffect(() => () => { result?.tex.dispose(); }, [result]);
  const drapedGeom = useMemo(() => {
    if (!result) return null;
    return buildDrapedOverlayGeometry({
      xmin: result.xMin,
      xmax: result.xMax,
      ymin: result.yMin,
      ymax: result.yMax,
      segX: Math.max(16, Math.min(192, result.gridSize - 1)),
      segY: Math.max(16, Math.min(192, result.gridSize - 1)),
      groundTerrainGrid,
      toLocal,
      zLift: lift + Math.max(0.04, sceneScale * 0.0003),
    });
  }, [groundTerrainGrid, lift, result, sceneScale, toLocal]);
  useEffect(() => () => { drapedGeom?.dispose(); }, [drapedGeom]);

  if (!result) return null;
  const { tex, xMin, xMax, yMin, yMax } = result;
  const cx = (xMin + xMax) / 2;
  const cy = (yMin + yMax) / 2;

  // Sample terrain at a 7×7 grid to find the maximum elevation under the extent
  // so the plane is always visible above uneven terrain.
  let maxTerrainZ = 0;
  if (groundTerrainGrid) {
    for (let xi = 0; xi <= 6; xi++) {
      for (let yi = 0; yi <= 6; yi++) {
        const z = sampleTerrainZ(groundTerrainGrid, xMin + (xi / 6) * (xMax - xMin), yMin + (yi / 6) * (yMax - yMin));
        if (z !== null) maxTerrainZ = Math.max(maxTerrainZ, z);
      }
    }
  }

  const planeW = xMax - xMin;
  const planeH = yMax - yMin;
  const elevation = maxTerrainZ + lift + Math.max(1, sceneScale * 0.0012);
  const center = toLocal(cx, cy, elevation);

  if (drapedGeom) {
    return (
      <mesh key={`hm3d-${layerId}`} geometry={drapedGeom} renderOrder={5}>
        <meshBasicMaterial
          map={tex}
          transparent
          opacity={style.opacity * 0.82}
          depthWrite={false}
          polygonOffset
          polygonOffsetFactor={-2}
          polygonOffsetUnits={-2}
          side={THREE.DoubleSide}
        />
      </mesh>
    );
  }

  return (
    <mesh
      key={`hm3d-${layerId}`}
      position={[center.x, center.y, center.z]}
      rotation={[-Math.PI / 2, 0, 0]}
      renderOrder={5}
    >
      <planeGeometry args={[planeW, planeH]} />
      <meshBasicMaterial
        map={tex}
        transparent
        opacity={style.opacity * 0.82}
        depthWrite={false}
        side={THREE.DoubleSide}
      />
    </mesh>
  );
}

// ── Pre-computed surface heatmap from assay_heatmap pipeline node ─────────────
// Grid values are the measure (e.g. Au ppm), stored south→north (row 0 = ymin).
// Rendered as a canvas-textured overlay draped onto terrain when available.
function SurfaceHeatmapLayer3D({
  heatmapSurface,
  style,
  toLocal,
  sceneScale,
  lift,
  groundTerrainGrid,
}: {
  heatmapSurface: HeatmapSurfaceGrid;
  style: LayerVizStyle;
  toLocal: (x: number, y: number, z: number) => THREE.Vector3;
  sceneScale: number;
  lift: number;
  groundTerrainGrid: TerrainGrid | null;
}) {
  const result = useMemo(() => {
    const { nx, ny, xmin, xmax, ymin, ymax, values } = heatmapSurface.grid;
    const finiteVals = values.filter((v): v is number => v !== null && Number.isFinite(v));
    if (finiteVals.length === 0) return null;

    // Value domain: use the full grid range; layer style can clip further.
    const vMin = Math.min(...finiteVals);
    const vMax = Math.max(...finiteVals);
    // Honour per-layer clamp if fixedMin/fixedMax are set, else use data range.
    const domLo = style.rampNormMode === "fixed" ? style.fixedMin : vMin;
    const domHi = style.rampNormMode === "fixed" ? style.fixedMax : vMax;
    const vRange = domHi - domLo || 1;
    const stops = style.colorStops?.length >= 2 ? style.colorStops : PALETTE_STOPS[style.palette] ?? PALETTE_STOPS.inferno;

    const canvas = document.createElement("canvas");
    canvas.width = nx;
    canvas.height = ny;
    const ctx = canvas.getContext("2d");
    if (!ctx) return null;
    const img = ctx.createImageData(nx, ny);

    // Grid is stored south→north (row 0 = ymin). Flip Y for canvas so yi=0=north.
    // tex.flipY=false below ensures Three.js doesn't double-flip.
    for (let yi = 0; yi < ny; yi++) {
      const srcYi = ny - 1 - yi; // flip: canvas top = grid north row
      for (let xi = 0; xi < nx; xi++) {
        const v = values[srcYi * nx + xi];
        const idx = (yi * nx + xi) * 4;
        if (v === null || !Number.isFinite(v)) { img.data[idx + 3] = 0; continue; }
        const t = Math.max(0, Math.min(1, (v - domLo) / vRange));
        const hex = interpolateStopsColor(stops, t);
        const [r, g, b] = hexToRgb(hex);
        img.data[idx] = r; img.data[idx + 1] = g; img.data[idx + 2] = b; img.data[idx + 3] = 255;
      }
    }
    ctx.putImageData(img, 0, 0);
    const tex = new THREE.CanvasTexture(canvas);
    tex.flipY = false; // canvas yi=0=north; correct with Rx(-90°) rotation
    return { tex, xmin, xmax, ymin, ymax };
  }, [heatmapSurface, style.colorStops, style.palette, style.rampNormMode, style.fixedMin, style.fixedMax]);

  useEffect(() => () => { result?.tex.dispose(); }, [result]);
  const drapedGeom = useMemo(() => {
    if (!result) return null;
    return buildDrapedOverlayGeometry({
      xmin: result.xmin,
      xmax: result.xmax,
      ymin: result.ymin,
      ymax: result.ymax,
      segX: Math.max(16, Math.min(192, heatmapSurface.grid.nx - 1)),
      segY: Math.max(16, Math.min(192, heatmapSurface.grid.ny - 1)),
      groundTerrainGrid,
      toLocal,
      zLift: lift + Math.max(0.04, sceneScale * 0.0003),
    });
  }, [groundTerrainGrid, heatmapSurface.grid.nx, heatmapSurface.grid.ny, lift, result, sceneScale, toLocal]);
  useEffect(() => () => { drapedGeom?.dispose(); }, [drapedGeom]);

  if (!result) return null;
  const { tex, xmin, xmax, ymin, ymax } = result;
  const cx = (xmin + xmax) / 2;
  const cy = (ymin + ymax) / 2;

  let maxTerrainZ = 0;
  if (groundTerrainGrid) {
    for (let xi = 0; xi <= 6; xi++) {
      for (let yi = 0; yi <= 6; yi++) {
        const z = sampleTerrainZ(groundTerrainGrid, xmin + (xi / 6) * (xmax - xmin), ymin + (yi / 6) * (ymax - ymin));
        if (z !== null) maxTerrainZ = Math.max(maxTerrainZ, z);
      }
    }
  }

  const elevation = maxTerrainZ + lift + Math.max(1, sceneScale * 0.0012);
  const center = toLocal(cx, cy, elevation);

  if (drapedGeom) {
    return (
      <mesh geometry={drapedGeom} renderOrder={5}>
        <meshBasicMaterial
          map={tex}
          transparent
          opacity={style.opacity * 0.85}
          depthWrite={false}
          polygonOffset
          polygonOffsetFactor={-2}
          polygonOffsetUnits={-2}
          side={THREE.DoubleSide}
        />
      </mesh>
    );
  }

  return (
    <mesh
      position={[center.x, center.y, center.z]}
      rotation={[-Math.PI / 2, 0, 0]}
      renderOrder={5}
    >
      <planeGeometry args={[xmax - xmin, ymax - ymin]} />
      <meshBasicMaterial
        map={tex}
        transparent
        opacity={style.opacity * 0.85}
        depthWrite={false}
        side={THREE.DoubleSide}
      />
    </mesh>
  );
}

function SectionPlaneLayer3D({
  sectionPlane,
  style,
  toLocal,
}: {
  sectionPlane: SectionPlaneGrid;
  style: LayerVizStyle;
  toLocal: (x: number, y: number, z: number) => THREE.Vector3;
}) {
  const result = useMemo(() => {
    const key =
      style.attributeKey.trim() ||
      Object.keys(sectionPlane.measureGrids)[0] ||
      "";
    if (!key) return null;
    const values = sectionPlane.measureGrids[key];
    if (!values || values.length !== sectionPlane.nx * sectionPlane.nz) return null;
    const finiteVals = values.filter((v): v is number => v !== null && Number.isFinite(v));
    if (finiteVals.length === 0) return null;
    const domLo = style.rampNormMode === "fixed" ? style.fixedMin : Math.min(...finiteVals);
    const domHi = style.rampNormMode === "fixed" ? style.fixedMax : Math.max(...finiteVals);
    const vRange = domHi - domLo || 1;
    const stops =
      style.colorStops?.length >= 2
        ? style.colorStops
        : PALETTE_STOPS[style.palette] ?? PALETTE_STOPS.inferno;

    const canvas = document.createElement("canvas");
    canvas.width = sectionPlane.nx;
    canvas.height = sectionPlane.nz;
    const ctx = canvas.getContext("2d");
    if (!ctx) return null;
    const img = ctx.createImageData(sectionPlane.nx, sectionPlane.nz);
    for (let iz = 0; iz < sectionPlane.nz; iz++) {
      for (let ix = 0; ix < sectionPlane.nx; ix++) {
        const v = values[iz * sectionPlane.nx + ix];
        const idx = (iz * sectionPlane.nx + ix) * 4;
        if (v === null || !Number.isFinite(v)) {
          img.data[idx + 3] = 0;
          continue;
        }
        const t = Math.max(0, Math.min(1, (v - domLo) / vRange));
        const hex = interpolateStopsColor(stops, t);
        const [r, g, b] = hexToRgb(hex);
        img.data[idx] = r;
        img.data[idx + 1] = g;
        img.data[idx + 2] = b;
        img.data[idx + 3] = 255;
      }
    }
    ctx.putImageData(img, 0, 0);
    const tex = new THREE.CanvasTexture(canvas);
    tex.flipY = false;
    return { tex };
  }, [sectionPlane, style.attributeKey, style.colorStops, style.palette, style.rampNormMode, style.fixedMin, style.fixedMax]);

  useEffect(() => () => { result?.tex.dispose(); }, [result]);
  if (!result) return null;

  const halfLen = 0.5 * (sectionPlane.sMax - sectionPlane.sMin);
  const centerS = 0.5 * (sectionPlane.sMax + sectionPlane.sMin);
  const theta = (sectionPlane.azimuthDeg * Math.PI) / 180.0;
  const ux = Math.cos(theta);
  const uy = Math.sin(theta);
  const worldCx = sectionPlane.centerX + centerS * ux;
  const worldCy = sectionPlane.centerY + centerS * uy;
  const worldCz = 0.5 * (sectionPlane.zTop + sectionPlane.zBottom);
  const center = toLocal(worldCx, worldCy, worldCz);
  const width = Math.max(1.0, sectionPlane.sMax - sectionPlane.sMin);
  const depth = Math.max(1.0, Math.abs(sectionPlane.zTop - sectionPlane.zBottom));

  return (
    <mesh
      position={[center.x, center.y, center.z]}
      rotation={[0, -theta, 0]}
      renderOrder={6}
    >
      <planeGeometry args={[width, depth]} />
      <meshBasicMaterial
        map={result.tex}
        transparent
        opacity={Math.max(0.05, style.opacity)}
        side={THREE.DoubleSide}
        depthWrite={false}
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
  const onLoadFailureRef = useRef(onLoadFailure);
  const onLoadSuccessRef = useRef(onLoadSuccess);
  useEffect(() => {
    onLoadFailureRef.current = onLoadFailure;
  }, [onLoadFailure]);
  useEffect(() => {
    onLoadSuccessRef.current = onLoadSuccess;
  }, [onLoadSuccess]);
  useEffect(() => {
    const candidates = urls
      .map((u) => u.trim())
      .filter((u, i, arr) => u.length > 0 && arr.indexOf(u) === i);
    if (candidates.length === 0) {
      setTexture((prev) => {
        // Don't dispose if it's in the module cache — another mount may reuse it.
        return null;
      });
      return;
    }

    // Fast path: first candidate is already in the module-level texture cache.
    // This avoids a network round-trip on Canvas remounts when the URL hasn't
    // changed (e.g. initial load, camera reset, tab switch).
    for (const candidate of candidates) {
      const cached = drapeTextureCache.get(candidate);
      if (cached) {
        drapeTextureCache.touch(candidate);
        setTexture(cached);
        onLoadSuccessRef.current?.(candidate);
        return;
      }
    }

    let cancelled = false;

    const tryLoad = (idx: number) => {
      if (cancelled) return;
      if (idx >= candidates.length) {
        setTexture(() => null);
        onLoadFailureRef.current?.(candidates);
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
          // Store in module cache before setting state so that any concurrent
          // mount can find it immediately.
          drapeTextureCache.set(candidates[idx], tex);
          setTexture(tex);
          onLoadSuccessRef.current?.(candidates[idx]);
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
      // Don't dispose the texture on cleanup — it lives in drapeTextureCache
      // and may be reused by the next mount.  The cache eviction logic handles
      // GPU memory lifecycle.
      setTexture(null);
    };
  }, [urls]);

  if (!texture) return null;
  // depthWrite must be false so that partial-opacity drape doesn't write solid
  // depth values — otherwise geometry behind the terrain (e.g. underground
  // voxels) fails the depth test and disappears instead of showing through.
  const clampedOpacity = Math.max(0, Math.min(1, opacity));
  const material = (
    <meshBasicMaterial
      map={texture}
      transparent
      opacity={clampedOpacity}
      depthWrite={false}
      side={THREE.DoubleSide}
    />
  );

  if (geometry) {
    return <mesh geometry={geometry}>{material}</mesh>;
  }
  if (width <= 0 || depth <= 0) return null;
  return (
    <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, y, 0]}>
      <planeGeometry args={[width, depth]} />
      {material}
    </mesh>
  );
}

// ── ColorRampEditor component ─────────────────────────────────────────────────

type ColorRampEditorProps = {
  stops: ColorStop[];
  onStopsChange: (s: ColorStop[]) => void;
  transform: "linear" | "log10" | "ln";
  onTransformChange: (t: "linear" | "log10" | "ln") => void;
  clampLow: number;
  clampHigh: number;
  onClampChange: (lo: number, hi: number) => void;
  rawValues: number[];
  dataMin: number;
  dataMax: number;
};

function ColorRampEditor({
  stops, onStopsChange, transform, onTransformChange,
  clampLow, clampHigh, onClampChange,
  rawValues, dataMin, dataMax,
}: ColorRampEditorProps) {
  const [selIdx, setSelIdx] = React.useState(0);
  const trackRef = React.useRef<HTMLDivElement>(null);
  const dragging = React.useRef<{origIdx: number} | null>(null);
  const [isDragging, setIsDragging] = React.useState(false);

  const sorted = React.useMemo(() => [...stops].sort((a,b) => a.pos - b.pos), [stops]);
  const gradCss = stopsToGradientCss(sorted);

  // Histogram bins
  const histBins = React.useMemo(() => {
    if (rawValues.length < 2) return Array(24).fill(0);
    const vals = rawValues.filter(v => Number.isFinite(v));
    if (vals.length < 2) return Array(24).fill(0);
    const vs = [...vals].sort((a,b)=>a-b);
    const lo = vs[Math.floor(clampLow/100 * (vs.length-1))];
    const hi = vs[Math.min(vs.length-1, Math.floor(clampHigh/100 * (vs.length-1)))];
    const range = hi - lo || 1;
    const N = 24;
    const bins = Array(N).fill(0);
    for (const v of vs) {
      const i = Math.min(N-1, Math.max(0, Math.floor((v - lo)/range * N)));
      bins[i]++;
    }
    const mx = Math.max(...bins, 1);
    return bins.map(b => b/mx);
  }, [rawValues, clampLow, clampHigh]);

  // Pointer capture drag — stays on the track element
  const handleStopPointerDown = React.useCallback((e: React.PointerEvent, origIdx: number, sortedI: number) => {
    e.preventDefault();
    e.stopPropagation();
    e.currentTarget.setPointerCapture(e.pointerId);
    setSelIdx(sortedI);
    dragging.current = { origIdx };
    setIsDragging(true);
  }, []);

  const handleTrackPointerMove = React.useCallback((e: React.PointerEvent) => {
    if (!dragging.current || !trackRef.current) return;
    const rect = trackRef.current.getBoundingClientRect();
    const pos = Math.max(0.001, Math.min(0.999, (e.clientX - rect.left) / rect.width));
    const newStops = stops.map((s, i) => i === dragging.current!.origIdx ? { ...s, pos } : s);
    onStopsChange(newStops);
  }, [stops, onStopsChange]);

  const handleTrackPointerUp = React.useCallback(() => {
    dragging.current = null;
    setIsDragging(false);
  }, []);

  const addStop = React.useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    if (isDragging || !trackRef.current) return;
    // Check if near existing stop
    const rect = trackRef.current.getBoundingClientRect();
    const pos = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
    const nearIdx = sorted.findIndex(s => Math.abs(s.pos - pos) < 0.04);
    if (nearIdx >= 0) { setSelIdx(nearIdx); return; }
    const color = interpolateStopsColor(sorted, pos);
    const newStops = [...stops, { pos, color }].sort((a,b) => a.pos - b.pos);
    onStopsChange(newStops);
    setSelIdx(newStops.findIndex(s => Math.abs(s.pos - pos) < 0.002));
  }, [isDragging, sorted, stops, onStopsChange]);

  const selStop = sorted[selIdx] ?? sorted[0];
  const selOrigIdx = selStop ? stops.findIndex(s => Math.abs(s.pos - selStop.pos) < 0.0001 && s.color === selStop.color) : -1;

  const rampSection: React.CSSProperties = {
    display: "flex", flexDirection: "column", gap: 0,
    background: "#0d1117", borderRadius: 6,
    border: "1px solid #21262d",
    overflow: "hidden",
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>

      {/* Scale selector row */}
      <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
        <span style={{ fontSize: 8.5, fontWeight: 700, letterSpacing: "0.07em", color: "#484f58", textTransform: "uppercase", flex: 1 }}>Scale</span>
        {(["linear","log10","ln"] as const).map(t => (
          <button key={t} type="button"
            onClick={() => onTransformChange(t)}
            style={{
              fontSize: 10, fontWeight: 600, padding: "2px 7px", borderRadius: 4, cursor: "pointer",
              letterSpacing: "0.03em", fontFamily: "inherit",
              background: transform === t ? "#388bfd22" : "transparent",
              border: `1px solid ${transform === t ? "#388bfd" : "#30363d"}`,
              color: transform === t ? "#58a6ff" : "#6e7681",
              transition: "all 0.1s",
            }}>
            {t === "log10" ? "Log₁₀" : t === "ln" ? "Ln" : "Lin"}
          </button>
        ))}
      </div>

      {/* Gradient + histogram + stop track — unified block */}
      <div style={rampSection}>
        {/* Gradient bar with histogram */}
        <div style={{
          height: 18,
          background: gradCss,
          cursor: "crosshair",
          position: "relative",
          overflow: "hidden",
        }}
          onClick={addStop}
        >
          {/* Histogram bars (inverted, from bottom, dark overlay) */}
          <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "flex-end", pointerEvents: "none" }}>
            {histBins.map((h, i) => (
              <div key={i} style={{
                flex: 1,
                height: `${Math.round(h * 70)}%`,
                background: "rgba(0,0,0,0.28)",
                borderLeft: i > 0 ? "0.5px solid rgba(0,0,0,0.12)" : "none",
              }} />
            ))}
          </div>
        </div>

        {/* Stop track */}
        <div
          ref={trackRef}
          style={{
            position: "relative",
            height: 14,
            background: "#161b22",
            cursor: isDragging ? "ew-resize" : "crosshair",
            borderTop: "1px solid #21262d",
            userSelect: "none",
          }}
          onClick={addStop}
          onPointerMove={handleTrackPointerMove}
          onPointerUp={handleTrackPointerUp}
        >
          {/* Guide line */}
          <div style={{ position: "absolute", top: "50%", left: 8, right: 8, height: 1, background: "#30363d", transform: "translateY(-50%)" }} />

          {sorted.map((s, i) => {
            const origIdx = stops.findIndex(os => Math.abs(os.pos - s.pos) < 0.0001 && os.color === s.color);
            const isSel = i === selIdx;
            return (
              <div
                key={i}
                title={`Stop ${i+1}: ${(s.pos*100).toFixed(0)}%`}
                style={{
                  position: "absolute",
                  left: `calc(${s.pos*100}% - 5px + ${s.pos < 0.05 ? 5 : s.pos > 0.95 ? -5 : 0}px)`,
                  top: 2,
                  width: 10,
                  height: 10,
                  background: s.color,
                  border: `2px solid ${isSel ? "#58a6ff" : "rgba(200,200,200,0.4)"}`,
                  borderRadius: 3,
                  cursor: "ew-resize",
                  zIndex: isSel ? 3 : 1,
                  boxShadow: isSel
                    ? `0 0 0 3px rgba(88,166,255,0.25), 0 2px 6px rgba(0,0,0,0.6)`
                    : "0 1px 4px rgba(0,0,0,0.5)",
                  touchAction: "none",
                }}
                onPointerDown={(e) => handleStopPointerDown(e, origIdx, i)}
                onClick={(e) => { e.stopPropagation(); setSelIdx(i); }}
              />
            );
          })}
        </div>
      </div>

      {/* Selected stop controls */}
      {selStop && (
        <div style={{
          display: "flex", alignItems: "center", gap: 4,
          background: "#161b22", borderRadius: 5, padding: "3px 6px",
          border: "1px solid #21262d",
        }}>
          <div style={{ position: "relative", width: 20, height: 20, flexShrink: 0 }}>
            <div style={{
              position: "absolute", inset: 0, background: selStop.color,
              borderRadius: 4, border: "2px solid rgba(255,255,255,0.2)",
              pointerEvents: "none",
            }} />
            <input type="color" value={selStop.color}
              onChange={(e) => {
                if (selOrigIdx < 0) return;
                onStopsChange(stops.map((s,i) => i === selOrigIdx ? {...s, color: e.target.value} : s));
              }}
              style={{ opacity: 0, position: "absolute", inset: 0, width: "100%", height: "100%", cursor: "pointer", padding: 0, border: "none" }}
            />
          </div>
          <span style={{ fontSize: 8.5, color: "#6e7681" }}>pos</span>
          <input type="number" min={0} max={100} step={0.5}
            value={+(selStop.pos * 100).toFixed(1)}
            onChange={(e) => {
              if (selOrigIdx < 0) return;
              const pos = Math.max(0, Math.min(1, Number(e.target.value) / 100));
              onStopsChange(stops.map((s,i) => i === selOrigIdx ? {...s, pos} : s));
            }}
            style={{
              width: 38, fontSize: 11, textAlign: "right", fontFamily: "ui-monospace,monospace",
              background: "#0d1117", border: "1px solid #30363d", borderRadius: 4,
              color: "#c9d1d9", padding: "2px 5px",
            }}
          />
          <span style={{ fontSize: 8.5, color: "#6e7681" }}>%</span>
          <div style={{ flex: 1 }} />
          <button type="button" title="Reverse ramp"
            onClick={() => {
              const rev = [...stops].map(s => ({ ...s, pos: 1 - s.pos }));
              onStopsChange(rev);
            }}
            style={{
              fontSize: 10, padding: "2px 6px", borderRadius: 3, cursor: "pointer",
              background: "transparent", border: "1px solid #30363d", color: "#8b949e",
            }}>↔</button>
          {stops.length > 2 && (
            <button type="button" title="Delete stop"
              onClick={() => {
                if (selOrigIdx < 0) return;
                const ns = stops.filter((_,i) => i !== selOrigIdx);
                onStopsChange(ns);
                setSelIdx(Math.min(selIdx, ns.length - 1));
              }}
              style={{
                fontSize: 11, padding: "2px 7px", borderRadius: 3, cursor: "pointer",
                background: "rgba(248,81,73,0.08)", border: "1px solid rgba(248,81,73,0.25)",
                color: "#f85149",
              }}>×</button>
          )}
        </div>
      )}

      {/* Preset palette row */}
      <div style={{ display: "flex", gap: 4 }}>
        {(Object.entries(PALETTE_STOPS) as [string, ColorStop[]][]).map(([key, ps]) => (
          <button key={key} type="button" title={key}
            onClick={() => { onStopsChange([...ps]); }}
            style={{
              flex: 1, height: 12, borderRadius: 3, cursor: "pointer", padding: 0,
              background: stopsToGradientCss(ps),
              border: stopsToGradientCss(stops) === stopsToGradientCss(ps)
                ? "2px solid #58a6ff" : "1px solid rgba(255,255,255,0.1)",
              boxSizing: "border-box",
            }}
          />
        ))}
      </div>

      {/* Clip range — dual compact sliders with value labels */}
      <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
          <span style={{ fontSize: 8.5, fontWeight: 700, letterSpacing: "0.07em", color: "#484f58", textTransform: "uppercase", flex: 1 }}>Clip</span>
          {dataMin !== dataMax && (
            <span style={{ fontSize: 8.5, color: "#484f58", fontFamily: "ui-monospace,monospace" }}>
              {dataMin.toPrecision(3)} – {dataMax.toPrecision(3)}
            </span>
          )}
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "22px 1fr 26px", gap: 4, alignItems: "center" }}>
          <span style={{ fontSize: 9.5, color: "#6e7681", textAlign: "right" }}>low</span>
          <input type="range" min={0} max={49} step={1} value={clampLow}
            onChange={(e) => onClampChange(Number(e.target.value), Math.max(Number(e.target.value)+1, clampHigh))}
            style={{ width: "100%" }}
          />
          <span style={{ fontSize: 9.5, color: "#c9d1d9", fontFamily: "ui-monospace,monospace", textAlign: "right" }}>{clampLow}%</span>
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "22px 1fr 26px", gap: 4, alignItems: "center" }}>
          <span style={{ fontSize: 9.5, color: "#6e7681", textAlign: "right" }}>high</span>
          <input type="range" min={51} max={100} step={1} value={clampHigh}
            onChange={(e) => onClampChange(Math.min(Number(e.target.value)-1, clampLow), Number(e.target.value))}
            style={{ width: "100%" }}
          />
          <span style={{ fontSize: 9.5, color: "#c9d1d9", fontFamily: "ui-monospace,monospace", textAlign: "right" }}>{clampHigh}%</span>
        </div>
      </div>

    </div>
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
    blockVoxels: [],
    terrainPoints: [],
    terrainGrids: [],
    terrainGrid: null,
    aoiBounds: null,
    totalArtifacts: 0,
    sourceLayers: [],
    heatmapSurfaces: [],
    sectionPlanes: [],
  });
  const [ui, setUi] = useState<SceneUiState>(DEFAULT_UI);
  const [configHydrated, setConfigHydrated] = useState(false);
  const [cameraResetToken, setCameraResetToken] = useState(0);
  const [drapeUrls, setDrapeUrls] = useState<string[]>([]);
  const [drapeStatus, setDrapeStatus] = useState<string>("Drape off.");
  const [drapeLoadError, setDrapeLoadError] = useState<string | null>(null);
  const [contractImagery, setContractImagery] = useState<RasterOverlayContract | null>(null);
  const [contractLayerStack, setContractLayerStack] = useState<LayerStackContract | null>(null);
  const [draggingLayer, setDraggingLayer] = useState<string | null>(null);
  const [expandedLayers, setExpandedLayers] = useState<Record<string, boolean>>({
    esri_drape: false,
    terrain_points: false,
    contours: false,
    trajectories: false,
    grade_segments: true,
    assay_points: false,
    section_plane: true,
  });
  const savedRef = useRef<string>("");
  const saveTidRef = useRef<number | null>(null);
  const autoRefitSigRef = useRef<string>("");
  const lastSceneLoadSigRef = useRef<string>("");
  const sceneLoadInFlightRef = useRef<string>("");

  const setDrapeUrlsStable = useCallback((next: string[]) => {
    setDrapeUrls((prev) => (sameStringArray(prev, next) ? prev : next));
  }, []);

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
          measureColorMode:
            uiRaw.measure_color_mode === "categorical" || uiRaw.measure_color_mode === "continuous"
              ? uiRaw.measure_color_mode
              : DEFAULT_UI.measureColorMode,
          measureTransform:
            uiRaw.measure_transform === "log10" || uiRaw.measure_transform === "ln" || uiRaw.measure_transform === "linear"
              ? uiRaw.measure_transform
              : DEFAULT_UI.measureTransform,
          categoricalColorMap:
            typeof uiRaw.categorical_color_map === "string"
              ? uiRaw.categorical_color_map
              : DEFAULT_UI.categoricalColorMap,
          groundSurfaceKey:
            typeof uiRaw.ground_surface_key === "string"
              ? uiRaw.ground_surface_key
              : DEFAULT_UI.groundSurfaceKey,
          pointShape:
            uiRaw.point_shape === "box" || uiRaw.point_shape === "diamond" || uiRaw.point_shape === "sphere"
              ? uiRaw.point_shape
              : DEFAULT_UI.pointShape,
          layerStyles:
            uiRaw.layer_styles &&
            typeof uiRaw.layer_styles === "object" &&
            !Array.isArray(uiRaw.layer_styles)
              ? (uiRaw.layer_styles as Record<string, LayerVizStyle>)
              : {},
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
          ambientIntensity:
            typeof uiRaw.ambient_intensity === "number"
              ? Math.max(0, Math.min(2, uiRaw.ambient_intensity))
              : DEFAULT_UI.ambientIntensity,
          fogEnabled: uiRaw.fog_enabled !== false,
          gridEnabled: uiRaw.grid_enabled !== false,
          bgPreset:
            uiRaw.bg_preset === "dusk" || uiRaw.bg_preset === "dawn" || uiRaw.bg_preset === "overcast"
              ? (uiRaw.bg_preset as SceneUiState["bgPreset"])
              : "night",
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
            measure_color_mode: ui.measureColorMode,
            measure_transform: ui.measureTransform,
            categorical_color_map: ui.categoricalColorMap,
            ground_surface_key: ui.groundSurfaceKey,
            point_shape: ui.pointShape,
            layer_styles: ui.layerStyles,
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
            ambient_intensity: ui.ambientIntensity,
            fog_enabled: ui.fogEnabled,
            grid_enabled: ui.gridEnabled,
            bg_preset: ui.bgPreset,
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
    const source = manifestArtifacts.length ? manifestArtifacts : artifacts.filter((a) => inputLinks.includes(a.node_id));
    const sourceSig = source
      .map((a) => `${a.node_id}:${a.key}:${a.content_hash}`)
      .sort((a, b) => a.localeCompare(b))
      .join("|");
    const manifestSig = manifestLayers
      .map((ml) => `${ml.source_node_id ?? ""}:${ml.source_node_kind ?? ""}:${ml.artifact_key}:${ml.content_hash}:${ml.to_port ?? ""}`)
      .sort((a, b) => a.localeCompare(b))
      .join("|");
    const loadSig = `${graphId}|${viewerNodeId}|${sourceSig}|${manifestSig}`;
    void (async () => {
      if (source.length === 0) {
        lastSceneLoadSigRef.current = loadSig;
        sceneLoadInFlightRef.current = "";
        setContractImagery(null);
        setContractLayerStack(null);
        setSceneData({
          traces: [],
          drillSegments: [],
          contourSegments: [],
          assayPoints: [],
          blockVoxels: [],
          terrainPoints: [],
          terrainGrids: [],
          terrainGrid: null,
          aoiBounds: null,
          totalArtifacts: 0,
          sourceLayers: [],
          heatmapSurfaces: [],
          sectionPlanes: [],
        });
        setStatus(inputLinks.length ? "No upstream 3D artifacts yet. Queue run, run worker, then refresh." : "No compatible inputs wired into this 3D viewer.");
        return;
      }
      if (lastSceneLoadSigRef.current === loadSig || sceneLoadInFlightRef.current === loadSig) {
        return;
      }
      sceneLoadInFlightRef.current = loadSig;

      const manifestByArtifact = new Map<string, ViewerManifestLayer>();
      for (const ml of manifestLayers) manifestByArtifact.set(`${ml.artifact_key}:${ml.content_hash}`, ml);

      const allTraces: Segment3D[] = [];
      const allSegs: Segment3D[] = [];
      const allContours: Segment3D[] = [];
      const allPoints: Point3D[] = [];
      const allBlocks: BlockVoxel[] = [];
      const allTerrain: TerrainPoint[] = [];
      const allSourceLayers: SourceLayer[] = [];
      const allHeatmapSurfaces: HeatmapSurfaceGrid[] = [];
      const allSectionPlanes: SectionPlaneGrid[] = [];
      const seenSourceLayerIds = new Set<string>();
      const terrainCandidates: Array<{
        id: string;
        label: string;
        rank: number;
        portOrder: number;
        grid: TerrainGrid;
      }> = [];
      let bestTerrainGrid: TerrainGrid | null = null;
      let bestTerrainPick: TerrainGridPick | null = null;
      let mergedAoiBounds: { xmin: number; xmax: number; ymin: number; ymax: number } | null = null;
      let imageryContract: RasterOverlayContract | null = null;
      let layerStackContract: LayerStackContract | null = null;
      let loaded = 0;

      for (const art of source) {
        try {
          const txt = await fetchArtifactTextCached(art.url);
          try {
            const raw = JSON.parse(txt) as unknown;
            const maybeImagery = parseRasterOverlayContract(raw);
            if (!imageryContract && maybeImagery) imageryContract = maybeImagery;
            const maybeLayerStack = parseLayerStackContract(raw);
            if (!layerStackContract && maybeLayerStack) layerStackContract = maybeLayerStack;
          } catch {
            /* ignore non-json */
          }
          const artManifest = manifestByArtifact.get(`${art.key}:${art.content_hash}`);
          const artNodeId = artManifest?.source_node_id ?? (art as unknown as Record<string,string>)["node_id"] ?? art.key.replace(/\//g, "_");
          const artNodeKind = artManifest?.source_node_kind ?? "unknown";

          // ── assay_heatmap: pre-computed IDW grid — render as coloured overlay, NOT terrain ──
          if (artNodeKind === "assay_heatmap") {
            try {
              const raw = JSON.parse(txt) as Record<string, unknown>;
              const sg = raw.surface_grid as Record<string, unknown> | undefined;
              if (sg && typeof sg === "object" && !Array.isArray(sg)) {
                const nx = typeof sg.nx === "number" ? Math.trunc(sg.nx) : 0;
                const ny = typeof sg.ny === "number" ? Math.trunc(sg.ny) : 0;
                const xmin = typeof sg.xmin === "number" ? sg.xmin : null;
                const xmax = typeof sg.xmax === "number" ? sg.xmax : null;
                const ymin = typeof sg.ymin === "number" ? sg.ymin : null;
                const ymax = typeof sg.ymax === "number" ? sg.ymax : null;
                const rawVals = Array.isArray(sg.values) ? sg.values as unknown[] : [];
                if (nx > 1 && ny > 1 && xmin !== null && xmax !== null && ymin !== null && ymax !== null && rawVals.length === nx * ny) {
                  const id = `heatmap_surface__${artNodeId}`;
                  if (!seenSourceLayerIds.has(id)) {
                    seenSourceLayerIds.add(id);
                    const values = rawVals.map(v => (typeof v === "number" && Number.isFinite(v)) ? v : null);
                    allHeatmapSurfaces.push({ id, nodeId: artNodeId, nodeKind: artNodeKind, label: "Assay Heatmap", grid: { nx, ny, xmin, xmax, ymin, ymax, values } });
                    allSourceLayers.push({
                      id,
                      baseType: "heatmap_surface",
                      nodeId: artNodeId,
                      nodeKind: artNodeKind,
                      label: "Assay Heatmap",
                      dotColor: "#fb923c",
                      measureCandidates: [],
                      editable: ["visible", "opacity"],
                    });
                  }
                }
              }
            } catch { /* ignore */ }
            loaded += 1;
            continue; // don't run through parseSceneJson — surface_grid values are NOT elevation
          }

          const parsed = parseSceneJson(txt, artManifest);
          loaded += 1;
          allSectionPlanes.push(...parsed.sectionPlanes);
          {
            const ml = artManifest;
            const nodeId = artNodeId;
            const nodeKind = artNodeKind;
            const makeId = (base: string) => `${base}__${nodeId}`;
            const fmtKind = (k: string) => k.replace(/_/g, " ").replace(/\b\w/g, (c: string) => c.toUpperCase());
            const editable =
              ml?.presentation &&
              typeof ml.presentation === "object" &&
              !Array.isArray(ml.presentation) &&
              Array.isArray(ml.presentation.editable)
                ? (ml.presentation.editable as unknown[])
                    .filter((x): x is string => typeof x === "string" && x.trim().length > 0)
                : [];
            const measureCandidates = parsed.measureCandidates.slice();

            const taggedSegs   = parsed.drillSegments.map(s => ({...s, sourceLayerId: makeId("grade_segments")}));
            const taggedPts    = parsed.assayPoints.map(p   => ({...p, sourceLayerId: makeId("assay_points")}));
            const taggedTraces = parsed.traces.map(t        => ({...t, sourceLayerId: makeId("trajectories")}));

            allSegs.push(...taggedSegs);
            allPoints.push(...taggedPts);
            allTraces.push(...taggedTraces);

            // Tag contour segments with the source node so they can be independently styled.
            const taggedContours = parsed.contourSegments.map(s => ({...s, sourceLayerId: makeId("contours")}));
            allContours.push(...taggedContours);
            const taggedBlocks = parsed.blockVoxels.map(b => ({...b, sourceLayerId: makeId("block_voxels")}));
            allBlocks.push(...taggedBlocks);

            for (const [baseType, hasItems] of [
              ["assay_points",   parsed.assayPoints.length > 0],
              ["grade_segments", parsed.drillSegments.length > 0],
              ["trajectories",   parsed.traces.length > 0],
              ["contours",       parsed.contourSegments.length > 0],
              ["block_voxels",   parsed.blockVoxels.length > 0],
              ["section_plane",  parsed.sectionPlanes.length > 0],
            ] as const) {
              if (!hasItems) continue;
              const id = makeId(baseType);
              if (seenSourceLayerIds.has(id)) continue;
              seenSourceLayerIds.add(id);
              allSourceLayers.push({
                id,
                baseType,
                nodeId,
                nodeKind,
                label:
                  baseType === "contours"
                    ? `Contours (${fmtKind(nodeKind)})`
                    : baseType === "block_voxels"
                      ? `Block voxels (${fmtKind(nodeKind)})`
                      : baseType === "assay_points" && nodeKind === "block_grade_model"
                        ? `Block centers (${fmtKind(nodeKind)})`
                        : fmtKind(nodeKind),
                dotColor: baseType === "assay_points" ? "#60a5fa"
                        : baseType === "grade_segments" ? "#f97316"
                        : baseType === "contours" ? "#34d399"
                        : baseType === "block_voxels" ? "#f43f5e"
                        : baseType === "section_plane" ? "#f59e0b"
                        : "#a78bfa",
                measureCandidates,
                editable,
              });
            }
          }
          allTerrain.push(...parsed.terrainPoints);
          if (parsed.aoiBounds) {
            if (!mergedAoiBounds) {
              mergedAoiBounds = { ...parsed.aoiBounds };
            } else {
              mergedAoiBounds = {
                xmin: Math.min(mergedAoiBounds.xmin, parsed.aoiBounds.xmin),
                xmax: Math.max(mergedAoiBounds.xmax, parsed.aoiBounds.xmax),
                ymin: Math.min(mergedAoiBounds.ymin, parsed.aoiBounds.ymin),
                ymax: Math.max(mergedAoiBounds.ymax, parsed.aoiBounds.ymax),
              };
            }
          }
          if (parsed.terrainGrid) {
            const ml = manifestByArtifact.get(`${art.key}:${art.content_hash}`);
            const curCells = parsed.terrainGrid.nx * parsed.terrainGrid.ny;
            const curRank = terrainGridRank(ml, art.key);
            const cur: TerrainGridPick = { grid: parsed.terrainGrid, rank: curRank, cells: curCells };
            terrainCandidates.push({
              id: `${art.key}:${art.content_hash}`,
              label: art.key.split("/").slice(-1)[0] ?? art.key,
              rank: curRank,
              portOrder: portOrderFromToPort(ml?.to_port),
              grid: parsed.terrainGrid,
            });
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
        } catch {
          /* ignore */
        }
      }
      if (cancelled) return;
      lastSceneLoadSigRef.current = loadSig;
      if (sceneLoadInFlightRef.current === loadSig) sceneLoadInFlightRef.current = "";
      setContractImagery(imageryContract);
      setContractLayerStack(layerStackContract);
      const orderedTerrain = terrainCandidates
        .slice()
        .sort((a, b) => a.portOrder - b.portOrder || b.rank - a.rank || a.label.localeCompare(b.label));
      const preferredTerrain =
        orderedTerrain.find((t) => t.id === ui.groundSurfaceKey)?.grid ??
        orderedTerrain[0]?.grid ??
        bestTerrainGrid;
      setSceneData({
        traces: allTraces,
        drillSegments: allSegs,
        contourSegments: allContours,
        assayPoints: allPoints,
        blockVoxels: allBlocks,
        terrainPoints: allTerrain,
        terrainGrids: orderedTerrain,
        terrainGrid: preferredTerrain,
        aoiBounds: mergedAoiBounds,
        totalArtifacts: loaded,
        sourceLayers: allSourceLayers,
        heatmapSurfaces: allHeatmapSurfaces,
        sectionPlanes: allSectionPlanes,
      });
      setStatus(`${allTraces.length + allSegs.length + allContours.length} line segment(s), ${allPoints.length} point(s), ${allBlocks.length} block voxel(s), ${allTerrain.length} terrain samples from ${loaded} artifact(s).`);
    })();
    return () => {
      cancelled = true;
      if (sceneLoadInFlightRef.current === loadSig) sceneLoadInFlightRef.current = "";
    };
  }, [artifacts, graphId, inputLinks, manifestArtifacts, manifestLayers, viewerNodeId]);

  useEffect(() => {
    if (sceneData.sourceLayers.length === 0) return;
    setUi((p) => {
      const newStyles: Record<string, LayerVizStyle> = { ...p.layerStyles };
      let changed = false;
      for (const sl of sceneData.sourceLayers) {
        const preferredMeasure = sl.measureCandidates[0] ?? "";
        const existing = newStyles[sl.id] ?? defaultLayerStyleForId(sl.id);
        let next = existing;
        if (preferredMeasure && !existing.attributeKey.trim()) {
          next = { ...next, attributeKey: preferredMeasure };
        }
        if (
          existing.attributeKey.trim() &&
          !sl.measureCandidates.includes(existing.attributeKey.trim())
        ) {
          next = { ...next, attributeKey: preferredMeasure };
        }
        if (
          existing.sizeAttribute.trim() &&
          !sl.measureCandidates.includes(existing.sizeAttribute.trim())
        ) {
          next = { ...next, sizeAttribute: "" };
        }
        if (next !== existing) {
          newStyles[sl.id] = next;
          changed = true;
        }
      }
      return changed ? { ...p, layerStyles: newStyles } : p;
    });
  }, [sceneData.sourceLayers]);

  useEffect(() => {
    if (sceneData.sourceLayers.length === 0) return;
    setUi((prev) => {
      let changed = false;
      const nextStyles: Record<string, LayerVizStyle> = { ...prev.layerStyles };
      for (const sl of sceneData.sourceLayers) {
        // Keep block centers available, but hidden by default so voxel styling is obvious.
        if (sl.baseType === "assay_points" && sl.nodeKind === "block_grade_model") {
          const existing = nextStyles[sl.id] ?? defaultLayerStyleForId(sl.id);
          if (existing.visible !== false || existing.opacity > 0.2) {
            nextStyles[sl.id] = { ...existing, visible: false, opacity: Math.min(existing.opacity, 0.2) };
            changed = true;
          }
        }
      }
      if (!changed) return prev;
      return { ...prev, layerStyles: nextStyles };
    });
  }, [sceneData.sourceLayers]);

  useEffect(() => {
    if (sceneData.terrainGrids.length === 0) return;
    if (ui.groundSurfaceKey && sceneData.terrainGrids.some((t) => t.id === ui.groundSurfaceKey)) return;
    setUi((p) => ({
      ...p,
      groundSurfaceKey: sceneData.terrainGrids[0]?.id ?? "",
    }));
  }, [sceneData.terrainGrids, ui.groundSurfaceKey]);

  const groundTerrainGrid = useMemo(() => {
    if (sceneData.terrainGrids.length > 0) {
      return (
        sceneData.terrainGrids.find((t) => t.id === ui.groundSurfaceKey)?.grid ??
        sceneData.terrainGrids[0].grid
      );
    }
    return sceneData.terrainGrid;
  }, [sceneData.terrainGrid, sceneData.terrainGrids, ui.groundSurfaceKey]);

  const world = useMemo(() => {
    const pts: Array<[number, number, number]> = [];
    for (const s of sceneData.traces) pts.push(s.from, s.to);
    for (const s of sceneData.drillSegments) pts.push(s.from, s.to);
    for (const s of sceneData.contourSegments) pts.push(s.from, s.to);
    for (const p of sceneData.assayPoints) {
      const z = resolvePointZ(p, groundTerrainGrid);
      if (typeof z === "number" && Number.isFinite(z)) pts.push([p.x, p.y, z]);
    }
    for (const b of sceneData.blockVoxels) {
      const hx = b.dx * 0.5;
      const hy = b.dy * 0.5;
      const hz = b.dz * 0.5;
      pts.push([b.x - hx, b.y - hy, b.z - hz], [b.x + hx, b.y + hy, b.z + hz]);
    }
    for (const p of sceneData.terrainPoints) pts.push([p.x, p.y, p.z]);
    for (const sp of sceneData.sectionPlanes) {
      const theta = (sp.azimuthDeg * Math.PI) / 180.0;
      const ux = Math.cos(theta);
      const uy = Math.sin(theta);
      const sCenter = 0.5 * (sp.sMin + sp.sMax);
      const sHalf = 0.5 * (sp.sMax - sp.sMin);
      const cx = sp.centerX + sCenter * ux;
      const cy = sp.centerY + sCenter * uy;
      pts.push(
        [cx - sHalf * ux, cy - sHalf * uy, sp.zTop],
        [cx + sHalf * ux, cy + sHalf * uy, sp.zBottom]
      );
    }
    if (groundTerrainGrid && groundTerrainGrid.values.length > 0) {
      const g = groundTerrainGrid;
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
    if (sceneData.aoiBounds) {
      pts.push(
        [sceneData.aoiBounds.xmin, sceneData.aoiBounds.ymin, 0],
        [sceneData.aoiBounds.xmax, sceneData.aoiBounds.ymax, 0]
      );
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
  }, [groundTerrainGrid, sceneData]);

  const dataWarnings = useMemo(() => {
    const warnings: string[] = [];
    const baseBounds = groundTerrainGrid
      ? {
          xmin: groundTerrainGrid.xmin,
          xmax: groundTerrainGrid.xmax,
          ymin: groundTerrainGrid.ymin,
          ymax: groundTerrainGrid.ymax,
        }
      : sceneData.aoiBounds;
    if (baseBounds) {
      const outsidePointCount = sceneData.assayPoints.filter(
        (p) =>
          p.x < baseBounds.xmin ||
          p.x > baseBounds.xmax ||
          p.y < baseBounds.ymin ||
          p.y > baseBounds.ymax
      ).length;
      let outsideSegmentEndCount = 0;
      const allSegs = [
        ...sceneData.traces,
        ...sceneData.drillSegments,
        ...sceneData.contourSegments,
      ];
      for (const s of allSegs) {
        if (
          s.from[0] < baseBounds.xmin ||
          s.from[0] > baseBounds.xmax ||
          s.from[1] < baseBounds.ymin ||
          s.from[1] > baseBounds.ymax
        ) {
          outsideSegmentEndCount += 1;
        }
        if (
          s.to[0] < baseBounds.xmin ||
          s.to[0] > baseBounds.xmax ||
          s.to[1] < baseBounds.ymin ||
          s.to[1] > baseBounds.ymax
        ) {
          outsideSegmentEndCount += 1;
        }
      }
      const outsideBlockCount = sceneData.blockVoxels.filter(
        (b) =>
          b.x < baseBounds.xmin ||
          b.x > baseBounds.xmax ||
          b.y < baseBounds.ymin ||
          b.y > baseBounds.ymax
      ).length;
      const outsideTotal = outsidePointCount + outsideSegmentEndCount + outsideBlockCount;
      if (outsideTotal > 0) {
        warnings.push(
          `${outsideTotal} feature endpoint(s) outside AOI/terrain extent (check CRS or AOI).`
        );
      }
    }

    const crsSamples: number[] = [];
    for (const p of sceneData.assayPoints) if (typeof p.epsg === "number") crsSamples.push(p.epsg);
    for (const b of sceneData.blockVoxels) if (typeof b.epsg === "number") crsSamples.push(b.epsg);
    for (const p of sceneData.terrainPoints) if (typeof p.epsg === "number") crsSamples.push(p.epsg);
    for (const s of [...sceneData.traces, ...sceneData.drillSegments, ...sceneData.contourSegments]) {
      if (typeof s.epsg === "number") crsSamples.push(s.epsg);
    }
    const crsMismatch = crsSamples.filter((e) => e !== projectEpsg).length;
    if (crsMismatch > 0) {
      warnings.push(
        `${crsMismatch} geometry item(s) carry EPSG different from project EPSG:${projectEpsg}.`
      );
    }

    const missingPointZ = sceneData.assayPoints.filter((p) => p.hasExplicitZ === false).length;
    const missingSegZ =
      sceneData.traces.filter((s) => s.hasExplicitZ === false).length +
      sceneData.drillSegments.filter((s) => s.hasExplicitZ === false).length;
    if (!groundTerrainGrid && (missingPointZ > 0 || missingSegZ > 0)) {
      warnings.push(
        `${missingPointZ + missingSegZ} XY-only feature(s) cannot be terrain-draped because no terrain layer is present.`
      );
    }
    return warnings;
  }, [groundTerrainGrid, projectEpsg, sceneData]);

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

  const layerStyle = useCallback(
    (layerId: string): LayerVizStyle => {
      // Each layer ID gets its own independent style — no cross-layer fallback.
      const s = ui.layerStyles[layerId];
      return { ...defaultLayerStyleForId(layerId), ...(s ?? {}) };
    },
    [ui.layerStyles]
  );

  const sourceLayerFor = useCallback(
    (layerId: string) => sceneData.sourceLayers.find((sl) => sl.id === layerId) ?? null,
    [sceneData.sourceLayers]
  );

  const layerMeasureCandidates = useCallback(
    (layerId: string) => sourceLayerFor(layerId)?.measureCandidates ?? [],
    [sourceLayerFor]
  );

  const layerEditableCaps = useCallback(
    (layerId: string) => sourceLayerFor(layerId)?.editable ?? [],
    [sourceLayerFor]
  );

  const layerSupports = useCallback(
    (layerId: string, capability: string) => layerEditableCaps(layerId).includes(capability),
    [layerEditableCaps]
  );

  const setLayerStyle = useCallback(
    (layerId: string, patch: Partial<LayerVizStyle>) => {
      setUi((p) => {
        const prev = p.layerStyles[layerId] ?? {
          ...defaultLayerStyleForId(layerId),
        };
        return {
          ...p,
          layerStyles: {
            ...p.layerStyles,
            [layerId]: { ...prev, ...patch },
          },
        };
      });
    },
    []
  );

  const domainFor = useCallback((key: string, transform: SceneUiState["measureTransform"], lowPct: number, highPct: number, sourceLayerIdFilter?: string) => {
    if (!key) return { lo: 0, hi: 1 };
    const vals: number[] = [];
    for (const s of sceneData.drillSegments) {
      if (sourceLayerIdFilter && s.sourceLayerId !== sourceLayerIdFilter) continue;
      const v = s.measures?.[key];
      if (typeof v === "number" && Number.isFinite(v)) {
        const tv = applyMeasureTransform(v, transform);
        if (tv !== null && Number.isFinite(tv)) vals.push(tv);
      }
    }
    for (const p of sceneData.assayPoints) {
      if (sourceLayerIdFilter && p.sourceLayerId !== sourceLayerIdFilter) continue;
      const v = p.measures?.[key];
      if (typeof v === "number" && Number.isFinite(v)) {
        const tv = applyMeasureTransform(v, transform);
        if (tv !== null && Number.isFinite(tv)) vals.push(tv);
      }
    }
    for (const b of sceneData.blockVoxels) {
      if (sourceLayerIdFilter && b.sourceLayerId !== sourceLayerIdFilter) continue;
      const v = b.measures?.[key];
      if (typeof v === "number" && Number.isFinite(v)) {
        const tv = applyMeasureTransform(v, transform);
        if (tv !== null && Number.isFinite(tv)) vals.push(tv);
      }
    }
    vals.sort((a, b) => a - b);
    return {
      lo: vals.length ? percentile(vals, lowPct) : 0,
      hi: vals.length ? percentile(vals, highPct) : 1,
    };
  }, [sceneData.assayPoints, sceneData.blockVoxels, sceneData.drillSegments]);

  const assayStyle = useMemo(() => layerStyle("assay_points"), [layerStyle]);
  const segmentStyle = useMemo(() => layerStyle("grade_segments"), [layerStyle]);
  const traceStyle = useMemo(() => layerStyle("trajectories"), [layerStyle]);
  const assayDomain = useMemo(
    () => domainFor(assayStyle.attributeKey, assayStyle.transform, assayStyle.clampLowPct, assayStyle.clampHighPct),
    [assayStyle, domainFor]
  );
  const segmentDomain = useMemo(
    () => domainFor(segmentStyle.attributeKey, segmentStyle.transform, segmentStyle.clampLowPct, segmentStyle.clampHighPct),
    [domainFor, segmentStyle]
  );
  const traceDomain = useMemo(
    () => domainFor(traceStyle.attributeKey, traceStyle.transform, traceStyle.clampLowPct, traceStyle.clampHighPct),
    [domainFor, traceStyle]
  );

  const domainForLayer = useCallback((layerId: string) => {
    const style = layerStyle(layerId);
    const filter = layerId.includes("__") ? layerId : undefined;
    return domainFor(style.attributeKey, style.transform, style.clampLowPct, style.clampHighPct, filter);
  }, [layerStyle, domainFor]);
  const drapeBounds = useMemo(() => {
    if (groundTerrainGrid) {
      return {
        xmin: groundTerrainGrid.xmin,
        xmax: groundTerrainGrid.xmax,
        ymin: groundTerrainGrid.ymin,
        ymax: groundTerrainGrid.ymax,
      };
    }
    if (sceneData.aoiBounds) return sceneData.aoiBounds;
    return world.bounds;
  }, [groundTerrainGrid, sceneData.aoiBounds, world.bounds]);
  const currentImageryProvider = IMAGERY_PROVIDERS[ui.imageryProvider] ?? IMAGERY_PROVIDERS.esri_world_imagery;

  useEffect(() => {
    if (!ui.showDrape) {
      setDrapeUrlsStable([]);
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
      const terrainBoundsForCheck = groundTerrainGrid
        ? {
            xmin: groundTerrainGrid.xmin,
            xmax: groundTerrainGrid.xmax,
            ymin: groundTerrainGrid.ymin,
            ymax: groundTerrainGrid.ymax,
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
        setDrapeUrlsStable(fromContract);
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
    if (!groundTerrainGrid) {
      setDrapeUrlsStable([]);
      setDrapeStatus("Imagery drape flat fallback only (no terrain DEM input wired).");
      setDrapeLoadError(null);
      return;
    }
    const { xmin, xmax, ymin, ymax } = drapeBounds;
    const hasFiniteBounds = [xmin, xmax, ymin, ymax].every((v) => Number.isFinite(v));
    if (!hasFiniteBounds) {
      setDrapeUrlsStable([]);
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
          setDrapeUrlsStable(buildImageryUrls(xmin, ymin, xmax, ymax));
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
            setDrapeUrlsStable([]);
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
          setDrapeUrlsStable([]);
          setDrapeStatus(
            `Imagery drape unavailable (reprojected bounds outside WGS84 extent; EPSG:${projectEpsg}).`
          );
          setDrapeLoadError(null);
        }
        return;
      }
      if (!cancelled) {
        setDrapeUrlsStable(buildImageryUrls(lonMin, latMin, lonMax, latMax));
        setDrapeLoadError(null);
        setDrapeStatus(`${currentImageryProvider.label} drape active.`);
      }
      })();
    }, debounceMs);
    return () => {
      cancelled = true;
      window.clearTimeout(tid);
    };
  }, [contractImagery, currentImageryProvider, drapeBounds, groundTerrainGrid, projectEpsg, setDrapeUrlsStable, ui.imageryProvider, ui.showDrape]);


  const localDrillSegments = useMemo(
    () =>
      sceneData.drillSegments.map((s) => ({
        a: toLocal(s.from[0], s.from[1], s.from[2]),
        b: toLocal(s.to[0], s.to[1], s.to[2]),
      })),
    [sceneData.drillSegments, toLocal]
  );

  const drapeTerrainGeom = useMemo(() => {
    if (!groundTerrainGrid) return null;
    const g = groundTerrainGrid;
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
  }, [groundTerrainGrid, sceneScale, toLocal]);

  const autoRefitSig = useMemo(() => {
    const g = groundTerrainGrid;
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
  }, [groundTerrainGrid, sceneData, viewerNodeId]);

  useEffect(() => {
    if (!active) return;
    if (sceneData.totalArtifacts <= 0) return;
    if (autoRefitSigRef.current === autoRefitSig) return;
    autoRefitSigRef.current = autoRefitSig;
    setCameraResetToken((v) => v + 1);
  }, [active, autoRefitSig, sceneData.totalArtifacts]);

  const orderedLayerIds = useMemo(() => {
    // Static layers: esri_drape is always available (uses default provider even without contract).
    // terrain_points only when there is actual terrain data.
    // Contour lines are dynamic source layers (baseType "contours") — no static "contours" slot.
    const hasTerrainData = sceneData.terrainGrid !== null || sceneData.terrainPoints.length > 0;
    const staticBase: string[] = ["esri_drape"];
    if (hasTerrainData) staticBase.push("terrain_points");
    const dynamicIds = sceneData.sourceLayers.map(l => l.id);
    const all = [...staticBase, ...dynamicIds];
    if (ui.layerOrderMode === "override" && ui.layerOrder.length > 0) {
      const known = new Set(all);
      return [
        ...ui.layerOrder.filter((id: string) => known.has(id)),
        ...all.filter(id => !ui.layerOrder.includes(id as LayerKey)),
      ];
    }
    return all;
  }, [sceneData.sourceLayers, sceneData.terrainGrid, sceneData.terrainPoints.length, ui.layerOrder, ui.layerOrderMode]);

  const hasLayerStackContract = Boolean(contractLayerStack?.layers?.length);
  const canDragLayers = !hasLayerStackContract || ui.layerOrderMode === "override";

  const onDropLayer = (targetId: string) => {
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
    terrain_points: "Terrain surface",
    contours: "Contours",
    trajectories: "Trajectories",
    grade_segments: "Grade segments",
    block_voxels: "Block voxels",
    assay_points: "Assay points",
  };
  const LAYER_DOT: Record<LayerKey, string> = {
    esri_drape: "#facc15",
    terrain_points: "#4ade80",
    contours: "#38bdf8",
    trajectories: "#a78bfa",
    grade_segments: "#f97316",
    block_voxels: "#f43f5e",
    assay_points: "#60a5fa",
  };
  const styleableLayers = new Set<LayerKey>([
    "trajectories",
    "grade_segments",
    "block_voxels",
    "assay_points",
  ]);

  const paletteOptions: Array<{ value: SceneUiState["palette"]; label: string }> = [
    { value: "inferno", label: "Inferno" },
    { value: "viridis", label: "Viridis" },
    { value: "turbo", label: "Turbo" },
    { value: "red_blue", label: "Red ↔ Blue" },
  ];

  const layerLift = useCallback(
    (layerId: string) => {
      const idx = Math.max(0, orderedLayerIds.indexOf(layerId));
      return sceneScale * 0.00015 * (idx + 1);
    },
    [orderedLayerIds, sceneScale]
  );

  const colorFromLayerStyle = useCallback(
    (style: LayerVizStyle, raw: number | string | null, domain: { lo: number; hi: number }): string => {
      if (style.colorMode === "categorical") {
        const custom = parseCategoricalColorMap(style.categoricalColorMap);
        const key = raw === null ? "" : String(raw);
        const customColor = custom[key];
        if (customColor) return customColor;
        if (typeof raw === "number" && Number.isFinite(raw)) {
          return categoricalMeasureColor(raw, style.palette);
        }
        return categoricalMeasureColor(key.length, style.palette);
      }
      if (typeof raw !== "number" || !Number.isFinite(raw)) return "#58a6ff";
      const tv = applyMeasureTransform(raw, style.transform);
      if (tv === null) return "#58a6ff";
      const t = Math.max(0, Math.min(1, (tv - domain.lo) / ((domain.hi - domain.lo) || 1)));
      const stopsToUse = style.colorStops?.length >= 2 ? style.colorStops : PALETTE_STOPS[style.palette] ?? PALETTE_STOPS.inferno;
      return interpolateStopsColor(stopsToUse, t);
    },
    []
  );

  useEffect(() => {
    return () => {
      drapeTerrainGeom?.dispose();
    };
  }, [drapeTerrainGeom]);

  const bgPalette = BG_PRESETS[ui.bgPreset] ?? BG_PRESETS.night;

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
          {dataWarnings.length > 0 ? (
            <div style={{ fontSize: 10, color: "#f7b955", marginTop: 2 }}>
              Warning: {dataWarnings.join(" · ")}
            </div>
          ) : null}
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
              key={`${viewerNodeId ?? "none"}`}
              camera={{
                position: [sceneScale * 0.85, sceneScale * 1.15, sceneScale * 1.05],
                fov: 50,
                near: 0.01,
                far: Math.max(5000, sceneScale * 500),
              }}
              gl={{ logarithmicDepthBuffer: true }}
            >
              {/* Imperative camera reset — avoids remounting the Canvas (and
                  destroying all GPU resources) just to reposition the camera. */}
              <CameraAutoFit resetToken={cameraResetToken} sceneScale={sceneScale} />
              <color attach="background" args={[bgPalette.sky]} />
              {ui.fogEnabled && <fog attach="fog" args={[bgPalette.fog, sceneScale * 2, sceneScale * 40]} />}
              <ambientLight intensity={ui.ambientIntensity} />
              <hemisphereLight args={[bgPalette.hemi[0], bgPalette.hemi[1], 0.55]} />
              <directionalLight position={[sceneScale, sceneScale * 0.8, sceneScale * 0.5]} intensity={0.42} />
              <directionalLight position={[-sceneScale * 0.7, sceneScale * 0.6, -sceneScale * 0.7]} intensity={0.24} />
              {ui.gridEnabled && <gridHelper args={[sceneScale * 1.25, 30, bgPalette.grid1, bgPalette.grid2]} position={[0, -sceneScale * 0.02, 0]} />}

              {orderedLayerIds.map((layerId) => {
                const layerBaseType = (id: string) => id.includes("__") ? id.split("__")[0] : id;
                const baseType = layerBaseType(layerId);

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
                if (baseType === "trajectories") {
                  if (!layerStyle(layerId).visible) return null;
                  const lift = layerLift(layerId);
                  const filterFn = layerId.includes("__")
                    ? (s: Segment3D) => s.sourceLayerId === layerId
                    : () => true;
                  const traces = sceneData.traces.filter(filterFn);
                  const thisTraceStyle = layerStyle(layerId);
                  const thisDomain = domainForLayer(layerId);
                  return (
                    <group key={`layer-traces-${layerId}`}>
                      {traces.map((s, i) => {
                        const az = s.hasExplicitZ === false
                          ? sampleTerrainZ(groundTerrainGrid, s.from[0], s.from[1]) ?? s.from[2]
                          : s.from[2];
                        const bz = s.hasExplicitZ === false
                          ? sampleTerrainZ(groundTerrainGrid, s.to[0], s.to[1]) ?? s.to[2]
                          : s.to[2];
                        const a = toLocal(s.from[0], s.from[1], az + lift);
                        const b = toLocal(s.to[0], s.to[1], bz + lift);
                        const raw = thisTraceStyle.attributeKey.trim() ? (s.measures?.[thisTraceStyle.attributeKey.trim()] ?? null) : null;
                        const color = thisTraceStyle.attributeKey.trim()
                          ? colorFromLayerStyle(thisTraceStyle, raw, thisDomain)
                          : ui.traceColor;
                        const r = Math.max(0.06, sceneScale * 0.0006 * ui.radiusScale * (ui.traceWidth / 2));
                        return <SegmentTube key={`trace-${i}`} from={a} to={b} radius={r} color={color} />;
                      })}
                    </group>
                  );
                }
                if (baseType === "grade_segments") {
                  if (!layerStyle(layerId).visible) return null;
                  const lift = layerLift(layerId);
                  const filterFn = layerId.includes("__")
                    ? (s: Segment3D) => s.sourceLayerId === layerId
                    : () => true;
                  const segs = sceneData.drillSegments.filter(filterFn);
                  const thisSegStyle = layerStyle(layerId);
                  const thisDomain = domainForLayer(layerId);
                  return (
                    <group key={`layer-segments-${layerId}`}>
                      {segs.map((s, i) => {
                        const az = s.hasExplicitZ === false
                          ? sampleTerrainZ(groundTerrainGrid, s.from[0], s.from[1]) ?? s.from[2]
                          : s.from[2];
                        const bz = s.hasExplicitZ === false
                          ? sampleTerrainZ(groundTerrainGrid, s.to[0], s.to[1]) ?? s.to[2]
                          : s.to[2];
                        const a = toLocal(s.from[0], s.from[1], az + lift);
                        const b = toLocal(s.to[0], s.to[1], bz + lift);
                        const attrKey = thisSegStyle.attributeKey.trim();
                        const mv = attrKey && s.measures ? (s.measures[attrKey] ?? null) : null;
                        const color = colorFromLayerStyle(thisSegStyle, mv, thisDomain);
                        const rFromData = projectEpsg !== 4326 && typeof s.radiusM === "number" ? s.radiusM : 0;
                        const r = Math.max(0.08, sceneScale * 0.0012 * ui.radiusScale * (ui.segmentWidth / 4), rFromData * ui.radiusScale);
                        return <SegmentTube key={`seg-${i}`} from={a} to={b} radius={r} color={color} />;
                      })}
                    </group>
                  );
                }
                if (baseType === "block_voxels") {
                  const thisBlockStyle = layerStyle(layerId);
                  if (!thisBlockStyle.visible) return null;
                  const voxels = sceneData.blockVoxels.filter((v) =>
                    layerId.includes("__") ? v.sourceLayerId === layerId : true
                  );
                  if (voxels.length === 0) return null;
                  const thisDomain = domainForLayer(layerId);
                  return (
                    <BlockVoxelLayer3D
                      key={`layer-block-voxels-${layerId}`}
                      voxels={voxels}
                      style={thisBlockStyle}
                      domain={thisDomain}
                      toLocal={toLocal}
                      lift={layerLift(layerId)}
                      colorForMeasure={(raw, domain) =>
                        colorFromLayerStyle(thisBlockStyle, raw, domain)
                      }
                    />
                  );
                }
                if (baseType === "contours") {
                  const thisContourStyle = layerStyle(layerId);
                  if (!thisContourStyle.visible) return null;
                  const filterFn = (s: Segment3D) => s.sourceLayerId === layerId;
                  const step = Math.max(1, Math.trunc(ui.contourIntervalStep));
                  let segs = sceneData.contourSegments.filter(filterFn);
                  if (step > 1) {
                    const levels = Array.from(new Set(
                      segs.map(s => typeof s.contourLevel === "number" && Number.isFinite(s.contourLevel) ? s.contourLevel : null)
                        .filter((v): v is number => v !== null)
                    )).sort((a, b) => a - b);
                    if (levels.length >= 2) {
                      const keep = new Set(levels.filter((_, idx) => idx % step === 0));
                      segs = segs.filter(s => !(typeof s.contourLevel === "number" && Number.isFinite(s.contourLevel)) || keep.has(s.contourLevel));
                    }
                  }
                  if (segs.length === 0) return null;
                  const contourColor = thisContourStyle.attributeKey ? undefined : (ui.contourColor);
                  const r = Math.max(0.01, sceneScale * 0.00015 * Math.max(0.25, ui.contourWidth));
                  return (
                    <ContourLayer3D
                      key={`layer-contours-${layerId}`}
                      layerId={layerId}
                      segs={segs}
                      style={thisContourStyle}
                      toLocal={toLocal}
                      sceneScale={sceneScale}
                      lift={layerLift(layerId)}
                      groundTerrainGrid={groundTerrainGrid}
                      radius={r}
                      opacity={ui.contourOpacity}
                      fixedColor={contourColor}
                    />
                  );
                }
                if (baseType === "assay_points") {
                  if (!layerStyle(layerId).visible) return null;
                  const lift = layerLift(layerId);
                  const filterFn = layerId.includes("__")
                    ? (p: Point3D) => p.sourceLayerId === layerId
                    : () => true;
                  const pts = sceneData.assayPoints.filter(filterFn);
                  const thisAssayStyle = layerStyle(layerId);
                  const thisDomain = domainForLayer(layerId);

                  // Heatmap mode — IDW canvas plane draped above terrain
                  if (thisAssayStyle.displayMode === "heatmap") {
                    return (
                      <HeatmapLayer3D
                        key={`hm3d-${layerId}`}
                        layerId={layerId}
                        pts={pts}
                        style={thisAssayStyle}
                        domain={thisDomain}
                        toLocal={toLocal}
                        sceneScale={sceneScale}
                        lift={lift}
                        groundTerrainGrid={groundTerrainGrid}
                      />
                    );
                  }

                  return (
                    <group key={`layer-assay-points-${layerId}`}>
                      {pts.map((p, i) => {
                        const pz = resolvePointZ(p, groundTerrainGrid);
                        if (pz === null) return null;
                        const lp = toLocal(p.x, p.y, pz + lift);
                        const attrKey = thisAssayStyle.attributeKey.trim();
                        const mv = attrKey && p.measures ? (p.measures[attrKey] ?? null) : null;
                        const color = colorFromLayerStyle(thisAssayStyle, mv, thisDomain);
                        const baseR = Math.max(0.09, sceneScale * 0.0009 * ui.radiusScale * (ui.sampleSize / 4));
                        const shape = thisAssayStyle.pointShape;
                        const sizeKey = thisAssayStyle.sizeAttribute.trim();
                        const sizeMv = sizeKey && p.measures ? (p.measures[sizeKey] ?? null) : null;
                        let sizeT = 0.5; // default middle
                        if (sizeMv !== null && typeof sizeMv === "number" && thisDomain.hi > thisDomain.lo) {
                          const sv = thisAssayStyle.sizeTransform === "log10"
                            ? Math.log10(Math.max(1e-9, sizeMv))
                            : thisAssayStyle.sizeTransform === "sqrt"
                              ? Math.sqrt(Math.max(0, sizeMv))
                              : sizeMv;
                          const slo = thisAssayStyle.sizeTransform === "log10"
                            ? Math.log10(Math.max(1e-9, thisDomain.lo))
                            : thisAssayStyle.sizeTransform === "sqrt"
                              ? Math.sqrt(Math.max(0, thisDomain.lo))
                              : thisDomain.lo;
                          const shi = thisAssayStyle.sizeTransform === "log10"
                            ? Math.log10(Math.max(1e-9, thisDomain.hi))
                            : thisAssayStyle.sizeTransform === "sqrt"
                              ? Math.sqrt(Math.max(0, thisDomain.hi))
                              : thisDomain.hi;
                          sizeT = shi > slo ? Math.max(0, Math.min(1, (sv - slo) / (shi - slo))) : 0.5;
                        }
                        const sizeMult = thisAssayStyle.sizeMin + (thisAssayStyle.sizeMax - thisAssayStyle.sizeMin) * sizeT;
                        const rr = Math.max(0.09, baseR * sizeMult);
                        return (
                          <mesh key={`pt-${i}`} position={[lp.x, lp.y, lp.z]}>
                            {shape === "sphere" ? <sphereGeometry args={[rr, 10, 10]} /> : null}
                            {shape === "box" ? <boxGeometry args={[rr * 1.8, rr * 1.8, rr * 1.8]} /> : null}
                            {shape === "diamond" ? <octahedronGeometry args={[rr * 1.25, 0]} /> : null}
                            {shape === "cone" ? <coneGeometry args={[rr * 0.9, rr * 2.5, 8]} /> : null}
                            {shape === "disc" ? <cylinderGeometry args={[rr, rr, rr * 0.28, 12]} /> : null}
                            {shape === "spike" ? <coneGeometry args={[rr * 0.35, rr * 3, 6]} /> : null}
                            <meshStandardMaterial color={color} transparent opacity={Math.max(0.05, thisAssayStyle.opacity)}
                              polygonOffset={true} polygonOffsetFactor={-1} polygonOffsetUnits={-1} />
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
                if (baseType === "heatmap_surface") {
                  const thisStyle = layerStyle(layerId);
                  if (!thisStyle.visible) return null;
                  const hms = sceneData.heatmapSurfaces.find(h => h.id === layerId);
                  if (!hms) return null;
                  const lift = layerLift(layerId);
                  return (
                    <SurfaceHeatmapLayer3D
                      key={`shm-${layerId}`}
                      heatmapSurface={hms}
                      style={thisStyle}
                      toLocal={toLocal}
                      sceneScale={sceneScale}
                      lift={lift}
                      groundTerrainGrid={groundTerrainGrid}
                    />
                  );
                }
                if (baseType === "section_plane") {
                  const thisStyle = layerStyle(layerId);
                  if (!thisStyle.visible) return null;
                  const plane = sceneData.sectionPlanes.find(
                    (p) => `section_plane__${p.nodeId}` === layerId
                  );
                  if (!plane) return null;
                  return (
                    <SectionPlaneLayer3D
                      key={`section-${layerId}`}
                      sectionPlane={plane}
                      style={thisStyle}
                      toLocal={toLocal}
                    />
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

          <aside style={{ position: "absolute", top: 12, right: 12, width: ui.panelCollapsed ? 40 : 300, maxHeight: "calc(100% - 24px)", display: "flex", flexDirection: "column", background: "rgba(13,17,23,0.96)", border: "1px solid #30363d", borderRadius: 12, boxShadow: "0 4px 24px rgba(0,0,0,0.5)", overflow: "hidden" }}>
            {ui.panelCollapsed ? (
              <button type="button" title="Expand panel" onClick={() => setUi((p) => ({ ...p, panelCollapsed: false }))} style={{ flex: 1, background: "transparent", border: "none", color: "#6e7681", cursor: "pointer", fontSize: 16, padding: "8px 0" }}>›</button>
            ) : (
              <>
                <div className="me-panel-header">
                  <span className="me-panel-header-title">Scene layers</span>
                  <button type="button" title="Collapse" className="me-panel-collapse-btn" onClick={() => setUi((p) => ({ ...p, panelCollapsed: true }))}>‹</button>
                </div>
                <div className="me-panel-body me-panel">
                <details open>
                  <summary>Layers</summary>
                  <div style={{ display: "grid", gap: 6 }}>
                    {hasLayerStackContract ? (
                      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                        <span className="me-section-note" style={{ flex: 1 }}>Order</span>
                        <select
                          value={ui.layerOrderMode}
                          onChange={(e) =>
                            setUi((p) => ({
                              ...p,
                              layerOrderMode: e.target.value as "contract" | "override",
                            }))
                          }
                        >
                          <option value="contract">Contract</option>
                          <option value="override">Override</option>
                        </select>
                        {ui.layerOrderMode === "contract" && (
                          <span className="me-section-note">(drag locked)</span>
                        )}
                      </div>
                    ) : null}
                    {orderedLayerIds.map((layerId) => {
                      const style = layerStyle(layerId);
                      const lBaseType = layerId.includes("__") ? layerId.split("__")[0] : layerId;
                      const styleable = lBaseType === "trajectories" || lBaseType === "grade_segments" || lBaseType === "block_voxels" || lBaseType === "assay_points" || lBaseType === "contours" || lBaseType === "heatmap_surface" || lBaseType === "section_plane";
                      const sourceLayer = sceneData.sourceLayers.find(sl => sl.id === layerId);
                      const thisLayerLabel = sourceLayer?.label
                        ?? (layerId === "esri_drape" ? "Imagery drape"
                          : layerId === "terrain_points" ? "Terrain surface"
                          : layerId === "contours" ? "Contours"
                          : layerId);
                      const thisLayerDot = sourceLayer?.dotColor ?? (LAYER_DOT[layerId as LayerKey] ?? "#484f58");
                      const visible =
                        layerId === "esri_drape"
                          ? ui.showDrape
                          : layerId === "terrain_points"
                            ? ui.showTerrain
                            : layerId === "contours"
                              ? ui.showContours
                              : style.visible;
                      return (
                        <div
                          key={`chip-${layerId}`}
                          onDragOver={(e) => e.preventDefault()}
                          onDrop={() => onDropLayer(layerId)}
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
                                setDraggingLayer(layerId);
                              }}
                              onDragEnd={() => setDraggingLayer(null)}
                            >
                              ⠿
                            </span>
                            <span className="me-layer-dot" style={{ background: thisLayerDot, opacity: visible ? 1 : 0.25 }} />
                            <div style={{ flex: 1, minWidth: 0, fontSize: 12, fontWeight: 600, color: visible ? "#e6edf3" : "#6e7681", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                              {thisLayerLabel}
                            </div>
                            <button
                              type="button"
                              className="me-layer-vis-btn"
                              title={visible ? "Hide layer" : "Show layer"}
                              style={{ color: visible ? "#e6edf3" : "#484f58" }}
                              onClick={() => {
                                if (layerId === "esri_drape") {
                                  setUi((p) => ({ ...p, showDrape: !p.showDrape }));
                                } else if (layerId === "terrain_points") {
                                  setUi((p) => ({ ...p, showTerrain: !p.showTerrain }));
                                } else if (layerId === "contours") {
                                  setUi((p) => ({ ...p, showContours: !p.showContours }));
                                } else {
                                  setLayerStyle(layerId, { visible: !style.visible });
                                }
                              }}
                            >
                              {visible ? "●" : "○"}
                            </button>
                            <button
                              type="button"
                              className="me-layer-expand-btn"
                              onClick={() => setExpandedLayers((prev) => ({ ...prev, [layerId]: !prev[layerId] }))}
                            >
                              {expandedLayers[layerId] ? "▾" : "›"}
                            </button>
                          </div>
                          {expandedLayers[layerId] && (
                            <div className="me-layer-card-body">
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
                                    <div className="me-section-note">
                                      Provider set by upstream imagery contract.
                                    </div>
                                  ) : (
                                    <div className="me-section-note">
                                      Default: Esri World Imagery (free tier).
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
                                <>
                                  {sceneData.terrainGrids.length > 1 ? (
                                    <label>
                                      Ground surface
                                      <select
                                        value={ui.groundSurfaceKey}
                                        onChange={(e) =>
                                          setUi((p) => ({ ...p, groundSurfaceKey: e.target.value }))
                                        }
                                      >
                                        {sceneData.terrainGrids.map((t) => (
                                          <option key={t.id} value={t.id}>
                                            {t.label}
                                          </option>
                                        ))}
                                      </select>
                                    </label>
                                  ) : null}
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
                                </>
                              ) : null}
                              {lBaseType === "contours" ? (
                                <>
                                  <div style={{ fontSize: 9.5, color: "#8b949e", marginBottom: 2 }}>
                                    Contour lines — colour mapped by elevation. Use the ramp below to customise.
                                  </div>
                                  <label>
                                    Opacity
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
                                    Line width
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
                                    Show every Nth level
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
                                  <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
                                    <input
                                      type="checkbox"
                                      checked={style.showLabels}
                                      onChange={(e) => setLayerStyle(layerId, { showLabels: e.target.checked })}
                                    />
                                    Inline elevation labels
                                  </label>
                                  {style.showLabels ? (
                                    <label>
                                      Label size
                                      <input
                                        type="range"
                                        min={8}
                                        max={20}
                                        step={1}
                                        value={style.labelSize}
                                        onChange={(e) =>
                                          setLayerStyle(layerId, {
                                            labelSize: Math.max(8, Math.min(20, Number(e.target.value) || 12)),
                                          })
                                        }
                                      />
                                    </label>
                                  ) : null}
                                </>
                              ) : null}
                              {lBaseType === "heatmap_surface" ? (
                                <>
                                  <div style={{ fontSize: 9.5, color: "#8b949e", marginBottom: 2, lineHeight: 1.4 }}>
                                    Pre-computed IDW surface from the Assay Heatmap node. Values are mapped to colour via the ramp below.
                                  </div>
                                  <label>
                                    Opacity
                                    <input type="range" min={0.05} max={1} step={0.02}
                                      value={style.opacity}
                                      onChange={(e) => setLayerStyle(layerId, { opacity: Number(e.target.value) })}
                                    />
                                  </label>
                                  <div style={{ display: "flex", gap: 4, marginTop: 2 }}>
                                    <button type="button" style={{ flex: 1, fontSize: 9, padding: "2px 0", borderRadius: 3, cursor: "pointer", fontFamily: "inherit", background: "transparent", border: "1px solid #30363d", color: "#6e7681" }}
                                      onClick={() => setLayerStyle(layerId, { rampNormMode: style.rampNormMode === "pct" ? "fixed" : "pct" })}>
                                      Norm: {style.rampNormMode === "pct" ? "Auto (data range)" : "Fixed"}
                                    </button>
                                  </div>
                                </>
                              ) : null}
                              {lBaseType === "trajectories" ? (
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
                              {styleable ? (
                                <div style={{ display: "flex", flexDirection: "column", gap: 6, paddingTop: 4 }}>

                                  {/* ── MEASURE (not shown for contour/heatmap layers — they colour by their own value domain automatically) ── */}
                                  {lBaseType !== "contours" &&
                                  lBaseType !== "heatmap_surface" &&
                                  layerSupports(layerId, "measure") ? (
                                  <div>
                                    <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 2 }}>
                                      <span style={{ fontSize: 8.5, fontWeight: 700, letterSpacing: "0.07em", color: "#484f58", textTransform: "uppercase" as const, flex: 1 }}>Measure</span>
                                      <button type="button"
                                        onClick={() => setLayerStyle(layerId, { colorMode: style.colorMode === "continuous" ? "categorical" : "continuous" })}
                                        style={{
                                          fontSize: 9, padding: "1px 6px", borderRadius: 3, cursor: "pointer", fontFamily: "inherit",
                                          background: "transparent", border: "1px solid #30363d", color: "#6e7681",
                                        }}>
                                        {style.colorMode === "continuous" ? "Continuous" : "Categorical"}
                                      </button>
                                    </div>
                                    <select style={ctlSelect} value={style.attributeKey}
                                      onChange={(e) => setLayerStyle(layerId, { attributeKey: e.target.value })}>
                                      <option value="">(constant color)</option>
                                      {layerMeasureCandidates(layerId).map((m) => (
                                        <option key={`${layerId}-${m}`} value={m}>{m}</option>
                                      ))}
                                    </select>
                                  </div>
                                  ) : (
                                    <div style={{ fontSize: 8.5, fontWeight: 700, letterSpacing: "0.07em", color: "#484f58", textTransform: "uppercase" as const }}>
                                      {lBaseType === "heatmap_surface" ? "Value colour ramp" : "Elevation colour ramp"}
                                    </div>
                                  )}

                                  {/* ── COLOR RAMP ── */}
                                  {(lBaseType === "contours" ||
                                    lBaseType === "heatmap_surface" ||
                                    layerSupports(layerId, "palette") ||
                                    layerSupports(layerId, "measure")) ? (
                                  style.colorMode === "continuous" ? (
                                    <ColorRampEditor
                                      stops={style.colorStops?.length >= 2 ? style.colorStops : PALETTE_STOPS[style.palette] ?? PALETTE_STOPS.inferno}
                                      onStopsChange={(s) => setLayerStyle(layerId, { colorStops: s })}
                                      transform={style.transform}
                                      onTransformChange={(t) => setLayerStyle(layerId, { transform: t })}
                                      clampLow={style.clampLowPct}
                                      clampHigh={style.clampHighPct}
                                      onClampChange={(lo, hi) => setLayerStyle(layerId, { clampLowPct: lo, clampHighPct: hi })}
                                      rawValues={(() => {
                                        // For contour layers, use elevation levels as the value domain.
                                        if (lBaseType === "contours") {
                                          return sceneData.contourSegments
                                            .filter(s => s.sourceLayerId === layerId)
                                            .map(s => s.contourLevel)
                                            .filter((v): v is number => typeof v === "number" && Number.isFinite(v));
                                        }
                                        // For pre-computed heatmap surface grids, use the grid values directly.
                                        if (lBaseType === "heatmap_surface") {
                                          const hms = sceneData.heatmapSurfaces.find(h => h.id === layerId);
                                          return (hms?.grid.values ?? []).filter((v): v is number => v !== null && Number.isFinite(v));
                                        }
                                        const key = style.attributeKey;
                                        if (!key) return [];
                                        const vals: number[] = [];
                                        for (const s of sceneData.drillSegments) {
                                          const v = s.measures?.[key];
                                          if (typeof v === "number" && Number.isFinite(v)) vals.push(v);
                                        }
                                        for (const p of sceneData.assayPoints) {
                                          const v = p.measures?.[key];
                                          if (typeof v === "number" && Number.isFinite(v)) vals.push(v);
                                        }
                                        for (const b of sceneData.blockVoxels) {
                                          if (b.sourceLayerId !== layerId) continue;
                                          const v = b.measures?.[key];
                                          if (typeof v === "number" && Number.isFinite(v)) vals.push(v);
                                        }
                                        return vals;
                                      })()}
                                      dataMin={(() => {
                                        if (lBaseType === "contours") {
                                          const levels = sceneData.contourSegments.filter(s => s.sourceLayerId === layerId).map(s => s.contourLevel).filter((v): v is number => typeof v === "number");
                                          return levels.length ? Math.min(...levels) : 0;
                                        }
                                        if (lBaseType === "heatmap_surface") {
                                          const vals = (sceneData.heatmapSurfaces.find(h => h.id === layerId)?.grid.values ?? []).filter((v): v is number => v !== null && Number.isFinite(v));
                                          return vals.length ? Math.min(...vals) : 0;
                                        }
                                        const key = style.attributeKey;
                                        if (!key) return 0;
                                        let mn = Infinity;
                                        for (const s of sceneData.drillSegments) { const v = s.measures?.[key]; if (typeof v === "number") mn = Math.min(mn, v); }
                                        for (const p of sceneData.assayPoints) { const v = p.measures?.[key]; if (typeof v === "number") mn = Math.min(mn, v); }
                                        for (const b of sceneData.blockVoxels) { const v = b.sourceLayerId === layerId ? b.measures?.[key] : undefined; if (typeof v === "number") mn = Math.min(mn, v); }
                                        return isFinite(mn) ? mn : 0;
                                      })()}
                                      dataMax={(() => {
                                        if (lBaseType === "contours") {
                                          const levels = sceneData.contourSegments.filter(s => s.sourceLayerId === layerId).map(s => s.contourLevel).filter((v): v is number => typeof v === "number");
                                          return levels.length ? Math.max(...levels) : 1;
                                        }
                                        if (lBaseType === "heatmap_surface") {
                                          const vals = (sceneData.heatmapSurfaces.find(h => h.id === layerId)?.grid.values ?? []).filter((v): v is number => v !== null && Number.isFinite(v));
                                          return vals.length ? Math.max(...vals) : 1;
                                        }
                                        const key = style.attributeKey;
                                        if (!key) return 1;
                                        let mx = -Infinity;
                                        for (const s of sceneData.drillSegments) { const v = s.measures?.[key]; if (typeof v === "number") mx = Math.max(mx, v); }
                                        for (const p of sceneData.assayPoints) { const v = p.measures?.[key]; if (typeof v === "number") mx = Math.max(mx, v); }
                                        for (const b of sceneData.blockVoxels) { const v = b.sourceLayerId === layerId ? b.measures?.[key] : undefined; if (typeof v === "number") mx = Math.max(mx, v); }
                                        return isFinite(mx) ? mx : 1;
                                      })()}
                                    />
                                  ) : (
                                    <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
                                      <div style={{ display: "flex", gap: 4 }}>
                                        {(Object.entries(PALETTE_STOPS) as [string, ColorStop[]][]).map(([key, ps]) => (
                                          <button key={key} type="button" title={key}
                                            onClick={() => setLayerStyle(layerId, { palette: key as LayerVizStyle["palette"] })}
                                            style={{
                                              flex: 1, height: 16, borderRadius: 3, cursor: "pointer", padding: 0,
                                              background: stopsToGradientCss(ps),
                                              border: style.palette === key ? "2px solid #58a6ff" : "1px solid rgba(255,255,255,0.1)",
                                              boxSizing: "border-box",
                                            }}
                                          />
                                        ))}
                                      </div>
                                      <label style={{ fontSize: 11, color: "#8b949e", display: "grid", gap: 3 }}>
                                        Category map (JSON)
                                        <textarea value={style.categoricalColorMap} rows={2}
                                          onChange={(e) => setLayerStyle(layerId, { categoricalColorMap: e.target.value })}
                                          style={{ width: "100%", resize: "vertical", fontFamily: "ui-monospace,monospace", fontSize: 10, background: "#0d1117", border: "1px solid #30363d", borderRadius: 4, color: "#e6edf3", padding: "4px 6px", boxSizing: "border-box" }}
                                          placeholder='{"ore":"#f97316","waste":"#3b82f6"}'
                                        />
                                      </label>
                                    </div>
                                  )
                                  ) : null}

                                  {/* ── WIDTH (grade_segments / trajectories) ── */}
                                  {lBaseType === "grade_segments" && layerSupports(layerId, "width") ? (
                                    <div style={{ display: "grid", gridTemplateColumns: "28px 1fr 30px", gap: 4, alignItems: "center" }}>
                                      <span style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.07em", color: "#484f58", textTransform: "uppercase" as const, gridColumn: "1/-1", marginBottom: 2 }}>Width</span>
                                      <span style={{ fontSize: 9.5, color: "#6e7681", textAlign: "right" }}>px</span>
                                      <input type="range" min={1} max={12} step={1}
                                        value={ui.segmentWidth}
                                        onChange={(e) => setUi((p) => ({ ...p, segmentWidth: Number(e.target.value) || 4 }))}
                                        style={{ width: "100%" }}
                                      />
                                      <span style={{ fontSize: 9.5, color: "#c9d1d9", fontFamily: "ui-monospace,monospace", textAlign: "right" }}>{ui.segmentWidth}</span>
                                    </div>
                                  ) : lBaseType === "trajectories" && layerSupports(layerId, "width") ? (
                                    <div style={{ display: "grid", gridTemplateColumns: "28px 1fr 30px", gap: 4, alignItems: "center" }}>
                                      <span style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.07em", color: "#484f58", textTransform: "uppercase" as const, gridColumn: "1/-1", marginBottom: 2 }}>Width</span>
                                      <span style={{ fontSize: 9.5, color: "#6e7681", textAlign: "right" }}>px</span>
                                      <input type="range" min={1} max={12} step={1}
                                        value={ui.traceWidth}
                                        onChange={(e) => setUi((p) => ({ ...p, traceWidth: Number(e.target.value) || 2 }))}
                                        style={{ width: "100%" }}
                                      />
                                      <span style={{ fontSize: 9.5, color: "#c9d1d9", fontFamily: "ui-monospace,monospace", textAlign: "right" }}>{ui.traceWidth}</span>
                                    </div>
                                  ) : null}

                                  {/* ── SIZE (assay_points only) ── */}
                                  {lBaseType === "assay_points" && layerSupports(layerId, "size") ? (
                                    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                                      <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
                                        <span style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.07em", color: "#484f58", textTransform: "uppercase" as const, flex: 1 }}>Size</span>
                                      </div>
                                      <select style={ctlSelect} value={style.sizeAttribute}
                                        onChange={(e) => setLayerStyle(layerId, { sizeAttribute: e.target.value })}>
                                        <option value="">(constant)</option>
                                        {layerMeasureCandidates(layerId).map((m) => (
                                          <option key={`sz-${m}`} value={m}>{m}</option>
                                        ))}
                                      </select>
                                      {style.sizeAttribute ? (
                                        <>
                                          <div style={{ display: "flex", gap: 3 }}>
                                            {(["linear","sqrt","log10"] as const).map((t) => (
                                              <button key={t} type="button"
                                                onClick={() => setLayerStyle(layerId, { sizeTransform: t })}
                                                style={{
                                                  flex: 1, fontSize: 10, fontWeight: 600, padding: "2px 0", borderRadius: 4, cursor: "pointer",
                                                  fontFamily: "inherit",
                                                  background: style.sizeTransform === t ? "#388bfd22" : "transparent",
                                                  border: `1px solid ${style.sizeTransform === t ? "#388bfd" : "#30363d"}`,
                                                  color: style.sizeTransform === t ? "#58a6ff" : "#6e7681",
                                                }}>
                                                {t === "log10" ? "Log₁₀" : t === "sqrt" ? "√" : "Lin"}
                                              </button>
                                            ))}
                                          </div>
                                          {/* Visual size range bar */}
                                          <div style={{ position: "relative", height: 18, background: "#0d1117", borderRadius: 5, border: "1px solid #21262d", padding: "0 10px", display: "flex", alignItems: "center" }}>
                                            <div style={{ position: "absolute", left: 10, top: "50%", transform: "translateY(-50%)", width: 8, height: 8, borderRadius: "50%", background: "#58a6ff", opacity: 0.7 }} />
                                            <div style={{ flex: 1, height: 1, background: "linear-gradient(to right, #58a6ff55, #58a6ff)", marginLeft: 16, marginRight: 16 }} />
                                            <div style={{ position: "absolute", right: 10, top: "50%", transform: "translateY(-50%)", width: 16, height: 16, borderRadius: "50%", background: "#58a6ff", opacity: 0.9 }} />
                                          </div>
                                          <div style={{ display: "grid", gridTemplateColumns: "28px 1fr 38px", gap: 4, alignItems: "center" }}>
                                            <span style={{ fontSize: 9.5, color: "#6e7681", textAlign: "right" }}>min</span>
                                            <input type="range" min={0.1} max={3} step={0.05} value={style.sizeMin}
                                              onChange={(e) => setLayerStyle(layerId, { sizeMin: Number(e.target.value) })}
                                              style={{ width: "100%" }}
                                            />
                                            <span style={{ fontSize: 9.5, color: "#c9d1d9", fontFamily: "ui-monospace,monospace", textAlign: "right" }}>×{style.sizeMin.toFixed(1)}</span>
                                          </div>
                                          <div style={{ display: "grid", gridTemplateColumns: "28px 1fr 38px", gap: 4, alignItems: "center" }}>
                                            <span style={{ fontSize: 9.5, color: "#6e7681", textAlign: "right" }}>max</span>
                                            <input type="range" min={0.1} max={8} step={0.1} value={style.sizeMax}
                                              onChange={(e) => setLayerStyle(layerId, { sizeMax: Number(e.target.value) })}
                                              style={{ width: "100%" }}
                                            />
                                            <span style={{ fontSize: 9.5, color: "#c9d1d9", fontFamily: "ui-monospace,monospace", textAlign: "right" }}>×{style.sizeMax.toFixed(1)}</span>
                                          </div>
                                        </>
                                      ) : (
                                        <div style={{ display: "grid", gridTemplateColumns: "28px 1fr 30px", gap: 4, alignItems: "center" }}>
                                          <span style={{ fontSize: 9.5, color: "#6e7681", textAlign: "right" }}>r</span>
                                          <input type="range" min={0.5} max={8} step={0.25} value={ui.sampleSize}
                                            onChange={(e) => setUi((p) => ({ ...p, sampleSize: Number(e.target.value) || 7 }))}
                                            style={{ width: "100%" }}
                                          />
                                          <span style={{ fontSize: 9.5, color: "#c9d1d9", fontFamily: "ui-monospace,monospace", textAlign: "right" }}>{ui.sampleSize}</span>
                                        </div>
                                      )}
                                    </div>
                                  ) : null}

                                  {/* ── SHAPE (assay_points only) ── */}
                                  {lBaseType === "assay_points" ? (
                                    <div>
                                      <div style={{ fontSize: 8.5, fontWeight: 700, letterSpacing: "0.07em", color: "#484f58", textTransform: "uppercase" as const, marginBottom: 2 }}>Shape</div>
                                      {/* Display mode toggle */}
                                      {/* Points / Heatmap mode toggle */}
                                      <div style={{ display: "flex", gap: 3, marginBottom: 5 }}>
                                        {(["points", "heatmap"] as const).map(m => (
                                          <button key={m} type="button"
                                            onClick={() => setLayerStyle(layerId, { displayMode: m })}
                                            style={{
                                              flex: 1, fontSize: 10, fontWeight: 600, padding: "3px 0", borderRadius: 4,
                                              fontFamily: "inherit", cursor: "pointer",
                                              background: style.displayMode === m ? "#388bfd22" : "transparent",
                                              border: `1px solid ${style.displayMode === m ? "#388bfd" : "#30363d"}`,
                                              color: style.displayMode === m ? "#58a6ff" : "#6e7681",
                                            }}>
                                            {m === "points" ? "● Points" : "▦ Heatmap"}
                                          </button>
                                        ))}
                                      </div>

                                      {/* Point shape grid — hidden in heatmap mode */}
                                      {style.displayMode === "points" ? (
                                        <div style={{ display: "grid", gridTemplateColumns: "repeat(6,1fr)", gap: 3 }}>
                                          {([
                                            { v: "sphere",  icon: "●", label: "Sphere" },
                                            { v: "box",     icon: "■", label: "Box" },
                                            { v: "diamond", icon: "◆", label: "Diamond" },
                                            { v: "cone",    icon: "▲", label: "Cone" },
                                            { v: "disc",    icon: "⬤", label: "Disc" },
                                            { v: "spike",   icon: "▼", label: "Spike" },
                                          ] as const).map(({ v, icon, label }) => (
                                            <button key={v} type="button" title={label}
                                              onClick={() => setLayerStyle(layerId, { pointShape: v })}
                                              style={{
                                                padding: "3px 0", fontSize: 14, borderRadius: 5, cursor: "pointer",
                                                background: style.pointShape === v ? "#388bfd22" : "transparent",
                                                border: `1px solid ${style.pointShape === v ? "#388bfd" : "#30363d"}`,
                                                color: style.pointShape === v ? "#58a6ff" : "#6e7681",
                                                fontFamily: "inherit",
                                              }}>
                                              {icon}
                                            </button>
                                          ))}
                                        </div>
                                      ) : null}

                                      {/* Heatmap-specific controls */}
                                      {style.displayMode === "heatmap" ? (
                                        <div style={{ display: "flex", flexDirection: "column", gap: 5, marginTop: 3, padding: "6px 8px", background: "#0d1117", borderRadius: 6, border: "1px solid #21262d" }}>
                                          <div style={{ fontSize: 8.5, color: "#484f58", fontWeight: 700, letterSpacing: "0.07em", textTransform: "uppercase" as const }}>IDW Heatmap</div>
                                          <div style={{ fontSize: 9, color: "#6e7681", lineHeight: 1.4 }}>
                                            Inverse-distance weighted interpolation draped on terrain. Pick a measure above to populate.
                                          </div>
                                          {/* Grid resolution */}
                                          <div style={{ display: "grid", gridTemplateColumns: "70px 1fr 36px", gap: 4, alignItems: "center" }}>
                                            <span style={{ fontSize: 9.5, color: "#8b949e" }}>Resolution</span>
                                            <input type="range" min={64} max={512} step={32}
                                              value={style.hmGridSize ?? 256}
                                              onChange={(e) => setLayerStyle(layerId, { hmGridSize: Number(e.target.value) })}
                                            />
                                            <span style={{ fontSize: 9, fontFamily: "ui-monospace,monospace", color: "#c9d1d9", textAlign: "right" }}>{style.hmGridSize ?? 256}</span>
                                          </div>
                                          {/* IDW power */}
                                          <div style={{ display: "grid", gridTemplateColumns: "70px 1fr 36px", gap: 4, alignItems: "center" }}>
                                            <span style={{ fontSize: 9.5, color: "#8b949e" }}>Blend power</span>
                                            <input type="range" min={1} max={4} step={0.1}
                                              value={style.hmPower ?? 2}
                                              onChange={(e) => setLayerStyle(layerId, { hmPower: Number(e.target.value) })}
                                            />
                                            <span style={{ fontSize: 9, fontFamily: "ui-monospace,monospace", color: "#c9d1d9", textAlign: "right" }}>{(style.hmPower ?? 2).toFixed(1)}</span>
                                          </div>
                                          <div style={{ fontSize: 8.5, color: "#484f58" }}>
                                            Higher blend power = sharper transitions; lower = smoother gradients.
                                          </div>
                                        </div>
                                      ) : null}
                                    </div>
                                  ) : null}

                                  {/* ── OPACITY ── */}
                                  <div style={{ display: "grid", gridTemplateColumns: "28px 1fr 30px", gap: 4, alignItems: "center" }}>
                                    <span style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.07em", color: "#484f58", textTransform: "uppercase" as const, gridColumn: "1/-1", marginBottom: 2 }}>Opacity</span>
                                    <span style={{ fontSize: 9.5, color: "#6e7681", textAlign: "right" }}>%</span>
                                    <input type="range" min={0.05} max={1} step={0.05}
                                      value={style.opacity}
                                      onChange={(e) => setLayerStyle(layerId, { opacity: Number(e.target.value) })}
                                      style={{ width: "100%" }}
                                    />
                                    <span style={{ fontSize: 9.5, color: "#c9d1d9", fontFamily: "ui-monospace,monospace", textAlign: "right" }}>{Math.round(style.opacity * 100)}</span>
                                  </div>

                                </div>
                              ) : null}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                </details>

                <details open>
                  <summary>View</summary>
                  <div style={{ display: "grid", gap: 6 }}>
                    <div className="me-section-note">X = easting · Y = elevation · Z = northing</div>
                    <label><input type="checkbox" checked={ui.invertDepth} onChange={(e) => setUi((p) => ({ ...p, invertDepth: e.target.checked }))} /> Invert vertical axis (depth-down view)</label>
                    <label>Radius scale<input type="range" min={0.25} max={4} step={0.05} value={ui.radiusScale} onChange={(e) => setUi((p) => ({ ...p, radiusScale: Number(e.target.value) || 1 }))} /></label>
                  </div>
                </details>

                <details>
                  <summary>Scene</summary>
                  <div style={{ display: "grid", gap: 6 }}>
                    <label>
                      Background
                      <select
                        value={ui.bgPreset}
                        onChange={(e) => setUi((p) => ({ ...p, bgPreset: e.target.value as SceneUiState["bgPreset"] }))}
                      >
                        <option value="night">Night (deep blue)</option>
                        <option value="dusk">Dusk (deep purple)</option>
                        <option value="dawn">Dawn (warm brown)</option>
                        <option value="overcast">Overcast (cool grey)</option>
                      </select>
                    </label>
                    <label>
                      Ambient light ({Math.round(ui.ambientIntensity * 100)}%)
                      <input
                        type="range"
                        min={0}
                        max={2}
                        step={0.05}
                        value={ui.ambientIntensity}
                        onChange={(e) => setUi((p) => ({ ...p, ambientIntensity: Number(e.target.value) }))}
                      />
                    </label>
                    <label>
                      <input
                        type="checkbox"
                        checked={ui.fogEnabled}
                        onChange={(e) => setUi((p) => ({ ...p, fogEnabled: e.target.checked }))}
                      />
                      Depth fog
                    </label>
                    <label>
                      <input
                        type="checkbox"
                        checked={ui.gridEnabled}
                        onChange={(e) => setUi((p) => ({ ...p, gridEnabled: e.target.checked }))}
                      />
                      Ground grid
                    </label>
                  </div>
                </details>
                </div>
              </>
            )}
          </aside>
        </div>
      </div>
    </div>
  );
}
