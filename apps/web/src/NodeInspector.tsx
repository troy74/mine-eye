import { useCallback, useEffect, useMemo, useState, type CSSProperties } from "react";
import { parseCsv } from "./csvParse";
import type { ApiNode, ArtifactEntry } from "./graphApi";
import { api, patchNodeParams, runGraph } from "./graphApi";
import { CrsPicker } from "./CrsPicker";
import { extractHeatmapMeasureCandidatesFromJson } from "./spatialExtract";
import { NodeOutputPanel } from "./NodeOutputPanel";
import { NodePreviewSnippet } from "./NodePreviewSnippet";
import { PORT_TAXONOMY_SUMMARY } from "./portTaxonomy";
import {
  PIPELINE_GEOMETRY_NOTES,
  isAcquisitionCsvKind,
} from "./pipelineSchema";
import type { InspectorTab } from "./graphInspectorContext";
import { ACQUISITION_EPSG_OPTIONS } from "./crsOptions";
import type { RegistryNodeSpec } from "./nodeRegistry";
import {
  lockLabel,
  resolveNodeInspectorCapabilities,
} from "./nodeInspectorActions";

const OUTPUT_CRS_OPTIONS: { value: string; label: string }[] = [
  { value: "project", label: "Project CRS (default)" },
  { value: "wgs84", label: "EPSG:4326 / WGS84 (web maps)" },
  { value: "source", label: "Same as source file CRS" },
  { value: "custom", label: "Custom EPSG…" },
];

const DEFAULT_TILE_PROVIDER_CATALOG: Array<{ id: string; label: string }> = [
  { id: "esri_world_imagery", label: "Esri World Imagery" },
  { id: "esri_world_topo", label: "Esri World Topo" },
  { id: "esri_natgeo", label: "Esri NatGeo World" },
  { id: "usgs_imagery", label: "USGS Imagery" },
];

function providerLabel(providerId: string): string {
  return (
    DEFAULT_TILE_PROVIDER_CATALOG.find((p) => p.id === providerId)?.label ??
    providerId
  );
}

function getUiParams(node: ApiNode): Record<string, unknown> {
  const p = node.config.params;
  if (p && typeof p === "object" && !Array.isArray(p)) {
    const ui = (p as Record<string, unknown>).ui;
    if (ui && typeof ui === "object" && !Array.isArray(ui)) {
      return ui as Record<string, unknown>;
    }
  }
  return {};
}

type Props = {
  graphId: string;
  activeBranchId?: string | null;
  node: ApiNode;
  nodeSpec?: RegistryNodeSpec;
  projectEpsg: number;
  workspaceUsedEpsgs?: number[];
  tab: InspectorTab;
  onTab: (t: InspectorTab) => void;
  onClose: () => void;
  onOpenEditor?: () => void;
  mode?: "sidebar" | "editor";
  onNodeUpdated: (n: ApiNode) => void;
  nodeArtifacts: ArtifactEntry[];
  onPipelineQueued?: () => void;
};

export function NodeInspector({
  graphId,
  activeBranchId,
  node,
  nodeSpec,
  projectEpsg,
  workspaceUsedEpsgs = [],
  tab,
  onTab,
  onClose,
  onOpenEditor,
  mode = "sidebar",
  onNodeUpdated,
  nodeArtifacts,
  onPipelineQueued,
}: Props) {
  const kind = node.config.kind;
  const csvCapable = isAcquisitionCsvKind(kind);
  const isHeatmapNode = kind === "assay_heatmap";
  const isDataModelTransformNode = kind === "data_model_transform";
  const isTerrainAdjustNode = kind === "terrain_adjust";
  const isDemFetchNode = kind === "dem_fetch";
  const isIsoExtractNode = kind === "surface_iso_extract";
  const isTilebrokerNode = kind === "tilebroker";
  const isAoiNode = kind === "aoi";
  const hasConfigTab =
    isHeatmapNode || isDataModelTransformNode || isTerrainAdjustNode || isDemFetchNode || isIsoExtractNode || isTilebrokerNode || isAoiNode;
  const hasMappingTab = csvCapable;
  const hasCrsTab = csvCapable;

  const initialUi = useMemo(() => getUiParams(node), [node]);

  const [crsMode, setCrsMode] = useState<string>(() => {
    const u = initialUi;
    if (u.use_project_crs === false && typeof u.source_crs_epsg === "number") {
      const known = ACQUISITION_EPSG_OPTIONS.some(
        (o) => o.value === String(u.source_crs_epsg)
      );
      return known ? String(u.source_crs_epsg) : "custom";
    }
    return "project";
  });
  const [sourceCustomEpsg, setSourceCustomEpsg] = useState<string>(() => {
    const v = initialUi.source_crs_epsg;
    return typeof v === "number" && Number.isFinite(v) ? String(v) : "4326";
  });

  const [mapping, setMapping] = useState<Record<string, string>>(() => {
    const m = initialUi.mapping;
    if (m && typeof m === "object" && !Array.isArray(m)) {
      return { ...(m as Record<string, string>) };
    }
    return {};
  });

  const [zRelative, setZRelative] = useState<boolean>(() =>
    Boolean(initialUi.z_is_relative)
  );

  const [outputCrsMode, setOutputCrsMode] = useState<string>(() => {
    const m = initialUi.output_crs_mode;
    if (m === "source" || m === "wgs84" || m === "custom" || m === "project") {
      return m;
    }
    return "project";
  });
  const [outputCustomEpsg, setOutputCustomEpsg] = useState<string>(() => {
    const e = initialUi.output_crs_epsg;
    return typeof e === "number" && Number.isFinite(e) ? String(e) : "28355";
  });

  const [csvName, setCsvName] = useState<string>(
    () => (typeof initialUi.csv_filename === "string" ? initialUi.csv_filename : "")
  );
  const [heatMeasure, setHeatMeasure] = useState<string>(
    () => (typeof initialUi.measure === "string" ? initialUi.measure : "")
  );
  const [heatMethod, setHeatMethod] = useState<string>(
    () => (typeof initialUi.method === "string" ? initialUi.method : "idw")
  );
  const [heatScale, setHeatScale] = useState<string>(
    () => (typeof initialUi.scale === "string" ? initialUi.scale : "linear")
  );
  const [heatPalette, setHeatPalette] = useState<string>(
    () => (typeof initialUi.palette === "string" ? initialUi.palette : "rainbow")
  );
  const [heatClampLow, setHeatClampLow] = useState<string>(
    () =>
      typeof initialUi.clamp_low_pct === "number"
        ? String(initialUi.clamp_low_pct)
        : "0"
  );
  const [heatClampHigh, setHeatClampHigh] = useState<string>(
    () =>
      typeof initialUi.clamp_high_pct === "number"
        ? String(initialUi.clamp_high_pct)
        : "100"
  );
  const [heatMinVisible, setHeatMinVisible] = useState<string>(
    () =>
      typeof initialUi.min_visible_value === "number"
        ? String(initialUi.min_visible_value)
        : ""
  );
  const [heatMaxVisible, setHeatMaxVisible] = useState<string>(
    () =>
      typeof initialUi.max_visible_value === "number"
        ? String(initialUi.max_visible_value)
        : ""
  );
  const [heatIdwPower, setHeatIdwPower] = useState<string>(
    () => (typeof initialUi.idw_power === "number" ? String(initialUi.idw_power) : "2")
  );
  const [heatSmoothness, setHeatSmoothness] = useState<string>(
    () =>
      typeof initialUi.smoothness === "number" ? String(initialUi.smoothness) : "256"
  );
  const [heatRadius, setHeatRadius] = useState<string>(
    () =>
      typeof initialUi.search_radius_m === "number"
        ? String(initialUi.search_radius_m)
        : "0"
  );
  const [heatMinPoints, setHeatMinPoints] = useState<string>(
    () => (typeof initialUi.min_points === "number" ? String(initialUi.min_points) : "3")
  );
  const [heatMaxPoints, setHeatMaxPoints] = useState<string>(
    () => (typeof initialUi.max_points === "number" ? String(initialUi.max_points) : "32")
  );
  const [heatContoursEnabled, setHeatContoursEnabled] = useState<boolean>(
    () => Boolean(initialUi.contours_enabled)
  );
  const [heatContourMode, setHeatContourMode] = useState<string>(
    () =>
      typeof initialUi.contour_mode === "string"
        ? initialUi.contour_mode
        : "fixed_interval"
  );
  const [heatContourInterval, setHeatContourInterval] = useState<string>(
    () =>
      typeof initialUi.contour_interval === "number"
        ? String(initialUi.contour_interval)
        : "1"
  );
  const [heatContourLevels, setHeatContourLevels] = useState<string>(
    () =>
      typeof initialUi.contour_levels === "number" ? String(initialUi.contour_levels) : "10"
  );
  const [heatContourLevelsList, setHeatContourLevelsList] = useState<string>(
    () =>
      Array.isArray(initialUi.contour_levels_list)
        ? (initialUi.contour_levels_list as unknown[])
            .filter((x): x is number => typeof x === "number" && Number.isFinite(x))
            .join(", ")
        : ""
  );
  const [heatGradientEnabled, setHeatGradientEnabled] = useState<boolean>(
    () => Boolean(initialUi.gradient_enabled)
  );
  const [heatGradientMode, setHeatGradientMode] = useState<string>(
    () =>
      typeof initialUi.gradient_mode === "string" ? initialUi.gradient_mode : "magnitude"
  );
  const [heatOutputCrsMode, setHeatOutputCrsMode] = useState<string>(
    () =>
      typeof initialUi.output_crs_mode === "string"
        ? initialUi.output_crs_mode
        : "project"
  );
  const [heatOutputCustomEpsg, setHeatOutputCustomEpsg] = useState<string>(
    () =>
      typeof initialUi.output_crs_epsg === "number"
        ? String(initialUi.output_crs_epsg)
        : "4326"
  );
  const [heatMeasureOptions, setHeatMeasureOptions] = useState<string[]>([]);
  const [terrainFitMode, setTerrainFitMode] = useState<string>(
    () => (typeof initialUi.fit_mode === "string" ? initialUi.fit_mode : "vertical_bias")
  );
  const [terrainShiftX, setTerrainShiftX] = useState<string>(
    () =>
      typeof initialUi.manual_shift_x === "number" ? String(initialUi.manual_shift_x) : "0"
  );
  const [terrainShiftY, setTerrainShiftY] = useState<string>(
    () =>
      typeof initialUi.manual_shift_y === "number" ? String(initialUi.manual_shift_y) : "0"
  );
  const [dmSourceKey, setDmSourceKey] = useState<string>(
    () => (typeof initialUi.source_key === "string" ? initialUi.source_key : "")
  );
  const [dmSelectColumns, setDmSelectColumns] = useState<string>(
    () =>
      Array.isArray(initialUi.select_columns)
        ? (initialUi.select_columns as unknown[])
            .filter((x): x is string => typeof x === "string" && x.trim().length > 0)
            .join(", ")
        : ""
  );
  const [dmRenameMap, setDmRenameMap] = useState<string>(
    () =>
      initialUi.rename_map && typeof initialUi.rename_map === "object" && !Array.isArray(initialUi.rename_map)
        ? JSON.stringify(initialUi.rename_map)
        : "{}"
  );
  const [dmDeriveConstants, setDmDeriveConstants] = useState<string>(
    () =>
      initialUi.derive_constants && typeof initialUi.derive_constants === "object" && !Array.isArray(initialUi.derive_constants)
        ? JSON.stringify(initialUi.derive_constants)
        : "{}"
  );
  const [demFitMode, setDemFitMode] = useState<string>(
    () => (typeof initialUi.fit_mode === "string" ? initialUi.fit_mode : "vertical_bias")
  );
  const [demFitMinPoints, setDemFitMinPoints] = useState<string>(
    () => (typeof initialUi.fit_min_points === "number" ? String(initialUi.fit_min_points) : "3")
  );
  const [demLowDensityCells, setDemLowDensityCells] = useState<string>(
    () => (typeof initialUi.low_density_cells === "number" ? String(initialUi.low_density_cells) : "8")
  );
  const [demAnchorCells, setDemAnchorCells] = useState<string>(
    () => (typeof initialUi.anchor_cells === "number" ? String(initialUi.anchor_cells) : "0.75")
  );
  const [isoMode, setIsoMode] = useState<string>(
    () => (typeof initialUi.mode === "string" ? initialUi.mode : "fixed_interval")
  );
  const [isoInterval, setIsoInterval] = useState<string>(
    () => (typeof initialUi.interval === "number" ? String(initialUi.interval) : "1")
  );
  const [isoLevels, setIsoLevels] = useState<string>(
    () => (typeof initialUi.levels === "number" ? String(initialUi.levels) : "10")
  );
  const [isoZBase, setIsoZBase] = useState<string>(
    () => (typeof initialUi.z_base === "number" ? String(initialUi.z_base) : "0")
  );
  const [isoZScale, setIsoZScale] = useState<string>(
    () => (typeof initialUi.z_scale === "number" ? String(initialUi.z_scale) : "1")
  );
  const [aoiMode, setAoiMode] = useState<string>(
    () => (typeof initialUi.mode === "string" ? initialUi.mode : "inferred")
  );
  const [aoiMarginPct, setAoiMarginPct] = useState<string>(
    () =>
      typeof initialUi.margin_pct === "number" && Number.isFinite(initialUi.margin_pct)
        ? String(initialUi.margin_pct)
        : "25"
  );
  const [aoiBbox, setAoiBbox] = useState<string>(
    () =>
      Array.isArray(initialUi.bbox)
        ? (initialUi.bbox as unknown[])
            .filter((x): x is number => typeof x === "number" && Number.isFinite(x))
            .slice(0, 4)
            .join(", ")
        : ""
  );
  const [aoiLocked, setAoiLocked] = useState<boolean>(() => Boolean(initialUi.locked));
  const [tbProviderCatalog, setTbProviderCatalog] = useState<string[]>(() => {
    const configuredCatalog = Array.isArray(initialUi.provider_catalog)
      ? (initialUi.provider_catalog as unknown[]).filter(
          (x): x is string => typeof x === "string" && x.trim().length > 0
        )
      : [];
    if (configuredCatalog.length) return configuredCatalog;
    return DEFAULT_TILE_PROVIDER_CATALOG.map((p) => p.id);
  });
  const [tbProviderPrecedence, setTbProviderPrecedence] = useState<string[]>(() => {
    const configured = Array.isArray(initialUi.provider_precedence)
      ? (initialUi.provider_precedence as unknown[]).filter(
          (x): x is string => typeof x === "string" && x.trim().length > 0
        )
      : [];
    if (configured.length) return configured;
    if (typeof initialUi.provider_id === "string" && initialUi.provider_id.trim().length > 0) {
      return [initialUi.provider_id.trim()];
    }
    return ["esri_world_imagery"];
  });
  const [tbProviderToAdd, setTbProviderToAdd] = useState<string>("");
  const [tbCustomTileset, setTbCustomTileset] = useState<string>(
    () => (typeof initialUi.custom_tileset === "string" ? initialUi.custom_tileset : "")
  );
  const [tbCrsPreference, setTbCrsPreference] = useState<string>(
    () =>
      Array.isArray(initialUi.crs_preference)
        ? (initialUi.crs_preference as unknown[])
            .filter((x): x is number => typeof x === "number" && Number.isFinite(x))
            .join(", ")
        : "3857, 4326"
  );
  const [tbResolutionLadder, setTbResolutionLadder] = useState<string>(
    () =>
      Array.isArray(initialUi.resolution_ladder_px)
        ? (initialUi.resolution_ladder_px as unknown[])
            .filter((x): x is number => typeof x === "number" && Number.isFinite(x))
            .join(", ")
        : "1024, 768, 512"
  );
  const [tbRetryLimit, setTbRetryLimit] = useState<string>(
    () => (typeof initialUi.retry_limit === "number" ? String(initialUi.retry_limit) : "2")
  );
  const [tbTimeoutMs, setTbTimeoutMs] = useState<string>(
    () => (typeof initialUi.timeout_ms === "number" ? String(initialUi.timeout_ms) : "8000")
  );
  const [tbMaxCandidates, setTbMaxCandidates] = useState<string>(
    () => (typeof initialUi.max_candidates === "number" ? String(initialUi.max_candidates) : "16")
  );
  const [tbCacheScope, setTbCacheScope] = useState<string>(
    () => (typeof initialUi.cache_scope === "string" ? initialUi.cache_scope : "project")
  );
  const [tbCacheTtl, setTbCacheTtl] = useState<string>(
    () => (typeof initialUi.cache_ttl_s === "number" ? String(initialUi.cache_ttl_s) : "604800")
  );
  const [tbAllowStale, setTbAllowStale] = useState<boolean>(
    () => (typeof initialUi.allow_stale_on_error === "boolean" ? initialUi.allow_stale_on_error : true)
  );
  const [tbDebounceProfile, setTbDebounceProfile] = useState<string>(
    () => (typeof initialUi.debounce_profile === "string" ? initialUi.debounce_profile : "free_default")
  );
  const [tbLastWarnings, setTbLastWarnings] = useState<string[]>([]);
  const [tbAoiSourceUsed, setTbAoiSourceUsed] = useState<string>("");
  const [tbEffectiveConfigText, setTbEffectiveConfigText] = useState<string>("");
  const [headers, setHeaders] = useState<string[]>([]);
  const [csvRows, setCsvRows] = useState<string[][]>([]);
  const [previewRows, setPreviewRows] = useState<string[][]>([]);
  const [saveMsg, setSaveMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [policyBusy, setPolicyBusy] = useState(false);
  const [policyMsg, setPolicyMsg] = useState<string | null>(null);
  const [policyErr, setPolicyErr] = useState<string | null>(null);
  const [runBusy, setRunBusy] = useState(false);
  const [runMsg, setRunMsg] = useState<string | null>(null);
  const [runErr, setRunErr] = useState<string | null>(null);

  useEffect(() => {
    const u = getUiParams(node);
    const m = u.mapping;
    if (m && typeof m === "object" && !Array.isArray(m)) {
      setMapping({ ...(m as Record<string, string>) });
    }
    if (u.use_project_crs === false && typeof u.source_crs_epsg === "number") {
      const v = String(u.source_crs_epsg);
      const known = ACQUISITION_EPSG_OPTIONS.some((o) => o.value === v);
      setCrsMode(known ? v : "custom");
      setSourceCustomEpsg(v);
    } else {
      setCrsMode("project");
      setSourceCustomEpsg("4326");
    }
    setZRelative(Boolean(u.z_is_relative));
    const ocm = u.output_crs_mode;
    if (ocm === "source" || ocm === "wgs84" || ocm === "custom" || ocm === "project") {
      setOutputCrsMode(ocm);
    } else {
      setOutputCrsMode("project");
    }
    const oce = u.output_crs_epsg;
    setOutputCustomEpsg(
      typeof oce === "number" && Number.isFinite(oce) ? String(oce) : "28355"
    );
    setCsvName(typeof u.csv_filename === "string" ? u.csv_filename : "");
    setHeatMeasure(typeof u.measure === "string" ? u.measure : "");
    setHeatMethod(typeof u.method === "string" ? u.method : "idw");
    setHeatScale(typeof u.scale === "string" ? u.scale : "linear");
    setHeatPalette(typeof u.palette === "string" ? u.palette : "rainbow");
    setHeatClampLow(
      typeof u.clamp_low_pct === "number" ? String(u.clamp_low_pct) : "0"
    );
    setHeatClampHigh(
      typeof u.clamp_high_pct === "number" ? String(u.clamp_high_pct) : "100"
    );
    setHeatMinVisible(
      typeof u.min_visible_value === "number" ? String(u.min_visible_value) : ""
    );
    setHeatMaxVisible(
      typeof u.max_visible_value === "number" ? String(u.max_visible_value) : ""
    );
    setHeatIdwPower(typeof u.idw_power === "number" ? String(u.idw_power) : "2");
    setHeatSmoothness(
      typeof u.smoothness === "number" ? String(u.smoothness) : "256"
    );
    setHeatRadius(
      typeof u.search_radius_m === "number" ? String(u.search_radius_m) : "0"
    );
    setHeatMinPoints(typeof u.min_points === "number" ? String(u.min_points) : "3");
    setHeatMaxPoints(typeof u.max_points === "number" ? String(u.max_points) : "32");
    setHeatContoursEnabled(Boolean(u.contours_enabled));
    setHeatContourMode(
      typeof u.contour_mode === "string" ? u.contour_mode : "fixed_interval"
    );
    setHeatContourInterval(
      typeof u.contour_interval === "number" ? String(u.contour_interval) : "1"
    );
    setHeatContourLevels(
      typeof u.contour_levels === "number" ? String(u.contour_levels) : "10"
    );
    setHeatContourLevelsList(
      Array.isArray(u.contour_levels_list)
        ? (u.contour_levels_list as unknown[])
            .filter((x): x is number => typeof x === "number" && Number.isFinite(x))
            .join(", ")
        : ""
    );
    setHeatGradientEnabled(Boolean(u.gradient_enabled));
    setHeatGradientMode(
      typeof u.gradient_mode === "string" ? u.gradient_mode : "magnitude"
    );
    setHeatOutputCrsMode(
      typeof u.output_crs_mode === "string" ? u.output_crs_mode : "project"
    );
    setHeatOutputCustomEpsg(
      typeof u.output_crs_epsg === "number" ? String(u.output_crs_epsg) : "4326"
    );
    setTerrainFitMode(
      typeof u.fit_mode === "string" ? u.fit_mode : "vertical_bias"
    );
    setTerrainShiftX(
      typeof u.manual_shift_x === "number" ? String(u.manual_shift_x) : "0"
    );
    setTerrainShiftY(
      typeof u.manual_shift_y === "number" ? String(u.manual_shift_y) : "0"
    );
    setDmSourceKey(typeof u.source_key === "string" ? u.source_key : "");
    setDmSelectColumns(
      Array.isArray(u.select_columns)
        ? (u.select_columns as unknown[])
            .filter((x): x is string => typeof x === "string" && x.trim().length > 0)
            .join(", ")
        : ""
    );
    setDmRenameMap(
      u.rename_map && typeof u.rename_map === "object" && !Array.isArray(u.rename_map)
        ? JSON.stringify(u.rename_map)
        : "{}"
    );
    setDmDeriveConstants(
      u.derive_constants &&
        typeof u.derive_constants === "object" &&
        !Array.isArray(u.derive_constants)
        ? JSON.stringify(u.derive_constants)
        : "{}"
    );
    setDemFitMode(typeof u.fit_mode === "string" ? u.fit_mode : "vertical_bias");
    setDemFitMinPoints(typeof u.fit_min_points === "number" ? String(u.fit_min_points) : "3");
    setDemLowDensityCells(
      typeof u.low_density_cells === "number" ? String(u.low_density_cells) : "8"
    );
    setDemAnchorCells(typeof u.anchor_cells === "number" ? String(u.anchor_cells) : "0.75");
    setIsoMode(typeof u.mode === "string" ? u.mode : "fixed_interval");
    setIsoInterval(typeof u.interval === "number" ? String(u.interval) : "1");
    setIsoLevels(typeof u.levels === "number" ? String(u.levels) : "10");
    setIsoZBase(typeof u.z_base === "number" ? String(u.z_base) : "0");
    setIsoZScale(typeof u.z_scale === "number" ? String(u.z_scale) : "1");
    setAoiMode(typeof u.mode === "string" ? u.mode : "inferred");
    setAoiMarginPct(
      typeof u.margin_pct === "number" && Number.isFinite(u.margin_pct) ? String(u.margin_pct) : "25"
    );
    setAoiBbox(
      Array.isArray(u.bbox)
        ? (u.bbox as unknown[])
            .filter((x): x is number => typeof x === "number" && Number.isFinite(x))
            .slice(0, 4)
            .join(", ")
        : ""
    );
    setAoiLocked(typeof u.locked === "boolean" ? u.locked : false);
    const nextCatalog = Array.isArray(u.provider_catalog)
      ? (u.provider_catalog as unknown[])
          .filter((x): x is string => typeof x === "string" && x.trim().length > 0)
      : [];
    setTbProviderCatalog(
      nextCatalog.length ? nextCatalog : DEFAULT_TILE_PROVIDER_CATALOG.map((p) => p.id)
    );
    const nextPrecedence = Array.isArray(u.provider_precedence)
      ? (u.provider_precedence as unknown[])
          .filter((x): x is string => typeof x === "string" && x.trim().length > 0)
      : typeof u.provider_id === "string" && u.provider_id.trim().length > 0
        ? [u.provider_id.trim()]
        : ["esri_world_imagery"];
    setTbProviderPrecedence(nextPrecedence);
    setTbProviderToAdd("");
    setTbCustomTileset(typeof u.custom_tileset === "string" ? u.custom_tileset : "");
    setTbCrsPreference(
      Array.isArray(u.crs_preference)
        ? (u.crs_preference as unknown[])
            .filter((x): x is number => typeof x === "number" && Number.isFinite(x))
            .join(", ")
        : "3857, 4326"
    );
    setTbResolutionLadder(
      Array.isArray(u.resolution_ladder_px)
        ? (u.resolution_ladder_px as unknown[])
            .filter((x): x is number => typeof x === "number" && Number.isFinite(x))
            .join(", ")
        : "1024, 768, 512"
    );
    setTbRetryLimit(typeof u.retry_limit === "number" ? String(u.retry_limit) : "2");
    setTbTimeoutMs(typeof u.timeout_ms === "number" ? String(u.timeout_ms) : "8000");
    setTbMaxCandidates(typeof u.max_candidates === "number" ? String(u.max_candidates) : "16");
    setTbCacheScope(typeof u.cache_scope === "string" ? u.cache_scope : "project");
    setTbCacheTtl(typeof u.cache_ttl_s === "number" ? String(u.cache_ttl_s) : "604800");
    setTbAllowStale(typeof u.allow_stale_on_error === "boolean" ? u.allow_stale_on_error : true);
    setTbDebounceProfile(typeof u.debounce_profile === "string" ? u.debounce_profile : "free_default");
    const h = u.csv_headers;
    if (Array.isArray(h) && h.every((x) => typeof x === "string")) {
      setHeaders(h as string[]);
    }
    const fullRows = u.csv_rows;
    if (Array.isArray(fullRows)) {
      setCsvRows(fullRows as string[][]);
    } else {
      setCsvRows([]);
    }
    const pr = u.csv_preview_rows;
    if (Array.isArray(pr)) {
      setPreviewRows(pr as string[][]);
    } else {
      setPreviewRows([]);
    }
  }, [node]);

  useEffect(() => {
    setPolicyMsg(null);
    setPolicyErr(null);
    setRunMsg(null);
    setRunErr(null);
  }, [node.id]);

  useEffect(() => {
    if (!isHeatmapNode) {
      setHeatMeasureOptions([]);
      return;
    }
    let cancelled = false;
    void (async () => {
      try {
        const hit =
          nodeArtifacts.find((a) => a.key.endsWith("/heatmap.json")) ??
          nodeArtifacts.find((a) => a.key.endsWith("heatmap.json")) ??
          null;
        if (!hit) {
          if (!cancelled) setHeatMeasureOptions([]);
          return;
        }
        const r = await fetch(api(hit.url));
        if (!r.ok) return;
        const txt = await r.text();
        const opts = extractHeatmapMeasureCandidatesFromJson(txt);
        if (!cancelled) setHeatMeasureOptions(opts);
      } catch {
        if (!cancelled) setHeatMeasureOptions([]);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [isHeatmapNode, nodeArtifacts]);

  useEffect(() => {
    if (!isTilebrokerNode) {
      setTbLastWarnings([]);
      setTbAoiSourceUsed("");
      setTbEffectiveConfigText("");
      return;
    }
    let cancelled = false;
    void (async () => {
      try {
        const hit =
          nodeArtifacts.find((a) => a.key.endsWith("/tilebroker_response.json")) ??
          nodeArtifacts.find((a) => a.key.endsWith("/imagery_drape.json")) ??
          null;
        if (!hit) return;
        const r = await fetch(api(hit.url));
        if (!r.ok) return;
        const raw = (await r.json()) as Record<string, unknown>;
        if (cancelled) return;
        const warnings = Array.isArray(raw.warnings)
          ? raw.warnings.filter((w): w is string => typeof w === "string")
          : [];
        setTbLastWarnings(warnings);
        setTbAoiSourceUsed(typeof raw.aoi_source_used === "string" ? raw.aoi_source_used : "");
        const eff =
          raw.effective_config && typeof raw.effective_config === "object"
            ? JSON.stringify(raw.effective_config, null, 2)
            : "";
        setTbEffectiveConfigText(eff);
      } catch {
        if (!cancelled) {
          setTbLastWarnings([]);
          setTbAoiSourceUsed("");
          setTbEffectiveConfigText("");
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [isTilebrokerNode, nodeArtifacts]);

  const onPickFile = useCallback(
    (file: File | null) => {
      if (!file) return;
      setErr(null);
      setCsvName(file.name);
      const reader = new FileReader();
      reader.onload = () => {
        const text = String(reader.result ?? "");
        const { headers: h, rows } = parseCsv(text);
        setHeaders(h);
        setCsvRows(rows);
        setPreviewRows(rows.slice(0, 8));
      };
      reader.readAsText(file, "UTF-8");
    },
    []
  );

  const applySave = useCallback(async () => {
    setErr(null);
    setSaveMsg(null);
    const useProject = crsMode === "project";
    const epsg = useProject
      ? projectEpsg
      : crsMode === "custom"
        ? parseInt(sourceCustomEpsg, 10)
        : parseInt(crsMode, 10);
    if (!Number.isFinite(epsg) || epsg <= 0) {
      setErr("Please provide a valid EPSG code.");
      return;
    }
    const ui: Record<string, unknown> = {
      mapping: { ...mapping },
      use_project_crs: useProject,
      source_crs_epsg: useProject ? undefined : epsg,
      z_is_relative: kind === "collar_ingest" ? zRelative : undefined,
      csv_filename: csvName || undefined,
      csv_headers: headers.length ? headers : undefined,
      csv_rows: csvRows,
      csv_preview_rows: csvRows.slice(0, 8),
    };
    const n = (v: string, fallback: number) => {
      const x = Number(v);
      return Number.isFinite(x) ? x : fallback;
    };
    if (isHeatmapNode) {
      ui.measure = heatMeasure.trim();
      ui.method = heatMethod;
      ui.scale = heatScale;
      ui.palette = heatPalette;
      ui.clamp_low_pct = Math.max(0, Math.min(100, n(heatClampLow, 0)));
      ui.clamp_high_pct = Math.max(0, Math.min(100, n(heatClampHigh, 100)));
      ui.min_visible_value =
        heatMinVisible.trim().length > 0 ? n(heatMinVisible, 0) : undefined;
      ui.max_visible_value =
        heatMaxVisible.trim().length > 0 ? n(heatMaxVisible, 0) : undefined;
      ui.idw_power = Math.max(1, Math.min(4, n(heatIdwPower, 2)));
      ui.smoothness = Math.max(128, Math.min(512, Math.trunc(n(heatSmoothness, 256))));
      ui.search_radius_m = Math.max(0, n(heatRadius, 0));
      ui.min_points = Math.max(1, Math.trunc(n(heatMinPoints, 3)));
      ui.max_points = Math.max(Math.trunc(n(heatMinPoints, 3)), Math.trunc(n(heatMaxPoints, 32)));
      ui.contours_enabled = heatContoursEnabled;
      ui.contour_mode = heatContourMode;
      ui.contour_interval = Math.max(0.0001, n(heatContourInterval, 1));
      ui.contour_levels = Math.max(2, Math.trunc(n(heatContourLevels, 10)));
      ui.contour_levels_list =
        heatContourLevelsList.trim().length > 0
          ? heatContourLevelsList
              .split(",")
              .map((x) => Number(x.trim()))
              .filter((x) => Number.isFinite(x))
          : undefined;
      ui.gradient_enabled = heatGradientEnabled;
      ui.gradient_mode = heatGradientMode;
      ui.output_crs_mode = heatOutputCrsMode;
      ui.output_crs_epsg =
        heatOutputCrsMode === "custom"
          ? Math.max(1, Math.trunc(n(heatOutputCustomEpsg, 4326)))
          : undefined;
    } else if (isTerrainAdjustNode) {
      ui.fit_mode = terrainFitMode;
      ui.manual_shift_x = n(terrainShiftX, 0);
      ui.manual_shift_y = n(terrainShiftY, 0);
    } else if (isDataModelTransformNode) {
      ui.source_key = dmSourceKey.trim() || undefined;
      ui.select_columns = dmSelectColumns
        .split(",")
        .map((x) => x.trim())
        .filter((x) => x.length > 0);
      try {
        const parsed = JSON.parse(dmRenameMap);
        ui.rename_map =
          parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
      } catch {
        ui.rename_map = {};
      }
      try {
        const parsed = JSON.parse(dmDeriveConstants);
        ui.derive_constants =
          parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
      } catch {
        ui.derive_constants = {};
      }
    } else if (isDemFetchNode) {
      ui.fit_mode = demFitMode;
      ui.fit_min_points = Math.max(3, Math.trunc(n(demFitMinPoints, 3)));
      ui.low_density_cells = Math.max(1, n(demLowDensityCells, 8));
      ui.anchor_cells = Math.max(0.05, n(demAnchorCells, 0.75));
    } else if (isIsoExtractNode) {
      ui.mode = isoMode;
      ui.interval = Math.max(0.000001, n(isoInterval, 1));
      ui.levels = Math.max(2, Math.trunc(n(isoLevels, 10)));
      ui.z_base = n(isoZBase, 0);
      ui.z_scale = n(isoZScale, 1);
    } else if (isTilebrokerNode) {
      const parseNumList = (raw: string): number[] =>
        raw
          .split(",")
          .map((x) => Number(x.trim()))
          .filter((x) => Number.isFinite(x))
          .map((x) => Math.trunc(x));
      const catalog = tbProviderCatalog
        .map((x) => x.trim())
        .filter((x) => x.length > 0);
      const providers = tbProviderPrecedence
        .map((x) => x.trim())
        .filter((x) => x.length > 0 && (catalog.includes(x) || x.length > 0));
      const defaultProvider = providers[0] ?? catalog[0] ?? "esri_world_imagery";
      ui.provider_catalog = catalog;
      ui.provider_precedence = providers.length ? providers : [defaultProvider];
      ui.provider_id = defaultProvider;
      ui.custom_tileset = tbCustomTileset.trim() || undefined;
      ui.crs_preference = parseNumList(tbCrsPreference);
      ui.resolution_ladder_px = parseNumList(tbResolutionLadder);
      ui.retry_limit = Math.max(0, Math.trunc(n(tbRetryLimit, 2)));
      ui.timeout_ms = Math.max(500, Math.trunc(n(tbTimeoutMs, 8000)));
      ui.max_candidates = Math.max(1, Math.trunc(n(tbMaxCandidates, 16)));
      ui.cache_scope = tbCacheScope;
      ui.cache_ttl_s = Math.max(60, Math.trunc(n(tbCacheTtl, 604800)));
      ui.allow_stale_on_error = tbAllowStale;
      ui.debounce_profile = tbDebounceProfile;
    } else if (isAoiNode) {
      ui.mode = aoiMode;
      ui.margin_pct = Math.max(0, n(aoiMarginPct, 25));
      const vals = aoiBbox
        .split(",")
        .map((x) => Number(x.trim()))
        .filter((x) => Number.isFinite(x));
      ui.bbox = vals.length >= 4 ? vals.slice(0, 4) : undefined;
      ui.locked = aoiLocked;
    }
    if (kind === "collar_ingest") {
      ui.output_crs_mode = outputCrsMode;
      ui.output_crs_epsg =
        outputCrsMode === "custom"
          ? Math.trunc(parseInt(outputCustomEpsg, 10) || 4326)
          : undefined;
    }
    try {
      const updated = await patchNodeParams(graphId, node.id, { ui }, { branchId: activeBranchId });
      onNodeUpdated(updated);
      setSaveMsg("Saved to node config (re-run pipeline to rebuild artifacts).");
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    }
  }, [
    crsMode,
    sourceCustomEpsg,
    projectEpsg,
    mapping,
    csvName,
    headers,
    csvRows,
    previewRows,
    isHeatmapNode,
    heatMeasure,
    heatMethod,
    heatScale,
    heatPalette,
    heatClampLow,
    heatClampHigh,
    heatMinVisible,
    heatMaxVisible,
    heatIdwPower,
    heatSmoothness,
    heatRadius,
    heatMinPoints,
    heatMaxPoints,
    heatContoursEnabled,
    heatContourMode,
    heatContourInterval,
    heatContourLevels,
    heatContourLevelsList,
    heatGradientEnabled,
    heatGradientMode,
    heatOutputCrsMode,
    heatOutputCustomEpsg,
    isTerrainAdjustNode,
    terrainFitMode,
    terrainShiftX,
    terrainShiftY,
    isDataModelTransformNode,
    dmSourceKey,
    dmSelectColumns,
    dmRenameMap,
    dmDeriveConstants,
    isDemFetchNode,
    demFitMode,
    demFitMinPoints,
    demLowDensityCells,
    demAnchorCells,
    isIsoExtractNode,
    isoMode,
    isoInterval,
    isoLevels,
    isoZBase,
    isoZScale,
    isAoiNode,
    aoiMode,
    aoiMarginPct,
    aoiBbox,
    aoiLocked,
    isTilebrokerNode,
    tbProviderPrecedence,
    tbProviderCatalog,
    tbCustomTileset,
    tbCrsPreference,
    tbResolutionLadder,
    tbRetryLimit,
    tbTimeoutMs,
    tbMaxCandidates,
    tbCacheScope,
    tbCacheTtl,
    tbAllowStale,
    tbDebounceProfile,
    graphId,
    activeBranchId,
    node.id,
    kind,
    zRelative,
    outputCrsMode,
    outputCustomEpsg,
    onNodeUpdated,
  ]);

  const setRecomputePolicy = useCallback(
    async (next: "auto" | "manual") => {
      setPolicyBusy(true);
      setPolicyMsg(null);
      setPolicyErr(null);
      try {
        const updated = await patchNodeParams(
          graphId,
          node.id,
          {},
          {
            branchId: activeBranchId,
            policy: {
              recompute: next,
              propagation:
                node.policy.propagation === "eager" ||
                node.policy.propagation === "debounce" ||
                node.policy.propagation === "hold"
                  ? node.policy.propagation
                  : "debounce",
              quality:
                node.policy.quality === "preview" || node.policy.quality === "final"
                  ? node.policy.quality
                  : "preview",
            },
          }
        );
        onNodeUpdated(updated);
        setPolicyMsg(
          next === "auto"
            ? "Autorun enabled for this node."
            : "Autorun disabled (node is locked until manually run)."
        );
      } catch (e) {
        setPolicyErr(e instanceof Error ? e.message : String(e));
      } finally {
        setPolicyBusy(false);
      }
    },
    [activeBranchId, graphId, node.id, node.policy.propagation, node.policy.quality, onNodeUpdated]
  );

  const runThisNode = useCallback(async () => {
    setRunBusy(true);
    setRunMsg(null);
    setRunErr(null);
    try {
      const res = await runGraph(graphId, { dirtyRoots: [node.id], includeManual: true });
      const nq = res.queued?.length ?? 0;
      const ns = res.skipped_manual?.length ?? 0;
      setRunMsg(
        nq > 0
          ? ns > 0
            ? `Queued ${nq} job(s); ${ns} still skipped manual.`
            : `Queued ${nq} job(s).`
          : "No jobs queued."
      );
      onPipelineQueued?.();
    } catch (e) {
      setRunErr(e instanceof Error ? e.message : String(e));
    } finally {
      setRunBusy(false);
    }
  }, [graphId, node.id, onPipelineQueued]);

  const selectCol = (field: string, label: string) => (
    <label style={lab}>
      <span style={labSpan}>{label}</span>
      <select
        value={mapping[field] ?? ""}
        onChange={(e) =>
          setMapping((m) => ({ ...m, [field]: e.target.value }))
        }
        style={sel}
      >
        <option value="">—</option>
        {headers.map((h) => (
          <option key={h} value={h}>
            {h}
          </option>
        ))}
      </select>
    </label>
  );

  const isProviderEnabled = useCallback(
    (providerId: string) => tbProviderPrecedence.includes(providerId),
    [tbProviderPrecedence]
  );

  const toggleProvider = useCallback((providerId: string, enabled: boolean) => {
    setTbProviderPrecedence((prev) => {
      if (enabled) {
        if (prev.includes(providerId)) return prev;
        return [...prev, providerId];
      }
      return prev.filter((p) => p !== providerId);
    });
  }, []);

  const moveProvider = useCallback((providerId: string, direction: -1 | 1) => {
    setTbProviderPrecedence((prev) => {
      const idx = prev.indexOf(providerId);
      if (idx < 0) return prev;
      const nextIdx = idx + direction;
      if (nextIdx < 0 || nextIdx >= prev.length) return prev;
      const next = [...prev];
      const [item] = next.splice(idx, 1);
      next.splice(nextIdx, 0, item);
      return next;
    });
  }, []);

  const addProviderToCatalog = useCallback(() => {
    const providerId = tbProviderToAdd.trim();
    if (!providerId.length) return;
    setTbProviderCatalog((prev) => {
      if (prev.includes(providerId)) return prev;
      return [...prev, providerId];
    });
    setTbProviderPrecedence((prev) => (prev.includes(providerId) ? prev : [...prev, providerId]));
    setTbProviderToAdd("");
  }, [tbProviderToAdd]);

  const removeProviderFromCatalog = useCallback((providerId: string) => {
    setTbProviderCatalog((prev) => prev.filter((p) => p !== providerId));
    setTbProviderPrecedence((prev) => prev.filter((p) => p !== providerId));
  }, []);

  const configTabLabel = useMemo(
    () =>
      isHeatmapNode
        ? "Heatmap"
        : isTerrainAdjustNode
          ? "Terrain fit"
          : isDataModelTransformNode
            ? "Data transform"
          : isDemFetchNode
            ? "DEM fit"
          : isIsoExtractNode
            ? "Iso extract"
            : isTilebrokerNode
              ? "Tilebroker"
              : isAoiNode
                ? "AOI"
              : "Config",
    [isDataModelTransformNode, isDemFetchNode, isHeatmapNode, isIsoExtractNode, isTerrainAdjustNode, isTilebrokerNode, isAoiNode]
  );

  const tabs = useMemo(() => {
    const base: Array<readonly [InspectorTab, string]> = [];
    if (mode === "editor") {
      base.push(["summary", "Summary"]);
    }
    if (hasConfigTab) {
      base.push(["config", configTabLabel]);
    }
    if (hasMappingTab) {
      base.push(["mapping", "Mapping"], ["crs", "CRS"]);
    }
    base.push(["output", "Output"], ["diagnostics", "Run"]);
    if (mode === "editor") {
      base.push(["preview", "View"]);
    }
    return base;
  }, [configTabLabel, hasConfigTab, hasMappingTab, mode]);

  const actions = useMemo(
    () =>
      resolveNodeInspectorCapabilities(node, {
        csvCapable,
        hasConfigTab,
        hasMappingTab,
        hasCrsTab,
        nodeSpec,
      }),
    [csvCapable, hasConfigTab, hasCrsTab, hasMappingTab, node, nodeSpec]
  );

  const configIconActive = tabs.some(([k]) => k === tab) && tab === actions.configTab;
  const previewIconActive =
    tabs.some(([k]) => k === tab) &&
    (tab === actions.previewTab || (actions.previewTab === "preview" && tab === "output"));

  useEffect(() => {
    if (tabs.some(([k]) => k === tab)) return;
    onTab((tabs[0]?.[0] ?? "output") as InspectorTab);
  }, [onTab, tab, tabs]);

  return (
    <aside
      style={{
        width: 320,
        minWidth: 280,
        borderLeft: "1px solid #30363d",
        background: "#161b22",
        display: "flex",
        flexDirection: "column",
        maxHeight: "100%",
        overflow: "hidden",
      }}
    >
      <div
        style={{
          padding: "10px 12px",
          borderBottom: "1px solid #30363d",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          gap: 8,
        }}
      >
        <span style={{ fontWeight: 600, fontSize: 13 }}>
          {mode === "editor" ? `${kind.replace(/_/g, " ")} editor` : kind.replace(/_/g, " ")}
        </span>
        <button type="button" onClick={onClose} style={btnGhost}>
          Close
        </button>
      </div>
      <div
        style={{
          padding: "8px 10px",
          borderBottom: "1px solid #30363d",
          display: "flex",
          gap: 6,
          flexWrap: "wrap",
          background: "#121821",
        }}
      >
        {actions.canRun && (
          <button
            type="button"
            style={actionIconBtn}
            title={runBusy ? "Queuing node run" : "Run node"}
            aria-label="Run node"
            onClick={() => void runThisNode()}
            disabled={runBusy}
          >
            <svg width="13" height="13" viewBox="0 0 14 14" aria-hidden>
              <polygon points="3,2 12,7 3,12" fill="currentColor" />
            </svg>
          </button>
        )}
        {actions.canLock && (
          <button
            type="button"
            style={actionIconBtn}
            title={lockLabel(node)}
            aria-label={lockLabel(node)}
            onClick={() =>
              void setRecomputePolicy(node.policy.recompute === "manual" ? "auto" : "manual")
            }
            disabled={policyBusy}
          >
            {node.policy.recompute === "manual" ? (
              <svg width="13" height="13" viewBox="0 0 14 14" aria-hidden>
                <rect x="3" y="6" width="8" height="6" rx="1.5" fill="none" stroke="currentColor" strokeWidth="1.4" />
                <path d="M4.5 6V4.8a2.5 2.5 0 0 1 5 0V6" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
              </svg>
            ) : (
              <svg width="13" height="13" viewBox="0 0 14 14" aria-hidden>
                <rect x="3" y="6" width="8" height="6" rx="1.5" fill="none" stroke="currentColor" strokeWidth="1.4" />
                <path d="M8.5 4.3a2.4 2.4 0 0 0-4 .5V6" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
              </svg>
            )}
          </button>
        )}
        {actions.canEdit && (
          <button
            type="button"
            style={actionIconBtn}
            title="Edit node"
            aria-label="Edit node"
            onClick={() => {
              if (onOpenEditor) onOpenEditor();
              else
                onTab(
                  tabs.some(([k]) => k === actions.editTab)
                    ? actions.editTab
                    : (tabs[0]?.[0] ?? "output")
                );
            }}
          >
            <svg width="13" height="13" viewBox="0 0 14 14" aria-hidden>
              <path d="m10.8 2.2 1 1a1.2 1.2 0 0 1 0 1.7l-6.6 6.6-2.5.7.7-2.5 6.6-6.6a1.2 1.2 0 0 1 1.7 0Z" fill="none" stroke="currentColor" strokeWidth="1.2" />
            </svg>
          </button>
        )}
        {actions.canConfig && (
          <button
            type="button"
            style={{ ...actionIconBtn, ...(configIconActive ? actionIconActive : null) }}
            title="Config panel"
            aria-label="Config panel"
            onClick={() =>
              onTab(
                tabs.some(([k]) => k === actions.configTab)
                  ? actions.configTab
                  : (tabs[0]?.[0] ?? "output")
              )
            }
          >
            <svg width="13" height="13" viewBox="0 0 14 14" aria-hidden>
              <path d="M7 2.2a4.8 4.8 0 1 0 0 9.6 4.8 4.8 0 0 0 0-9.6Zm0 2a2.8 2.8 0 1 1 0 5.6 2.8 2.8 0 0 1 0-5.6Z" fill="currentColor" />
            </svg>
          </button>
        )}
        {actions.canPreview && (
          <button
            type="button"
            style={{
              ...actionIconBtn,
              ...(previewIconActive ? actionIconPrimary : null),
              ...(previewIconActive ? actionIconActive : null),
            }}
            title="Preview node output"
            aria-label="Preview node output"
            onClick={() =>
              onTab(
                tabs.some(([k]) => k === actions.previewTab) ? actions.previewTab : "output"
              )
            }
          >
            <svg width="13" height="13" viewBox="0 0 14 14" aria-hidden>
              <path d="M1.4 7s2.2-3.6 5.6-3.6S12.6 7 12.6 7s-2.2 3.6-5.6 3.6S1.4 7 1.4 7Z" fill="none" stroke="currentColor" strokeWidth="1.2" />
              <circle cx="7" cy="7" r="1.8" fill="currentColor" />
            </svg>
          </button>
        )}
      </div>
      <div
        role="tablist"
        style={{
          display: "flex",
          borderBottom: "1px solid #30363d",
          overflowX: "auto",
          background: "#111821",
        }}
      >
        {tabs.map(([k, lab]) => (
          <button
            key={k}
            type="button"
            role="tab"
            aria-selected={tab === k}
            onClick={() => onTab(k)}
            style={{
              flex: 1,
              minWidth: 76,
              padding: "8px 6px",
              fontSize: 12,
              border: "none",
              background: tab === k ? "#0f1419" : "transparent",
              color: tab === k ? "#e6edf3" : "#8b949e",
              borderBottom: tab === k ? "2px solid #58a6ff" : "2px solid transparent",
              cursor: "pointer",
            }}
          >
            {lab}
          </button>
        ))}
      </div>
      <div style={{ flex: 1, overflow: "auto", padding: 12, fontSize: 12 }}>
        {tab === "summary" && (
          <div style={{ lineHeight: 1.5 }}>
            <div style={{ opacity: 0.85, marginBottom: 8 }}>
              <strong>{kind.replace(/_/g, " ")}</strong>
            </div>
            <div style={{ opacity: 0.65, fontSize: 11 }}>id: {node.id}</div>
            <p style={{ marginTop: 12, opacity: 0.8 }}>{PIPELINE_GEOMETRY_NOTES}</p>
            {csvCapable && (
              <p style={{ opacity: 0.75 }}>
                Use <strong>Mapping</strong> to attach a CSV and map columns. CRS overrides live
                under <strong>CRS</strong>.
              </p>
            )}
            {isHeatmapNode && (
              <p style={{ opacity: 0.75 }}>
                Use <strong>Heatmap</strong> to tune interpolation method, cutoffs, transforms,
                contour strategy, and gradient options.
              </p>
            )}
            {isTerrainAdjustNode && (
              <p style={{ opacity: 0.75 }}>
                Use <strong>Terrain fit</strong> to bias/tilt DEM surfaces toward control points
                (collars or other ground-truth XYZ), with optional XY shift.
              </p>
            )}
            {isDataModelTransformNode && (
              <p style={{ opacity: 0.75 }}>
                Use <strong>Data transform</strong> to normalize tabular payloads across sources
                (select, rename, and derive constants) before downstream modeling.
              </p>
            )}
            {isDemFetchNode && (
              <p style={{ opacity: 0.75 }}>
                Use <strong>DEM fit</strong> to fetch public elevation and optionally fit it to
                upstream XYZ control points (collars, meshes, trajectories, samples), while
                emitting a confidence overlay contract.
              </p>
            )}
            {isIsoExtractNode && (
              <p style={{ opacity: 0.75 }}>
                Use <strong>Iso extract</strong> to generate contour lines from a surface grid,
                including 3D Z projection controls for scene overlays.
              </p>
            )}
            {isTilebrokerNode && (
              <p style={{ opacity: 0.75 }}>
                Use <strong>Tilebroker</strong> to set provider precedence, CRS/size strategy, and
                cache policy. Outputs include effective config + warnings for drift-resistant
                rendering.
              </p>
            )}
            <p style={{ opacity: 0.7, marginTop: 14, fontSize: 11 }}>
              After saving, use <strong>Queue pipeline run</strong> in the header (or{" "}
              <strong>Output</strong> → queue from this node). The worker must be running to
              rebuild artifacts.
            </p>
            <NodePreviewSnippet
              graphId={graphId}
              nodeId={node.id}
              kind={kind}
              artifacts={nodeArtifacts}
            />
            <details style={{ marginTop: 12, fontSize: 10, opacity: 0.55 }}>
              <summary style={{ cursor: "pointer" }}>Port types & compatibility (V1)</summary>
              <pre
                style={{
                  marginTop: 8,
                  whiteSpace: "pre-wrap",
                  fontFamily: "inherit",
                  lineHeight: 1.45,
                }}
              >
                {PORT_TAXONOMY_SUMMARY}
              </pre>
            </details>
          </div>
        )}

        {tab === "diagnostics" && (
          <div style={{ lineHeight: 1.5 }}>
            <p style={{ fontSize: 11, opacity: 0.75, marginTop: 0 }}>
              Execution state comes from the worker after each job. Open this tab when a node shows{" "}
              <strong style={{ color: "#f85149" }}>failed</strong> on the graph.
            </p>
            <dl style={{ margin: "12px 0", fontSize: 12 }}>
              <dt style={{ opacity: 0.55, fontSize: 10 }}>Execution</dt>
              <dd style={{ margin: "2px 0 10px" }}>{node.execution}</dd>
              <dt style={{ opacity: 0.55, fontSize: 10 }}>Cache</dt>
              <dd style={{ margin: "2px 0 10px" }}>{node.cache}</dd>
              <dt style={{ opacity: 0.55, fontSize: 10 }}>Recompute policy</dt>
              <dd style={{ margin: "2px 0 10px" }}>
                {node.policy.recompute === "manual" ? "manual (locked)" : "auto"}
              </dd>
              <dt style={{ opacity: 0.55, fontSize: 10 }}>Content hash</dt>
              <dd style={{ margin: "2px 0 10px", wordBreak: "break-all" }}>
                {node.content_hash ?? "—"}
              </dd>
            </dl>
            {node.last_error ? (
              <div
                style={{
                  background: "rgba(248,81,73,0.12)",
                  border: "1px solid rgba(248,81,73,0.45)",
                  borderRadius: 8,
                  padding: 10,
                  fontSize: 11,
                  color: "#ffb1a8",
                  whiteSpace: "pre-wrap",
                  wordBreak: "break-word",
                }}
              >
                <strong style={{ color: "#f85149" }}>Last error</strong>
                <pre
                  style={{
                    margin: "8px 0 0",
                    fontFamily: "ui-monospace, monospace",
                    fontSize: 10,
                    lineHeight: 1.45,
                  }}
                >
                  {node.last_error}
                </pre>
              </div>
            ) : (
              <p style={{ opacity: 0.6, fontSize: 11 }}>
                No error stored on this node. If jobs fail before the worker updates the DB, check
                the worker terminal logs.
              </p>
            )}
            <p style={{ opacity: 0.55, fontSize: 10, marginTop: 16 }}>
              Survey/collar/assay CSV nodes need <strong>Save to node</strong> after mapping so the
              orchestrator can attach preview rows to the job payload.
            </p>
            <div
              style={{
                marginTop: 14,
                borderTop: "1px solid #30363d",
                paddingTop: 12,
              }}
            >
              <div style={{ fontSize: 11, opacity: 0.75, marginBottom: 8 }}>
                <strong>Run Controls</strong>
              </div>
              <label style={lab}>
                <span style={labSpan}>Autorun on change</span>
                <select
                  value={node.policy.recompute === "manual" ? "manual" : "auto"}
                  onChange={(e) =>
                    void setRecomputePolicy(
                      e.target.value === "manual" ? "manual" : "auto"
                    )
                  }
                  disabled={policyBusy}
                  style={sel}
                >
                  <option value="auto">Auto</option>
                  <option value="manual">Manual (locked)</option>
                </select>
              </label>
              <button
                type="button"
                onClick={() => void runThisNode()}
                disabled={runBusy}
                style={{
                  ...btnPrimary,
                  width: "100%",
                  marginTop: 4,
                  background: "#1f6feb",
                }}
              >
                {runBusy ? "Queuing…" : "Run this node now"}
              </button>
              {policyMsg && <p style={{ color: "#3fb950", marginTop: 8, fontSize: 11 }}>{policyMsg}</p>}
              {policyErr && <p style={{ color: "#f85149", marginTop: 8, fontSize: 11 }}>{policyErr}</p>}
              {runMsg && <p style={{ color: "#3fb950", marginTop: 8, fontSize: 11 }}>{runMsg}</p>}
              {runErr && <p style={{ color: "#f85149", marginTop: 8, fontSize: 11 }}>{runErr}</p>}
            </div>
          </div>
        )}

        {tab === "mapping" && (
          <div>
            {!csvCapable && (
              <p style={{ opacity: 0.75 }}>
                Mapping tab is focused on CSV acquisition nodes. This node uses upstream artifacts
                or inline payloads instead.
              </p>
            )}
            {csvCapable && (
              <>
                <label style={fileLab}>
                  <span style={labSpan}>Load CSV file</span>
                  <input
                    type="file"
                    accept=".csv,text/csv"
                    onChange={(e) => onPickFile(e.target.files?.[0] ?? null)}
                    style={{ fontSize: 11 }}
                  />
                </label>
                {csvName && (
                  <div style={{ fontSize: 11, opacity: 0.7, marginBottom: 10 }}>
                    {csvName} · {headers.length} columns · {previewRows.length} preview rows
                  </div>
                )}
                {kind === "collar_ingest" && (
                  <div style={mapGrid}>
                    {selectCol("hole_id", "Hole id")}
                    {selectCol("x", "X / Easting")}
                    {selectCol("y", "Y / Northing")}
                    {selectCol("z", "Z / RL / elevation")}
                    {selectCol("azimuth_deg", "Azimuth (optional)")}
                    {selectCol("dip_deg", "Dip (optional)")}
                    <label style={lab}>
                      <input
                        type="checkbox"
                        checked={zRelative}
                        onChange={(e) => setZRelative(e.target.checked)}
                      />
                      <span style={{ marginLeft: 6 }}>Z is relative (not absolute RL)</span>
                    </label>
                  </div>
                )}
                {kind === "survey_ingest" && (
                  <div style={mapGrid}>
                    {selectCol("hole_id", "Hole id")}
                    {selectCol("azimuth_deg", "Azimuth")}
                    {selectCol("dip_deg", "Dip")}
                    {selectCol("depth_or_length_m", "Depth or segment length (m)")}
                    {selectCol("segment_id", "Segment id (optional)")}
                  </div>
                )}
                {kind === "surface_sample_ingest" && (
                  <div style={mapGrid}>
                    {selectCol("sample_id", "Sample id (optional)")}
                    {selectCol("x", "X / Easting")}
                    {selectCol("y", "Y / Northing")}
                    {selectCol("z", "Z / elevation (optional)")}
                  </div>
                )}
                {kind === "assay_ingest" && (
                  <div style={mapGrid}>
                    {selectCol("hole_id", "Hole id")}
                    {selectCol("from_m", "From depth (m)")}
                    {selectCol("to_m", "To depth (m)")}
                  </div>
                )}
                {previewRows.length > 0 && (
                  <div style={{ marginTop: 12, overflow: "auto" }}>
                    <div style={{ fontSize: 10, opacity: 0.6, marginBottom: 4 }}>Preview</div>
                    <table style={tbl}>
                      <thead>
                        <tr>
                          {headers.map((h) => (
                            <th key={h}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {previewRows.map((row, i) => (
                          <tr key={i}>
                            {row.map((c, j) => (
                              <td key={j}>{c}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </>
            )}
          </div>
        )}

        {tab === "config" && isHeatmapNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Configure interpolation, cutoffs, contours, gradient products, and output CRS for{" "}
              <strong>assay heatmap</strong>.
            </p>
            <div style={mapGrid}>
              <label style={lab}>
                <span style={labSpan}>Primary measure field</span>
                {heatMeasureOptions.length > 0 ? (
                  <select
                    value={heatMeasure}
                    onChange={(e) => setHeatMeasure(e.target.value)}
                    style={sel}
                  >
                    <option value="">Auto (first numeric measure)</option>
                    {heatMeasureOptions.map((m) => (
                      <option key={m} value={m}>
                        {m}
                      </option>
                    ))}
                  </select>
                ) : (
                  <input
                    type="text"
                    value={heatMeasure}
                    onChange={(e) => setHeatMeasure(e.target.value)}
                    placeholder="e.g. au_ppm"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                )}
              </label>
              <label style={lab}>
                <span style={labSpan}>Interpolation method</span>
                <select value={heatMethod} onChange={(e) => setHeatMethod(e.target.value)} style={sel}>
                  <option value="idw">IDW</option>
                  <option value="rbf">RBF</option>
                  <option value="nearest">Nearest</option>
                  <option value="kriging">Ordinary kriging (starter)</option>
                </select>
              </label>
              <label style={lab}>
                <span style={labSpan}>Value transform</span>
                <select value={heatScale} onChange={(e) => setHeatScale(e.target.value)} style={sel}>
                  <option value="linear">Linear</option>
                  <option value="log10">Log10</option>
                  <option value="ln">Natural log</option>
                  <option value="sqrt">Square root</option>
                </select>
              </label>
              <label style={lab}>
                <span style={labSpan}>Palette</span>
                <select value={heatPalette} onChange={(e) => setHeatPalette(e.target.value)} style={sel}>
                  <option value="rainbow">Rainbow</option>
                  <option value="viridis">Viridis</option>
                  <option value="inferno">Inferno</option>
                  <option value="terrain">Terrain</option>
                </select>
              </label>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Clamp low (%)</span>
                  <input
                    type="number"
                    value={heatClampLow}
                    onChange={(e) => setHeatClampLow(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Clamp high (%)</span>
                  <input
                    type="number"
                    value={heatClampHigh}
                    onChange={(e) => setHeatClampHigh(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Min visible grade (mask below)</span>
                  <input
                    type="number"
                    value={heatMinVisible}
                    onChange={(e) => setHeatMinVisible(e.target.value)}
                    placeholder="optional"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Max visible grade (mask above)</span>
                  <input
                    type="number"
                    value={heatMaxVisible}
                    onChange={(e) => setHeatMaxVisible(e.target.value)}
                    placeholder="optional"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>IDW power</span>
                  <input
                    type="number"
                    step="0.1"
                    value={heatIdwPower}
                    onChange={(e) => setHeatIdwPower(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Smoothness</span>
                  <input
                    type="number"
                    value={heatSmoothness}
                    onChange={(e) => setHeatSmoothness(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Search radius (m; 0=all)</span>
                  <input
                    type="number"
                    value={heatRadius}
                    onChange={(e) => setHeatRadius(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Min points</span>
                  <input
                    type="number"
                    value={heatMinPoints}
                    onChange={(e) => setHeatMinPoints(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Max points</span>
                  <input
                    type="number"
                    value={heatMaxPoints}
                    onChange={(e) => setHeatMaxPoints(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <label style={lab}>
                <input
                  type="checkbox"
                  checked={heatContoursEnabled}
                  onChange={(e) => setHeatContoursEnabled(e.target.checked)}
                />
                <span style={{ marginLeft: 6 }}>Generate contours</span>
              </label>
              {heatContoursEnabled && (
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                  <label style={lab}>
                    <span style={labSpan}>Contour mode</span>
                    <select
                      value={heatContourMode}
                      onChange={(e) => setHeatContourMode(e.target.value)}
                      style={sel}
                    >
                      <option value="fixed_interval">Fixed interval</option>
                      <option value="quantile">Quantile</option>
                    </select>
                  </label>
                  <label style={lab}>
                    <span style={labSpan}>Interval</span>
                    <input
                      type="number"
                      step="0.01"
                      value={heatContourInterval}
                      onChange={(e) => setHeatContourInterval(e.target.value)}
                      style={{ ...sel, fontFamily: "inherit" }}
                    />
                  </label>
                  <label style={lab}>
                    <span style={labSpan}>Levels</span>
                    <input
                      type="number"
                      value={heatContourLevels}
                      onChange={(e) => setHeatContourLevels(e.target.value)}
                      style={{ ...sel, fontFamily: "inherit" }}
                    />
                  </label>
                </div>
              )}
              {heatContoursEnabled && (
                <label style={lab}>
                  <span style={labSpan}>Explicit contour grades (comma separated)</span>
                  <input
                    type="text"
                    value={heatContourLevelsList}
                    onChange={(e) => setHeatContourLevelsList(e.target.value)}
                    placeholder="e.g. 0.1, 0.25, 0.5, 1.0"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              )}
              <label style={lab}>
                <input
                  type="checkbox"
                  checked={heatGradientEnabled}
                  onChange={(e) => setHeatGradientEnabled(e.target.checked)}
                />
                <span style={{ marginLeft: 6 }}>Emit gradient analysis</span>
              </label>
              {heatGradientEnabled && (
                <label style={lab}>
                  <span style={labSpan}>Gradient mode</span>
                  <select
                    value={heatGradientMode}
                    onChange={(e) => setHeatGradientMode(e.target.value)}
                    style={sel}
                  >
                    <option value="magnitude">Magnitude</option>
                    <option value="directional">Directional</option>
                  </select>
                </label>
              )}
              <label style={lab}>
                <span style={labSpan}>Output CRS</span>
                <select
                  value={heatOutputCrsMode}
                  onChange={(e) => setHeatOutputCrsMode(e.target.value)}
                  style={sel}
                >
                  <option value="project">Project CRS</option>
                  <option value="source">Source CRS</option>
                  <option value="custom">Custom EPSG</option>
                </select>
              </label>
              {heatOutputCrsMode === "custom" && (
                <label style={lab}>
                  <span style={labSpan}>Output EPSG</span>
                  <input
                    type="number"
                    value={heatOutputCustomEpsg}
                    onChange={(e) => setHeatOutputCustomEpsg(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              )}
            </div>
          </div>
        )}

        {tab === "config" && isTerrainAdjustNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Fit/nudge a DEM surface to control points with known XYZ.
            </p>
            <div style={mapGrid}>
              <label style={lab}>
                <span style={labSpan}>Fit mode</span>
                <select
                  value={terrainFitMode}
                  onChange={(e) => setTerrainFitMode(e.target.value)}
                  style={sel}
                >
                  <option value="vertical_bias">Vertical bias only</option>
                  <option value="affine_xy_z">Affine XY + Z tilt</option>
                </select>
              </label>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Manual shift X</span>
                  <input
                    type="number"
                    value={terrainShiftX}
                    onChange={(e) => setTerrainShiftX(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Manual shift Y</span>
                  <input
                    type="number"
                    value={terrainShiftY}
                    onChange={(e) => setTerrainShiftY(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
            </div>
          </div>
        )}

        {tab === "config" && isDataModelTransformNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Normalize incoming tabular artifacts for downstream joins/modeling.
            </p>
            <div style={mapGrid}>
              <label style={lab}>
                <span style={labSpan}>Source key (optional)</span>
                <input
                  type="text"
                  value={dmSourceKey}
                  onChange={(e) => setDmSourceKey(e.target.value)}
                  placeholder="rows, assays, collars, ..."
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Select columns (comma separated)</span>
                <input
                  type="text"
                  value={dmSelectColumns}
                  onChange={(e) => setDmSelectColumns(e.target.value)}
                  placeholder="hole_id, x, y, z, au_ppm"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Rename map (JSON object)</span>
                <textarea
                  value={dmRenameMap}
                  onChange={(e) => setDmRenameMap(e.target.value)}
                  rows={3}
                  style={{ ...sel, fontFamily: "monospace", resize: "vertical" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Derive constants (JSON object)</span>
                <textarea
                  value={dmDeriveConstants}
                  onChange={(e) => setDmDeriveConstants(e.target.value)}
                  rows={3}
                  style={{ ...sel, fontFamily: "monospace", resize: "vertical" }}
                />
              </label>
            </div>
          </div>
        )}

        {tab === "config" && isDemFetchNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Fetch public DEM then optionally fit to any upstream XYZ controls (collars, points,
              trajectory vertices, mesh vertices) inside AOI.
            </p>
            <div style={mapGrid}>
              <label style={lab}>
                <span style={labSpan}>Fit mode</span>
                <select value={demFitMode} onChange={(e) => setDemFitMode(e.target.value)} style={sel}>
                  <option value="none">No fit (provider only)</option>
                  <option value="vertical_bias">Vertical bias only</option>
                  <option value="affine_xy_z">Affine XY + Z tilt</option>
                </select>
              </label>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Min control points</span>
                  <input
                    type="number"
                    value={demFitMinPoints}
                    onChange={(e) => setDemFitMinPoints(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Anchor radius (cells)</span>
                  <input
                    type="number"
                    step="0.1"
                    value={demAnchorCells}
                    onChange={(e) => setDemAnchorCells(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <label style={lab}>
                <span style={labSpan}>Low-density threshold (cells)</span>
                <input
                  type="number"
                  step="0.5"
                  value={demLowDensityCells}
                  onChange={(e) => setDemLowDensityCells(e.target.value)}
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
            </div>
            <p style={{ opacity: 0.65, fontSize: 11, marginTop: 10 }}>
              Outputs include <code>confidence_grid</code> classes: raw provider, fitted provider,
              interpolation, ground-truth anchor, and low-density/missing.
            </p>
          </div>
        )}

        {tab === "config" && isIsoExtractNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Extract contour lines / iso bands from a connected surface grid.
            </p>
            <div style={mapGrid}>
              <label style={lab}>
                <span style={labSpan}>Break mode</span>
                <select value={isoMode} onChange={(e) => setIsoMode(e.target.value)} style={sel}>
                  <option value="fixed_interval">Fixed interval</option>
                  <option value="quantile">Quantile</option>
                </select>
              </label>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Interval</span>
                  <input
                    type="number"
                    step="0.01"
                    value={isoInterval}
                    onChange={(e) => setIsoInterval(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Levels (quantile)</span>
                  <input
                    type="number"
                    value={isoLevels}
                    onChange={(e) => setIsoLevels(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>3D Z base</span>
                  <input
                    type="number"
                    value={isoZBase}
                    onChange={(e) => setIsoZBase(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>3D Z scale</span>
                  <input
                    type="number"
                    value={isoZScale}
                    onChange={(e) => setIsoZScale(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
            </div>
          </div>
        )}

        {tab === "config" && isAoiNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Define area-of-interest from connected georef inputs, with optional manual bbox
              override.
            </p>
            <div style={mapGrid}>
              <label style={lab}>
                <span style={labSpan}>Mode</span>
                <select value={aoiMode} onChange={(e) => setAoiMode(e.target.value)} style={sel}>
                  <option value="inferred">inferred (from inputs)</option>
                  <option value="manual">manual bbox preferred</option>
                </select>
              </label>
              <label style={lab}>
                <span style={labSpan}>Margin (%) around inferred extent</span>
                <input
                  type="number"
                  value={aoiMarginPct}
                  onChange={(e) => setAoiMarginPct(e.target.value)}
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Manual bbox (xmin, ymin, xmax, ymax)</span>
                <input
                  type="text"
                  value={aoiBbox}
                  onChange={(e) => setAoiBbox(e.target.value)}
                  placeholder="5.9036, 6.0660, 5.9093, 6.0679"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <input
                  type="checkbox"
                  checked={aoiLocked}
                  onChange={(e) => setAoiLocked(e.target.checked)}
                />
                <span style={{ marginLeft: 6 }}>Lock AOI extent</span>
              </label>
            </div>
            <p style={{ opacity: 0.65, fontSize: 11, marginTop: 10 }}>
              Tip: wire `collars`, `trajectory`, `assay_points`, or `terrain` bounds into AOI
              inputs for automatic extent tracking.
            </p>
          </div>
        )}

        {tab === "config" && isTilebrokerNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Configure tilebroker provider precedence, fetch strategy, and cache policy.
            </p>
            <div style={mapGrid}>
              <label style={lab}>
                <span style={labSpan}>Provider order (checked providers are used in sequence)</span>
                <div
                  style={{
                    border: "1px solid #30363d",
                    borderRadius: 8,
                    background: "#0f1419",
                    overflow: "hidden",
                  }}
                >
                  {tbProviderCatalog.map((providerId, rowIndex) => {
                    const idx = tbProviderPrecedence.indexOf(providerId);
                    const enabled = idx >= 0;
                    return (
                      <div
                        key={providerId}
                        style={{
                          display: "grid",
                          gridTemplateColumns: "min-content 1fr min-content min-content min-content",
                          alignItems: "center",
                          gap: 8,
                          padding: "7px 8px",
                          borderTop: rowIndex > 0 ? "1px solid #21262d" : "none",
                          opacity: enabled ? 1 : 0.78,
                        }}
                      >
                        <input
                          type="checkbox"
                          checked={enabled}
                          onChange={(e) => toggleProvider(providerId, e.target.checked)}
                        />
                        <div style={{ lineHeight: 1.3 }}>
                          <div>{providerLabel(providerId)}</div>
                          <div style={{ opacity: 0.6, fontSize: 10 }}>{providerId}</div>
                        </div>
                        <button
                          type="button"
                          style={miniIconBtn}
                          title="Move up"
                          onClick={() => moveProvider(providerId, -1)}
                          disabled={!enabled || idx <= 0}
                        >
                          ↑
                        </button>
                        <button
                          type="button"
                          style={miniIconBtn}
                          title="Move down"
                          onClick={() => moveProvider(providerId, 1)}
                          disabled={!enabled || idx < 0 || idx >= tbProviderPrecedence.length - 1}
                        >
                          ↓
                        </button>
                        <button
                          type="button"
                          style={miniIconBtn}
                          title="Remove from catalog"
                          onClick={() => removeProviderFromCatalog(providerId)}
                        >
                          ×
                        </button>
                      </div>
                    );
                  })}
                </div>
              </label>
              <label style={lab}>
                <span style={labSpan}>Add provider id to catalog</span>
                <div style={{ display: "grid", gridTemplateColumns: "1fr auto", gap: 6 }}>
                  <input
                    type="text"
                    value={tbProviderToAdd}
                    onChange={(e) => setTbProviderToAdd(e.target.value)}
                    placeholder="my_provider_id"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                  <button type="button" onClick={addProviderToCatalog} style={btnGhost}>
                    Add
                  </button>
                </div>
              </label>
              <label style={lab}>
                <span style={labSpan}>Custom tileset URL template (optional; overrides provider)</span>
                <input
                  type="text"
                  value={tbCustomTileset}
                  onChange={(e) => setTbCustomTileset(e.target.value)}
                  placeholder="https://.../{z}/{x}/{y}.png"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>CRS preference order</span>
                <input
                  type="text"
                  value={tbCrsPreference}
                  onChange={(e) => setTbCrsPreference(e.target.value)}
                  placeholder="3857, 4326"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Resolution ladder px</span>
                <input
                  type="text"
                  value={tbResolutionLadder}
                  onChange={(e) => setTbResolutionLadder(e.target.value)}
                  placeholder="1024, 768, 512"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Retry limit</span>
                  <input
                    type="number"
                    value={tbRetryLimit}
                    onChange={(e) => setTbRetryLimit(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Timeout ms</span>
                  <input
                    type="number"
                    value={tbTimeoutMs}
                    onChange={(e) => setTbTimeoutMs(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Max candidates</span>
                  <input
                    type="number"
                    value={tbMaxCandidates}
                    onChange={(e) => setTbMaxCandidates(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Cache scope</span>
                  <select value={tbCacheScope} onChange={(e) => setTbCacheScope(e.target.value)} style={sel}>
                    <option value="project">project</option>
                    <option value="workspace">workspace</option>
                    <option value="session">session</option>
                  </select>
                </label>
                <label style={lab}>
                  <span style={labSpan}>Cache TTL (seconds)</span>
                  <input
                    type="number"
                    value={tbCacheTtl}
                    onChange={(e) => setTbCacheTtl(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <label style={lab}>
                <span style={labSpan}>Debounce profile</span>
                <select
                  value={tbDebounceProfile}
                  onChange={(e) => setTbDebounceProfile(e.target.value)}
                  style={sel}
                >
                  <option value="free_default">free_default</option>
                  <option value="paid_conservative">paid_conservative</option>
                </select>
              </label>
              <label style={lab}>
                <input
                  type="checkbox"
                  checked={tbAllowStale}
                  onChange={(e) => setTbAllowStale(e.target.checked)}
                />
                <span style={{ marginLeft: 6 }}>Allow stale cache on fetch error</span>
              </label>
            </div>
            {(tbAoiSourceUsed || tbLastWarnings.length > 0 || tbEffectiveConfigText) && (
              <div
                style={{
                  marginTop: 12,
                  border: "1px solid #30363d",
                  borderRadius: 8,
                  padding: 10,
                  background: "#0f1419",
                }}
              >
                <div style={{ fontWeight: 600, fontSize: 11, marginBottom: 6 }}>Last Run Diagnostics</div>
                {tbAoiSourceUsed && (
                  <div style={{ fontSize: 11, opacity: 0.85, marginBottom: 4 }}>
                    AOI source used: <code>{tbAoiSourceUsed}</code>
                  </div>
                )}
                {tbLastWarnings.length > 0 && (
                  <div style={{ fontSize: 11, color: "#d29922", marginBottom: 6 }}>
                    Warnings: {tbLastWarnings.join(", ")}
                  </div>
                )}
                {tbEffectiveConfigText && (
                  <pre style={{ ...preBox, marginTop: 0 }}>{tbEffectiveConfigText}</pre>
                )}
              </div>
            )}
          </div>
        )}

        {tab === "crs" && (
          <div>
            <p style={{ opacity: 0.8, marginBottom: 10 }}>
              Workspace project CRS: <strong>EPSG:{projectEpsg}</strong> (from the graph’s
              workspace; used when you choose project CRS below).
            </p>
            <label style={lab}>
              <span style={labSpan}>Source file CRS (single picker)</span>
              <CrsPicker
                value={crsMode === "project" ? "project" : sourceCustomEpsg}
                projectEpsg={projectEpsg}
                workspaceUsedEpsgs={workspaceUsedEpsgs}
                includeProject
                onChange={(v) => {
                  if (v === "project") {
                    setCrsMode("project");
                    return;
                  }
                  setCrsMode("custom");
                  setSourceCustomEpsg(v);
                }}
              />
            </label>
            <p style={{ fontSize: 11, opacity: 0.6, marginTop: 12 }}>
              Coordinates in the CSV are interpreted in this CRS. The worker reprojects to the
              collar output CRS when they differ.
            </p>
            {kind === "collar_ingest" && (
              <>
                <hr
                  style={{
                    border: "none",
                    borderTop: "1px solid #30363d",
                    margin: "16px 0",
                  }}
                />
                <p style={{ fontSize: 12, fontWeight: 600, marginBottom: 8 }}>
                  Collar output CRS
                </p>
                <p style={{ fontSize: 11, opacity: 0.65, marginBottom: 10 }}>
                  Written <code style={{ fontSize: 10 }}>collars.json</code> uses this CRS for{" "}
                  <code style={{ fontSize: 10 }}>x</code>, <code style={{ fontSize: 10 }}>y</code>{" "}
                  (Z unchanged). Default is project CRS so downstream nodes share one frame.
                </p>
                <label style={lab}>
                  <span style={labSpan}>Output coordinates</span>
                  <select
                    value={outputCrsMode}
                    onChange={(e) => setOutputCrsMode(e.target.value)}
                    style={sel}
                  >
                    {OUTPUT_CRS_OPTIONS.map((o) => (
                      <option key={o.value} value={o.value}>
                        {o.label}
                      </option>
                    ))}
                  </select>
                </label>
                {outputCrsMode === "custom" && (
                  <label style={lab}>
                    <span style={labSpan}>Output EPSG code</span>
                    <input
                      type="number"
                      value={outputCustomEpsg}
                      onChange={(e) => setOutputCustomEpsg(e.target.value)}
                      style={{ ...sel, fontFamily: "inherit" }}
                    />
                  </label>
                )}
              </>
            )}
          </div>
        )}

        {(tab === "preview" || tab === "output") && (
          <NodeOutputPanel
            graphId={graphId}
            nodeId={node.id}
            kind={kind}
            artifacts={nodeArtifacts}
            onQueued={() => onPipelineQueued?.()}
          />
        )}

        {(tab === "mapping" || tab === "crs" || tab === "config") && (
          <>
            {err && <p style={{ color: "#f85149", marginTop: 10 }}>{err}</p>}
            {saveMsg && <p style={{ color: "#3fb950", marginTop: 10 }}>{saveMsg}</p>}

            <button
              type="button"
              onClick={applySave}
              style={{ ...btnPrimary, marginTop: 16, width: "100%" }}
            >
              Save to node
            </button>
          </>
        )}
      </div>
    </aside>
  );
}

const lab: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  gap: 4,
  marginBottom: 10,
  fontSize: 11,
};
const labSpan: CSSProperties = { opacity: 0.75 };
const sel: CSSProperties = {
  background: "#0f1419",
  color: "#e6edf3",
  border: "1px solid #30363d",
  borderRadius: 6,
  padding: "6px 8px",
  fontSize: 12,
};
const mapGrid: CSSProperties = { marginTop: 8 };
const fileLab: CSSProperties = { ...lab, marginBottom: 14 };
const tbl: CSSProperties = {
  borderCollapse: "collapse",
  fontSize: 10,
  width: "100%",
};
const btnGhost: CSSProperties = {
  background: "transparent",
  border: "1px solid #30363d",
  color: "#8b949e",
  borderRadius: 6,
  padding: "4px 10px",
  cursor: "pointer",
  fontSize: 12,
};
const btnPrimary: CSSProperties = {
  background: "#238636",
  border: "none",
  color: "#fff",
  borderRadius: 6,
  padding: "8px 12px",
  cursor: "pointer",
  fontSize: 13,
  fontWeight: 600,
};
const preBox: CSSProperties = {
  margin: "8px 0 0",
  background: "#0b1220",
  border: "1px solid #30363d",
  borderRadius: 6,
  padding: 8,
  fontSize: 10,
  lineHeight: 1.4,
  color: "#c9d1d9",
  overflow: "auto",
  whiteSpace: "pre-wrap",
  wordBreak: "break-word",
};
const actionIconBtn: CSSProperties = {
  background: "#0f1419",
  border: "1px solid #30363d",
  color: "#c9d1d9",
  borderRadius: 7,
  width: 32,
  height: 32,
  display: "grid",
  placeItems: "center",
  cursor: "pointer",
};
const actionIconPrimary: CSSProperties = {
  background: "#1f6feb",
  borderColor: "#1f6feb",
  color: "#ffffff",
};
const actionIconActive: CSSProperties = {
  boxShadow: "0 0 0 1px rgba(88,166,255,0.45) inset",
};
const miniIconBtn: CSSProperties = {
  background: "#161b22",
  border: "1px solid #30363d",
  color: "#8b949e",
  borderRadius: 6,
  minWidth: 22,
  height: 22,
  cursor: "pointer",
  fontSize: 11,
  lineHeight: 1,
};
