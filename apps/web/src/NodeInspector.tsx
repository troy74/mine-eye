import { useCallback, useEffect, useMemo, useState, type CSSProperties } from "react";
import { parseCsv } from "./csvParse";
import type { ApiNode, ArtifactEntry, ChartTemplate } from "./graphApi";
import { api, getNodeJobRuntime, listChartTemplates, patchNodeParams, runGraph, uploadTabularArtifact } from "./graphApi";
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
  onOpenAoiEditor?: (nodeId: string) => void;
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
  onOpenAoiEditor,
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
  const isBlockGradeModelNode = kind === "block_grade_model";
  const isMagneticMapperNode = kind === "magnetic_model";
  const isIpSurveyIngestNode = kind === "ip_survey_ingest";
  const isIpCorridorModelNode = kind === "ip_corridor_model";
  const isIpInversionMeshNode = kind === "ip_inversion_mesh";
  const isIpInversionPreviewNode = kind === "ip_inversion_preview";
  const isHeatmapRasterTileCacheNode = kind === "heatmap_raster_tile_cache";
  const isArtifactIngestNode = kind === "observation_ingest";
  const isMdViewerNode = kind === "md_viewer";
  const isPlotChartNode = kind === "plot_chart";
  const hasConfigTab =
    isHeatmapNode || isDataModelTransformNode || isTerrainAdjustNode || isDemFetchNode || isIsoExtractNode || isTilebrokerNode || isAoiNode || isBlockGradeModelNode || isMagneticMapperNode || isIpCorridorModelNode || isIpInversionMeshNode || isIpInversionPreviewNode || isHeatmapRasterTileCacheNode || isMdViewerNode || isPlotChartNode;
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
  const [csvArtifactKey, setCsvArtifactKey] = useState<string>(
    () => (typeof initialUi.csv_artifact_key === "string" ? initialUi.csv_artifact_key : "")
  );
  const [csvArtifactHash, setCsvArtifactHash] = useState<string>(
    () => (typeof initialUi.csv_artifact_hash === "string" ? initialUi.csv_artifact_hash : "")
  );
  const [csvDelimiter, setCsvDelimiter] = useState<string>(
    () => (typeof initialUi.csv_delimiter === "string" ? initialUi.csv_delimiter : ",")
  );
  const [csvFormat, setCsvFormat] = useState<string>(
    () => (typeof initialUi.csv_format === "string" ? initialUi.csv_format : "")
  );
  const [csvMediaType, setCsvMediaType] = useState<string>(
    () => (typeof initialUi.csv_media_type === "string" ? initialUi.csv_media_type : "")
  );
  const [csvPreviewText, setCsvPreviewText] = useState<string>(
    () => (typeof initialUi.csv_preview_text === "string" ? initialUi.csv_preview_text : "")
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
  // bbox_epsg is set by the map editor; preserved on save, displayed as info only
  const [aoiBboxEpsg, setAoiBboxEpsg] = useState<number>(
    () => (typeof initialUi.bbox_epsg === "number" && Number.isFinite(initialUi.bbox_epsg)
      ? initialUi.bbox_epsg
      : 4326)
  );
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
  const [bgElementField, setBgElementField] = useState<string>(
    () => (typeof initialUi.element_field === "string" ? initialUi.element_field : "")
  );
  const [bgElementOptions, setBgElementOptions] = useState<string[]>([]);
  const [bgBlockSizeX, setBgBlockSizeX] = useState<string>(
    () => (typeof initialUi.block_size_x === "number" ? String(initialUi.block_size_x) : "20")
  );
  const [bgBlockSizeY, setBgBlockSizeY] = useState<string>(
    () => (typeof initialUi.block_size_y === "number" ? String(initialUi.block_size_y) : "20")
  );
  const [bgBlockSizeZ, setBgBlockSizeZ] = useState<string>(
    () => (typeof initialUi.block_size_z === "number" ? String(initialUi.block_size_z) : "10")
  );
  const [bgCutoffGrade, setBgCutoffGrade] = useState<string>(
    () => (typeof initialUi.cutoff_grade === "number" ? String(initialUi.cutoff_grade) : "0")
  );
  const [bgSgMode, setBgSgMode] = useState<string>(
    () => (typeof initialUi.sg_mode === "string" ? initialUi.sg_mode : "constant")
  );
  const [bgSgField, setBgSgField] = useState<string>(
    () => (typeof initialUi.sg_field === "string" ? initialUi.sg_field : "")
  );
  const [bgSgConstant, setBgSgConstant] = useState<string>(
    () => (typeof initialUi.sg_constant === "number" ? String(initialUi.sg_constant) : "2.5")
  );
  const [bgGradeUnit, setBgGradeUnit] = useState<string>(
    () => (typeof initialUi.grade_unit === "string" ? initialUi.grade_unit : "ppm")
  );
  const [bgEstimationMethod, setBgEstimationMethod] = useState<string>(
    () => (typeof initialUi.estimation_method === "string" ? initialUi.estimation_method : "idw")
  );
  const [bgIdwPower, setBgIdwPower] = useState<string>(
    () => (typeof initialUi.idw_power === "number" ? String(initialUi.idw_power) : "2")
  );
  const [bgSearchRadiusM, setBgSearchRadiusM] = useState<string>(
    () => (typeof initialUi.search_radius_m === "number" ? String(initialUi.search_radius_m) : "0")
  );
  const [bgSearchAzimuthDeg, setBgSearchAzimuthDeg] = useState<string>(
    () =>
      typeof initialUi.search_azimuth_deg === "number"
        ? String(initialUi.search_azimuth_deg)
        : "0"
  );
  const [bgAnisotropyX, setBgAnisotropyX] = useState<string>(
    () => (typeof initialUi.anisotropy_x === "number" ? String(initialUi.anisotropy_x) : "1")
  );
  const [bgAnisotropyY, setBgAnisotropyY] = useState<string>(
    () => (typeof initialUi.anisotropy_y === "number" ? String(initialUi.anisotropy_y) : "1")
  );
  const [bgAnisotropyZ, setBgAnisotropyZ] = useState<string>(
    () => (typeof initialUi.anisotropy_z === "number" ? String(initialUi.anisotropy_z) : "1")
  );
  const [bgMinSamples, setBgMinSamples] = useState<string>(
    () => (typeof initialUi.min_samples === "number" ? String(initialUi.min_samples) : "3")
  );
  const [bgMaxSamples, setBgMaxSamples] = useState<string>(
    () => (typeof initialUi.max_samples === "number" ? String(initialUi.max_samples) : "24")
  );
  const [bgGradeMin, setBgGradeMin] = useState<string>(
    () => (typeof initialUi.grade_min === "number" ? String(initialUi.grade_min) : "")
  );
  const [bgGradeMax, setBgGradeMax] = useState<string>(
    () => (typeof initialUi.grade_max === "number" ? String(initialUi.grade_max) : "")
  );
  const [bgClipMode, setBgClipMode] = useState<string>(
    () => (typeof initialUi.clip_mode === "string" ? initialUi.clip_mode : "topography")
  );
  const [bgBelowCutoffOpacity, setBgBelowCutoffOpacity] = useState<string>(
    () =>
      typeof initialUi.below_cutoff_opacity === "number"
        ? String(initialUi.below_cutoff_opacity)
        : "0.08"
  );
  const [bgPalette, setBgPalette] = useState<string>(
    () => (typeof initialUi.palette === "string" ? initialUi.palette : "viridis")
  );
  const [bgMaxBlocks, setBgMaxBlocks] = useState<string>(
    () => (typeof initialUi.max_blocks === "number" ? String(initialUi.max_blocks) : "45000")
  );
  const [bgDomainMode, setBgDomainMode] = useState<string>(
    () => (typeof initialUi.domain_mode === "string" ? initialUi.domain_mode : "full_extent")
  );
  const [bgDomainConstraintMode, setBgDomainConstraintMode] = useState<string>(
    () =>
      typeof initialUi.domain_constraint_mode === "string"
        ? initialUi.domain_constraint_mode
        : "none"
  );
  const [bgHullBufferM, setBgHullBufferM] = useState<string>(
    () => (typeof initialUi.hull_buffer_m === "number" ? String(initialUi.hull_buffer_m) : "0")
  );
  const [bgExtrapolationBufferM, setBgExtrapolationBufferM] = useState<string>(
    () =>
      typeof initialUi.extrapolation_buffer_m === "number"
        ? String(initialUi.extrapolation_buffer_m)
        : "20"
  );
  const [bgCompositeLengthM, setBgCompositeLengthM] = useState<string>(
    () =>
      typeof initialUi.composite_length_m === "number"
        ? String(initialUi.composite_length_m)
        : "0"
  );
  const [bgTopCutMode, setBgTopCutMode] = useState<string>(
    () => (typeof initialUi.top_cut_mode === "string" ? initialUi.top_cut_mode : "none")
  );
  const [bgTopCutValue, setBgTopCutValue] = useState<string>(
    () => (typeof initialUi.top_cut_value === "number" ? String(initialUi.top_cut_value) : "")
  );
  const [bgTopCutPercentile, setBgTopCutPercentile] = useState<string>(
    () =>
      typeof initialUi.top_cut_percentile === "number"
        ? String(initialUi.top_cut_percentile)
        : "99.5"
  );
  const [bgSensitivityMin, setBgSensitivityMin] = useState<string>(
    () =>
      typeof initialUi.sensitivity_min_cutoff === "number"
        ? String(initialUi.sensitivity_min_cutoff)
        : ""
  );
  const [bgSensitivityMax, setBgSensitivityMax] = useState<string>(
    () =>
      typeof initialUi.sensitivity_max_cutoff === "number"
        ? String(initialUi.sensitivity_max_cutoff)
        : ""
  );
  const [bgSensitivitySteps, setBgSensitivitySteps] = useState<string>(
    () =>
      typeof initialUi.sensitivity_steps === "number"
        ? String(initialUi.sensitivity_steps)
        : "8"
  );
  const [bgVariogramLags, setBgVariogramLags] = useState<string>(
    () =>
      typeof initialUi.variogram_lags === "number"
        ? String(initialUi.variogram_lags)
        : "12"
  );
  const [bgVariogramMaxPairs, setBgVariogramMaxPairs] = useState<string>(
    () =>
      typeof initialUi.variogram_max_pairs === "number"
        ? String(initialUi.variogram_max_pairs)
        : "300000"
  );
  const [bgVariogramRange, setBgVariogramRange] = useState<string>(
    () =>
      typeof initialUi.variogram_max_range_m === "number"
        ? String(initialUi.variogram_max_range_m)
        : "0"
  );
  const [mmGridMethod, setMmGridMethod] = useState<string>(
    () => (typeof initialUi.grid_method === "string" ? initialUi.grid_method : "idw")
  );
  const [mmGridResolutionM, setMmGridResolutionM] = useState<string>(
    () =>
      typeof initialUi.grid_resolution_m === "number"
        ? String(initialUi.grid_resolution_m)
        : "25"
  );
  const [mmIdwPower, setMmIdwPower] = useState<string>(
    () => (typeof initialUi.idw_power === "number" ? String(initialUi.idw_power) : "2")
  );
  const [mmSearchRadiusM, setMmSearchRadiusM] = useState<string>(
    () =>
      typeof initialUi.search_radius_m === "number"
        ? String(initialUi.search_radius_m)
        : "0"
  );
  const [mmMaxPoints, setMmMaxPoints] = useState<string>(
    () => (typeof initialUi.max_points === "number" ? String(initialUi.max_points) : "32")
  );
  const [mmMaxGridCells, setMmMaxGridCells] = useState<string>(
    () => (typeof initialUi.max_grid_cells === "number" ? String(initialUi.max_grid_cells) : "250000")
  );
  const [mmDespikeSigma, setMmDespikeSigma] = useState<string>(
    () =>
      typeof initialUi.despike_sigma === "number"
        ? String(initialUi.despike_sigma)
        : "6"
  );
  const [mmSmoothWindowM, setMmSmoothWindowM] = useState<string>(
    () =>
      typeof initialUi.smooth_window_m === "number"
        ? String(initialUi.smooth_window_m)
        : "0"
  );
  const [mmResampleSpacingM, setMmResampleSpacingM] = useState<string>(
    () =>
      typeof initialUi.resample_spacing_m === "number"
        ? String(initialUi.resample_spacing_m)
        : "0"
  );
  const [mmDecimatePct, setMmDecimatePct] = useState<string>(
    () =>
      typeof initialUi.decimate_pct === "number"
        ? String(initialUi.decimate_pct)
        : "100"
  );
  const [mmLlmEnabled, setMmLlmEnabled] = useState<boolean>(
    () => (typeof initialUi.llm_enabled === "boolean" ? initialUi.llm_enabled : false)
  );
  const [ipCorridorHalfWidthM, setIpCorridorHalfWidthM] = useState<string>(
    () => (typeof initialUi.corridor_half_width_m === "number" ? String(initialUi.corridor_half_width_m) : "12.5")
  );
  const [ipCorridorDepthCellScale, setIpCorridorDepthCellScale] = useState<string>(
    () => (typeof initialUi.depth_cell_scale === "number" ? String(initialUi.depth_cell_scale) : "0.9")
  );
  const [ipCorridorMinCellThicknessM, setIpCorridorMinCellThicknessM] = useState<string>(
    () => (typeof initialUi.min_cell_thickness_m === "number" ? String(initialUi.min_cell_thickness_m) : "10")
  );
  const [ipMeshCellXM, setIpMeshCellXM] = useState<string>(
    () => (typeof initialUi.cell_x_m === "number" ? String(initialUi.cell_x_m) : "25")
  );
  const [ipMeshCellYM, setIpMeshCellYM] = useState<string>(
    () => (typeof initialUi.cell_y_m === "number" ? String(initialUi.cell_y_m) : "20")
  );
  const [ipMeshCellZM, setIpMeshCellZM] = useState<string>(
    () => (typeof initialUi.cell_z_m === "number" ? String(initialUi.cell_z_m) : "15")
  );
  const [ipMeshLateralPaddingM, setIpMeshLateralPaddingM] = useState<string>(
    () => (typeof initialUi.lateral_padding_m === "number" ? String(initialUi.lateral_padding_m) : "40")
  );
  const [ipMeshDepthPaddingM, setIpMeshDepthPaddingM] = useState<string>(
    () => (typeof initialUi.depth_padding_m === "number" ? String(initialUi.depth_padding_m) : "80")
  );
  const [ipMeshMaxCells, setIpMeshMaxCells] = useState<string>(
    () => (typeof initialUi.max_cells === "number" ? String(initialUi.max_cells) : "18000")
  );
  const [ipPreviewInfluenceRadiusM, setIpPreviewInfluenceRadiusM] = useState<string>(
    () => (typeof initialUi.influence_radius_m === "number" ? String(initialUi.influence_radius_m) : "90")
  );
  const [ipPreviewIdwPower, setIpPreviewIdwPower] = useState<string>(
    () => (typeof initialUi.idw_power === "number" ? String(initialUi.idw_power) : "2")
  );
  const [ipPreviewMinSupport, setIpPreviewMinSupport] = useState<string>(
    () => (typeof initialUi.min_support === "number" ? String(initialUi.min_support) : "2")
  );
  const [ipPreviewConductivityBias, setIpPreviewConductivityBias] = useState<string>(
    () => (typeof initialUi.conductivity_bias === "number" ? String(initialUi.conductivity_bias) : "0.35")
  );
  const [rtcMeasure, setRtcMeasure] = useState<string>(
    () => (typeof initialUi.measure === "string" ? initialUi.measure : "")
  );
  const [rtcMeasureOptions, setRtcMeasureOptions] = useState<string[]>([]);
  const [rtcMethod, setRtcMethod] = useState<string>(
    () => (typeof initialUi.method === "string" ? initialUi.method : "idw")
  );
  const [rtcPalette, setRtcPalette] = useState<string>(
    () => (typeof initialUi.palette === "string" ? initialUi.palette : "terrain")
  );
  const [rtcOpacity, setRtcOpacity] = useState<string>(
    () => (typeof initialUi.opacity === "number" ? String(initialUi.opacity) : "0.72")
  );
  const [rtcGridNx, setRtcGridNx] = useState<string>(
    () => (typeof initialUi.grid_nx === "number" ? String(initialUi.grid_nx) : "384")
  );
  const [rtcGridNy, setRtcGridNy] = useState<string>(
    () => (typeof initialUi.grid_ny === "number" ? String(initialUi.grid_ny) : "384")
  );
  const [rtcClampLowPct, setRtcClampLowPct] = useState<string>(
    () => (typeof initialUi.clamp_low_pct === "number" ? String(initialUi.clamp_low_pct) : "2")
  );
  const [rtcClampHighPct, setRtcClampHighPct] = useState<string>(
    () => (typeof initialUi.clamp_high_pct === "number" ? String(initialUi.clamp_high_pct) : "98")
  );
  const [rtcIdwPower, setRtcIdwPower] = useState<string>(
    () => (typeof initialUi.idw_power === "number" ? String(initialUi.idw_power) : "2")
  );
  const [rtcMaxPoints, setRtcMaxPoints] = useState<string>(
    () => (typeof initialUi.max_points === "number" ? String(initialUi.max_points) : "32")
  );
  const [rtcTileSize, setRtcTileSize] = useState<string>(
    () => (typeof initialUi.tile_size === "number" ? String(initialUi.tile_size) : "256")
  );
  const [rtcMinZoom, setRtcMinZoom] = useState<string>(
    () => (typeof initialUi.min_zoom === "number" ? String(initialUi.min_zoom) : "0")
  );
  const [rtcMaxZoom, setRtcMaxZoom] = useState<string>(
    () => (typeof initialUi.max_zoom === "number" ? String(initialUi.max_zoom) : "4")
  );
  const [mdTitle, setMdTitle] = useState<string>(
    () => (typeof initialUi.title === "string" ? initialUi.title : "Semantic JSON Report")
  );
  const [mdLlmEnabled, setMdLlmEnabled] = useState<boolean>(
    () => (typeof initialUi.llm_enabled === "boolean" ? initialUi.llm_enabled : true)
  );
  const [chartTemplates, setChartTemplates] = useState<ChartTemplate[]>([]);
  const [chartTemplateKey, setChartTemplateKey] = useState<string>(
    () => (typeof initialUi.template_key === "string" ? initialUi.template_key : "variogram")
  );
  const [chartTemplateId, setChartTemplateId] = useState<string>(
    () => (typeof initialUi.template_id === "string" ? initialUi.template_id : "")
  );
  const [chartDataPointer, setChartDataPointer] = useState<string>(
    () => (typeof initialUi.data_json_pointer === "string" ? initialUi.data_json_pointer : "")
  );
  const [chartDataFragment, setChartDataFragment] = useState<string>(
    () => (typeof initialUi.data_fragment === "string" ? initialUi.data_fragment : "auto")
  );
  const [chartTitle, setChartTitle] = useState<string>(
    () => (typeof initialUi.title === "string" ? initialUi.title : "")
  );
  const [chartLlmEnabled, setChartLlmEnabled] = useState<boolean>(
    () => (typeof initialUi.llm_enabled === "boolean" ? initialUi.llm_enabled : false)
  );
  const [chartObjective, setChartObjective] = useState<string>(
    () => (typeof initialUi.user_objective === "string" ? initialUi.user_objective : "")
  );
  const [chartMaxContextRows, setChartMaxContextRows] = useState<string>(
    () => (typeof initialUi.max_context_rows === "number" ? String(initialUi.max_context_rows) : "8")
  );
  const [chartMaxRenderRows, setChartMaxRenderRows] = useState<string>(
    () => (typeof initialUi.max_render_rows === "number" ? String(initialUi.max_render_rows) : "3000")
  );
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
  const [jobRuntimeText, setJobRuntimeText] = useState<string | null>(null);

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
    setBgElementField(typeof u.element_field === "string" ? u.element_field : "");
    setBgBlockSizeX(typeof u.block_size_x === "number" ? String(u.block_size_x) : "20");
    setBgBlockSizeY(typeof u.block_size_y === "number" ? String(u.block_size_y) : "20");
    setBgBlockSizeZ(typeof u.block_size_z === "number" ? String(u.block_size_z) : "10");
    setBgCutoffGrade(typeof u.cutoff_grade === "number" ? String(u.cutoff_grade) : "0");
    setBgSgMode(typeof u.sg_mode === "string" ? u.sg_mode : "constant");
    setBgSgField(typeof u.sg_field === "string" ? u.sg_field : "");
    setBgSgConstant(typeof u.sg_constant === "number" ? String(u.sg_constant) : "2.5");
    setBgGradeUnit(typeof u.grade_unit === "string" ? u.grade_unit : "ppm");
    setBgEstimationMethod(typeof u.estimation_method === "string" ? u.estimation_method : "idw");
    setBgIdwPower(typeof u.idw_power === "number" ? String(u.idw_power) : "2");
    setBgSearchRadiusM(typeof u.search_radius_m === "number" ? String(u.search_radius_m) : "0");
    setBgSearchAzimuthDeg(
      typeof u.search_azimuth_deg === "number" ? String(u.search_azimuth_deg) : "0"
    );
    setBgAnisotropyX(typeof u.anisotropy_x === "number" ? String(u.anisotropy_x) : "1");
    setBgAnisotropyY(typeof u.anisotropy_y === "number" ? String(u.anisotropy_y) : "1");
    setBgAnisotropyZ(typeof u.anisotropy_z === "number" ? String(u.anisotropy_z) : "1");
    setBgMinSamples(typeof u.min_samples === "number" ? String(u.min_samples) : "3");
    setBgMaxSamples(typeof u.max_samples === "number" ? String(u.max_samples) : "24");
    setBgGradeMin(typeof u.grade_min === "number" ? String(u.grade_min) : "");
    setBgGradeMax(typeof u.grade_max === "number" ? String(u.grade_max) : "");
    setBgClipMode(typeof u.clip_mode === "string" ? u.clip_mode : "topography");
    setBgBelowCutoffOpacity(
      typeof u.below_cutoff_opacity === "number" ? String(u.below_cutoff_opacity) : "0.08"
    );
    setBgPalette(typeof u.palette === "string" ? u.palette : "viridis");
    setBgMaxBlocks(typeof u.max_blocks === "number" ? String(u.max_blocks) : "45000");
    setBgDomainMode(typeof u.domain_mode === "string" ? u.domain_mode : "full_extent");
    setBgDomainConstraintMode(
      typeof u.domain_constraint_mode === "string" ? u.domain_constraint_mode : "none"
    );
    setBgHullBufferM(typeof u.hull_buffer_m === "number" ? String(u.hull_buffer_m) : "0");
    setBgExtrapolationBufferM(
      typeof u.extrapolation_buffer_m === "number" ? String(u.extrapolation_buffer_m) : "20"
    );
    setBgCompositeLengthM(
      typeof u.composite_length_m === "number" ? String(u.composite_length_m) : "0"
    );
    setBgTopCutMode(typeof u.top_cut_mode === "string" ? u.top_cut_mode : "none");
    setBgTopCutValue(typeof u.top_cut_value === "number" ? String(u.top_cut_value) : "");
    setBgTopCutPercentile(
      typeof u.top_cut_percentile === "number" ? String(u.top_cut_percentile) : "99.5"
    );
    setBgSensitivityMin(
      typeof u.sensitivity_min_cutoff === "number" ? String(u.sensitivity_min_cutoff) : ""
    );
    setBgSensitivityMax(
      typeof u.sensitivity_max_cutoff === "number" ? String(u.sensitivity_max_cutoff) : ""
    );
    setBgSensitivitySteps(typeof u.sensitivity_steps === "number" ? String(u.sensitivity_steps) : "8");
    setBgVariogramLags(typeof u.variogram_lags === "number" ? String(u.variogram_lags) : "12");
    setBgVariogramMaxPairs(
      typeof u.variogram_max_pairs === "number" ? String(u.variogram_max_pairs) : "300000"
    );
    setBgVariogramRange(
      typeof u.variogram_max_range_m === "number" ? String(u.variogram_max_range_m) : "0"
    );
    setMmGridMethod(typeof u.grid_method === "string" ? u.grid_method : "idw");
    setMmGridResolutionM(
      typeof u.grid_resolution_m === "number" ? String(u.grid_resolution_m) : "25"
    );
    setMmIdwPower(typeof u.idw_power === "number" ? String(u.idw_power) : "2");
    setMmSearchRadiusM(
      typeof u.search_radius_m === "number" ? String(u.search_radius_m) : "0"
    );
    setMmMaxPoints(typeof u.max_points === "number" ? String(u.max_points) : "32");
    setMmDespikeSigma(
      typeof u.despike_sigma === "number" ? String(u.despike_sigma) : "6"
    );
    setMmSmoothWindowM(
      typeof u.smooth_window_m === "number" ? String(u.smooth_window_m) : "0"
    );
    setMmResampleSpacingM(
      typeof u.resample_spacing_m === "number" ? String(u.resample_spacing_m) : "0"
    );
    setMmDecimatePct(typeof u.decimate_pct === "number" ? String(u.decimate_pct) : "100");
    setMmLlmEnabled(typeof u.llm_enabled === "boolean" ? u.llm_enabled : false);
    setRtcMeasure(typeof u.measure === "string" ? u.measure : "");
    setRtcMethod(typeof u.method === "string" ? u.method : "idw");
    setRtcPalette(typeof u.palette === "string" ? u.palette : "terrain");
    setRtcOpacity(typeof u.opacity === "number" ? String(u.opacity) : "0.72");
    setRtcGridNx(typeof u.grid_nx === "number" ? String(u.grid_nx) : "384");
    setRtcGridNy(typeof u.grid_ny === "number" ? String(u.grid_ny) : "384");
    setRtcClampLowPct(typeof u.clamp_low_pct === "number" ? String(u.clamp_low_pct) : "2");
    setRtcClampHighPct(typeof u.clamp_high_pct === "number" ? String(u.clamp_high_pct) : "98");
    setRtcIdwPower(typeof u.idw_power === "number" ? String(u.idw_power) : "2");
    setRtcMaxPoints(typeof u.max_points === "number" ? String(u.max_points) : "32");
    setRtcTileSize(typeof u.tile_size === "number" ? String(u.tile_size) : "256");
    setRtcMinZoom(typeof u.min_zoom === "number" ? String(u.min_zoom) : "0");
    setRtcMaxZoom(typeof u.max_zoom === "number" ? String(u.max_zoom) : "4");
    setMdTitle(typeof u.title === "string" ? u.title : "Semantic JSON Report");
    setMdLlmEnabled(typeof u.llm_enabled === "boolean" ? u.llm_enabled : true);
    setChartTemplateKey(typeof u.template_key === "string" ? u.template_key : "variogram");
    setChartTemplateId(typeof u.template_id === "string" ? u.template_id : "");
    setChartDataPointer(typeof u.data_json_pointer === "string" ? u.data_json_pointer : "");
    setChartDataFragment(typeof u.data_fragment === "string" ? u.data_fragment : "auto");
    setChartTitle(typeof u.title === "string" ? u.title : "");
    setChartLlmEnabled(typeof u.llm_enabled === "boolean" ? u.llm_enabled : false);
    setChartObjective(typeof u.user_objective === "string" ? u.user_objective : "");
    setChartMaxContextRows(typeof u.max_context_rows === "number" ? String(u.max_context_rows) : "8");
    setChartMaxRenderRows(typeof u.max_render_rows === "number" ? String(u.max_render_rows) : "3000");
    setCsvArtifactKey(typeof u.csv_artifact_key === "string" ? u.csv_artifact_key : "");
    setCsvArtifactHash(typeof u.csv_artifact_hash === "string" ? u.csv_artifact_hash : "");
    setCsvDelimiter(typeof u.csv_delimiter === "string" ? u.csv_delimiter : ",");
    setCsvFormat(typeof u.csv_format === "string" ? u.csv_format : "");
    setCsvMediaType(typeof u.csv_media_type === "string" ? u.csv_media_type : "");
    setCsvPreviewText(typeof u.csv_preview_text === "string" ? u.csv_preview_text : "");
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
    setJobRuntimeText(null);
  }, [node.id]);

  useEffect(() => {
    let stop = false;
    let timer: number | null = null;
    const poll = async () => {
      if (stop) return;
      try {
        const rt = await getNodeJobRuntime(graphId, node.id);
        if (stop) return;
        if (!rt) {
          setJobRuntimeText(null);
        } else {
          const p = rt.progress;
          const pct =
            typeof p?.percent === "number" && Number.isFinite(p.percent)
              ? `${Math.round(p.percent * 100)}%`
              : "";
          const stage = p?.stage || rt.status;
          const msg = p?.message || "";
          const startedMs = rt.started_at ? new Date(rt.started_at).getTime() : null;
          const progressMs =
            typeof p?.updated_at === "string" ? new Date(p.updated_at).getTime() : null;
          const heartbeatMs =
            typeof p?.heartbeat_at === "string" ? new Date(p.heartbeat_at).getTime() : progressMs;
          const hbAge =
            heartbeatMs && Number.isFinite(heartbeatMs)
              ? Math.max(0, Math.round((Date.now() - heartbeatMs) / 1000))
              : null;
          const stale = rt.status === "running" && (hbAge === null || hbAge > 20);
          const elapsed =
            startedMs && Number.isFinite(startedMs)
              ? stale
                ? progressMs && Number.isFinite(progressMs)
                  ? Math.max(0, Math.round((progressMs - startedMs) / 1000))
                  : 0
                : Math.max(0, Math.round((Date.now() - startedMs) / 1000))
              : 0;
          setJobRuntimeText(
            `${stage}${pct ? ` · ${pct}` : ""}${msg ? ` · ${msg}` : ""}${
              elapsed > 0 ? ` · ${elapsed}s` : ""
            }${
              hbAge !== null ? stale ? ` · stale heartbeat (${hbAge}s)` : ` · heartbeat ${hbAge}s ago` : ""
            }`
          );
        }
      } catch {
        // ignore polling failures
      } finally {
        if (!stop) {
          timer = window.setTimeout(poll, 2000);
        }
      }
    };
    void poll();
    return () => {
      stop = true;
      if (timer !== null) window.clearTimeout(timer);
    };
  }, [graphId, node.id, node.execution]);

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

  // Populate measure dropdown for heatmap_raster_tile_cache from its manifest.
  useEffect(() => {
    if (!isHeatmapRasterTileCacheNode) {
      setRtcMeasureOptions([]);
      return;
    }
    let cancelled = false;
    void (async () => {
      try {
        const hit =
          nodeArtifacts.find((a) => a.key.endsWith("/raster_tile_manifest.json")) ??
          nodeArtifacts.find((a) => a.key.endsWith("raster_tile_manifest.json")) ??
          null;
        if (!hit) {
          if (!cancelled) setRtcMeasureOptions([]);
          return;
        }
        const r = await fetch(api(hit.url));
        if (!r.ok) return;
        const txt = await r.text();
        const opts = extractHeatmapMeasureCandidatesFromJson(txt);
        if (!cancelled) setRtcMeasureOptions(opts);
      } catch {
        if (!cancelled) setRtcMeasureOptions([]);
      }
    })();
    return () => { cancelled = true; };
  }, [isHeatmapRasterTileCacheNode, nodeArtifacts]);

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

  useEffect(() => {
    if (!isBlockGradeModelNode) {
      setBgElementOptions([]);
      return;
    }
    let cancelled = false;
    void (async () => {
      try {
        const [graphResp, artifactsResp] = await Promise.all([
          fetch(api(`/graphs/${graphId}`), { cache: "no-store" }),
          fetch(api(`/graphs/${graphId}/artifacts`), { cache: "no-store" }),
        ]);
        if (!graphResp.ok || !artifactsResp.ok) {
          if (!cancelled) setBgElementOptions([]);
          return;
        }
        const g = (await graphResp.json()) as {
          edges?: Array<{ from_node: string; to_node: string }>;
        };
        const arts = (await artifactsResp.json()) as Array<{
          node_id: string;
          key: string;
          url: string;
        }>;
        const incomingNodeIds = new Set(
          (g.edges ?? []).filter((e) => e.to_node === node.id).map((e) => e.from_node)
        );
        const candidates = arts
          .filter((a) => incomingNodeIds.has(a.node_id) && a.key.endsWith(".json"))
          .slice(0, 20);
        const fields = new Set<string>();
        const addFromRow = (row: unknown) => {
          if (!row || typeof row !== "object" || Array.isArray(row)) return;
          const obj = row as Record<string, unknown>;
          const attrs =
            obj.attributes && typeof obj.attributes === "object" && !Array.isArray(obj.attributes)
              ? (obj.attributes as Record<string, unknown>)
              : null;
          if (!attrs) return;
          for (const [k, v] of Object.entries(attrs)) {
            if (typeof v === "number" && Number.isFinite(v)) fields.add(k);
            if (typeof v === "string") {
              const n = Number(v);
              if (Number.isFinite(n)) fields.add(k);
            }
          }
        };
        for (const art of candidates) {
          const r = await fetch(api(art.url), { cache: "no-store" });
          if (!r.ok) continue;
          const text = await r.text();
          let root: unknown;
          try {
            root = JSON.parse(text) as unknown;
          } catch {
            continue;
          }
          if (root && typeof root === "object" && !Array.isArray(root)) {
            const obj = root as Record<string, unknown>;
            if (Array.isArray(obj.assay_points)) obj.assay_points.slice(0, 250).forEach(addFromRow);
            if (Array.isArray(obj.points)) obj.points.slice(0, 250).forEach(addFromRow);
          }
          if (Array.isArray(root)) {
            root.slice(0, 250).forEach(addFromRow);
          }
        }
        const next = [...fields].sort((a, b) => a.localeCompare(b));
        if (!cancelled) setBgElementOptions(next);
      } catch {
        if (!cancelled) setBgElementOptions([]);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [graphId, isBlockGradeModelNode, node.id]);

  useEffect(() => {
    if (!isPlotChartNode) {
      setChartTemplates([]);
      return;
    }
    let cancelled = false;
    void (async () => {
      try {
        const items = await listChartTemplates();
        if (!cancelled) setChartTemplates(items);
      } catch {
        if (!cancelled) setChartTemplates([]);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [isPlotChartNode]);

  useEffect(() => {
    if (!isPlotChartNode || chartTemplates.length === 0) return;
    const byId = chartTemplateId
      ? chartTemplates.find((t) => t.id === chartTemplateId)
      : null;
    const byKey = chartTemplates.find((t) => t.key === chartTemplateKey);
    const picked = byId ?? byKey ?? chartTemplates[0];
    if (!picked) return;
    if (picked.id !== chartTemplateId) setChartTemplateId(picked.id);
    if (picked.key !== chartTemplateKey) setChartTemplateKey(picked.key);
    if (chartDataPointer.trim().length === 0) {
      const ptr = (picked.template_schema?.default_data_pointer_candidates as unknown[] | undefined)
        ?.find((x): x is string => typeof x === "string");
      if (ptr) setChartDataPointer(ptr);
    }
  }, [isPlotChartNode, chartTemplates, chartTemplateId, chartTemplateKey, chartDataPointer]);

  const chartFragmentOptions = useMemo(() => {
    const picked =
      chartTemplates.find((t) => t.id === chartTemplateId) ??
      chartTemplates.find((t) => t.key === chartTemplateKey) ??
      null;
    const ptrs = new Set<string>([
      "/variogram/bins",
      "/cutoff_sensitivity",
      "/grade_histogram",
      "/summary",
      "/semantic_summary",
      "/points",
      "/rows",
      "/bins",
    ]);
    const fromTemplate = (picked?.template_schema?.default_data_pointer_candidates as unknown[] | undefined)
      ?.filter((x): x is string => typeof x === "string");
    for (const p of fromTemplate ?? []) ptrs.add(p);
    return ["auto", "custom", ...Array.from(ptrs)];
  }, [chartTemplates, chartTemplateId, chartTemplateKey]);

  const onPickFile = useCallback(
    async (file: File | null) => {
      if (!file) return;
      setErr(null);
      setCsvName(file.name);
      const shouldUpload =
        isMagneticMapperNode ||
        isArtifactIngestNode ||
        isIpSurveyIngestNode ||
        file.size > 8 * 1024 * 1024;
      if (shouldUpload) {
        try {
          const up = await uploadTabularArtifact(graphId, file);
          setCsvArtifactKey(up.artifact_key);
          setCsvArtifactHash(up.content_hash);
          setCsvDelimiter(up.delimiter || ",");
          setCsvFormat(up.format || "");
          setCsvMediaType(up.media_type || "");
          setCsvPreviewText(up.preview_text || "");
          setHeaders(up.headers ?? []);
          setCsvRows([]);
          setPreviewRows(up.preview_rows ?? []);
          setSaveMsg(
            `Uploaded ${up.filename} (${(up.size_bytes / (1024 * 1024)).toFixed(1)} MB) as artifact; mapping uses preview and run reads full file from artifact.`
          );
          return;
        } catch (e) {
          setErr(e instanceof Error ? e.message : String(e));
          if (file.size > 16 * 1024 * 1024) {
            return;
          }
        }
      }
      const reader = new FileReader();
      reader.onload = () => {
        const text = String(reader.result ?? "");
        const { headers: h, rows } = parseCsv(text);
        setHeaders(h);
        setCsvRows(rows);
        setPreviewRows(rows.slice(0, 8));
        setCsvArtifactKey("");
        setCsvArtifactHash("");
        setCsvFormat("");
        setCsvMediaType(file.type || "");
        setCsvPreviewText(text.slice(0, 2500));
      };
      reader.readAsText(file, "UTF-8");
    },
    [graphId, isArtifactIngestNode, isIpSurveyIngestNode, isMagneticMapperNode]
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
    const MAX_ROWS_SAVED_TO_NODE = 12000;
    const usingArtifact = csvArtifactKey.trim().length > 0;
    const rowsForNode =
      usingArtifact
        ? []
        : csvRows.length > MAX_ROWS_SAVED_TO_NODE
        ? csvRows.slice(0, MAX_ROWS_SAVED_TO_NODE)
        : csvRows;
    const ui: Record<string, unknown> = {
      mapping: { ...mapping },
      use_project_crs: useProject,
      source_crs_epsg: useProject ? undefined : epsg,
      z_is_relative: kind === "collar_ingest" ? zRelative : undefined,
      csv_filename: csvName || undefined,
      csv_artifact_key: usingArtifact ? csvArtifactKey : undefined,
      csv_artifact_hash: usingArtifact ? csvArtifactHash : undefined,
      csv_delimiter: usingArtifact ? csvDelimiter : undefined,
      csv_format: usingArtifact ? csvFormat : undefined,
      csv_media_type: usingArtifact ? csvMediaType : undefined,
      csv_preview_text: usingArtifact ? csvPreviewText : undefined,
      csv_headers: headers.length ? headers : undefined,
      csv_rows: rowsForNode,
      csv_preview_rows: usingArtifact ? previewRows.slice(0, 8) : csvRows.slice(0, 8),
    };
    const n = (v: string, fallback: number) => {
      const normalized = String(v ?? "")
        .trim()
        .replace(/\s+/g, "")
        .replace(",", ".");
      const x = Number(normalized);
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
    } else if (isBlockGradeModelNode) {
      ui.element_field = bgElementField.trim() || undefined;
      ui.block_size_x = Math.max(0.5, n(bgBlockSizeX, 20));
      ui.block_size_y = Math.max(0.5, n(bgBlockSizeY, 20));
      ui.block_size_z = Math.max(0.5, n(bgBlockSizeZ, 10));
      ui.cutoff_grade = n(bgCutoffGrade, 0);
      ui.sg_mode = bgSgMode === "field" ? "field" : "constant";
      ui.sg_field = bgSgField.trim().length > 0 ? bgSgField.trim() : undefined;
      ui.sg_constant = Math.max(0.2, n(bgSgConstant, 2.5));
      ui.grade_unit = bgGradeUnit;
      ui.estimation_method = bgEstimationMethod === "nearest" ? "nearest" : "idw";
      ui.idw_power = Math.max(1, Math.min(4, n(bgIdwPower, 2)));
      ui.search_radius_m = Math.max(0, n(bgSearchRadiusM, 0));
      ui.search_azimuth_deg = n(bgSearchAzimuthDeg, 0);
      ui.anisotropy_x = Math.max(0.05, n(bgAnisotropyX, 1));
      ui.anisotropy_y = Math.max(0.05, n(bgAnisotropyY, 1));
      ui.anisotropy_z = Math.max(0.05, n(bgAnisotropyZ, 1));
      ui.min_samples = Math.max(1, Math.trunc(n(bgMinSamples, 3)));
      ui.max_samples = Math.max(
        Math.trunc(n(bgMinSamples, 3)),
        Math.trunc(n(bgMaxSamples, 24))
      );
      ui.grade_min = bgGradeMin.trim().length > 0 ? n(bgGradeMin, 0) : undefined;
      ui.grade_max = bgGradeMax.trim().length > 0 ? n(bgGradeMax, 0) : undefined;
      ui.clip_mode = bgClipMode === "none" ? "none" : "topography";
      ui.below_cutoff_opacity = Math.max(0, Math.min(1, n(bgBelowCutoffOpacity, 0.08)));
      ui.palette = bgPalette;
      ui.max_blocks = Math.max(1000, Math.trunc(n(bgMaxBlocks, 45000)));
      ui.domain_mode = bgDomainMode;
      ui.domain_constraint_mode = bgDomainConstraintMode;
      ui.hull_buffer_m = Math.max(0, n(bgHullBufferM, 0));
      ui.extrapolation_buffer_m = Math.max(0, n(bgExtrapolationBufferM, 20));
      ui.composite_length_m = Math.max(0, n(bgCompositeLengthM, 0));
      ui.top_cut_mode = bgTopCutMode;
      ui.top_cut_value = bgTopCutValue.trim().length > 0 ? n(bgTopCutValue, 0) : undefined;
      ui.top_cut_percentile = Math.max(50, Math.min(100, n(bgTopCutPercentile, 99.5)));
      ui.sensitivity_min_cutoff =
        bgSensitivityMin.trim().length > 0 ? n(bgSensitivityMin, 0) : undefined;
      ui.sensitivity_max_cutoff =
        bgSensitivityMax.trim().length > 0 ? n(bgSensitivityMax, 0) : undefined;
      ui.sensitivity_steps = Math.max(3, Math.min(40, Math.trunc(n(bgSensitivitySteps, 8))));
      ui.variogram_lags = Math.max(6, Math.min(40, Math.trunc(n(bgVariogramLags, 12))));
      ui.variogram_max_pairs = Math.max(2000, Math.trunc(n(bgVariogramMaxPairs, 300000)));
      ui.variogram_max_range_m = Math.max(0, n(bgVariogramRange, 0));
    } else if (isPlotChartNode) {
      const picked =
        chartTemplates.find((t) => t.id === chartTemplateId) ??
        chartTemplates.find((t) => t.key === chartTemplateKey) ??
        null;
      ui.template_key = chartTemplateKey || "variogram";
      ui.template_id = picked?.id;
      ui.template_snapshot = picked?.template_schema;
      ui.data_fragment = chartDataFragment;
      ui.data_json_pointer = chartDataPointer.trim() || undefined;
      ui.title = chartTitle.trim() || undefined;
      ui.llm_enabled = chartLlmEnabled;
      ui.user_objective = chartObjective.trim() || undefined;
      ui.max_context_rows = Math.max(3, Math.min(40, Math.trunc(n(chartMaxContextRows, 8))));
      ui.max_render_rows = Math.max(100, Math.min(50000, Math.trunc(n(chartMaxRenderRows, 3000))));
    } else if (isMagneticMapperNode) {
      ui.grid_method = mmGridMethod === "minimum_curvature" ? "minimum_curvature" : "idw";
      ui.grid_resolution_m = Math.max(1, n(mmGridResolutionM, 25));
      ui.idw_power = Math.max(1, Math.min(6, n(mmIdwPower, 2)));
      ui.search_radius_m = Math.max(0, n(mmSearchRadiusM, 0));
      ui.max_points = Math.max(4, Math.min(256, Math.trunc(n(mmMaxPoints, 32))));
      ui.max_grid_cells = Math.max(10000, Math.min(2000000, Math.trunc(n(mmMaxGridCells, 250000))));
      ui.despike_sigma = Math.max(2, Math.min(20, n(mmDespikeSigma, 6)));
      ui.smooth_window_m = Math.max(0, n(mmSmoothWindowM, 0));
      ui.resample_spacing_m = Math.max(0, n(mmResampleSpacingM, 0));
      ui.decimate_pct = Math.max(1, Math.min(100, n(mmDecimatePct, 100)));
      ui.llm_enabled = mmLlmEnabled;
    } else if (isIpCorridorModelNode) {
      ui.corridor_half_width_m = Math.max(2, n(ipCorridorHalfWidthM, 12.5));
      ui.depth_cell_scale = Math.max(0.2, Math.min(3, n(ipCorridorDepthCellScale, 0.9)));
      ui.min_cell_thickness_m = Math.max(1, n(ipCorridorMinCellThicknessM, 10));
    } else if (isIpInversionMeshNode) {
      ui.cell_x_m = Math.max(5, n(ipMeshCellXM, 25));
      ui.cell_y_m = Math.max(5, n(ipMeshCellYM, 20));
      ui.cell_z_m = Math.max(5, n(ipMeshCellZM, 15));
      ui.lateral_padding_m = Math.max(0, n(ipMeshLateralPaddingM, 40));
      ui.depth_padding_m = Math.max(5, n(ipMeshDepthPaddingM, 80));
      ui.max_cells = Math.max(1000, Math.trunc(n(ipMeshMaxCells, 18000)));
    } else if (isIpInversionPreviewNode) {
      ui.influence_radius_m = Math.max(10, n(ipPreviewInfluenceRadiusM, 90));
      ui.idw_power = Math.max(0.5, Math.min(8, n(ipPreviewIdwPower, 2)));
      ui.min_support = Math.max(1, Math.trunc(n(ipPreviewMinSupport, 2)));
      ui.conductivity_bias = Math.max(0, Math.min(1, n(ipPreviewConductivityBias, 0.35)));
    } else if (isHeatmapRasterTileCacheNode) {
      ui.measure = rtcMeasure.trim() || undefined;
      ui.method = rtcMethod === "nearest" ? "nearest" : "idw";
      ui.palette = rtcPalette.trim() || "terrain";
      ui.opacity = Math.max(0.05, Math.min(1, n(rtcOpacity, 0.72)));
      ui.grid_nx = Math.max(64, Math.min(2048, Math.trunc(n(rtcGridNx, 384))));
      ui.grid_ny = Math.max(64, Math.min(2048, Math.trunc(n(rtcGridNy, 384))));
      ui.clamp_low_pct = Math.max(0, Math.min(100, n(rtcClampLowPct, 2)));
      ui.clamp_high_pct = Math.max(0, Math.min(100, n(rtcClampHighPct, 98)));
      ui.idw_power = Math.max(1, Math.min(6, n(rtcIdwPower, 2)));
      ui.max_points = Math.max(4, Math.min(256, Math.trunc(n(rtcMaxPoints, 32))));
      ui.tile_size = Math.max(128, Math.min(512, Math.trunc(n(rtcTileSize, 256))));
      ui.min_zoom = Math.max(0, Math.min(10, Math.trunc(n(rtcMinZoom, 0))));
      ui.max_zoom = Math.max(
        Math.max(0, Math.min(10, Math.trunc(n(rtcMinZoom, 0)))),
        Math.min(12, Math.trunc(n(rtcMaxZoom, 4)))
      );
    } else if (isMdViewerNode) {
      ui.title = mdTitle.trim() || "Semantic JSON Report";
      ui.llm_enabled = mdLlmEnabled;
    } else if (isAoiNode) {
      ui.mode = aoiMode;
      ui.margin_pct = Math.max(0, n(aoiMarginPct, 25));
      const vals = aoiBbox
        .split(",")
        .map((x) => Number(x.trim()))
        .filter((x) => Number.isFinite(x));
      ui.bbox = vals.length >= 4 ? vals.slice(0, 4) : undefined;
      ui.bbox_epsg = aoiBboxEpsg !== 4326 ? aoiBboxEpsg : undefined;
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
      if (usingArtifact) {
        setSaveMsg("Saved node mapping/config with artifact pointer + hash.");
      } else if (rowsForNode.length < csvRows.length) {
        setSaveMsg(
          `Saved to node config with first ${rowsForNode.length.toLocaleString()} rows (file is larger). Use Run-this-node to process full in-memory CSV now.`
        );
      } else {
        setSaveMsg("Saved to node config (re-run pipeline to rebuild artifacts).");
      }
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    }
  }, [
    crsMode,
    sourceCustomEpsg,
    projectEpsg,
    mapping,
    csvName,
    csvArtifactKey,
    csvArtifactHash,
    csvDelimiter,
    csvFormat,
    csvMediaType,
    csvPreviewText,
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
    aoiBboxEpsg,
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
    isBlockGradeModelNode,
    bgElementField,
    bgBlockSizeX,
    bgBlockSizeY,
    bgBlockSizeZ,
    bgCutoffGrade,
    bgSgMode,
    bgSgField,
    bgSgConstant,
    bgGradeUnit,
    bgEstimationMethod,
    bgIdwPower,
    bgSearchRadiusM,
    bgSearchAzimuthDeg,
    bgAnisotropyX,
    bgAnisotropyY,
    bgAnisotropyZ,
    bgMinSamples,
    bgMaxSamples,
    bgGradeMin,
    bgGradeMax,
    bgClipMode,
    bgBelowCutoffOpacity,
    bgPalette,
    bgMaxBlocks,
    bgDomainMode,
    bgDomainConstraintMode,
    bgHullBufferM,
    bgExtrapolationBufferM,
    bgCompositeLengthM,
    bgTopCutMode,
    bgTopCutValue,
    bgTopCutPercentile,
    bgSensitivityMin,
    bgSensitivityMax,
    bgSensitivitySteps,
    bgVariogramLags,
    bgVariogramMaxPairs,
    bgVariogramRange,
    isMagneticMapperNode,
    mmGridMethod,
    mmGridResolutionM,
    mmIdwPower,
    mmSearchRadiusM,
    mmMaxPoints,
    mmMaxGridCells,
    mmDespikeSigma,
    mmSmoothWindowM,
    mmResampleSpacingM,
    mmDecimatePct,
    mmLlmEnabled,
    isIpCorridorModelNode,
    ipCorridorHalfWidthM,
    ipCorridorDepthCellScale,
    ipCorridorMinCellThicknessM,
    isIpInversionMeshNode,
    ipMeshCellXM,
    ipMeshCellYM,
    ipMeshCellZM,
    ipMeshLateralPaddingM,
    ipMeshDepthPaddingM,
    ipMeshMaxCells,
    isIpInversionPreviewNode,
    ipPreviewInfluenceRadiusM,
    ipPreviewIdwPower,
    ipPreviewMinSupport,
    ipPreviewConductivityBias,
    isHeatmapRasterTileCacheNode,
    rtcMeasure,
    rtcMethod,
    rtcPalette,
    rtcOpacity,
    rtcGridNx,
    rtcGridNy,
    rtcClampLowPct,
    rtcClampHighPct,
    rtcIdwPower,
    rtcMaxPoints,
    rtcTileSize,
    rtcMinZoom,
    rtcMaxZoom,
    isPlotChartNode,
    chartTemplateKey,
    chartTemplateId,
    chartDataFragment,
    chartDataPointer,
    chartTitle,
    chartLlmEnabled,
    chartObjective,
    chartMaxContextRows,
    chartMaxRenderRows,
    chartTemplates,
    isMdViewerNode,
    mdTitle,
    mdLlmEnabled,
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
      const inputPayloads: Record<string, unknown> = {};
      if (csvCapable && headers.length > 0 && csvRows.length > 0) {
        const parseCell = (s: string): unknown => {
          const t = String(s ?? "").trim();
          if (!t.length) return "";
          const n = Number(t.replace(",", "."));
          return Number.isFinite(n) ? n : t;
        };
        if (kind === "magnetic_model") {
          const rows = csvRows.map((r) => {
            const obj: Record<string, unknown> = {};
            headers.forEach((h, i) => {
              if (i < r.length) obj[h] = parseCell(r[i]);
            });
            return obj;
          });
          const useProject = crsMode === "project";
          const epsg = useProject
            ? projectEpsg
            : crsMode === "custom"
              ? parseInt(sourceCustomEpsg, 10)
              : parseInt(crsMode, 10);
          inputPayloads[node.id] = {
            rows,
            source_crs: { epsg: Number.isFinite(epsg) && epsg > 0 ? epsg : 4326, wkt: null },
          };
        }
      }
      if (kind === "observation_ingest" && csvArtifactKey.trim().length > 0) {
        inputPayloads[node.id] = {
          csv_artifact_key: csvArtifactKey,
          csv_artifact_hash: csvArtifactHash || undefined,
          csv_filename: csvName || undefined,
          csv_media_type: csvMediaType || undefined,
          csv_format: csvFormat || undefined,
          csv_delimiter: csvDelimiter || ",",
        };
      }
      if (kind === "ip_survey_ingest" && csvArtifactKey.trim().length > 0) {
        inputPayloads[node.id] = {
          csv_artifact_key: csvArtifactKey,
          csv_filename: csvName || undefined,
          csv_delimiter: csvDelimiter || ",",
        };
      }
      const res = await runGraph(graphId, {
        dirtyRoots: [node.id],
        includeManual: true,
        inputPayloads: Object.keys(inputPayloads).length ? inputPayloads : undefined,
      });
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
  }, [csvCapable, headers, csvRows, kind, crsMode, projectEpsg, sourceCustomEpsg, graphId, node.id, onPipelineQueued, csvArtifactKey, csvArtifactHash, csvName, csvMediaType, csvFormat, csvDelimiter]);

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
                  : isBlockGradeModelNode
                    ? "Block model"
                  : isMagneticMapperNode
                    ? "Mag model"
                  : isIpCorridorModelNode
                    ? "IP corridor"
                  : isIpInversionMeshNode
                    ? "IP mesh"
                  : isIpInversionPreviewNode
                    ? "IP preview"
                  : isHeatmapRasterTileCacheNode
                    ? "Heatmap tiles"
                  : isMdViewerNode
                    ? "Report"
                    : isPlotChartNode
                      ? "Chart"
              : "Config",
    [isDataModelTransformNode, isDemFetchNode, isHeatmapNode, isIsoExtractNode, isTerrainAdjustNode, isTilebrokerNode, isAoiNode, isBlockGradeModelNode, isMagneticMapperNode, isIpCorridorModelNode, isIpInversionMeshNode, isIpInversionPreviewNode, isHeatmapRasterTileCacheNode, isMdViewerNode, isPlotChartNode]
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
            {isBlockGradeModelNode && (
              <p style={{ opacity: 0.75 }}>
                Use <strong>Block model</strong> to choose grade element, block dimensions, cut-off,
                SG, interpolation controls, and topography clipping. Outputs are voxels, block
                centers, and a resource summary JSON report.
              </p>
            )}
            <p style={{ opacity: 0.7, marginTop: 14, fontSize: 11 }}>
              After saving, use <strong>Run pipeline</strong> in the workspace tab (or{" "}
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
              {jobRuntimeText && (
                <p style={{ color: "#9fb3c8", marginTop: 8, fontSize: 11 }}>
                  Runtime: {jobRuntimeText}
                </p>
              )}
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
                  <span style={labSpan}>Load file</span>
                  <input
                    type="file"
                    accept=".csv,.tsv,.txt,.json,.geojson,text/csv,text/tab-separated-values,text/plain,application/json,application/geo+json"
                    onChange={(e) => onPickFile(e.target.files?.[0] ?? null)}
                    style={{ fontSize: 11 }}
                  />
                </label>
                {csvName && (
                  <div style={{ fontSize: 11, opacity: 0.7, marginBottom: 10 }}>
                    {csvName} · {headers.length} columns · {previewRows.length} preview rows
                  </div>
                )}
                {csvArtifactKey && (
                  <div style={{ fontSize: 11, opacity: 0.75, marginBottom: 10 }}>
                    Artifact source: <code>{csvArtifactKey}</code> · hash <code>{csvArtifactHash.slice(0, 12)}</code> · format <code>{csvFormat || "auto"}</code>
                  </div>
                )}
                {kind === "observation_ingest" && (
                  <div style={{ fontSize: 12, opacity: 0.85, marginBottom: 10 }}>
                    This node emits observation points, a table pointer artifact, and an ingest audit report for downstream modeling nodes.
                  </div>
                )}
                {kind === "observation_ingest" && (
                  <div style={mapGrid}>
                    {selectCol("x", "Projected X / Easting")}
                    {selectCol("y", "Projected Y / Northing")}
                    {selectCol("z", "Z / elevation (optional)")}
                    {selectCol("lon", "Longitude (WGS84 fallback)")}
                    {selectCol("lat", "Latitude (WGS84 fallback)")}
                    {selectCol("t", "Timestamp (optional)")}
                    {selectCol("line_id", "Line / segment id (optional)")}
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
                {kind === "magnetic_model" && (
                  <div style={mapGrid}>
                    {selectCol("line_id", "Flight line id")}
                    {selectCol("utc", "UTC / timestamp")}
                    {selectCol("fid", "FID / sequence")}
                    {selectCol("x", "Projected X / Easting")}
                    {selectCol("y", "Projected Y / Northing")}
                    {selectCol("lon", "Longitude (WGS84)")}
                    {selectCol("lat", "Latitude (WGS84)")}
                    {selectCol("tmf", "TMF / total field")}
                    {selectCol("mag_lev", "MAG_LEV fallback")}
                    {selectCol("igrf", "IGRF (optional)")}
                    {selectCol("radar", "Radar / clearance Z")}
                    {selectCol("gps_alt", "GPS altitude Z fallback")}
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
                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                  <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                  <input
                    type="text"
                    value={aoiBbox}
                    onChange={(e) => {
                      setAoiBbox(e.target.value);
                      // If user edits bbox text directly, reset EPSG to WGS84
                      setAoiBboxEpsg(4326);
                    }}
                    placeholder="5.9036, 6.0660, 5.9093, 6.0679"
                    style={{ ...sel, fontFamily: "inherit", flex: 1 }}
                  />
                  <button
                    type="button"
                    title="Edit bounding box on map"
                    onClick={() => onOpenAoiEditor?.(node.id)}
                    style={{
                      display: "inline-flex",
                      alignItems: "center",
                      gap: 5,
                      background: "rgba(247,183,49,0.12)",
                      border: "1px solid rgba(247,183,49,0.45)",
                      borderRadius: 6,
                      color: "#f7b731",
                      cursor: "pointer",
                      fontSize: 12,
                      fontWeight: 600,
                      padding: "5px 10px",
                      whiteSpace: "nowrap",
                      flexShrink: 0,
                      fontFamily: "inherit",
                    }}
                  >
                    ✏️ Edit
                  </button>
                  </div>
                  {aoiBboxEpsg !== 4326 && (
                    <span style={{ fontSize: 10, color: "#8b949e" }}>
                      Coordinates in EPSG:{aoiBboxEpsg} — edit on map to change CRS
                    </span>
                  )}
                </div>
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

        {tab === "config" && isBlockGradeModelNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Build a topography-clipped block model from 3D grade points. Outputs include voxel
              blocks, block-center points, and a resource summary report.
            </p>
            <div style={mapGrid}>
              <label style={lab}>
                <span style={labSpan}>Grade element field</span>
                {bgElementOptions.length > 0 ? (
                  <select value={bgElementField} onChange={(e) => setBgElementField(e.target.value)} style={sel}>
                    <option value="">Auto (first numeric field)</option>
                    {bgElementOptions.map((m) => (
                      <option key={m} value={m}>
                        {m}
                      </option>
                    ))}
                  </select>
                ) : (
                  <input
                    type="text"
                    value={bgElementField}
                    onChange={(e) => setBgElementField(e.target.value)}
                    placeholder="e.g. au_ppm (blank = auto)"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                )}
              </label>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Block size X</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgBlockSizeX}
                    onChange={(e) => setBgBlockSizeX(e.target.value)}
                    placeholder="20.0"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Block size Y</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgBlockSizeY}
                    onChange={(e) => setBgBlockSizeY(e.target.value)}
                    placeholder="20.0"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Block size Z</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgBlockSizeZ}
                    onChange={(e) => setBgBlockSizeZ(e.target.value)}
                    placeholder="10.0"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Cutoff grade</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgCutoffGrade}
                    onChange={(e) => setBgCutoffGrade(e.target.value)}
                    placeholder="0.5"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>SG mode</span>
                  <select value={bgSgMode} onChange={(e) => setBgSgMode(e.target.value)} style={sel}>
                    <option value="constant">Constant</option>
                    <option value="field">From field</option>
                  </select>
                </label>
                <label style={lab}>
                  <span style={labSpan}>SG constant</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgSgConstant}
                    onChange={(e) => setBgSgConstant(e.target.value)}
                    placeholder="2.5"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>SG field (if field mode)</span>
                  <input
                    type="text"
                    value={bgSgField}
                    onChange={(e) => setBgSgField(e.target.value)}
                    placeholder="e.g. sg, density_t_m3"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Grade units</span>
                  <select value={bgGradeUnit} onChange={(e) => setBgGradeUnit(e.target.value)} style={sel}>
                    <option value="ppm">ppm</option>
                    <option value="gpt">g/t</option>
                    <option value="percent">%</option>
                    <option value="fraction">fraction</option>
                  </select>
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Method</span>
                  <select
                    value={bgEstimationMethod}
                    onChange={(e) => setBgEstimationMethod(e.target.value)}
                    style={sel}
                  >
                    <option value="idw">IDW</option>
                    <option value="nearest">Nearest</option>
                  </select>
                </label>
                <label style={lab}>
                  <span style={labSpan}>IDW power</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgIdwPower}
                    onChange={(e) => setBgIdwPower(e.target.value)}
                    placeholder="2.0"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Search radius (m)</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgSearchRadiusM}
                    onChange={(e) => setBgSearchRadiusM(e.target.value)}
                    placeholder="0"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Search azimuth (deg)</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgSearchAzimuthDeg}
                    onChange={(e) => setBgSearchAzimuthDeg(e.target.value)}
                    placeholder="0"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Max blocks</span>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={bgMaxBlocks}
                    onChange={(e) => setBgMaxBlocks(e.target.value)}
                    placeholder="45000"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Anisotropy X</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgAnisotropyX}
                    onChange={(e) => setBgAnisotropyX(e.target.value)}
                    placeholder="1.0"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Anisotropy Y</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgAnisotropyY}
                    onChange={(e) => setBgAnisotropyY(e.target.value)}
                    placeholder="1.0"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Anisotropy Z</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgAnisotropyZ}
                    onChange={(e) => setBgAnisotropyZ(e.target.value)}
                    placeholder="1.0"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Min samples</span>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={bgMinSamples}
                    onChange={(e) => setBgMinSamples(e.target.value)}
                    placeholder="3"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Max samples</span>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={bgMaxSamples}
                    onChange={(e) => setBgMaxSamples(e.target.value)}
                    placeholder="24"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Grade min clamp</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgGradeMin}
                    onChange={(e) => setBgGradeMin(e.target.value)}
                    placeholder="optional"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Grade max clamp</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgGradeMax}
                    onChange={(e) => setBgGradeMax(e.target.value)}
                    placeholder="optional"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Domain mode</span>
                  <select value={bgDomainMode} onChange={(e) => setBgDomainMode(e.target.value)} style={sel}>
                    <option value="full_extent">Full extent</option>
                    <option value="convex_hull">Convex hull</option>
                    <option value="buffered_hull">Buffered hull</option>
                    <option value="input_domain_mask">Input domain mask</option>
                  </select>
                </label>
                <label style={lab}>
                  <span style={labSpan}>Hull buffer (m)</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgHullBufferM}
                    onChange={(e) => setBgHullBufferM(e.target.value)}
                    placeholder="0"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Extrapolation buffer (m)</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgExtrapolationBufferM}
                    onChange={(e) => setBgExtrapolationBufferM(e.target.value)}
                    placeholder="20"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Composite length (m)</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgCompositeLengthM}
                    onChange={(e) => setBgCompositeLengthM(e.target.value)}
                    placeholder="0 (off)"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Top-cut mode</span>
                  <select value={bgTopCutMode} onChange={(e) => setBgTopCutMode(e.target.value)} style={sel}>
                    <option value="none">None</option>
                    <option value="hard_cap">Hard cap (value)</option>
                    <option value="percentile">Percentile cap</option>
                  </select>
                </label>
                <label style={lab}>
                  <span style={labSpan}>Top-cut value</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgTopCutValue}
                    onChange={(e) => setBgTopCutValue(e.target.value)}
                    placeholder="used for hard cap"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Top-cut percentile</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgTopCutPercentile}
                    onChange={(e) => setBgTopCutPercentile(e.target.value)}
                    placeholder="99.5"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Domain constraint source</span>
                  <select
                    value={bgDomainConstraintMode}
                    onChange={(e) => setBgDomainConstraintMode(e.target.value)}
                    style={sel}
                  >
                    <option value="none">None (current)</option>
                    <option value="polygon_mask">Polygon/AOI mask</option>
                    <option value="mesh_containment">Containing mesh (future)</option>
                    <option value="mesh_clipping_planes">Clipping planes (future)</option>
                  </select>
                </label>
                <label style={lab}>
                  <span style={labSpan}>Clip mode</span>
                  <select value={bgClipMode} onChange={(e) => setBgClipMode(e.target.value)} style={sel}>
                    <option value="topography">Topography (ground)</option>
                    <option value="none">None</option>
                  </select>
                </label>
                <label style={lab}>
                  <span style={labSpan}>Below-cutoff opacity</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgBelowCutoffOpacity}
                    onChange={(e) => setBgBelowCutoffOpacity(e.target.value)}
                    placeholder="0.05"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Palette hint</span>
                  <select value={bgPalette} onChange={(e) => setBgPalette(e.target.value)} style={sel}>
                    <option value="inferno">Inferno</option>
                    <option value="viridis">Viridis</option>
                    <option value="turbo">Turbo</option>
                    <option value="red_blue">Red/Blue</option>
                  </select>
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Sensitivity cutoff min</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgSensitivityMin}
                    onChange={(e) => setBgSensitivityMin(e.target.value)}
                    placeholder="auto"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Sensitivity cutoff max</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgSensitivityMax}
                    onChange={(e) => setBgSensitivityMax(e.target.value)}
                    placeholder="auto"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Sensitivity steps</span>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={bgSensitivitySteps}
                    onChange={(e) => setBgSensitivitySteps(e.target.value)}
                    placeholder="8"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Variogram lags</span>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={bgVariogramLags}
                    onChange={(e) => setBgVariogramLags(e.target.value)}
                    placeholder="12"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Variogram max pairs</span>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={bgVariogramMaxPairs}
                    onChange={(e) => setBgVariogramMaxPairs(e.target.value)}
                    placeholder="300000"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Variogram max range (m)</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={bgVariogramRange}
                    onChange={(e) => setBgVariogramRange(e.target.value)}
                    placeholder="auto"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
            </div>
          </div>
        )}

        {tab === "config" && isIpCorridorModelNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Inflate pseudosection rows into a fast corridor pseudo-volume for immediate 3D IP
              review.
            </p>
            <div style={mapGrid}>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Corridor half-width (m)</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={ipCorridorHalfWidthM}
                    onChange={(e) => setIpCorridorHalfWidthM(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Depth cell scale</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={ipCorridorDepthCellScale}
                    onChange={(e) => setIpCorridorDepthCellScale(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Min cell thickness (m)</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={ipCorridorMinCellThicknessM}
                    onChange={(e) => setIpCorridorMinCellThicknessM(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
            </div>
          </div>
        )}

        {tab === "config" && isIpInversionMeshNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Build a regular preview mesh from pseudosection extents. This is the handoff layer
              between TDIP observations and any future inversion engine.
            </p>
            <div style={mapGrid}>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Cell X (m)</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={ipMeshCellXM}
                    onChange={(e) => setIpMeshCellXM(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Cell Y (m)</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={ipMeshCellYM}
                    onChange={(e) => setIpMeshCellYM(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Cell Z (m)</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={ipMeshCellZM}
                    onChange={(e) => setIpMeshCellZM(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Lateral padding (m)</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={ipMeshLateralPaddingM}
                    onChange={(e) => setIpMeshLateralPaddingM(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Depth padding (m)</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={ipMeshDepthPaddingM}
                    onChange={(e) => setIpMeshDepthPaddingM(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Max cells</span>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={ipMeshMaxCells}
                    onChange={(e) => setIpMeshMaxCells(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
            </div>
          </div>
        )}

        {tab === "config" && isIpInversionPreviewNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Interpolate pseudosection responses onto the preview mesh for 3D inversion-style
              testing. Confidence is shown explicitly so low-support areas stay visible but honest.
            </p>
            <div style={mapGrid}>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Influence radius (m)</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={ipPreviewInfluenceRadiusM}
                    onChange={(e) => setIpPreviewInfluenceRadiusM(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>IDW power</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={ipPreviewIdwPower}
                    onChange={(e) => setIpPreviewIdwPower(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                <label style={lab}>
                  <span style={labSpan}>Minimum support</span>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={ipPreviewMinSupport}
                    onChange={(e) => setIpPreviewMinSupport(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
                <label style={lab}>
                  <span style={labSpan}>Conductivity bias</span>
                  <input
                    type="text"
                    inputMode="decimal"
                    value={ipPreviewConductivityBias}
                    onChange={(e) => setIpPreviewConductivityBias(e.target.value)}
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                </label>
              </div>
            </div>
          </div>
        )}

        {tab === "config" && isMdViewerNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Generate a concise report from semantic JSON. Uses OpenRouter model{" "}
              <code>openai/gpt-5-mini</code> when enabled and API key is available.
            </p>
            <div style={mapGrid}>
              <label style={lab}>
                <span style={labSpan}>Report title</span>
                <input
                  type="text"
                  value={mdTitle}
                  onChange={(e) => setMdTitle(e.target.value)}
                  placeholder="Semantic JSON Report"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <input
                  type="checkbox"
                  checked={mdLlmEnabled}
                  onChange={(e) => setMdLlmEnabled(e.target.checked)}
                />
                <span style={{ marginLeft: 6 }}>LLM summarization enabled</span>
              </label>
              <p style={{ opacity: 0.65, fontSize: 11, marginTop: 0 }}>
                If disabled or API call fails, the node emits deterministic fallback summary markdown.
              </p>
            </div>
          </div>
        )}

        {tab === "config" && isMagneticMapperNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Clean and map airborne magnetic survey data to render-ready points and gridded products.
            </p>
            <div style={mapGrid}>
              <label style={lab}>
                <span style={labSpan}>Grid method</span>
                <select value={mmGridMethod} onChange={(e) => setMmGridMethod(e.target.value)} style={sel}>
                  <option value="idw">IDW</option>
                  <option value="minimum_curvature">Minimum curvature (smoothed)</option>
                </select>
              </label>
              <label style={lab}>
                <span style={labSpan}>Grid resolution (m)</span>
                <input
                  type="text"
                  inputMode="decimal"
                  value={mmGridResolutionM}
                  onChange={(e) => setMmGridResolutionM(e.target.value)}
                  placeholder="25"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>IDW power</span>
                <input
                  type="text"
                  inputMode="decimal"
                  value={mmIdwPower}
                  onChange={(e) => setMmIdwPower(e.target.value)}
                  placeholder="2"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Search radius (m, 0=auto)</span>
                <input
                  type="text"
                  inputMode="decimal"
                  value={mmSearchRadiusM}
                  onChange={(e) => setMmSearchRadiusM(e.target.value)}
                  placeholder="0"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Max points per cell</span>
                <input
                  type="text"
                  inputMode="numeric"
                  value={mmMaxPoints}
                  onChange={(e) => setMmMaxPoints(e.target.value)}
                  placeholder="32"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Max grid cells</span>
                <input
                  type="text"
                  inputMode="numeric"
                  value={mmMaxGridCells}
                  onChange={(e) => setMmMaxGridCells(e.target.value)}
                  placeholder="250000"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Despike sigma</span>
                <input
                  type="text"
                  inputMode="decimal"
                  value={mmDespikeSigma}
                  onChange={(e) => setMmDespikeSigma(e.target.value)}
                  placeholder="6"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Smoothing window (m)</span>
                <input
                  type="text"
                  inputMode="decimal"
                  value={mmSmoothWindowM}
                  onChange={(e) => setMmSmoothWindowM(e.target.value)}
                  placeholder="0"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Resample spacing (m)</span>
                <input
                  type="text"
                  inputMode="decimal"
                  value={mmResampleSpacingM}
                  onChange={(e) => setMmResampleSpacingM(e.target.value)}
                  placeholder="0"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Preview decimation (%)</span>
                <input
                  type="text"
                  inputMode="decimal"
                  value={mmDecimatePct}
                  onChange={(e) => setMmDecimatePct(e.target.value)}
                  placeholder="100"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <input
                  type="checkbox"
                  checked={mmLlmEnabled}
                  onChange={(e) => setMmLlmEnabled(e.target.checked)}
                />
                <span style={{ marginLeft: 6 }}>LLM assist for QA commentary/mutation hints</span>
              </label>
            </div>
          </div>
        )}

        {tab === "config" && isHeatmapRasterTileCacheNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Build a cached raster + tile pyramid from XY points for fast 2D heatmaps and 3D drape overlays.
            </p>
            <div style={mapGrid}>
              <label style={lab}>
                <span style={labSpan}>Measure field</span>
                {rtcMeasureOptions.length > 0 ? (
                  <select
                    value={rtcMeasure}
                    onChange={(e) => setRtcMeasure(e.target.value)}
                    style={sel}
                  >
                    <option value="">Auto (first numeric measure)</option>
                    {rtcMeasureOptions.map((m) => (
                      <option key={m} value={m}>{m}</option>
                    ))}
                  </select>
                ) : (
                  <input
                    type="text"
                    value={rtcMeasure}
                    onChange={(e) => setRtcMeasure(e.target.value)}
                    placeholder="auto-select first numeric measure"
                    style={{ ...sel, fontFamily: "inherit" }}
                  />
                )}
              </label>
              <label style={lab}>
                <span style={labSpan}>Interpolation method</span>
                <select value={rtcMethod} onChange={(e) => setRtcMethod(e.target.value)} style={sel}>
                  <option value="idw">IDW</option>
                  <option value="nearest">Nearest</option>
                </select>
              </label>
              <label style={lab}>
                <span style={labSpan}>Palette</span>
                <select value={rtcPalette} onChange={(e) => setRtcPalette(e.target.value)} style={sel}>
                  <option value="terrain">Terrain</option>
                  <option value="rainbow">Rainbow</option>
                  <option value="viridis">Viridis</option>
                  <option value="inferno">Inferno</option>
                </select>
              </label>
              <label style={lab}>
                <span style={labSpan}>Opacity (0-1)</span>
                <input
                  type="text"
                  inputMode="decimal"
                  value={rtcOpacity}
                  onChange={(e) => setRtcOpacity(e.target.value)}
                  placeholder="0.72"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Grid width (cells)</span>
                <input
                  type="text"
                  inputMode="numeric"
                  value={rtcGridNx}
                  onChange={(e) => setRtcGridNx(e.target.value)}
                  placeholder="384"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Grid height (cells)</span>
                <input
                  type="text"
                  inputMode="numeric"
                  value={rtcGridNy}
                  onChange={(e) => setRtcGridNy(e.target.value)}
                  placeholder="384"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Clamp low (%)</span>
                <input
                  type="text"
                  inputMode="decimal"
                  value={rtcClampLowPct}
                  onChange={(e) => setRtcClampLowPct(e.target.value)}
                  placeholder="2"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Clamp high (%)</span>
                <input
                  type="text"
                  inputMode="decimal"
                  value={rtcClampHighPct}
                  onChange={(e) => setRtcClampHighPct(e.target.value)}
                  placeholder="98"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>IDW power</span>
                <input
                  type="text"
                  inputMode="decimal"
                  value={rtcIdwPower}
                  onChange={(e) => setRtcIdwPower(e.target.value)}
                  placeholder="2"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Max neighbors</span>
                <input
                  type="text"
                  inputMode="numeric"
                  value={rtcMaxPoints}
                  onChange={(e) => setRtcMaxPoints(e.target.value)}
                  placeholder="32"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Tile size</span>
                <input
                  type="text"
                  inputMode="numeric"
                  value={rtcTileSize}
                  onChange={(e) => setRtcTileSize(e.target.value)}
                  placeholder="256"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Min zoom</span>
                <input
                  type="text"
                  inputMode="numeric"
                  value={rtcMinZoom}
                  onChange={(e) => setRtcMinZoom(e.target.value)}
                  placeholder="0"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Max zoom</span>
                <input
                  type="text"
                  inputMode="numeric"
                  value={rtcMaxZoom}
                  onChange={(e) => setRtcMaxZoom(e.target.value)}
                  placeholder="4"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
            </div>
          </div>
        )}

        {tab === "config" && isPlotChartNode && (
          <div>
            <p style={{ opacity: 0.8, marginTop: 0, marginBottom: 10 }}>
              Build a reusable chart from semantic JSON using a backend template. Use{" "}
              <code>data JSON pointer</code> to target a specific fragment (for example{" "}
              <code>/variogram/bins</code>) and avoid processing unrelated payload sections.
            </p>
            <div style={mapGrid}>
              <label style={lab}>
                <span style={labSpan}>Template</span>
                <select
                  value={chartTemplateKey}
                  onChange={(e) => {
                    const nextKey = e.target.value;
                    setChartTemplateKey(nextKey);
                    const picked = chartTemplates.find((t) => t.key === nextKey);
                    setChartTemplateId(picked?.id ?? "");
                    const ptr = (picked?.template_schema?.default_data_pointer_candidates as unknown[] | undefined)
                      ?.find((x): x is string => typeof x === "string");
                    if (typeof ptr === "string" && chartDataPointer.trim().length === 0) {
                      setChartDataPointer(ptr);
                    }
                  }}
                  style={sel}
                >
                  {chartTemplates.length === 0 ? (
                    <>
                      <option value="variogram">Variogram</option>
                      <option value="scatter">Scatter</option>
                      <option value="histogram">Histogram</option>
                      <option value="profile">Profile</option>
                    </>
                  ) : (
                    chartTemplates.map((t) => (
                      <option key={t.id} value={t.key}>
                        {t.name}
                      </option>
                    ))
                  )}
                </select>
              </label>
              <label style={lab}>
                <span style={labSpan}>Data fragment</span>
                <select
                  value={chartDataFragment}
                  onChange={(e) => {
                    const v = e.target.value;
                    setChartDataFragment(v);
                    if (v === "auto") {
                      setChartDataPointer("");
                    } else {
                      setChartDataPointer(v);
                    }
                  }}
                  style={sel}
                >
                  {chartFragmentOptions.map((p) => (
                    <option key={p} value={p}>
                      {p === "auto" ? "Auto" : p === "custom" ? "Custom (manual pointer)" : p}
                    </option>
                  ))}
                </select>
              </label>
              <label style={lab}>
                <span style={labSpan}>Data JSON pointer</span>
                <input
                  type="text"
                  value={chartDataPointer}
                  onChange={(e) => {
                    setChartDataPointer(e.target.value);
                    setChartDataFragment("custom");
                  }}
                  placeholder="/variogram/bins"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Chart title (optional)</span>
                <input
                  type="text"
                  value={chartTitle}
                  onChange={(e) => setChartTitle(e.target.value)}
                  placeholder="Auto title"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>User objective (optional)</span>
                <input
                  type="text"
                  value={chartObjective}
                  onChange={(e) => setChartObjective(e.target.value)}
                  placeholder="Highlight nugget / sill behavior"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Context rows (top/tail)</span>
                <input
                  type="text"
                  inputMode="numeric"
                  value={chartMaxContextRows}
                  onChange={(e) => setChartMaxContextRows(e.target.value)}
                  placeholder="8"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <span style={labSpan}>Max render rows</span>
                <input
                  type="text"
                  inputMode="numeric"
                  value={chartMaxRenderRows}
                  onChange={(e) => setChartMaxRenderRows(e.target.value)}
                  placeholder="3000"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              <label style={lab}>
                <input
                  type="checkbox"
                  checked={chartLlmEnabled}
                  onChange={(e) => setChartLlmEnabled(e.target.checked)}
                />
                <span style={{ marginLeft: 6 }}>LLM planning assist (optional)</span>
              </label>
            </div>
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
