import { useCallback, useEffect, useMemo, useState, type CSSProperties } from "react";
import { parseCsv } from "./csvParse";
import type { ApiNode, ArtifactEntry } from "./graphApi";
import { api, patchNodeParams, runGraph } from "./graphApi";
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
import { searchEpsg, type EpsgSearchHit } from "./epsgSearch";

const OUTPUT_CRS_OPTIONS: { value: string; label: string }[] = [
  { value: "project", label: "Project CRS (default)" },
  { value: "wgs84", label: "EPSG:4326 / WGS84 (web maps)" },
  { value: "source", label: "Same as source file CRS" },
  { value: "custom", label: "Custom EPSG…" },
];

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
  projectEpsg: number;
  tab: InspectorTab;
  onTab: (t: InspectorTab) => void;
  onClose: () => void;
  onNodeUpdated: (n: ApiNode) => void;
  nodeArtifacts: ArtifactEntry[];
  onPipelineQueued?: () => void;
};

export function NodeInspector({
  graphId,
  activeBranchId,
  node,
  projectEpsg,
  tab,
  onTab,
  onClose,
  onNodeUpdated,
  nodeArtifacts,
  onPipelineQueued,
}: Props) {
  const kind = node.config.kind;
  const csvCapable = isAcquisitionCsvKind(kind);
  const isHeatmapNode = kind === "assay_heatmap";

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
  const [crsSearchQuery, setCrsSearchQuery] = useState<string>("");
  const [crsSearchBusy, setCrsSearchBusy] = useState(false);
  const [crsSearchErr, setCrsSearchErr] = useState<string | null>(null);
  const [crsSearchHits, setCrsSearchHits] = useState<EpsgSearchHit[]>([]);

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
    const q = crsSearchQuery.trim();
    if (q.length < 2) {
      setCrsSearchHits([]);
      setCrsSearchErr(null);
      return;
    }
    let cancelled = false;
    const tid = window.setTimeout(() => {
      void (async () => {
        setCrsSearchBusy(true);
        setCrsSearchErr(null);
        try {
          const hits = await searchEpsg(q);
          if (!cancelled) setCrsSearchHits(hits);
        } catch (e) {
          if (!cancelled) {
            setCrsSearchErr(e instanceof Error ? e.message : String(e));
            setCrsSearchHits([]);
          }
        } finally {
          if (!cancelled) setCrsSearchBusy(false);
        }
      })();
    }, 250);
    return () => {
      cancelled = true;
      window.clearTimeout(tid);
    };
  }, [crsSearchQuery]);

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
    if (isHeatmapNode) {
      const n = (v: string, fallback: number) => {
        const x = Number(v);
        return Number.isFinite(x) ? x : fallback;
      };
      ui.measure = heatMeasure.trim();
      ui.method = heatMethod;
      ui.scale = heatScale;
      ui.palette = heatPalette;
      ui.clamp_low_pct = Math.max(0, Math.min(100, n(heatClampLow, 0)));
      ui.clamp_high_pct = Math.max(0, Math.min(100, n(heatClampHigh, 100)));
      ui.idw_power = Math.max(1, Math.min(4, n(heatIdwPower, 2)));
      ui.smoothness = Math.max(128, Math.min(512, Math.trunc(n(heatSmoothness, 256))));
      ui.search_radius_m = Math.max(0, n(heatRadius, 0));
      ui.min_points = Math.max(1, Math.trunc(n(heatMinPoints, 3)));
      ui.max_points = Math.max(Math.trunc(n(heatMinPoints, 3)), Math.trunc(n(heatMaxPoints, 32)));
      ui.contours_enabled = heatContoursEnabled;
      ui.contour_mode = heatContourMode;
      ui.contour_interval = Math.max(0.0001, n(heatContourInterval, 1));
      ui.contour_levels = Math.max(2, Math.trunc(n(heatContourLevels, 10)));
      ui.gradient_enabled = heatGradientEnabled;
      ui.gradient_mode = heatGradientMode;
      ui.output_crs_mode = heatOutputCrsMode;
      ui.output_crs_epsg =
        heatOutputCrsMode === "custom"
          ? Math.max(1, Math.trunc(n(heatOutputCustomEpsg, 4326)))
          : undefined;
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
    heatIdwPower,
    heatSmoothness,
    heatRadius,
    heatMinPoints,
    heatMaxPoints,
    heatContoursEnabled,
    heatContourMode,
    heatContourInterval,
    heatContourLevels,
    heatGradientEnabled,
    heatGradientMode,
    heatOutputCrsMode,
    heatOutputCustomEpsg,
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

  const tabs = useMemo(() => {
    const base: Array<readonly [InspectorTab, string]> = [
      ["summary", "Summary"],
      ["diagnostics", "Run"],
    ];
    if (isHeatmapNode) {
      base.push(["config", "Heatmap"]);
    }
    if (csvCapable) {
      base.push(["mapping", "Mapping"], ["crs", "CRS"]);
    }
    base.push(["output", "Output"]);
    return base;
  }, [csvCapable, isHeatmapNode]);

  useEffect(() => {
    if (tabs.some(([k]) => k === tab)) return;
    onTab("summary");
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
        <span style={{ fontWeight: 600, fontSize: 13 }}>Node</span>
        <button type="button" onClick={onClose} style={btnGhost}>
          Close
        </button>
      </div>
      <div role="tablist" style={{ display: "flex", borderBottom: "1px solid #30363d" }}>
        {tabs.map(([k, lab]) => (
          <button
            key={k}
            type="button"
            role="tab"
            aria-selected={tab === k}
            onClick={() => onTab(k)}
            style={{
              flex: 1,
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

        {tab === "crs" && (
          <div>
            <p style={{ opacity: 0.8, marginBottom: 10 }}>
              Workspace project CRS: <strong>EPSG:{projectEpsg}</strong> (from the graph’s
              workspace; used when you choose project CRS below).
            </p>
            <label style={lab}>
              <span style={labSpan}>Source file CRS</span>
              <select
                value={crsMode}
                onChange={(e) => setCrsMode(e.target.value)}
                style={sel}
              >
                {ACQUISITION_EPSG_OPTIONS.map((o) => (
                  <option key={o.value} value={o.value}>
                    {o.label}
                  </option>
                ))}
              </select>
            </label>
            {crsMode === "custom" && (
              <label style={lab}>
                <span style={labSpan}>Custom source EPSG</span>
                <input
                  type="number"
                  value={sourceCustomEpsg}
                  onChange={(e) => setSourceCustomEpsg(e.target.value)}
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
            )}
            <div style={{ marginTop: 8 }}>
              <label style={lab}>
                <span style={labSpan}>Search EPSG code or description</span>
                <input
                  type="text"
                  value={crsSearchQuery}
                  onChange={(e) => setCrsSearchQuery(e.target.value)}
                  placeholder="e.g. GDA2020 zone 55, WGS84, 28356"
                  style={{ ...sel, fontFamily: "inherit" }}
                />
              </label>
              {crsSearchBusy && (
                <div style={{ fontSize: 11, opacity: 0.65 }}>Searching EPSG registry…</div>
              )}
              {crsSearchErr && (
                <div style={{ fontSize: 11, color: "#f85149" }}>{crsSearchErr}</div>
              )}
              {!crsSearchBusy && crsSearchHits.length > 0 && (
                <div
                  style={{
                    marginTop: 6,
                    maxHeight: 150,
                    overflow: "auto",
                    border: "1px solid #30363d",
                    borderRadius: 6,
                    background: "#0f1419",
                  }}
                >
                  {crsSearchHits.map((h) => (
                    <button
                      key={h.code}
                      type="button"
                      onClick={() => {
                        setCrsMode("custom");
                        setSourceCustomEpsg(h.code);
                      }}
                      style={{
                        display: "block",
                        width: "100%",
                        textAlign: "left",
                        border: "none",
                        background: "transparent",
                        color: "#e6edf3",
                        padding: "7px 8px",
                        fontSize: 11,
                        cursor: "pointer",
                      }}
                    >
                      <strong>EPSG:{h.code}</strong> - {h.name}
                    </button>
                  ))}
                </div>
              )}
            </div>
            <p style={{ fontSize: 11, opacity: 0.6, marginTop: 12 }}>
              Coordinates in the CSV are interpreted in this CRS. The worker reprojects to the
              collar output CRS when they differ. Use the search box to find EPSG entries by code
              or full description.
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

        {tab === "output" && (
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
