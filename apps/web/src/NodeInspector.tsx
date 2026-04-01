import { useCallback, useEffect, useMemo, useState, type CSSProperties } from "react";
import { parseCsv } from "./csvParse";
import type { ApiNode, ArtifactEntry } from "./graphApi";
import { patchNodeParams } from "./graphApi";
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
  const [headers, setHeaders] = useState<string[]>([]);
  const [csvRows, setCsvRows] = useState<string[][]>([]);
  const [previewRows, setPreviewRows] = useState<string[][]>([]);
  const [saveMsg, setSaveMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

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
    graphId,
    activeBranchId,
    node.id,
    kind,
    zRelative,
    outputCrsMode,
    outputCustomEpsg,
    onNodeUpdated,
  ]);

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
        {(
          [
            ["summary", "Summary"],
            ["diagnostics", "Run"],
            ["mapping", "Mapping"],
            ["crs", "CRS"],
            ["output", "Output"],
          ] as const
        ).map(([k, lab]) => (
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

        {tab !== "output" && tab !== "diagnostics" && (
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
