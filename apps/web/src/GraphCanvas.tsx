import {
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
} from "react";
import { createPortal } from "react-dom";
import {
  Background,
  Controls,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodeId,
  useNodesState,
  type Connection,
  type Edge,
  type Node,
  type NodeProps,
  type ReactFlowInstance,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { AddNodePreset } from "./graphAddNodes";
import { getAddNodePresets } from "./graphAddNodes";
import {
  addGraphNode,
  createGraphEdge,
  deleteGraphEdge,
  deleteGraphNode,
  fetchGraph,
  patchNodeParams,
  runGraph,
  type ApiEdge,
  type ApiNode,
  type ArtifactEntry,
} from "./graphApi";
import {
  GraphInspectorContext,
  type InspectorTab,
} from "./graphInspectorContext";
import { NodeInspector } from "./NodeInspector";
import {
  edgeColorForApiEdge,
  pickHandleColorForPortWithSemantic,
} from "./portTypes";
import { DYNAMIC_PORT_GROUPS, dynPortInfo, incomingPortIds, outgoingPortIds } from "./nodePortLayout";
import { isAcquisitionCsvKind } from "./pipelineSchema";
import { nodePorts, nodeRole, nodeSpec, portSemantic } from "./nodeRegistry";

const CAT_ACCENT: Record<string, string> = {
  input: "#238636",
  transform: "#1f6feb",
  model: "#a371f7",
  qa: "#d29922",
  visualisation: "#db61a2",
  export: "#8b949e",
};

type ExecTone =
  | "unset"
  | "stale"
  | "locked"
  | "current"
  | "error";

type PipelineData = {
  kind: string;
  title: string;
  alias: string | null;
  role: string;
  category: string;
  categoryAccent: string;
  statusLine: string;
  hashShort: string | null;
  isRunning: boolean;
  nodeState: ExecTone;
  isLocked: boolean;
  lastErrorShort: string | null;
  lastErrorFull: string | null;
  feeds: string;
  showCsv: boolean;
  incomingPorts: string[];
  outgoingPorts: string[];
  portColorsIn: Record<string, string>;
  portColorsOut: Record<string, string>;
  portLabelsIn: Record<string, string>;
  portLabelsOut: Record<string, string>;
  portSemanticsIn: Record<string, string>;
  portSemanticsOut: Record<string, string>;
  portOptionalsIn: Record<string, boolean>;
  portOptionalsOut: Record<string, boolean>;
};

function computeExecTone(
  node: ApiNode,
  incomingPortCount: number,
  kind: string,
  hasOutputArtifact: boolean,
  hasUpstreamArtifact: boolean
): ExecTone {
  const ex = node.execution;
  const cache = node.cache;
  const viewerVirtualOk =
    (
      kind === "plan_view_2d" ||
      kind === "plan_view_3d" ||
      kind === "cesium_display_node" ||
      kind === "threejs_display_node"
    ) &&
    hasUpstreamArtifact;
  const isFailed =
    !viewerVirtualOk && (ex === "failed" || (node.last_error ?? "").trim().length > 0);
  if (isFailed) return "error";

  const ui = (
    node.config?.params?.ui &&
    typeof node.config.params.ui === "object" &&
    !Array.isArray(node.config.params.ui)
      ? node.config.params.ui
      : {}
  ) as Record<string, unknown>;
  const csvRows = ui.csv_rows;
  const hasCsvRows = Array.isArray(csvRows) && csvRows.length > 0;
  const needsCsv = isAcquisitionCsvKind(kind);
  const noWiredInputs = !needsCsv && incomingPortCount === 0;
  const missingInputConfig = needsCsv && !hasCsvRows;
  if (noWiredInputs || missingInputConfig) return "unset";

  if (node.policy.recompute === "manual") return "locked";
  if (
    ex === "succeeded" &&
    cache === "hit" &&
    (hasOutputArtifact || viewerVirtualOk)
  ) {
    return "current";
  }
  if (cache === "stale" || cache === "miss") return "stale";
  return "stale";
}

function borderStyleForExec(
  tone: ExecTone,
  categoryAccent: string
): { width: number; color: string; style: "solid" | "dashed" } {
  switch (tone) {
    case "error":
      return { width: 2, color: "#f85149", style: "solid" };
    case "current":
      return { width: 2, color: "#3fb950", style: "solid" };
    case "stale":
      return { width: 2, color: "#d29922", style: "solid" };
    case "locked":
      return { width: 2, color: "#a371f7", style: "solid" };
    case "unset":
      return { width: 2, color: "#6e7681", style: "dashed" };
    default:
      return { width: 1, color: categoryAccent, style: "solid" };
  }
}

function shortErr(s: string | null, max = 96): string | null {
  if (!s) return null;
  const t = s.trim();
  if (!t.length) return null;
  if (t.length <= max) return t;
  return `${t.slice(0, max)}…`;
}

function loadSavedPositions(
  graphId: string
): Record<string, { x: number; y: number }> {
  try {
    const raw = localStorage.getItem(`mineeye:npos:${graphId}`);
    if (!raw) return {};
    const o = JSON.parse(raw) as Record<string, { x: number; y: number }>;
    return o && typeof o === "object" ? o : {};
  } catch {
    return {};
  }
}

function saveNodePosition(
  graphId: string,
  nodeId: string,
  pos: { x: number; y: number }
) {
  const all = loadSavedPositions(graphId);
  all[nodeId] = pos;
  localStorage.setItem(`mineeye:npos:${graphId}`, JSON.stringify(all));
}

function removeNodePosition(graphId: string, nodeId: string) {
  const all = loadSavedPositions(graphId);
  delete all[nodeId];
  localStorage.setItem(`mineeye:npos:${graphId}`, JSON.stringify(all));
}

function StatusIndicator({ state, isRunning }: { state: ExecTone; isRunning: boolean }) {
  if (isRunning) {
    return (
      <span style={{ display: "inline-flex", alignItems: "center", gap: 4, fontSize: 10, color: "#58a6ff" }}>
        <span style={{
          display: "inline-block", width: 7, height: 7, borderRadius: "50%",
          background: "#58a6ff", flexShrink: 0,
          animation: "mineeye-status-pulse 0.85s ease-in-out infinite",
        }} />
        Running
      </span>
    );
  }
  const cfg = {
    current: { color: "#3fb950", label: "Current", hollow: false },
    stale:   { color: "#d29922", label: "Stale",   hollow: false },
    error:   { color: "#f85149", label: "Error",   hollow: false },
    locked:  { color: "#a371f7", label: "Locked",  hollow: false },
    unset:   { color: "#6e7681", label: "Unset",   hollow: true  },
  } as const;
  const c = (cfg as Record<string, { color: string; label: string; hollow: boolean }>)[state]
    ?? { color: "#6e7681", label: state, hollow: true };
  return (
    <span title={c.label} style={{ display: "inline-flex", alignItems: "center", gap: 4, fontSize: 10, color: c.color }}>
      <span style={{
        display: "inline-block", width: 7, height: 7, borderRadius: "50%", flexShrink: 0,
        background: c.hollow ? "transparent" : c.color,
        border: c.hollow ? `1.5px solid ${c.color}` : "none",
      }} />
      {c.label}
    </span>
  );
}

// ── compact icon-button style helper ────────────────────────────────────────
function iconBtn(opts: {
  accent: string;
  active?: boolean;
  disabled?: boolean;
  viewer?: boolean;
}): CSSProperties {
  const isViewer = opts.viewer === true;
  return {
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    width: 24,
    height: 24,
    padding: 0,
    border: `1px solid ${
      opts.active || isViewer ? opts.accent : "rgba(55,62,70,1)"
    }`,
    borderRadius: 5,
    background: opts.active
      ? `${opts.accent}30`
      : isViewer
        ? `${opts.accent}18`
        : "rgba(22,27,34,0.75)",
    color: opts.active || isViewer
      ? opts.accent
      : opts.disabled
        ? "#30363d"
        : "#8b949e",
    cursor: opts.disabled ? "default" : "pointer",
    fontSize: isViewer ? 14 : 12,
    lineHeight: 1,
    transition: "border-color 0.1s, color 0.1s, background 0.1s",
    flexShrink: 0,
    boxShadow: isViewer ? `0 0 6px ${opts.accent}44` : "none",
  };
}

const VIEWER_KINDS = new Set([
  "plan_view_2d",
  "plan_view_3d",
  "cesium_display_node",
  "threejs_display_node",
]);

// ── State dot config ─────────────────────────────────────────────────────────
const STATE_DOT: Record<string, { color: string; label: string }> = {
  unset:   { color: "#6e7681", label: "Not configured" },
  stale:   { color: "#d29922", label: "Stale" },
  current: { color: "#3fb950", label: "Current" },
  locked:  { color: "#a371f7", label: "Locked" },
  error:   { color: "#f85149", label: "Error" },
};

// ── Layout constants — must match the rendered heights exactly ───────────────
// Top bar:  7px top-pad + 20px row (buttons dominate) + 5px bot-pad = 32px
// Port row: 17px per row with 6px section padding top + bottom
const H_TOP = 32;       // height of the top bar
const H_PORT_PAD = 6;   // padding above/below port rows in their section
const H_PORT_ROW = 17;  // height of each port row

// Pixel Y from card top (padding edge) to center of port row i
function portHandleTop(i: number): number {
  return H_TOP + 1 /* section border */ + H_PORT_PAD + i * H_PORT_ROW + H_PORT_ROW / 2;
}

function PipelineNode({ data }: NodeProps<PipelineData>) {
  const id = useNodeId();
  const ctx = useContextOptional();
  const [aliasEdit, setAliasEdit] = useState(false);
  const [aliasVal, setAliasVal] = useState(data.alias ?? data.title);
  const [errExpanded, setErrExpanded] = useState(false);

  useEffect(() => {
    if (!aliasEdit) setAliasVal(data.alias ?? data.title);
  }, [data.alias, data.title, aliasEdit]);

  const nIn = data.incomingPorts.length;
  const nOut = data.outgoingPorts.length;
  const portRows = Math.max(nIn, nOut);
  const hasPorts = portRows > 0;

  const b = borderStyleForExec(data.nodeState, data.categoryAccent);
  const hasCustomAlias = data.alias != null && data.alias !== data.title;
  const displayName = data.alias ?? data.title;
  const hasFullError = !!data.lastErrorFull && data.lastErrorFull !== data.lastErrorShort;
  const dotCfg = data.isRunning
    ? { color: "#58a6ff", label: "Running…" }
    : STATE_DOT[data.nodeState] ?? { color: "#6e7681", label: data.nodeState };

  function commitAlias() {
    setAliasEdit(false);
    const trimmed = aliasVal.trim() || data.title;
    setAliasVal(trimmed);
    if (trimmed !== (data.alias ?? data.title) && id) {
      void ctx?.renameNode(id, trimmed === data.title ? "" : trimmed);
    }
  }

  return (
    <div
      className={data.isRunning ? "mineeye-node-running" : undefined}
      style={{
        position: "relative",
        background: "#161b22",
        color: "#e6edf3",
        borderTop: `1px ${b.style} ${b.color}`,
        borderRight: `1px ${b.style} ${b.color}`,
        borderBottom: `1px ${b.style} ${b.color}`,
        borderLeft: `3px solid ${data.categoryAccent}`,
        borderRadius: 10,
        minWidth: 200,
        maxWidth: 270,
        fontSize: 12,
      }}
    >
      {/* ── Handles (absolutely positioned at pixel offsets) ─────────── */}
      {data.incomingPorts.map((port, i) => (
        <Handle key={`in-${port}`}
          type="target" position={Position.Left} id={port} isConnectable
          title={`${data.portLabelsIn[port] ?? port}${data.portSemanticsIn[port] ? ` · ${data.portSemanticsIn[port].replace(/_/g, " ")}` : ""}${data.portOptionalsIn[port] ? " (optional)" : ""}`}
          style={{
            top: portHandleTop(i),
            left: -6,
            background: data.portColorsIn[port] ?? "#484f58",
            border: "2px solid #161b22",
            width: 11, height: 11, borderRadius: "50%",
            opacity: data.portOptionalsIn[port] ? 0.5 : 1,
          }}
        />
      ))}
      {data.outgoingPorts.map((port, i) => (
        <Handle key={`out-${port}`}
          type="source" position={Position.Right} id={port} isConnectable
          title={`${data.portLabelsOut[port] ?? port}${data.portSemanticsOut[port] ? ` · ${data.portSemanticsOut[port].replace(/_/g, " ")}` : ""}`}
          style={{
            top: portHandleTop(i),
            right: -6,
            background: data.portColorsOut[port] ?? "#484f58",
            border: "2px solid #161b22",
            width: 11, height: 11, borderRadius: "50%",
          }}
        />
      ))}

      {/* ── TOP BAR: name · status dot · 4 icon buttons ─────────────── */}
      {/* height = 8 + 22 (button height) + 6 = H_TOP = 36px exactly   */}
      <div style={{
        display: "flex", alignItems: "center", gap: 3,
        padding: "7px 8px 5px",
        boxSizing: "border-box",
      }}>
        {/* Editable name */}
        {aliasEdit ? (
          <input
            autoFocus
            value={aliasVal}
            onChange={(e) => setAliasVal(e.target.value)}
            onBlur={commitAlias}
            onKeyDown={(e) => {
              e.stopPropagation();
              if (e.key === "Enter") commitAlias();
              if (e.key === "Escape") { setAliasEdit(false); setAliasVal(data.alias ?? data.title); }
            }}
            style={{
              flex: 1, minWidth: 0,
              background: "rgba(56,139,253,0.1)",
              border: "1px solid #388bfd",
              borderRadius: 4,
              color: "#e6edf3", fontSize: 12, fontWeight: 600,
              padding: "0 5px", height: 20,
              outline: "none", fontFamily: "inherit",
            }}
          />
        ) : (
          <span
            title="Click to rename"
            onClick={(e) => { e.stopPropagation(); setAliasEdit(true); }}
            style={{
              flex: 1, minWidth: 0,
              fontWeight: 600, fontSize: 12, lineHeight: "20px",
              letterSpacing: "-0.01em", cursor: "text",
              overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
              color: "#e6edf3",
            }}
          >
            {displayName}
          </span>
        )}
        {/* Status dot */}
        <span title={dotCfg.label} style={{
          display: "inline-block", width: 7, height: 7,
          borderRadius: "50%", flexShrink: 0,
          background: data.nodeState === "unset" ? "transparent" : dotCfg.color,
          border: data.nodeState === "unset" ? `1.5px solid ${dotCfg.color}` : "none",
          animation: data.isRunning ? "mineeye-status-pulse 0.85s ease-in-out infinite" : "none",
        }} />
        {/* Icon buttons */}
        {ctx && id && (<>
          {data.kind === "aoi" && (
            <button type="button"
              title="Edit AOI on map"
              onClick={(e) => { e.stopPropagation(); ctx.openAoiEditor?.(id); }}
              style={{
                ...iconBtn({ accent: "#f7b731" }),
                fontSize: 13,
              }}
            >✏</button>
          )}
          <button type="button"
            title={data.isRunning ? "Running…" : "Run this node and downstream"}
            disabled={data.isRunning}
            onClick={(e) => { e.stopPropagation(); void ctx.queueNodeRun(id, { includeManual: true }); }}
            style={iconBtn({ accent: "#388bfd", disabled: data.isRunning })}
          >▶</button>
          <button type="button"
            title={data.isLocked ? "Locked — click to enable auto-recompute" : "Unlocked — click to lock"}
            onClick={(e) => { e.stopPropagation(); void ctx.toggleLock(id, data.isLocked); }}
            style={iconBtn({ accent: "#a371f7", active: data.isLocked })}
          >{data.isLocked ? "⊗" : "⊙"}</button>
          <button type="button"
            title="Edit configuration"
            onClick={(e) => { e.stopPropagation(); ctx.openInspector(id, "config"); }}
            style={iconBtn({ accent: "#58a6ff" })}
          >⚙</button>
          <button type="button"
            title="Open viewer / preview"
            onClick={(e) => { e.stopPropagation(); ctx.openNodeViewer(id); }}
            style={iconBtn({
              accent: VIEWER_KINDS.has(data.kind) ? "#db61a2" : "#3fb950",
              viewer: VIEWER_KINDS.has(data.kind),
            })}
          >{VIEWER_KINDS.has(data.kind) ? "👁" : "⊞"}</button>
        </>)}
      </div>

      {/* ── PORT ROWS (middle section) ───────────────────────────────── */}
      {hasPorts && (
        <div style={{
          borderTop: "1px solid #21262d",
          padding: `${H_PORT_PAD}px 8px`,
        }}>
          {Array.from({ length: portRows }).map((_, rowIdx) => {
            const inPort = data.incomingPorts[rowIdx];
            const outPort = data.outgoingPorts[rowIdx];
            const inColor = inPort ? (data.portColorsIn[inPort] ?? "#484f58") : undefined;
            const outColor = outPort ? (data.portColorsOut[outPort] ?? "#484f58") : undefined;
            const inOptional = inPort ? (data.portOptionalsIn[inPort] ?? false) : false;
            return (
              <div key={rowIdx} style={{
                display: "flex", alignItems: "center",
                height: H_PORT_ROW,
              }}>
                {/* Input label — left-aligned, port colour */}
                <span style={{
                  flex: 1, minWidth: 0,
                  fontSize: 7.5, lineHeight: 1,
                  color: inColor ?? "transparent",
                  opacity: inOptional ? 0.48 : 0.82,
                  fontStyle: inOptional ? "italic" : "normal",
                  overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                  letterSpacing: "0.01em",
                }}>
                  {inPort ? (data.portLabelsIn[inPort] ?? inPort.replace(/_/g, " ")) : ""}
                </span>
                {/* Output label — right-aligned, port colour */}
                <span style={{
                  flex: 1, minWidth: 0,
                  fontSize: 7.5, lineHeight: 1,
                  color: outColor ?? "transparent",
                  opacity: 0.82,
                  overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                  textAlign: "right",
                  letterSpacing: "0.01em",
                }}>
                  {outPort ? (data.portLabelsOut[outPort] ?? outPort.replace(/_/g, " ")) : ""}
                </span>
              </div>
            );
          })}
        </div>
      )}

      {/* ── BOTTOM: category · role · error · footer ─────────────────── */}
      <div style={{
        borderTop: "1px solid #21262d",
        padding: "5px 8px 6px",
      }}>
        {/* Category + original type when aliased */}
        <div style={{ display: "flex", alignItems: "center", gap: 4, marginBottom: 2 }}>
          <span style={{
            fontSize: 7.5, fontWeight: 700, letterSpacing: "0.08em",
            textTransform: "uppercase", color: data.categoryAccent, flexShrink: 0,
          }}>
            {data.category}
          </span>
          {hasCustomAlias && (
            <>
              <span style={{ fontSize: 7.5, color: "#21262d" }}>·</span>
              <span style={{
                fontSize: 7.5, color: "#484f58",
                overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
              }}>
                {data.title}
              </span>
            </>
          )}
        </div>

        {/* Role — single truncated line */}
        <div style={{
          fontSize: 9.5, color: "#6e7681", lineHeight: 1.3,
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
        }}>
          {data.role}
        </div>

        {/* Error (collapsible) */}
        {data.nodeState === "error" && data.lastErrorShort && (
          <div style={{
            marginTop: 5,
            background: "rgba(248,81,73,0.07)",
            border: "1px solid rgba(248,81,73,0.18)",
            borderRadius: 4, overflow: "hidden",
          }}>
            <div
              style={{
                padding: "3px 6px",
                display: "flex", alignItems: "flex-start", gap: 4,
                cursor: hasFullError ? "pointer" : "default",
              }}
              onClick={(e) => { e.stopPropagation(); if (hasFullError) setErrExpanded((v) => !v); }}
            >
              <span style={{ flex: 1, fontSize: 9, color: "#fca5a5", lineHeight: 1.4 }}>
                {data.lastErrorShort}
              </span>
              {hasFullError && (
                <span style={{ flexShrink: 0, fontSize: 8.5, color: "#484f58", marginTop: 1 }}>
                  {errExpanded ? "▴" : "▾"}
                </span>
              )}
            </div>
            {errExpanded && data.lastErrorFull && (
              <div style={{
                padding: "0 6px 5px",
                fontSize: 8.5, color: "#f78166",
                whiteSpace: "pre-wrap", wordBreak: "break-word",
                maxHeight: 150, overflowY: "auto",
                borderTop: "1px solid rgba(248,81,73,0.12)",
              }}>
                {data.lastErrorFull}
              </div>
            )}
          </div>
        )}

        {/* Footer: upstream feeds + content hash */}
        {(data.feeds || data.hashShort) && (
          <div style={{
            marginTop: 5, paddingTop: 4,
            borderTop: "1px solid #1c2128",
            display: "flex", alignItems: "center",
            justifyContent: "space-between", gap: 6,
          }}>
            {data.feeds && (
              <div style={{
                fontSize: 9, color: "#484f58", lineHeight: 1.3,
                flex: 1, minWidth: 0,
                overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
              }}>
                ← {data.feeds}
              </div>
            )}
            {data.hashShort && (
              <div style={{
                fontSize: 8, color: "#30363d",
                fontFamily: "ui-monospace, monospace", flexShrink: 0,
              }}>
                {data.hashShort}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function useContextOptional() {
  return useContext(GraphInspectorContext);
}

const menuBtn: CSSProperties = {
  display: "block",
  width: "100%",
  textAlign: "left",
  padding: "5px 8px",
  marginBottom: 2,
  border: "none",
  borderRadius: 6,
  background: "#21262d",
  color: "#e6edf3",
  cursor: "pointer",
  fontSize: 10.5,
  lineHeight: 1.15,
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
};

const nodeTypes = { pipeline: PipelineNode };

function layoutNodes(nodes: ApiNode[], edges: ApiEdge[]): Map<string, { x: number; y: number }> {
  const ids = new Set(nodes.map((n) => n.id));
  const preds = new Map<string, string[]>();
  for (const n of nodes) preds.set(n.id, []);
  for (const e of edges) {
    if (ids.has(e.from_node) && ids.has(e.to_node)) {
      preds.get(e.to_node)!.push(e.from_node);
    }
  }

  const depthMemo = new Map<string, number>();
  const visiting = new Set<string>();
  function depth(id: string): number {
    if (depthMemo.has(id)) return depthMemo.get(id)!;
    if (visiting.has(id)) {
      depthMemo.set(id, 0);
      return 0;
    }
    visiting.add(id);
    const ps = preds.get(id) ?? [];
    let d = 0;
    if (ps.length > 0) {
      const ds = ps.map((p) => depth(p)).filter((x) => Number.isFinite(x));
      d = ds.length > 0 ? 1 + Math.max(...ds) : 0;
    }
    visiting.delete(id);
    depthMemo.set(id, d);
    return d;
  }

  const byLayer = new Map<number, string[]>();
  for (const n of nodes) {
    const d = depth(n.id);
    if (!byLayer.has(d)) byLayer.set(d, []);
    byLayer.get(d)!.push(n.id);
  }

  const pos = new Map<string, { x: number; y: number }>();
  const layerWidth = 300;
  const layerHeight = 118;
  const sortedLayers = [...byLayer.keys()].sort((a, b) => a - b);
  for (const layer of sortedLayers) {
    const row = (byLayer.get(layer) ?? []).sort();
    row.forEach((id, i) => {
      pos.set(id, { x: 20 + layer * layerWidth, y: 20 + i * layerHeight });
    });
  }
  return pos;
}

function incomingFeedLabels(
  nodes: ApiNode[],
  edges: ApiEdge[]
): Map<string, string> {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const m = new Map<string, string[]>();
  for (const e of edges) {
    if (!m.has(e.to_node)) m.set(e.to_node, []);
    const from = byId.get(e.from_node);
    const fromKind = from?.config.kind ?? "";
    const title = (fromKind && nodeSpec(fromKind)?.label) || fromKind.replace(/_/g, " ") || e.from_node.slice(0, 8);
    m.get(e.to_node)!.push(`${title} (${e.from_port}→${e.to_port})`);
  }
  const out = new Map<string, string>();
  for (const [id, parts] of m) {
    out.set(id, parts.join(" · "));
  }
  return out;
}

function toFlowElements(
  graphId: string,
  nodes: ApiNode[],
  edges: ApiEdge[],
  artifacts: ArtifactEntry[]
): { n: Node[]; e: Edge[] } {
  const positions = layoutNodes(nodes, edges);
  const saved = loadSavedPositions(graphId);
  const feeds = incomingFeedLabels(nodes, edges);
  const hasArtifactByNode = new Set(artifacts.map((a) => a.node_id));
  const n: Node[] = nodes.map((node) => {
    const auto = positions.get(node.id) ?? { x: 0, y: 0 };
    const p = saved[node.id] ?? auto;
    const kind = node.config.kind;
    const title = nodeSpec(kind)?.label ?? kind.replace(/_/g, " ");
    const role = nodeRole(kind) ?? `Node · ${kind}`;
    const rawAlias = node.config?.params?._alias;
    const alias = typeof rawAlias === "string" && rawAlias.trim() ? rawAlias.trim() : null;
    const incomingPorts = incomingPortIds(node.id, kind, edges);
    const hasOutputArtifact = hasArtifactByNode.has(node.id);
    const hasUpstreamArtifact = edges.some(
      (e) => e.to_node === node.id && hasArtifactByNode.has(e.from_node)
    );
    const nodeState = computeExecTone(
      node,
      incomingPorts.length,
      kind,
      hasOutputArtifact,
      hasUpstreamArtifact
    );
    const isRunning = node.execution === "running" || node.execution === "pending";
    const isLocked = node.policy.recompute === "manual";
    const cat = node.category;
    const categoryAccent = CAT_ACCENT[cat] ?? "#484f58";
    const hashShort = node.content_hash
      ? `sha256:${node.content_hash.slice(0, 10)}…`
      : null;
    const outgoingPorts = outgoingPortIds(node.id, kind, edges);
    const inPortSpecs = nodePorts(kind, "in");
    const outPortSpecs = nodePorts(kind, "out");
    const portColorsIn = Object.fromEntries(
      incomingPorts.map((p) => [
        p,
        pickHandleColorForPortWithSemantic(
          edges,
          node.id,
          "in",
          p,
          portSemantic(kind, "in", p)
        ),
      ])
    );
    const portColorsOut = Object.fromEntries(
      outgoingPorts.map((p) => [
        p,
        pickHandleColorForPortWithSemantic(
          edges,
          node.id,
          "out",
          p,
          portSemantic(kind, "out", p)
        ),
      ])
    );
    const portLabelsIn = Object.fromEntries(
      incomingPorts.map((p) => {
        const spec = inPortSpecs.find((s) => s.id === p);
        const dyn = dynPortInfo(kind, p);
        return [p, spec?.label ?? dyn?.label ?? p.replace(/_/g, " ")];
      })
    );
    const portLabelsOut = Object.fromEntries(
      outgoingPorts.map((p) => {
        const spec = outPortSpecs.find((s) => s.id === p);
        return [p, spec?.label ?? p.replace(/_/g, " ")];
      })
    );
    const portSemanticsIn = Object.fromEntries(
      incomingPorts.map((p) => {
        const dyn = dynPortInfo(kind, p);
        return [p, portSemantic(kind, "in", p) ?? dyn?.semantic ?? ""];
      })
    );
    const portSemanticsOut = Object.fromEntries(
      outgoingPorts.map((p) => [p, portSemantic(kind, "out", p) ?? ""])
    );
    const portOptionalsIn = Object.fromEntries(
      incomingPorts.map((p) => {
        const spec = inPortSpecs.find((s) => s.id === p);
        // dynamic group ports are always optional (none required to connect)
        const isDyn = dynPortInfo(kind, p) !== null;
        return [p, spec?.optional ?? isDyn];
      })
    );
    const portOptionalsOut = Object.fromEntries(
      outgoingPorts.map((p) => {
        const spec = outPortSpecs.find((s) => s.id === p);
        return [p, spec?.optional ?? false];
      })
    );
    return {
      id: node.id,
      type: "pipeline",
      position: p,
      data: {
        kind,
        title,
        alias,
        role,
        category: cat,
        categoryAccent,
        statusLine: `Execution: ${node.execution} · cache: ${node.cache}`,
        hashShort,
        isRunning,
        nodeState,
        isLocked,
        lastErrorShort: shortErr(node.last_error),
        lastErrorFull: node.last_error,
        feeds: feeds.get(node.id) ?? "",
        showCsv: isAcquisitionCsvKind(kind),
        incomingPorts,
        outgoingPorts,
        portColorsIn,
        portColorsOut,
        portLabelsIn,
        portLabelsOut,
        portSemanticsIn,
        portSemanticsOut,
        portOptionalsIn,
        portOptionalsOut,
      },
    };
  });

  const e: Edge[] = edges.map((edge) => {
    const stroke = edgeColorForApiEdge(edge);
    return {
      id: edge.id,
      source: edge.from_node,
      target: edge.to_node,
      sourceHandle: edge.from_port,
      targetHandle: edge.to_port,
      label: `${edge.from_port} → ${edge.to_port} · ${edge.semantic_type.replace(/_/g, " ")}`,
      labelStyle: { fill: stroke, fontSize: 9 },
      style: { stroke, strokeWidth: 2 },
    };
  });

  return { n, e };
}

type Props = {
  graphId: string | null;
  activeBranchId?: string | null;
  refreshToken?: number;
  projectEpsg?: number;
  workspaceUsedEpsgs?: number[];
  artifacts?: ArtifactEntry[];
  onPipelineQueued?: () => void;
  onOpenNodeViewer?: (nodeId: string) => void;
  onOpenNodeEditor?: (nodeId: string) => void;
  onOpenAoiEditor?: (nodeId: string) => void;
  onGraphChanged?: () => void;
};

type CtxMenu =
  | { kind: "none" }
  | { kind: "pane"; x: number; y: number; flowX: number; flowY: number }
  | { kind: "node"; x: number; y: number; nodeId: string }
  | { kind: "edge"; x: number; y: number; edgeId: string };

function FlowWorkspace({
  graphId,
  activeBranchId,
  refreshToken,
  projectEpsg,
  artifacts,
  workspaceUsedEpsgs,
  onPipelineQueued,
  onOpenNodeViewer,
  onOpenNodeEditor,
  onOpenAoiEditor,
  onGraphChanged,
}: {
  graphId: string;
  activeBranchId?: string | null;
  refreshToken: number;
  projectEpsg: number;
  artifacts: ArtifactEntry[];
  workspaceUsedEpsgs: number[];
  onPipelineQueued?: () => void;
  onOpenNodeViewer?: (nodeId: string) => void;
  onOpenNodeEditor?: (nodeId: string) => void;
  onOpenAoiEditor?: (nodeId: string) => void;
  onGraphChanged?: () => void;
}) {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<PipelineData>>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [apiNodes, setApiNodes] = useState<ApiNode[]>([]);
  const [workspaceEpsg, setWorkspaceEpsg] = useState(projectEpsg);
  const [ctxMenu, setCtxMenu] = useState<CtxMenu>({ kind: "none" });
  const [menuFilter, setMenuFilter] = useState<string>("");
  const menuFilterRef = useRef<HTMLInputElement | null>(null);
  const ctxMenuRef = useRef<HTMLDivElement | null>(null);
  // Cascading menu hover state — refs store viewport Y positions (synchronous), state drives render
  const [hoveredGroupKey, setHoveredGroupKey] = useState<string | null>(null);
  const hoveredGroupTop = useRef<number>(0);
  const [hoveredSubGroupKey, setHoveredSubGroupKey] = useState<string | null>(null);
  const hoveredSubGroupTop = useRef<number>(0);
  const [viewport, setViewport] = useState<{ w: number; h: number }>(() => ({
    w: typeof window !== "undefined" ? window.innerWidth : 1280,
    h: typeof window !== "undefined" ? window.innerHeight : 800,
  }));
  const [error, setError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [inspectorTab, setInspectorTab] = useState<InspectorTab>("summary");
  const rfRef = useRef<ReactFlowInstance | null>(null);
  const fittedForGraphRef = useRef<string | null>(null);

  useEffect(() => {
    fittedForGraphRef.current = null;
  }, [graphId]);

  useEffect(() => {
    setWorkspaceEpsg(projectEpsg);
  }, [projectEpsg, graphId]);

  useEffect(() => {
    if (nodes.length === 0) return;
    if (fittedForGraphRef.current === graphId) return;
    const tid = window.setTimeout(() => {
      try {
        rfRef.current?.fitView({ padding: 0.12 });
      } catch {
        /* ignore */
      }
      fittedForGraphRef.current = graphId;
    }, 100);
    return () => window.clearTimeout(tid);
  }, [graphId, nodes.length]);

  const openInspector = useCallback((nodeId: string, tab?: InspectorTab) => {
    setSelectedId(nodeId);
    if (tab) setInspectorTab(tab);
  }, []);

  const openNodeViewer = useCallback(
    (nodeId: string) => {
      onOpenNodeViewer?.(nodeId);
    },
    [onOpenNodeViewer]
  );

  const queueNodeRun = useCallback(
    async (nodeId: string, opts?: { includeManual?: boolean }) => {
      try {
        await runGraph(graphId, {
          dirtyRoots: [nodeId],
          includeManual: opts?.includeManual ?? true,
        });
        onPipelineQueued?.();
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    },
    [graphId, onPipelineQueued]
  );

  const renameNode = useCallback(
    async (nodeId: string, alias: string) => {
      try {
        const updated = await patchNodeParams(graphId, nodeId, { _alias: alias || null }, { branchId: activeBranchId });
        setApiNodes((prev) => prev.map((x) => (x.id === updated.id ? updated : x)));
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    },
    [graphId, activeBranchId]
  );

  const toggleLock = useCallback(
    async (nodeId: string, isCurrentlyLocked: boolean) => {
      try {
        const updated = await patchNodeParams(graphId, nodeId, {}, {
          branchId: activeBranchId,
          policy: { recompute: isCurrentlyLocked ? "auto" : "manual" },
        });
        setApiNodes((prev) => prev.map((x) => (x.id === updated.id ? updated : x)));
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    },
    [graphId, activeBranchId]
  );

  const ctxValue = useMemo(
    () => ({ openInspector, openNodeViewer, queueNodeRun, renameNode, toggleLock, openAoiEditor: onOpenAoiEditor }),
    [openInspector, openNodeViewer, queueNodeRun, renameNode, toggleLock, onOpenAoiEditor]
  );

  const load = useCallback(async () => {
    setError(null);
    try {
      const data = await fetchGraph(graphId);
      const crs = data.project_crs;
      const pe =
        crs &&
        typeof crs === "object" &&
        typeof crs.epsg === "number" &&
        Number.isFinite(crs.epsg)
          ? crs.epsg
          : 4326;
      setWorkspaceEpsg(pe);
      setApiNodes(data.nodes);
      const { n, e } = toFlowElements(graphId, data.nodes, data.edges, artifacts);
      setNodes(n);
      setEdges(e);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, [artifacts, graphId, setEdges, setNodes]);

  useEffect(() => {
    if (ctxMenu.kind === "none") return;
    const closeOnOutsidePointer = (ev: PointerEvent) => {
      const root = ctxMenuRef.current;
      const t = ev.target as Node | null;
      if (!root || !t) return;
      if (root.contains(t)) return;
      setCtxMenu({ kind: "none" });
    };
    window.addEventListener("pointerdown", closeOnOutsidePointer, true);
    return () => {
      window.removeEventListener("pointerdown", closeOnOutsidePointer, true);
    };
  }, [ctxMenu]);

  useEffect(() => {
    if (ctxMenu.kind === "none") {
      setMenuFilter("");
      setHoveredGroupKey(null);
      setHoveredSubGroupKey(null);
    } else if (ctxMenu.kind === "pane") {
      window.setTimeout(() => menuFilterRef.current?.focus(), 30);
    }
  }, [ctxMenu.kind]);

  useEffect(() => {
    const onResize = () =>
      setViewport({
        w: window.innerWidth,
        h: window.innerHeight,
      });
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  const persistDeleteNodes = useCallback(
    async (ids: string[]) => {
      try {
        for (const nid of ids) {
          await deleteGraphNode(graphId, nid, { branchId: activeBranchId });
          removeNodePosition(graphId, nid);
        }
        setSelectedId((s) => (s && ids.includes(s) ? null : s));
        await load();
        onGraphChanged?.();
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
        await load();
      }
    },
    [activeBranchId, graphId, load, onGraphChanged]
  );

  const persistDeleteEdgeIds = useCallback(
    async (edgeIds: string[]) => {
      try {
        for (const edgeId of edgeIds) {
          if (edgeId.startsWith("legacy-")) continue;
          await deleteGraphEdge(graphId, edgeId, { branchId: activeBranchId });
        }
        await load();
        onGraphChanged?.();
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
        await load();
      }
    },
    [activeBranchId, graphId, load, onGraphChanged]
  );

  const onConnect = useCallback(
    async (c: Connection) => {
      if (!c.source || !c.target || c.source === c.target) return;
      const from_port = c.sourceHandle ?? "out";
      let to_port = c.targetHandle ?? "in";
      const sourceNode = apiNodes.find((n) => n.id === c.source);
      const sourceKind = sourceNode?.config.kind ?? "";
      const targetNode = apiNodes.find((n) => n.id === c.target);
      const targetKind = targetNode?.config.kind ?? "";
      // Auto-route: if the target port belongs to a dynamic group, find the first free slot
      const dynGroups = DYNAMIC_PORT_GROUPS[targetKind] ?? [];
      const activeGroup = dynGroups.find(
        (g) => g.direction === "in" && g.slotIndex(to_port) !== null
      );
      if (activeGroup) {
        const used = new Set<number>();
        for (const e of edges) {
          if (e.target !== c.target) continue;
          const idx = activeGroup.slotIndex(String(e.targetHandle ?? ""));
          if (idx !== null) used.add(idx);
        }
        let freeIdx = 0;
        while (used.has(freeIdx)) freeIdx += 1;
        to_port = activeGroup.slotId(freeIdx);
      }
      const semantic_type = portSemantic(sourceKind, "out", from_port) ?? "table";
      try {
        await createGraphEdge(graphId, {
          from_node: c.source,
          to_node: c.target,
          from_port,
          to_port,
          semantic_type,
          branch_id: activeBranchId ?? null,
        });
        await load();
        onGraphChanged?.();
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    },
    [activeBranchId, apiNodes, edges, graphId, load, onGraphChanged]
  );

  const addNodeFromPreset = useCallback(
    async (preset: AddNodePreset, flowPos: { x: number; y: number }) => {
      setCtxMenu({ kind: "none" });
      try {
        const { id } = await addGraphNode(graphId, {
          category: preset.category,
          kind: preset.kind,
          params: {},
          policy: preset.policy,
          branch_id: activeBranchId ?? null,
        });
        saveNodePosition(graphId, id, flowPos);
        await load();
        onGraphChanged?.();
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    },
    [activeBranchId, graphId, load, onGraphChanged]
  );

  const focusNode = useCallback(
    (nodeId: string) => {
      setSelectedId(nodeId);
      setInspectorTab("summary");
      try {
        rfRef.current?.fitView({
          padding: 0.2,
          nodes: [{ id: nodeId }],
          duration: 180,
        });
      } catch {
        // ignore focus animation failures
      }
    },
    []
  );

  useEffect(() => {
    void load();
  }, [load, refreshToken]);

  useEffect(() => {
    if (selectedId && !apiNodes.some((n) => n.id === selectedId)) {
      setSelectedId(null);
    }
  }, [apiNodes, selectedId]);

  const onNodeDragStop = useCallback(
    (_: unknown, node: Node) => {
      saveNodePosition(graphId, node.id, node.position);
    },
    [graphId]
  );

  const selectedNode = useMemo(
    () => apiNodes.find((n) => n.id === selectedId) ?? null,
    [apiNodes, selectedId]
  );
  const addNodePresets = getAddNodePresets();
  const groupedAddNodePresets = useMemo(() => {
    const byGroup = new Map<string, Map<string, AddNodePreset[]>>();
    for (const p of addNodePresets) {
      const group = p.frameworkGroup || "other";
      const submenu = p.submenu || "general";
      if (!byGroup.has(group)) byGroup.set(group, new Map());
      const bySub = byGroup.get(group)!;
      if (!bySub.has(submenu)) bySub.set(submenu, []);
      bySub.get(submenu)!.push(p);
    }
    return Array.from(byGroup.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([group, subMap]) => ({
        group,
        submenus: Array.from(subMap.entries())
          .sort((a, b) => a[0].localeCompare(b[0]))
          .map(([submenu, items]) => ({
            submenu,
            items: items.slice().sort((a, b) => a.label.localeCompare(b.label)),
          })),
      }));
  }, [addNodePresets]);
  const classFlyoutMenus = useMemo(() => {
    return groupedAddNodePresets.map((g) => {
      const directItems: AddNodePreset[] = [];
      const groupedItems: Array<{
        key: string;
        label: string;
        items: AddNodePreset[];
      }> = [];
      for (const s of g.submenus) {
        if (s.items.length <= 1) {
          const only = s.items[0];
          if (only) directItems.push(only);
          continue;
        }
        groupedItems.push({
          key: s.submenu,
          label: s.submenu.replace(/_/g, " "),
          items: s.items,
        });
      }
      directItems.sort((a, b) => a.label.localeCompare(b.label));
      groupedItems.sort((a, b) => a.label.localeCompare(b.label));
      return {
        key: g.group,
        label: g.group.replace(/_/g, " "),
        directItems,
        groupedItems,
      };
    });
  }, [groupedAddNodePresets]);
  // Single-column add-node menu: position + filtered items
  const paneMenuLayout = useMemo(() => {
    const w = 240;
    const maxH = Math.max(220, Math.min(Math.round(viewport.h * 0.65), viewport.h - 32));
    const pad = 12;
    const left = ctxMenu.kind === "none" ? 0
      : Math.max(pad, Math.min(ctxMenu.x, viewport.w - w - pad));
    const top = ctxMenu.kind === "none" ? 0
      : Math.max(pad, Math.min(ctxMenu.y, viewport.h - maxH - pad));
    return { w, maxH, left, top };
  }, [ctxMenu, viewport.h, viewport.w]);

  const nodeEdgeMenuLayout = useMemo(() => {
    const w = 176;
    // node/edge menus are short — estimate ~200px max content, keep it near the click
    const estimatedH = ctxMenu.kind === "edge" ? 60 : 220;
    const pad = 8;
    const left = ctxMenu.kind === "none" ? 0
      : Math.max(pad, Math.min(ctxMenu.x, viewport.w - w - pad));
    const top = ctxMenu.kind === "none" ? 0
      : Math.max(pad, Math.min(ctxMenu.y, viewport.h - estimatedH - pad));
    return { w, maxH: 320, left, top };
  }, [ctxMenu, viewport.h, viewport.w]);

  const filteredAddNodeItems = useMemo(() => {
    const q = menuFilter.trim().toLowerCase();
    if (!q) return classFlyoutMenus; // no filter: return all grouped
    // Filter: merge all items into flat list, match on label + group
    return classFlyoutMenus.map((c) => {
      const allItems = [
        ...c.directItems,
        ...c.groupedItems.flatMap((g) => g.items),
      ].filter(
        (p) =>
          p.label.toLowerCase().includes(q) ||
          p.kind.toLowerCase().includes(q) ||
          c.label.toLowerCase().includes(q)
      );
      return { ...c, directItems: allItems, groupedItems: [] };
    }).filter((c) => c.directItems.length > 0);
  }, [classFlyoutMenus, menuFilter]);

  const artifactsForSelected = useMemo(
    () =>
      selectedNode
        ? artifacts.filter((a) => a.node_id === selectedNode.id)
        : [],
    [artifacts, selectedNode]
  );

  const onNodeUpdated = useCallback((n: ApiNode) => {
    setApiNodes((prev) => prev.map((x) => (x.id === n.id ? n : x)));
    void load();
  }, [load]);

  const proOptions = useMemo(() => ({ hideAttribution: true }), []);

  if (error) {
    return (
      <div style={{ padding: 16, color: "#f85149" }}>
        Could not load graph: {error}
      </div>
    );
  }

  return (
    <GraphInspectorContext.Provider value={ctxValue}>
      <div style={{ display: "flex", height: "100%", minHeight: 0, minWidth: 0 }}>
        <div style={{ flex: 1, minWidth: 0, position: "relative" }}>
          <ReactFlow
            onInit={(inst) => {
              rfRef.current = inst;
            }}
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            nodeTypes={nodeTypes}
            colorMode="dark"
            nodesDraggable
            nodesConnectable
            edgesReconnectable={false}
            elementsSelectable
            selectNodesOnDrag={false}
            panOnScroll
            zoomOnScroll
            zoomOnPinch
            onConnect={(c) => void onConnect(c)}
            onNodesDelete={(arr) => void persistDeleteNodes(arr.map((n) => n.id))}
            onEdgesDelete={(arr) =>
              void persistDeleteEdgeIds(arr.map((e) => String(e.id)))
            }
            onPaneContextMenu={(e) => {
              e.preventDefault();
              const p =
                rfRef.current?.screenToFlowPosition({
                  x: e.clientX,
                  y: e.clientY,
                }) ?? { x: 0, y: 0 };
              setCtxMenu({
                kind: "pane",
                x: e.clientX,
                y: e.clientY,
                flowX: p.x,
                flowY: p.y,
              });
            }}
            onNodeContextMenu={(e, node) => {
              e.preventDefault();
              setCtxMenu({
                kind: "node",
                x: e.clientX,
                y: e.clientY,
                nodeId: node.id,
              });
            }}
            onEdgeContextMenu={(e, edge) => {
              e.preventDefault();
              setCtxMenu({
                kind: "edge",
                x: e.clientX,
                y: e.clientY,
                edgeId: String(edge.id),
              });
            }}
            onNodeDragStop={onNodeDragStop}
            onNodeClick={(_, n) => {
              setSelectedId(n.id);
              setInspectorTab("summary");
            }}
            onPaneClick={() => setSelectedId(null)}
            proOptions={proOptions}
          >
            <Background color="#30363d" gap={20} size={1} />
            <Controls />
            <MiniMap
              nodeStrokeWidth={3}
              maskColor="rgb(15, 20, 25, 0.85)"
              style={{ background: "#161b22" }}
            />
          </ReactFlow>
          {ctxMenu.kind !== "none" && (() => {
            const activeLayout = ctxMenu.kind === "pane" ? paneMenuLayout : nodeEdgeMenuLayout;
            return createPortal(
              <div
                ref={ctxMenuRef}
                style={{ position: "fixed", inset: 0, zIndex: 10000, pointerEvents: "none" }}
                onContextMenu={(e) => e.preventDefault()}
              >
                <div
                  role="menu"
                  style={{
                    position: "fixed",
                    left: activeLayout.left,
                    top: activeLayout.top,
                    zIndex: 10000,
                    background: "#161b22",
                    border: "1px solid #30363d",
                    borderRadius: 8,
                    padding: 5,
                    width: activeLayout.w,
                    maxHeight: activeLayout.maxH,
                    display: "flex",
                    flexDirection: "column",
                    overflow: "hidden",
                    boxShadow: "0 8px 24px rgba(0,0,0,0.45)",
                    pointerEvents: "auto",
                  }}
                  onClick={(e) => e.stopPropagation()}
                >
                  {ctxMenu.kind === "pane" ? (
                    <>
                      {/* Search filter */}
                      <div style={{ padding: "4px 4px 3px", flexShrink: 0 }}>
                        <input
                          ref={menuFilterRef}
                          type="text"
                          value={menuFilter}
                          onChange={(e) => {
                            setMenuFilter(e.target.value);
                            setHoveredGroupKey(null);
                            setHoveredSubGroupKey(null);
                          }}
                          placeholder="Search nodes…"
                          style={{
                            width: "100%",
                            background: "#0d1117",
                            border: "1px solid #30363d",
                            borderRadius: 5,
                            color: "#e6edf3",
                            fontSize: 11,
                            padding: "5px 8px",
                            outline: "none",
                            fontFamily: "inherit",
                            boxSizing: "border-box",
                          }}
                          onKeyDown={(e) => {
                            if (e.key === "Escape") setCtxMenu({ kind: "none" });
                          }}
                        />
                      </div>
                      {/* Menu body — cascading when no filter, flat when searching */}
                      <div style={{ overflowY: "auto", flex: 1, paddingTop: 2 }}>
                        {menuFilter.trim() ? (
                          /* ── Flat search results ── */
                          filteredAddNodeItems.length === 0 ? (
                            <div style={{ fontSize: 11, color: "#484f58", padding: "6px 10px" }}>
                              No nodes match
                            </div>
                          ) : (
                            filteredAddNodeItems.map((c) => (
                              <div key={c.key}>
                                <div style={{
                                  fontSize: 9.5, fontWeight: 700, letterSpacing: "0.07em",
                                  textTransform: "uppercase", color: "#484f58",
                                  padding: "6px 8px 2px", userSelect: "none",
                                }}>
                                  {c.label}
                                </div>
                                {c.directItems.map((p) => (
                                  <button key={p.kind} type="button"
                                    onClick={() => void addNodeFromPreset(p, { x: ctxMenu.flowX, y: ctxMenu.flowY })}
                                    style={{ ...menuBtn, marginBottom: 1 }}
                                    title={`${p.frameworkGroup}/${p.submenu}`}
                                  >
                                    {p.label}
                                  </button>
                                ))}
                              </div>
                            ))
                          )
                        ) : (
                          /* ── Cascading L1 group buttons ── */
                          classFlyoutMenus.map((c) => (
                            <button
                              key={c.key}
                              type="button"
                              onMouseEnter={(e) => {
                                // Store Y position synchronously in ref — no timing issue
                                hoveredGroupTop.current = e.currentTarget.getBoundingClientRect().top;
                                setHoveredGroupKey(c.key);
                                setHoveredSubGroupKey(null);
                              }}
                              style={{
                                ...menuBtn,
                                marginBottom: 1,
                                display: "flex",
                                justifyContent: "space-between",
                                alignItems: "center",
                                background: hoveredGroupKey === c.key ? "#1c2638" : menuBtn.background,
                                border: `1px solid ${hoveredGroupKey === c.key ? "#3b82f6" : "transparent"}`,
                              }}
                            >
                              <span style={{ textTransform: "capitalize" }}>{c.label}</span>
                              <span style={{ opacity: 0.55, fontSize: 10 }}>›</span>
                            </button>
                          ))
                        )}
                      </div>
                    </>
                  ) : ctxMenu.kind === "node" ? (
                    <>
                      {/* Node header label */}
                      <div style={{ fontSize: 10, color: "#6e7681", padding: "2px 8px 4px", userSelect: "none" }}>
                        {apiNodes.find((n) => n.id === ctxMenu.nodeId)?.config.kind.replace(/_/g, " ") ?? ctxMenu.nodeId.slice(0, 8)}
                      </div>
                      <button
                        type="button"
                        onClick={() => {
                          const id = ctxMenu.nodeId;
                          setCtxMenu({ kind: "none" });
                          focusNode(id);
                        }}
                        style={menuBtn}
                      >
                        Focus in canvas
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          const id = ctxMenu.nodeId;
                          setCtxMenu({ kind: "none" });
                          openInspector(id, "summary");
                        }}
                        style={menuBtn}
                      >
                        Open inspector
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          const id = ctxMenu.nodeId;
                          setCtxMenu({ kind: "none" });
                          openNodeViewer(id);
                        }}
                        style={menuBtn}
                      >
                        Preview output
                      </button>
                      <button
                        type="button"
                        disabled={apiNodes.find((n) => n.id === ctxMenu.nodeId)?.execution === "running"}
                        onClick={() => {
                          const id = ctxMenu.nodeId;
                          setCtxMenu({ kind: "none" });
                          void queueNodeRun(id, { includeManual: true });
                        }}
                        style={{ ...menuBtn, color: "#58a6ff" }}
                      >
                        Run from here
                      </button>
                      <div style={{ margin: "4px 4px", borderTop: "1px solid #30363d" }} />
                      <button
                        type="button"
                        onClick={() => {
                          setCtxMenu({ kind: "none" });
                          void persistDeleteNodes([ctxMenu.nodeId]);
                        }}
                        style={{ ...menuBtn, color: "#f85149", marginBottom: 0 }}
                      >
                        Delete node
                      </button>
                    </>
                  ) : (
                    <button
                      type="button"
                      onClick={() => {
                        const id = ctxMenu.edgeId;
                        setCtxMenu({ kind: "none" });
                        void persistDeleteEdgeIds([id]);
                      }}
                      style={{ ...menuBtn, color: "#f85149" }}
                    >
                      Delete connection
                    </button>
                  )}
                </div>

                {/* ── L2 submenu: items for the hovered group ── */}
                {ctxMenu.kind === "pane" && !menuFilter.trim() && hoveredGroupKey && (() => {
                  const grp = classFlyoutMenus.find((c) => c.key === hoveredGroupKey);
                  if (!grp) return null;
                  const subW = 200;
                  const subPad = 4;
                  const rawLeft = paneMenuLayout.left + paneMenuLayout.w + subPad;
                  const left = Math.min(rawLeft, viewport.w - subW - 8);
                  const estH = Math.min(
                    (grp.directItems.length + grp.groupedItems.length) * 28 + 16,
                    viewport.h * 0.6
                  );
                  const top = Math.max(8, Math.min(hoveredGroupTop.current - 4, viewport.h - estH - 8));
                  return (
                    <div
                      role="menu"
                      style={{
                        position: "fixed", left, top, zIndex: 10001,
                        background: "#161b22", border: "1px solid #30363d",
                        borderRadius: 8, padding: 5, width: subW,
                        maxHeight: Math.round(viewport.h * 0.6),
                        overflowY: "auto",
                        boxShadow: "0 8px 32px rgba(0,0,0,0.5)",
                        pointerEvents: "auto",
                      }}
                      onClick={(e) => e.stopPropagation()}
                      onMouseEnter={() => {/* stay open */}}
                    >
                      {grp.directItems.map((p) => (
                        <button
                          key={p.kind}
                          type="button"
                          onClick={() => void addNodeFromPreset(p, { x: ctxMenu.flowX, y: ctxMenu.flowY })}
                          style={{ ...menuBtn, marginBottom: 1 }}
                          title={`${p.submenu} (${p.pluginSource})`}
                        >
                          {p.label}
                        </button>
                      ))}
                      {grp.groupedItems.map((g) => (
                        <button
                          key={g.key}
                          type="button"
                          onMouseEnter={(e) => {
                            hoveredSubGroupTop.current = e.currentTarget.getBoundingClientRect().top;
                            setHoveredSubGroupKey(`${hoveredGroupKey}:${g.key}`);
                          }}
                          style={{
                            ...menuBtn,
                            marginBottom: 1,
                            display: "flex",
                            justifyContent: "space-between",
                            alignItems: "center",
                            background: hoveredSubGroupKey === `${hoveredGroupKey}:${g.key}` ? "#1c2638" : menuBtn.background,
                            border: `1px solid ${hoveredSubGroupKey === `${hoveredGroupKey}:${g.key}` ? "#3b82f6" : "transparent"}`,
                          }}
                        >
                          <span>{g.label}</span>
                          <span style={{ opacity: 0.55, fontSize: 10 }}>›</span>
                        </button>
                      ))}
                    </div>
                  );
                })()}

                {/* ── L3 submenu: items for the hovered sub-group ── */}
                {ctxMenu.kind === "pane" && !menuFilter.trim() && hoveredGroupKey && hoveredSubGroupKey && (() => {
                  const grp = classFlyoutMenus.find((c) => c.key === hoveredGroupKey);
                  if (!grp) return null;
                  const subGroupKey = hoveredSubGroupKey.replace(`${hoveredGroupKey}:`, "");
                  const sub = grp.groupedItems.find((g) => g.key === subGroupKey);
                  if (!sub) return null;
                  const l2W = 200;
                  const subW = 190;
                  const subPad = 4;
                  const rawLeft = paneMenuLayout.left + paneMenuLayout.w + subPad + l2W + subPad;
                  const left = Math.min(rawLeft, viewport.w - subW - 8);
                  const estH = Math.min(sub.items.length * 28 + 12, viewport.h * 0.55);
                  const top = Math.max(8, Math.min(hoveredSubGroupTop.current - 4, viewport.h - estH - 8));
                  return (
                    <div
                      role="menu"
                      style={{
                        position: "fixed", left, top, zIndex: 10002,
                        background: "#161b22", border: "1px solid #30363d",
                        borderRadius: 8, padding: 5, width: subW,
                        maxHeight: Math.round(viewport.h * 0.55),
                        overflowY: "auto",
                        boxShadow: "0 8px 32px rgba(0,0,0,0.5)",
                        pointerEvents: "auto",
                      }}
                      onClick={(e) => e.stopPropagation()}
                    >
                      {sub.items.map((p) => (
                        <button
                          key={p.kind}
                          type="button"
                          onClick={() => void addNodeFromPreset(p, { x: ctxMenu.flowX, y: ctxMenu.flowY })}
                          style={{ ...menuBtn, marginBottom: 1 }}
                          title={`${p.frameworkGroup}/${p.submenu} (${p.pluginSource})`}
                        >
                          {p.label}
                        </button>
                      ))}
                    </div>
                  );
                })()}

              </div>,
              document.body
            );
          })()}
        </div>
        {selectedNode && (
          <NodeInspector
            graphId={graphId}
            activeBranchId={activeBranchId}
            node={selectedNode}
            nodeSpec={nodeSpec(selectedNode.config.kind)}
            projectEpsg={workspaceEpsg}
            workspaceUsedEpsgs={workspaceUsedEpsgs}
            tab={inspectorTab}
            onTab={setInspectorTab}
            onClose={() => setSelectedId(null)}
            onOpenEditor={() => onOpenNodeEditor?.(selectedNode.id)}
            onNodeUpdated={onNodeUpdated}
            nodeArtifacts={artifactsForSelected}
            onPipelineQueued={onPipelineQueued}
          />
        )}
      </div>
    </GraphInspectorContext.Provider>
  );
}

export function GraphCanvas({
  graphId,
  activeBranchId,
  refreshToken = 0,
  projectEpsg = 4326,
  workspaceUsedEpsgs = [],
  artifacts = [],
  onPipelineQueued,
  onOpenNodeViewer,
  onOpenNodeEditor,
  onOpenAoiEditor,
  onGraphChanged,
}: Props) {
  if (!graphId) {
    return (
      <div
        style={{
          height: "100%",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: "#8b949e",
          fontSize: 14,
          textAlign: "center",
          padding: 24,
          lineHeight: 1.6,
        }}
      >
        Use <strong style={{ color: "#e6edf3" }}>Seed demo graph</strong> or{" "}
        <strong style={{ color: "#e6edf3" }}>New project</strong> + right-click the canvas to add
        nodes. Drag between ports to connect; Delete removes the selection. Right-click an edge to
        remove it.
        <br />
        Positions persist locally. Acquisition nodes support <strong style={{ color: "#e6edf3" }}>Map CSV</strong>.
      </div>
    );
  }

  return (
    <ReactFlowProvider>
      <FlowWorkspace
        graphId={graphId}
        activeBranchId={activeBranchId}
        refreshToken={refreshToken}
        projectEpsg={projectEpsg}
        workspaceUsedEpsgs={workspaceUsedEpsgs}
        artifacts={artifacts}
        onPipelineQueued={onPipelineQueued}
        onOpenNodeViewer={onOpenNodeViewer}
        onOpenNodeEditor={onOpenNodeEditor}
        onOpenAoiEditor={onOpenAoiEditor}
        onGraphChanged={onGraphChanged}
      />
    </ReactFlowProvider>
  );
}
