import {
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
} from "react";
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
import { incomingPortIds, outgoingPortIds } from "./nodePortLayout";
import { isAcquisitionCsvKind } from "./pipelineSchema";
import { nodeRole, portSemantic } from "./nodeRegistry";

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
    (kind === "plan_view_2d" || kind === "plan_view_3d" || kind === "cesium_display_node") &&
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

function PipelineNode({ data }: NodeProps<PipelineData>) {
  const id = useNodeId();
  const ctx = useContextOptional();
  const nIn = data.incomingPorts.length;
  const nOut = data.outgoingPorts.length;
  const b = borderStyleForExec(data.nodeState, data.categoryAccent);

  return (
    <div
      className={data.isRunning ? "mineeye-node-running" : undefined}
      style={{
        background: "#21262d",
        color: "#e6edf3",
        border: `${b.width}px ${b.style} ${b.color}`,
        borderRadius: 10,
        padding: "10px 12px 10px 10px",
        fontSize: 12,
        minWidth: 188,
        maxWidth: 280,
      }}
    >
      {data.incomingPorts.map((port, i) => {
        const pct = ((i + 1) / (nIn + 1)) * 100;
        return (
          <Handle
            key={`in-${port}`}
            type="target"
            position={Position.Left}
            id={port}
            isConnectable
            style={{
              top: `${pct}%`,
              background: data.portColorsIn[port] ?? "#484f58",
              border: "1px solid #0f1419",
              width: 10,
              height: 10,
            }}
          />
        );
      })}
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
        <span
          style={{
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: "0.06em",
            color: "#0f1419",
            background: data.categoryAccent,
            padding: "2px 6px",
            borderRadius: 4,
            textTransform: "uppercase",
          }}
        >
          {data.category}
        </span>
        {data.isRunning && <span style={{ fontSize: 10, color: "#58a6ff" }}>Running…</span>}
        {data.nodeState === "error" && (
          <span style={{ fontSize: 10, color: "#f85149", fontWeight: 600 }}>Error</span>
        )}
        {data.nodeState === "stale" && !data.isRunning && (
          <span style={{ fontSize: 10, color: "#d29922" }}>Stale</span>
        )}
        {data.nodeState === "current" && !data.isRunning && (
          <span style={{ fontSize: 10, color: "#3fb950" }}>Current</span>
        )}
        {data.nodeState === "locked" && !data.isRunning && (
          <span style={{ fontSize: 10, color: "#a371f7" }}>Locked</span>
        )}
        {data.nodeState === "unset" && !data.isRunning && (
          <span style={{ fontSize: 10, color: "#8b949e" }}>Unset</span>
        )}
      </div>
      <div style={{ fontWeight: 600, fontSize: 13, lineHeight: 1.3 }}>{data.title}</div>
      <div style={{ fontSize: 11, opacity: 0.82, marginTop: 5, lineHeight: 1.35 }}>{data.role}</div>
      {data.nodeState === "error" && data.lastErrorShort && (
        <div
          style={{
            fontSize: 10,
            color: "#f88",
            marginTop: 6,
            lineHeight: 1.35,
          }}
          title={data.lastErrorFull ?? undefined}
        >
          {data.lastErrorShort}
        </div>
      )}
      {((data.showCsv && ctx && id) || (ctx && id) || (data.nodeState === "error" && ctx && id)) && (
        <div style={{ marginTop: 8, display: "flex", gap: 6, flexWrap: "wrap" }}>
          {data.showCsv && ctx && id && (
            <>
              <button
                type="button"
                style={miniBtn}
                onClick={(e) => {
                  e.stopPropagation();
                  ctx.openInspector(id, "mapping");
                }}
              >
                Map CSV…
              </button>
              <button
                type="button"
                style={miniBtn}
                onClick={(e) => {
                  e.stopPropagation();
                  ctx.openInspector(id, "summary");
                }}
              >
                Details
              </button>
            </>
          )}
          {ctx && id && (
            <button
              type="button"
              style={runBtn}
              title={data.isLocked ? "Run this node now (includes manual nodes for this run)." : "Run this node now"}
              disabled={data.isRunning}
              onClick={(e) => {
                e.stopPropagation();
                void ctx.queueNodeRun(id, { includeManual: true });
              }}
            >
              Run
            </button>
          )}
          {ctx && id && (
            <button
              type="button"
              style={mapBtn}
              title="Open node preview"
              onClick={(e) => {
                e.stopPropagation();
                ctx.openNodeViewer(id);
              }}
            >
              Preview
            </button>
          )}
          {data.nodeState === "error" && ctx && id && (
            <button
              type="button"
              style={miniBtn}
              title="Open full error message"
              onClick={(e) => {
                e.stopPropagation();
                ctx.openInspector(id, "diagnostics");
              }}
            >
              Error details…
            </button>
          )}
        </div>
      )}
      {data.feeds && (
        <div
          style={{
            fontSize: 10,
            opacity: 0.65,
            marginTop: 6,
            borderTop: "1px solid #30363d",
            paddingTop: 6,
            lineHeight: 1.3,
          }}
        >
          In: {data.feeds}
        </div>
      )}
      <div style={{ fontSize: 10, opacity: 0.55, marginTop: 6 }}>{data.statusLine}</div>
      {data.hashShort && (
        <div
          style={{
            fontSize: 9,
            opacity: 0.45,
            marginTop: 4,
            fontFamily: "ui-monospace, monospace",
          }}
        >
          {data.hashShort}
        </div>
      )}
      {data.outgoingPorts.map((port, i) => {
        const pct = ((i + 1) / (nOut + 1)) * 100;
        return (
          <Handle
            key={`out-${port}`}
            type="source"
            position={Position.Right}
            id={port}
            isConnectable
            style={{
              top: `${pct}%`,
              background: data.portColorsOut[port] ?? "#484f58",
              border: "1px solid #0f1419",
              width: 10,
              height: 10,
            }}
          />
        );
      })}
    </div>
  );
}

function useContextOptional() {
  return useContext(GraphInspectorContext);
}

const miniBtn: CSSProperties = {
  fontSize: 10,
  padding: "4px 8px",
  borderRadius: 6,
  border: "1px solid #30363d",
  background: "#0f1419",
  color: "#58a6ff",
  cursor: "pointer",
};

const mapBtn: CSSProperties = {
  ...miniBtn,
  color: "#34d399",
  borderColor: "#1f6f55",
};

const runBtn: CSSProperties = {
  ...miniBtn,
  color: "#58a6ff",
  borderColor: "#1f6feb",
};

const menuBtn: CSSProperties = {
  display: "block",
  width: "100%",
  textAlign: "left",
  padding: "8px 10px",
  marginBottom: 4,
  border: "none",
  borderRadius: 6,
  background: "#21262d",
  color: "#e6edf3",
  cursor: "pointer",
  fontSize: 12,
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
    const title = from?.config.kind.replace(/_/g, " ") ?? e.from_node.slice(0, 8);
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
    const title = kind.replace(/_/g, " ");
    const role = nodeRole(kind) ?? `Node · ${kind}`;
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
    return {
      id: node.id,
      type: "pipeline",
      position: p,
      data: {
        kind,
        title,
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
  artifacts?: ArtifactEntry[];
  onPipelineQueued?: () => void;
  onOpenNodeViewer?: (nodeId: string) => void;
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
  onPipelineQueued,
  onOpenNodeViewer,
  onGraphChanged,
}: {
  graphId: string;
  activeBranchId?: string | null;
  refreshToken: number;
  projectEpsg: number;
  artifacts: ArtifactEntry[];
  onPipelineQueued?: () => void;
  onOpenNodeViewer?: (nodeId: string) => void;
  onGraphChanged?: () => void;
}) {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<PipelineData>>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [apiNodes, setApiNodes] = useState<ApiNode[]>([]);
  const [workspaceEpsg, setWorkspaceEpsg] = useState(projectEpsg);
  const [ctxMenu, setCtxMenu] = useState<CtxMenu>({ kind: "none" });
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

  const ctxValue = useMemo(
    () => ({ openInspector, openNodeViewer, queueNodeRun }),
    [openInspector, openNodeViewer, queueNodeRun]
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
    const close = () => setCtxMenu({ kind: "none" });
    const tid = window.setTimeout(() => {
      window.addEventListener("click", close);
    }, 0);
    return () => {
      window.clearTimeout(tid);
      window.removeEventListener("click", close);
    };
  }, [ctxMenu]);

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
      const to_port = c.targetHandle ?? "in";
      const sourceNode = apiNodes.find((n) => n.id === c.source);
      const sourceKind = sourceNode?.config.kind ?? "";
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
    [activeBranchId, apiNodes, graphId, load, onGraphChanged]
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
          {ctxMenu.kind !== "none" && (
            <div
              role="menu"
              style={{
                position: "fixed",
                left: Math.min(ctxMenu.x, window.innerWidth - 224),
                top: Math.min(ctxMenu.y, window.innerHeight - 160),
                zIndex: 10000,
                background: "#161b22",
                border: "1px solid #30363d",
                borderRadius: 8,
                padding: 8,
                minWidth: 200,
                maxWidth: 280,
                boxShadow: "0 8px 24px rgba(0,0,0,0.45)",
              }}
              onClick={(e) => e.stopPropagation()}
              onContextMenu={(e) => e.preventDefault()}
            >
              {ctxMenu.kind === "pane" && (
                <>
                  <div
                    style={{
                      fontSize: 11,
                      fontWeight: 600,
                      marginBottom: 6,
                      color: "#e6edf3",
                    }}
                  >
                    Add node
                  </div>
                  <div style={{ maxHeight: 280, overflowY: "auto" }}>
                    {addNodePresets.map((p) => (
                      <button
                        key={p.kind}
                        type="button"
                        onClick={() =>
                          void addNodeFromPreset(p, {
                            x: ctxMenu.flowX,
                            y: ctxMenu.flowY,
                          })
                        }
                        style={menuBtn}
                      >
                        {p.label}
                      </button>
                    ))}
                  </div>
                </>
              )}
              {ctxMenu.kind === "node" && (
                <button
                  type="button"
                  onClick={() => {
                    setCtxMenu({ kind: "none" });
                    void persistDeleteNodes([ctxMenu.nodeId]);
                  }}
                  style={{ ...menuBtn, color: "#f85149" }}
                >
                  Delete node
                </button>
              )}
              {ctxMenu.kind === "edge" && (
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
          )}
        </div>
        {selectedNode && (
          <NodeInspector
            graphId={graphId}
            activeBranchId={activeBranchId}
            node={selectedNode}
            projectEpsg={workspaceEpsg}
            tab={inspectorTab}
            onTab={setInspectorTab}
            onClose={() => setSelectedId(null)}
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
  artifacts = [],
  onPipelineQueued,
  onOpenNodeViewer,
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
        artifacts={artifacts}
        onPipelineQueued={onPipelineQueued}
        onOpenNodeViewer={onOpenNodeViewer}
        onGraphChanged={onGraphChanged}
      />
    </ReactFlowProvider>
  );
}
