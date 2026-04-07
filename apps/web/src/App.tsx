import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AoiBboxEditor } from "./AoiBboxEditor";
import { extractBboxAndEpsg, toWgs84Bbox, type AoiBbox } from "./aoiBounds";
import { GraphCanvas } from "./GraphCanvas";
import { GraphErrorBoundary } from "./GraphErrorBoundary";
import {
  api,
  checkoutGraphBranch,
  commitCurrentToBranch,
  createGraph,
  createGraphBranch,
  createWorkspace,
  diffGraphRevisions,
  executeGraphPromotion,
  fetchGraph,
  listGraphBranches,
  listGraphPromotions,
  listGraphRevisions,
  patchNodeParams,
  runGraph,
  updateWorkspaceProjectCrs,
  type ApiBranch,
  type ApiEdge,
  type ApiNode,
  type ApiPromotion,
  type ApiRevision,
  type ApiRevisionDiff,
  type ArtifactEntry,
} from "./graphApi";
import { LeftSidebar } from "./LeftSidebar";
import { Map2DPanel } from "./Map2DPanel";
import { Map3DPanel } from "./Map3DPanel";
import { Map3DThreePanel } from "./Map3DThreePanel";
import { NodeInspector } from "./NodeInspector";
import { NodePreviewPanel } from "./NodePreviewPanel";
import { loadNodeRegistryFromApi, nodeSpec } from "./nodeRegistry";
import type { InspectorTab } from "./graphInspectorContext";
import {
  deleteProject,
  getActiveProjectId,
  loadProjects,
  setActiveProjectId,
  upsertProject,
  type StoredProject,
} from "./projectStorage";

type SeedResponse = {
  workspace_id: string;
  graph_id: string;
  nodes: Record<string, string>;
};

type MainTab = "workspace" | `node:${string}` | `edit:${string}` | `aoi:${string}`;

function collectWorkspaceUsedEpsg(nodes: ApiNode[]): number[] {
  const out = new Set<number>();
  const scan = (v: unknown, keyHint = "") => {
    if (typeof v === "number" && Number.isFinite(v)) {
      const n = Math.trunc(v);
      if (n > 0 && (keyHint.toLowerCase().includes("epsg") || n === 4326)) out.add(n);
      return;
    }
    if (typeof v === "string") {
      const s = v.trim();
      if (/^\d+$/.test(s) && keyHint.toLowerCase().includes("epsg")) {
        const n = parseInt(s, 10);
        if (n > 0) out.add(n);
      }
      return;
    }
    if (Array.isArray(v)) {
      for (const it of v) scan(it, keyHint);
      return;
    }
    if (v && typeof v === "object") {
      for (const [k, val] of Object.entries(v as Record<string, unknown>)) scan(val, k);
    }
  };
  for (const node of nodes) {
    scan(node.config.params);
  }
  return [...out.values()].sort((a, b) => a - b);
}

export default function App() {
  const [projects, setProjects] = useState<StoredProject[]>(loadProjects);
  const [activeProjectId, setActiveProjectIdState] = useState<string | null>(null);
  const [graphId, setGraphId] = useState<string | null>(null);
  const [artifacts, setArtifacts] = useState<ArtifactEntry[]>([]);
  const [status, setStatus] = useState<string>("");
  const [mainTab, setMainTab] = useState<MainTab>("workspace");
  const [graphRefreshToken, setGraphRefreshToken] = useState(0);
  const [runBusy, setRunBusy] = useState(false);
  const [graphEdges, setGraphEdges] = useState<ApiEdge[]>([]);
  const [graphNodes, setGraphNodes] = useState<ApiNode[]>([]);
  const [workspaceId, setWorkspaceId] = useState<string | null>(null);
  const [projectEpsg, setProjectEpsg] = useState<number>(4326);
  const [pendingProjectEpsg, setPendingProjectEpsg] = useState<number | null>(null);
  const [openViewerNodeIds, setOpenViewerNodeIds] = useState<string[]>([]);
  const [openEditorNodeIds, setOpenEditorNodeIds] = useState<string[]>([]);
  const [openAoiEditorNodeIds, setOpenAoiEditorNodeIds] = useState<string[]>([]);
  const [editorTabs, setEditorTabs] = useState<Record<string, InspectorTab>>({});
  const [branches, setBranches] = useState<ApiBranch[]>([]);
  const [revisions, setRevisions] = useState<ApiRevision[]>([]);
  const [promotions, setPromotions] = useState<ApiPromotion[]>([]);
  const [activeBranchId, setActiveBranchId] = useState<string | null>(null);
  const [revisionDiff, setRevisionDiff] = useState<ApiRevisionDiff | null>(null);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const lastCheckoutRef = useRef<string | null>(null);
  const workspaceUsedEpsgs = useMemo(
    () => collectWorkspaceUsedEpsg(graphNodes),
    [graphNodes]
  );

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const el = e.target as HTMLElement | null;
      const tag = (el?.tagName ?? "").toLowerCase();
      const typing =
        tag === "input" ||
        tag === "textarea" ||
        tag === "select" ||
        Boolean(el?.isContentEditable);
      if (typing) return;
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "b") {
        e.preventDefault();
        setSidebarCollapsed((v) => !v);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  useEffect(() => {
    const all = loadProjects();
    setProjects(all);
    const saved = getActiveProjectId();
    const pick =
      saved && all.some((p) => p.localId === saved) ? saved : all[0]?.localId ?? null;
    setActiveProjectIdState(pick);
    const g = pick ? all.find((p) => p.localId === pick)?.graphId ?? null : null;
    setGraphId(g);
    if (pick) setActiveProjectId(pick);
    else setActiveProjectId(null);
  }, []);

  useEffect(() => {
    let cancelled = false;
    const loadRegistry = async () => {
      try {
        await loadNodeRegistryFromApi();
        if (!cancelled) {
          setGraphRefreshToken((t) => t + 1);
        }
      } catch (e) {
        // Keep bundled registry fallback if backend registry is unavailable.
        console.warn("loadNodeRegistryFromApi:", e);
      }
    };
    void loadRegistry();
    return () => {
      cancelled = true;
    };
  }, []);

  const refreshArtifacts = useCallback(async (gid: string) => {
    try {
      const r = await fetch(api(`/graphs/${gid}/artifacts`), { cache: "no-store" });
      if (!r.ok) return;
      const data: unknown = await r.json();
      if (!Array.isArray(data)) return;
      const list = data as ArtifactEntry[];
      list.sort((a, b) => a.key.localeCompare(b.key));
      setArtifacts(list);
    } catch (e) {
      console.warn("refreshArtifacts:", e);
    }
  }, []);

  const refreshGraphEdges = useCallback(async (gid: string) => {
    try {
      const g = await fetchGraph(gid);
      setGraphEdges(g.edges);
      setGraphNodes(g.nodes);
      setWorkspaceId(g.workspace_id ?? null);
      const e =
        g.project_crs &&
        typeof g.project_crs === "object" &&
        typeof g.project_crs.epsg === "number" &&
        Number.isFinite(g.project_crs.epsg)
          ? g.project_crs.epsg
          : 4326;
      setProjectEpsg(e);
    } catch (e) {
      console.warn("refreshGraphEdges:", e);
    }
  }, []);

  const refreshBranching = useCallback(async (gid: string) => {
    try {
      const [b, r, p] = await Promise.all([
        listGraphBranches(gid),
        listGraphRevisions(gid),
        listGraphPromotions(gid),
      ]);
      setBranches(b);
      setRevisions(r);
      setPromotions(p);
      setActiveBranchId((prev) => {
        if (prev && b.some((x) => x.id === prev)) return prev;
        return b.find((x) => x.name === "main")?.id ?? b[0]?.id ?? null;
      });
    } catch (e) {
      console.warn("refreshBranching:", e);
    }
  }, []);

  const refreshAll = useCallback(() => {
    if (!graphId) return;
    void refreshArtifacts(graphId);
    void refreshGraphEdges(graphId);
    void refreshBranching(graphId);
    setGraphRefreshToken((t) => t + 1);
  }, [graphId, refreshArtifacts, refreshGraphEdges, refreshBranching]);

  const selectProject = useCallback((p: StoredProject) => {
    setActiveProjectId(p.localId);
    setActiveProjectIdState(p.localId);
    setGraphId(p.graphId);
    setArtifacts([]);
    setGraphEdges([]);
    setGraphNodes([]);
    setWorkspaceId(null);
    setProjectEpsg(4326);
    setPendingProjectEpsg(null);
    setOpenViewerNodeIds([]);
    setOpenEditorNodeIds([]);
    setOpenAoiEditorNodeIds([]);
    setEditorTabs({});
    setRevisionDiff(null);
    setStatus("");
    setActiveBranchId(null);
    setMainTab("workspace");
  }, []);

  const handleDeleteProject = useCallback(
    (localId: string) => {
      deleteProject(localId);
      const remaining = loadProjects();
      setProjects(remaining);
      if (activeProjectId === localId) {
        // Switch to next available project, or clear state
        if (remaining.length > 0) {
          selectProject(remaining[0]);
        } else {
          setActiveProjectIdState(null);
          setGraphId(null);
          setArtifacts([]);
          setGraphEdges([]);
          setGraphNodes([]);
          setWorkspaceId(null);
          setProjectEpsg(4326);
          setOpenViewerNodeIds([]);
          setOpenEditorNodeIds([]);
          setOpenAoiEditorNodeIds([]);
          setActiveBranchId(null);
          setMainTab("workspace");
        }
      }
    },
    [activeProjectId, selectProject]
  );

  const queuePipelineRun = useCallback(async () => {
    if (!graphId) return;
    setRunBusy(true);
    setStatus("Queuing jobs…");
    try {
      const res = await runGraph(graphId);
      const nq = res.queued?.length ?? 0;
      const ns = res.skipped_manual?.length ?? 0;
      if (nq === 0) {
        setStatus(
          ns > 0
            ? `No jobs queued (${ns} manual). Run worker if you already queued work.`
            : "No jobs queued."
        );
      } else {
        setStatus(
          `Queued ${nq} job(s)${ns ? `; ${ns} skipped (manual)` : ""}. Run worker, then refresh.`
        );
      }
      refreshAll();
    } catch (e) {
      setStatus(e instanceof Error ? e.message : String(e));
    } finally {
      setRunBusy(false);
    }
  }, [graphId, refreshAll]);

  const seedDemo = async () => {
    setStatus("Seeding…");
    try {
      const r = await fetch(api("/demo/seed"), { method: "POST" });
      if (!r.ok) {
        setStatus(`Seed failed: ${r.status}`);
        return;
      }
      const data: SeedResponse = await r.json();
      const sp: StoredProject = {
        localId: crypto.randomUUID(),
        name: "Demo pipeline",
        workspaceId: data.workspace_id,
        graphId: data.graph_id,
        createdAt: Date.now(),
      };
      upsertProject(sp);
      setProjects(loadProjects());
      setActiveProjectId(sp.localId);
      setActiveProjectIdState(sp.localId);
      setGraphId(data.graph_id);
      setMainTab("workspace");
      setOpenViewerNodeIds([]);
      setOpenEditorNodeIds([]);
      setOpenAoiEditorNodeIds([]);
      setEditorTabs({});
      setPendingProjectEpsg(null);
      setStatus(
        "Demo graph ready. Queue pipeline run, start worker, then refresh. Project saved in this browser."
      );
      setGraphRefreshToken((t) => t + 1);
      void refreshArtifacts(data.graph_id);
      void refreshGraphEdges(data.graph_id);
      void refreshBranching(data.graph_id);
    } catch (e) {
      setStatus(`Seed failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const newProject = async () => {
    const name = window.prompt("New project name?");
    if (!name?.trim()) return;
    setStatus("Creating project…");
    try {
      const ws = await createWorkspace({
        name: name.trim(),
        owner_user_id: "local",
        project_crs: { epsg: 4326, wkt: null },
      });
      const g = await createGraph(ws.id, {
        name: `${name.trim()} · graph`,
        workspace_id: ws.id,
        owner_user_id: "local",
      });
      const sp: StoredProject = {
        localId: crypto.randomUUID(),
        name: name.trim(),
        workspaceId: ws.id,
        graphId: g.id,
        createdAt: Date.now(),
      };
      upsertProject(sp);
      setProjects(loadProjects());
      setActiveProjectId(sp.localId);
      setActiveProjectIdState(sp.localId);
      setGraphId(g.id);
      setMainTab("workspace");
      setOpenViewerNodeIds([]);
      setOpenEditorNodeIds([]);
      setOpenAoiEditorNodeIds([]);
      setEditorTabs({});
      setPendingProjectEpsg(null);
      setStatus(
        "Empty graph created. Add nodes via API or seed a demo in another project. This browser remembers projects locally."
      );
      setGraphRefreshToken((t) => t + 1);
      setArtifacts([]);
      void refreshBranching(g.id);
    } catch (e) {
      setStatus(e instanceof Error ? e.message : String(e));
    }
  };

  useEffect(() => {
    if (!graphId) return;
    void refreshArtifacts(graphId);
    void refreshGraphEdges(graphId);
    void refreshBranching(graphId);
    const es = new EventSource(api(`/graphs/${graphId}/events`));
    es.addEventListener("changed", () => {
      void refreshArtifacts(graphId);
      void refreshGraphEdges(graphId);
      void refreshBranching(graphId);
      // Clear stale one-shot queue messages once graph/artifacts have changed.
      setStatus("");
      setGraphRefreshToken((t) => t + 1);
    });
    es.addEventListener("error", () => {
      // EventSource auto-reconnects; keep UI quiet unless manual refresh is needed.
    });
    return () => {
      es.close();
    };
  }, [graphId, refreshArtifacts, refreshGraphEdges, refreshBranching]);

  useEffect(() => {
    if (!graphId || !activeBranchId) return;
    const checkoutKey = `${graphId}:${activeBranchId}`;
    if (lastCheckoutRef.current === checkoutKey) return;
    lastCheckoutRef.current = checkoutKey;
    let cancelled = false;
    const run = async () => {
      try {
        await checkoutGraphBranch(graphId, activeBranchId, { created_by: "local" });
        if (!cancelled) refreshAll();
      } catch (e) {
        if (!cancelled) {
          setStatus(e instanceof Error ? e.message : String(e));
        }
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [activeBranchId, graphId, refreshAll]);

  const onCreateBranch = useCallback(
    async (name: string) => {
      if (!graphId) return;
      const base = branches.find((b) => b.id === activeBranchId) ?? branches[0];
      await createGraphBranch(graphId, {
        name,
        created_by: "local",
        base_revision_id: base?.head_revision_id ?? null,
        status: "draft",
      });
      await refreshBranching(graphId);
    },
    [activeBranchId, branches, graphId, refreshBranching]
  );

  const onCommitCurrentToBranch = useCallback(
    async (branchId: string, event: string) => {
      if (!graphId) return;
      await commitCurrentToBranch(graphId, branchId, {
        created_by: "local",
        event,
        details: { from_ui: true },
      });
      await refreshBranching(graphId);
    },
    [graphId, refreshBranching]
  );

  const onPromoteBranch = useCallback(
    async (sourceBranchId: string, targetBranchId: string) => {
      if (!graphId) return;
      const res = await executeGraphPromotion(graphId, {
        source_branch_id: sourceBranchId,
        target_branch_id: targetBranchId,
        created_by: "local",
        apply_to_graph: true,
      });
      if (res.status === "conflict") {
        setStatus(`Promotion conflict (${res.mode}). Check Branches panel for details.`);
      } else {
        setStatus(`Promotion succeeded (${res.mode}).`);
      }
      await refreshBranching(graphId);
      refreshAll();
    },
    [graphId, refreshAll, refreshBranching]
  );

  const onDiffRevisions = useCallback(
    async (fromRevisionId: string, toRevisionId: string) => {
      if (!graphId) return;
      const d = await diffGraphRevisions(graphId, fromRevisionId, toRevisionId);
      setRevisionDiff(d);
    },
    [graphId]
  );

  const openNodeViewer = useCallback(
    (nodeId: string) => {
      setOpenViewerNodeIds((prev) => (prev.includes(nodeId) ? prev : [...prev, nodeId]));
      setMainTab(`node:${nodeId}`);

      if (!graphId) return;
      const node = graphNodes.find((n) => n.id === nodeId);
      if (!node) return;
      const isViewerNode =
        node.config.kind === "plan_view_2d" ||
        node.config.kind === "plan_view_3d" ||
        node.config.kind === "cesium_display_node" ||
        node.config.kind === "threejs_display_node";
      if (!isViewerNode) return;

      const upstream = graphEdges
        .filter((e) => e.to_node === nodeId)
        .map((e) => e.from_node);

      if (node.execution === "failed") {
        setStatus(`${node.config.kind.replace(/_/g, " ")} is failed; re-queuing now…`);
        void (async () => {
          try {
            await runGraph(graphId, { dirtyRoots: [nodeId], includeManual: true });
            refreshAll();
          } catch (e) {
            setStatus(e instanceof Error ? e.message : String(e));
          }
        })();
      }

      if (upstream.length === 0) return;
      const upstreamSet = new Set(upstream);
      const hasUpstreamArtifacts = artifacts.some((a) => upstreamSet.has(a.node_id));
      if (hasUpstreamArtifacts) return;

      setStatus(
        `No upstream artifacts for ${node.config.kind.replace(/_/g, " ")}. Queue pipeline run when ready.`
      );

    },
    [artifacts, graphEdges, graphId, graphNodes, refreshAll]
  );

  const closeNodeViewer = useCallback((nodeId: string) => {
    setOpenViewerNodeIds((prev) => prev.filter((id) => id !== nodeId));
    setMainTab((prev) => (prev === `node:${nodeId}` ? "workspace" : prev));
  }, []);

  const openNodeEditor = useCallback((nodeId: string) => {
    setOpenEditorNodeIds((prev) => (prev.includes(nodeId) ? prev : [...prev, nodeId]));
    setEditorTabs((prev) => ({ ...prev, [nodeId]: prev[nodeId] ?? "config" }));
    setMainTab(`edit:${nodeId}`);
  }, []);

  const closeNodeEditor = useCallback((nodeId: string) => {
    setOpenEditorNodeIds((prev) => prev.filter((id) => id !== nodeId));
    setEditorTabs((prev) => {
      const next = { ...prev };
      delete next[nodeId];
      return next;
    });
    setMainTab((prev) => (prev === `edit:${nodeId}` ? "workspace" : prev));
  }, []);

  const onNodeUpdatedFromEditor = useCallback((updated: ApiNode) => {
    setGraphNodes((prev) => prev.map((n) => (n.id === updated.id ? updated : n)));
  }, []);

  // Pre-fetched WGS84 bboxes for AOI editor tabs.
  // undefined = not yet attempted, null = attempted but nothing found, [...] = ready.
  const [aoiInitialBboxes, setAoiInitialBboxes] = useState<
    Record<string, [number, number, number, number] | null>
  >({});

  // Fetch an artifact, extract bbox, reproject to WGS84.
  const fetchWgs84BboxForNode = useCallback(
    async (nodeId: string) => {
      const nodeArtifacts = artifacts.filter((a) => a.node_id === nodeId);
      if (nodeArtifacts.length === 0) return;
      for (const artifact of nodeArtifacts) {
        try {
          const r = await fetch(api(artifact.url), { cache: "no-store" });
          if (!r.ok) continue;
          const data = (await r.json()) as Record<string, unknown>;
          const extracted = extractBboxAndEpsg(data);
          if (!extracted) continue;
          const wgs84 = await toWgs84Bbox(extracted.bbox, extracted.epsg);
          if (!wgs84) continue;
          setAoiInitialBboxes((prev) => ({ ...prev, [nodeId]: wgs84 }));
          return;
        } catch {
          // try next artifact
        }
      }
      // Nothing found — mark as attempted so we don't spin
      setAoiInitialBboxes((prev) => ({ ...prev, [nodeId]: null }));
    },
    [artifacts]
  );

  // If an AOI tab was opened before artifacts existed, retry once artifacts land.
  useEffect(() => {
    for (const nodeId of openAoiEditorNodeIds) {
      if (nodeId in aoiInitialBboxes) continue;
      void fetchWgs84BboxForNode(nodeId);
    }
  }, [openAoiEditorNodeIds, aoiInitialBboxes, fetchWgs84BboxForNode]);

  const openAoiEditor = useCallback(
    (nodeId: string) => {
      setOpenAoiEditorNodeIds((prev) => (prev.includes(nodeId) ? prev : [...prev, nodeId]));
      setMainTab(`aoi:${nodeId}`);
      // Kick off artifact fetch so the map gets a real initial bbox
      setAoiInitialBboxes((prev) => {
        if (nodeId in prev) return prev; // already fetched
        return prev; // don't reset, fetch below
      });
      void fetchWgs84BboxForNode(nodeId);
    },
    [fetchWgs84BboxForNode]
  );

  const closeAoiEditor = useCallback((nodeId: string) => {
    setOpenAoiEditorNodeIds((prev) => prev.filter((id) => id !== nodeId));
    setMainTab((prev) => (prev === `aoi:${nodeId}` ? "workspace" : prev));
  }, []);

  const saveAoiBbox = useCallback(
    async (nodeId: string, bbox: [number, number, number, number], epsg: number) => {
      if (!graphId) return;
      try {
        const updated = await patchNodeParams(
          graphId,
          nodeId,
          { ui: { bbox, bbox_epsg: epsg } },
          { branchId: activeBranchId }
        );
        setGraphNodes((prev) => prev.map((n) => (n.id === updated.id ? updated : n)));
      } catch (e) {
        setStatus(`AOI save failed: ${e instanceof Error ? e.message : String(e)}`);
      }
    },
    [graphId, activeBranchId]
  );

  const applyProjectCrs = useCallback(
    async (wsId: string, epsg: number) => {
      const next = Math.trunc(epsg);
      await updateWorkspaceProjectCrs(wsId, { epsg: next, wkt: null });
      setProjectEpsg(next);
      setStatus(`Project CRS updated to EPSG:${next}.`);
      refreshAll();
    },
    [refreshAll]
  );

  const onSetProjectCrs = useCallback(
    async (epsg: number) => {
      if (!Number.isFinite(epsg) || epsg <= 0) return;
      const next = Math.trunc(epsg);
      setProjectEpsg(next);
      if (!workspaceId) {
        setPendingProjectEpsg(next);
        setStatus(`Project CRS EPSG:${next} queued; applying when workspace metadata loads…`);
        return;
      }
      try {
        setPendingProjectEpsg(null);
        await applyProjectCrs(workspaceId, next);
      } catch (e) {
        setStatus(`Project CRS update failed: ${e instanceof Error ? e.message : String(e)}`);
        if (graphId) {
          void refreshGraphEdges(graphId);
        }
      }
    },
    [applyProjectCrs, graphId, refreshGraphEdges, workspaceId]
  );

  useEffect(() => {
    if (!workspaceId || pendingProjectEpsg === null) return;
    let cancelled = false;
    void (async () => {
      try {
        await applyProjectCrs(workspaceId, pendingProjectEpsg);
        if (!cancelled) setPendingProjectEpsg(null);
      } catch (e) {
        if (!cancelled) {
          setStatus(`Project CRS update failed: ${e instanceof Error ? e.message : String(e)}`);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [applyProjectCrs, pendingProjectEpsg, workspaceId]);

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <header
        style={{
          padding: "12px 16px",
          borderBottom: "1px solid #30363d",
          display: "flex",
          gap: 12,
          alignItems: "center",
          flexWrap: "wrap",
        }}
      >
        <strong>mine-eye</strong>
        {graphId && (
          <>
            <button
              type="button"
              onClick={() => void queuePipelineRun()}
              disabled={runBusy}
            >
              {runBusy ? "Queuing…" : "Queue pipeline run"}
            </button>
            <button type="button" onClick={refreshAll}>
              Refresh graph + artifacts
            </button>
          </>
        )}
        <span style={{ opacity: 0.85, fontSize: 14 }}>{status}</span>
      </header>
      <div
        style={{
          flex: 1,
          display: "grid",
          gridTemplateColumns: sidebarCollapsed ? "46px 1fr" : "minmax(260px, 28vw) 1fr",
          minHeight: 0,
        }}
      >
        {!sidebarCollapsed ? (
          <div style={{ position: "relative", minHeight: 0 }}>
            <button
              type="button"
              onClick={() => setSidebarCollapsed(true)}
              title="Minimize project panel (Ctrl/Cmd+B)"
              aria-label="Minimize project panel"
              style={{
                position: "absolute",
                top: 8,
                right: 8,
                zIndex: 20,
                width: 28,
                height: 28,
                borderRadius: 8,
                border: "1px solid #30363d",
                background: "#161b22",
                color: "#e6edf3",
                cursor: "pointer",
                padding: 0,
                display: "grid",
                placeItems: "center",
              }}
            >
              <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden>
                <path d="M9.5 2.5 5 7l4.5 4.5" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </button>
            <LeftSidebar
              projects={projects}
              activeLocalId={activeProjectId}
              onSelectProject={selectProject}
              onNewProject={() => void newProject()}
              onSeedDemo={() => void seedDemo()}
              graphId={graphId}
              projectEpsg={projectEpsg}
              workspaceUsedEpsgs={workspaceUsedEpsgs}
              artifacts={artifacts}
              onSetProjectCrs={(epsg) => void onSetProjectCrs(epsg)}
              branches={branches}
              revisions={revisions}
              promotions={promotions}
              activeBranchId={activeBranchId}
              onActiveBranchId={setActiveBranchId}
              onCreateBranch={(name) => void onCreateBranch(name)}
              onCommitCurrentToBranch={(branchId, event) =>
                void onCommitCurrentToBranch(branchId, event)
              }
              onPromoteBranch={(source, target) => void onPromoteBranch(source, target)}
              revisionDiff={revisionDiff}
              onDiffRevisions={(from, to) => void onDiffRevisions(from, to)}
              onDeleteProject={handleDeleteProject}
            />
          </div>
        ) : (
          <div
            style={{
              borderRight: "1px solid #30363d",
              background: "#0f1419",
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              justifyContent: "flex-start",
              paddingTop: 8,
              gap: 8,
            }}
          >
            <button
              type="button"
              onClick={() => setSidebarCollapsed(false)}
              title="Expand project panel (Ctrl/Cmd+B)"
              aria-label="Expand project panel"
              style={{
                width: 28,
                height: 28,
                borderRadius: 8,
                border: "1px solid #30363d",
                background: "#161b22",
                color: "#e6edf3",
                cursor: "pointer",
                padding: 0,
                display: "grid",
                placeItems: "center",
              }}
            >
              <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden>
                <path d="M4.5 2.5 9 7l-4.5 4.5" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </button>
            <div
              style={{
                writingMode: "vertical-rl",
                transform: "rotate(180deg)",
                fontSize: 10,
                letterSpacing: "0.08em",
                opacity: 0.65,
                userSelect: "none",
              }}
            >
              PROJECT
            </div>
          </div>
        )}
        <div style={{ display: "flex", flexDirection: "column", minHeight: 0, minWidth: 0 }}>
          <div
            role="tablist"
            style={{
              display: "flex",
              gap: 0,
              borderBottom: "1px solid #30363d",
              background: "#161b22",
              overflowX: "auto",
            }}
          >
            <div
              style={{
                display: "flex",
                alignItems: "stretch",
                minWidth: 0,
                background: mainTab === "workspace" ? "#0f1419" : "transparent",
                borderBottom:
                  mainTab === "workspace" ? "2px solid #58a6ff" : "2px solid transparent",
              }}
            >
              <button
                type="button"
                className={`mineeye-play ${runBusy ? "mineeye-play-running" : ""}`}
                disabled={!graphId || runBusy}
                title={
                  runBusy
                    ? "Queuing jobs…"
                    : "Run pipeline — queue all dirty nodes (start worker, then refresh)"
                }
                aria-label="Queue pipeline run"
                onClick={() => void queuePipelineRun()}
                style={{ alignSelf: "center", marginLeft: 8, marginRight: 2 }}
              >
                <svg
                  className="mineeye-play-icon"
                  width="14"
                  height="14"
                  viewBox="0 0 14 14"
                  aria-hidden
                >
                  <polygon points="2,1 12,7 2,13" fill="currentColor" />
                </svg>
              </button>
              <TabButton
                active={mainTab === "workspace"}
                onClick={() => setMainTab("workspace")}
                label="Workspace"
                suppressBorder
              />
            </div>
            {openViewerNodeIds
              .filter((id) => graphNodes.some((n) => n.id === id))
              .map((id) => {
                const node = graphNodes.find((n) => n.id === id) ?? null;
                const label = node ? node.config.kind.replace(/_/g, " ") : `Node ${id.slice(0, 6)}`;
                const tabId = `node:${id}` as MainTab;
                return (
                  <TabButton
                    key={id}
                    active={mainTab === tabId}
                    onClick={() => setMainTab(tabId)}
                    onClose={() => closeNodeViewer(id)}
                    label={label}
                  />
                );
              })}
            {openEditorNodeIds
              .filter((id) => graphNodes.some((n) => n.id === id))
              .map((id) => {
                const node = graphNodes.find((n) => n.id === id) ?? null;
                const label = node
                  ? `${node.config.kind.replace(/_/g, " ")} edit`
                  : `Edit ${id.slice(0, 6)}`;
                const tabId = `edit:${id}` as MainTab;
                return (
                  <TabButton
                    key={`edit-tab:${id}`}
                    active={mainTab === tabId}
                    onClick={() => setMainTab(tabId)}
                    onClose={() => closeNodeEditor(id)}
                    label={label}
                  />
                );
              })}
            {openAoiEditorNodeIds
              .filter((id) => graphNodes.some((n) => n.id === id))
              .map((id) => {
                const node = graphNodes.find((n) => n.id === id) ?? null;
                const nodeAlias = node ? (node.config.params._alias as string | undefined | null) : null;
                const label = node
                  ? `${(nodeAlias ?? node.config.kind).replace(/_/g, " ")} map`
                  : `AOI map ${id.slice(0, 6)}`;
                const tabId = `aoi:${id}` as MainTab;
                return (
                  <TabButton
                    key={`aoi-tab:${id}`}
                    active={mainTab === tabId}
                    onClick={() => setMainTab(tabId)}
                    onClose={() => closeAoiEditor(id)}
                    label={label}
                  />
                );
              })}
          </div>
          <div style={{ flex: 1, minHeight: 0, position: "relative" }}>
            <div
              style={{
                position: "absolute",
                inset: 0,
                display: mainTab === "workspace" ? "block" : "none",
              }}
            >
              <GraphErrorBoundary key={graphId ?? "none"}>
                <GraphCanvas
                  graphId={graphId}
                  activeBranchId={activeBranchId}
                  refreshToken={graphRefreshToken}
                  projectEpsg={projectEpsg}
                  workspaceUsedEpsgs={workspaceUsedEpsgs}
                  artifacts={artifacts}
                  onPipelineQueued={refreshAll}
                  onOpenNodeViewer={openNodeViewer}
                  onOpenNodeEditor={openNodeEditor}
                  onOpenAoiEditor={openAoiEditor}
                  onGraphChanged={refreshAll}
                />
              </GraphErrorBoundary>
            </div>
            {openViewerNodeIds.map((nodeId) => {
              const node = graphNodes.find((n) => n.id === nodeId) ?? null;
              const active = mainTab === (`node:${nodeId}` as MainTab);
              return (
                <div
                  key={`viewer:${nodeId}`}
                  style={{
                    position: "absolute",
                    inset: 0,
                    display: active ? "block" : "none",
                  }}
                >
                  {!node ? (
                    <div style={{ padding: 16, fontSize: 13, opacity: 0.8 }}>
                      Node preview unavailable.
                    </div>
                  ) : node.config.kind === "plan_view_2d" ? (
                    <Map2DPanel
                      graphId={graphId}
                      activeBranchId={activeBranchId}
                      active={active}
                      edges={graphEdges}
                      artifacts={artifacts}
                      viewerNodeId={nodeId}
                      onClearViewer={() => closeNodeViewer(nodeId)}
                    />
                  ) : node.config.kind === "threejs_display_node" ? (
                    <Map3DThreePanel
                      graphId={graphId}
                      activeBranchId={activeBranchId}
                      active={active}
                      edges={graphEdges}
                      artifacts={artifacts}
                      viewerNodeId={nodeId}
                      onClearViewer={() => closeNodeViewer(nodeId)}
                    />
                  ) : node.config.kind === "plan_view_3d" ||
                    node.config.kind === "cesium_display_node" ? (
                    <Map3DPanel
                      graphId={graphId}
                      activeBranchId={activeBranchId}
                      active={active}
                      edges={graphEdges}
                      artifacts={artifacts}
                      viewerNodeId={nodeId}
                      onClearViewer={() => closeNodeViewer(nodeId)}
                    />
                  ) : (
                    <NodePreviewPanel
                      graphId={graphId}
                      nodeId={nodeId}
                      nodeKind={node.config.kind}
                      artifacts={artifacts}
                    />
                  )}
                </div>
              );
            })}
            {openEditorNodeIds.map((nodeId) => {
              const node = graphNodes.find((n) => n.id === nodeId) ?? null;
              const active = mainTab === (`edit:${nodeId}` as MainTab);
              const tab = editorTabs[nodeId] ?? "config";
              const nodeArtifacts = artifacts.filter((a) => a.node_id === nodeId);
              return (
                <div
                  key={`editor:${nodeId}`}
                  style={{
                    position: "absolute",
                    inset: 0,
                    display: active ? "block" : "none",
                    background: "#0f1419",
                  }}
                >
                  {!node ? (
                    <div style={{ padding: 16, fontSize: 13, opacity: 0.8 }}>
                      Node editor unavailable.
                    </div>
                  ) : (
                    <div style={{ height: "100%", display: "flex" }}>
                      <NodeInspector
                        graphId={graphId ?? ""}
                        activeBranchId={activeBranchId}
                        node={node}
                        nodeSpec={nodeSpec(node.config.kind)}
                        projectEpsg={projectEpsg}
                        workspaceUsedEpsgs={workspaceUsedEpsgs}
                        tab={tab}
                        onTab={(next) =>
                          setEditorTabs((prev) => ({ ...prev, [nodeId]: next }))
                        }
                        onClose={() => closeNodeEditor(nodeId)}
                        onOpenAoiEditor={openAoiEditor}
                        mode="editor"
                        onNodeUpdated={onNodeUpdatedFromEditor}
                        nodeArtifacts={nodeArtifacts}
                        onPipelineQueued={refreshAll}
                      />
                    </div>
                  )}
                </div>
              );
            })}
            {openAoiEditorNodeIds.map((nodeId) => {
              const node = graphNodes.find((n) => n.id === nodeId) ?? null;
              const active = mainTab === (`aoi:${nodeId}` as MainTab);
              const nodeArtifacts = artifacts.filter((a) => a.node_id === nodeId);

              // Prefer the pre-fetched WGS84 bbox from the node's cached artifact output.
              // Fall back to ui.bbox only if that exists and is already in WGS84 range.
              const prefetched = aoiInitialBboxes[nodeId]; // undefined=loading, null=none, [...]=ready
              const nodeUi = (
                node?.config?.params?.ui &&
                typeof node.config.params.ui === "object" &&
                !Array.isArray(node.config.params.ui)
                  ? node.config.params.ui
                  : {}
              ) as Record<string, unknown>;
              const uiBboxRaw = nodeUi.bbox;
              const uiBbox: AoiBbox | null =
                Array.isArray(uiBboxRaw) && uiBboxRaw.length >= 4
                  ? [Number(uiBboxRaw[0]), Number(uiBboxRaw[1]), Number(uiBboxRaw[2]), Number(uiBboxRaw[3])]
                  : null;
              const uiBboxEpsg =
                typeof nodeUi.bbox_epsg === "number" && Number.isFinite(nodeUi.bbox_epsg)
                  ? Math.trunc(nodeUi.bbox_epsg)
                  : 4326;

              // Prefer artifact-derived WGS84 bounds; fallback to ui.bbox in its own EPSG.
              const initialBbox: AoiBbox | null =
                prefetched !== undefined
                  ? (prefetched ?? uiBbox)
                  : uiBbox;
              const initialBboxEpsg = prefetched ? 4326 : uiBboxEpsg;

              // Don't mount the map until we've finished the artifact fetch attempt,
              // so Leaflet always gets a real bbox at init time (no world-view flash).
              const fetchDone = nodeId in aoiInitialBboxes;
              const mapReady = active && (fetchDone || nodeArtifacts.length === 0);

              return (
                <div
                  key={`aoi:${nodeId}`}
                  style={{
                    position: "absolute",
                    inset: 0,
                    display: active ? "flex" : "none",
                    flexDirection: "row",
                    background: "#0d1117",
                  }}
                >
                  {/* ── Left: config panel (always mounted so form state persists) ── */}
                  <div style={{
                    width: 340,
                    minWidth: 340,
                    flexShrink: 0,
                    borderRight: "2px solid #30363d",
                    overflowY: "auto",
                    overflowX: "hidden",
                    display: "flex",
                    flexDirection: "column",
                    background: "#0d1117",
                  }}>
                    {node ? (
                      <NodeInspector
                        graphId={graphId ?? ""}
                        activeBranchId={activeBranchId}
                        node={node}
                        nodeSpec={nodeSpec(node.config.kind)}
                        projectEpsg={projectEpsg}
                        workspaceUsedEpsgs={workspaceUsedEpsgs}
                        tab="config"
                        onTab={() => {}}
                        onClose={() => closeAoiEditor(nodeId)}
                        mode="sidebar"
                        onNodeUpdated={onNodeUpdatedFromEditor}
                        nodeArtifacts={nodeArtifacts}
                        onPipelineQueued={refreshAll}
                        onOpenAoiEditor={openAoiEditor}
                      />
                    ) : (
                      <div style={{ padding: 16, fontSize: 13, opacity: 0.6 }}>Node unavailable</div>
                    )}
                  </div>
                  {/* ── Right: map — only mounts once artifact fetch is done ──────── */}
                  <div style={{ flex: 1, minWidth: 0, position: "relative", overflow: "hidden" }}>
                    {!mapReady && (
                      <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "#8b949e", fontSize: 13 }}>
                        Loading extent…
                      </div>
                    )}
                    {mapReady && (
                      <AoiBboxEditor
                        mode="panel"
                        initialBbox={initialBbox}
                        initialBboxEpsg={initialBboxEpsg}
                        projectEpsg={projectEpsg}
                        workspaceUsedEpsgs={workspaceUsedEpsgs}
                        onSave={(bbox, epsg) => {
                          void saveAoiBbox(nodeId, bbox, epsg);
                          closeAoiEditor(nodeId);
                        }}
                        onCancel={() => closeAoiEditor(nodeId)}
                      />
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}

function TabButton({
  active,
  onClick,
  label,
  onClose,
  suppressBorder,
}: {
  active: boolean;
  onClick: () => void;
  label: string;
  onClose?: () => void;
  /** When nested in a tab group shell (e.g. Play + Workspace). */
  suppressBorder?: boolean;
}) {
  return (
    <div
      role="tab"
      aria-selected={active}
      style={{
        display: "flex",
        alignItems: "center",
        borderBottom: suppressBorder
          ? "2px solid transparent"
          : active
            ? "2px solid #58a6ff"
            : "2px solid transparent",
        background: suppressBorder ? "transparent" : active ? "#0f1419" : "transparent",
      }}
    >
      <button
        type="button"
        onClick={onClick}
        style={{
          padding: "10px 14px",
          border: "none",
          background: "transparent",
          color: active ? "#e6edf3" : "#8b949e",
          cursor: "pointer",
          fontSize: 14,
          fontWeight: active ? 600 : 400,
          whiteSpace: "nowrap",
        }}
      >
        {label}
      </button>
      {onClose && (
        <button
          type="button"
          onClick={onClose}
          title="Close tab"
          style={{
            border: "none",
            background: "transparent",
            color: "#8b949e",
            padding: "0 8px 0 0",
            cursor: "pointer",
            fontSize: 14,
          }}
        >
          x
        </button>
      )}
    </div>
  );
}
