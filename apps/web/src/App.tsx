import { useCallback, useEffect, useState } from "react";
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
  runGraph,
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
import { NodePreviewPanel } from "./NodePreviewPanel";
import { loadNodeRegistryFromApi } from "./nodeRegistry";
import {
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

type MainTab = "workspace" | `node:${string}`;

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
  const [openViewerNodeIds, setOpenViewerNodeIds] = useState<string[]>([]);
  const [branches, setBranches] = useState<ApiBranch[]>([]);
  const [revisions, setRevisions] = useState<ApiRevision[]>([]);
  const [promotions, setPromotions] = useState<ApiPromotion[]>([]);
  const [activeBranchId, setActiveBranchId] = useState<string | null>(null);
  const [revisionDiff, setRevisionDiff] = useState<ApiRevisionDiff | null>(null);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

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
      const r = await fetch(api(`/graphs/${gid}/artifacts`));
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
    setOpenViewerNodeIds([]);
    setRevisionDiff(null);
    setStatus("");
    setActiveBranchId(null);
    setMainTab("workspace");
  }, []);

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
    const t = setInterval(() => refreshArtifacts(graphId), 12000);
    return () => clearInterval(t);
  }, [graphId, refreshArtifacts, refreshGraphEdges, refreshBranching]);

  useEffect(() => {
    if (!graphId || !activeBranchId) return;
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

  const openNodeViewer = useCallback((nodeId: string) => {
    setOpenViewerNodeIds((prev) => (prev.includes(nodeId) ? prev : [...prev, nodeId]));
    setMainTab(`node:${nodeId}`);
  }, []);

  const closeNodeViewer = useCallback((nodeId: string) => {
    setOpenViewerNodeIds((prev) => prev.filter((id) => id !== nodeId));
    setMainTab((prev) => (prev === `node:${nodeId}` ? "workspace" : prev));
  }, []);

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
              artifacts={artifacts}
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
                  projectEpsg={4326}
                  artifacts={artifacts}
                  onPipelineQueued={refreshAll}
                  onOpenNodeViewer={openNodeViewer}
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
