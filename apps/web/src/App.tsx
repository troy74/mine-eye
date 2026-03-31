import { useCallback, useEffect, useState } from "react";
import { GraphCanvas } from "./GraphCanvas";
import { GraphErrorBoundary } from "./GraphErrorBoundary";
import {
  api,
  createGraph,
  createWorkspace,
  fetchGraph,
  runGraph,
  type ApiEdge,
  type ArtifactEntry,
} from "./graphApi";
import { LeftSidebar } from "./LeftSidebar";
import { Map2DPanel } from "./Map2DPanel";
import { Scene } from "./Scene";
import { ViewportErrorBoundary } from "./ViewportErrorBoundary";
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

type MainTab = "graph" | "preview" | "map2d";

export default function App() {
  const [projects, setProjects] = useState<StoredProject[]>(loadProjects);
  const [activeProjectId, setActiveProjectIdState] = useState<string | null>(null);
  const [graphId, setGraphId] = useState<string | null>(null);
  const [artifacts, setArtifacts] = useState<ArtifactEntry[]>([]);
  const [trajectoryUrl, setTrajectoryUrl] = useState<string | null>(null);
  const [status, setStatus] = useState<string>("");
  const [mainTab, setMainTab] = useState<MainTab>("graph");
  const [graphRefreshToken, setGraphRefreshToken] = useState(0);
  const [runBusy, setRunBusy] = useState(false);
  const [mapViewerNodeId, setMapViewerNodeId] = useState<string | null>(null);
  const [graphEdges, setGraphEdges] = useState<ApiEdge[]>([]);

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

  const refreshArtifacts = useCallback(async (gid: string) => {
    try {
      const r = await fetch(api(`/graphs/${gid}/artifacts`));
      if (!r.ok) return;
      const data: unknown = await r.json();
      if (!Array.isArray(data)) return;
      const list = data as ArtifactEntry[];
      list.sort((a, b) => a.key.localeCompare(b.key));
      setArtifacts(list);
      const traj = list.find((a) => a.key.endsWith("trajectory.json"));
      setTrajectoryUrl(traj ? api(traj.url) : null);
    } catch (e) {
      console.warn("refreshArtifacts:", e);
    }
  }, []);

  const refreshGraphEdges = useCallback(async (gid: string) => {
    try {
      const g = await fetchGraph(gid);
      setGraphEdges(g.edges);
    } catch (e) {
      console.warn("refreshGraphEdges:", e);
    }
  }, []);

  const refreshAll = useCallback(() => {
    if (!graphId) return;
    void refreshArtifacts(graphId);
    void refreshGraphEdges(graphId);
    setGraphRefreshToken((t) => t + 1);
  }, [graphId, refreshArtifacts, refreshGraphEdges]);

  const selectProject = useCallback((p: StoredProject) => {
    setActiveProjectId(p.localId);
    setActiveProjectIdState(p.localId);
    setGraphId(p.graphId);
    setMapViewerNodeId(null);
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
      setMainTab("graph");
      setMapViewerNodeId(null);
      setStatus(
        "Demo graph ready. Queue pipeline run, start worker, then refresh. Project saved in this browser."
      );
      setGraphRefreshToken((t) => t + 1);
      void refreshArtifacts(data.graph_id);
      void refreshGraphEdges(data.graph_id);
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
      setMapViewerNodeId(null);
      setStatus(
        "Empty graph created. Add nodes via API or seed a demo in another project. This browser remembers projects locally."
      );
      setGraphRefreshToken((t) => t + 1);
      setArtifacts([]);
    } catch (e) {
      setStatus(e instanceof Error ? e.message : String(e));
    }
  };

  useEffect(() => {
    if (!graphId) return;
    void refreshArtifacts(graphId);
    void refreshGraphEdges(graphId);
    const t = setInterval(() => refreshArtifacts(graphId), 12000);
    return () => clearInterval(t);
  }, [graphId, refreshArtifacts, refreshGraphEdges]);

  const openPlanMapViewer = useCallback((nodeId: string) => {
    setMapViewerNodeId(nodeId);
    setMainTab("map2d");
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
          gridTemplateColumns: "minmax(260px, 28vw) 1fr",
          minHeight: 0,
        }}
      >
        <LeftSidebar
          projects={projects}
          activeLocalId={activeProjectId}
          onSelectProject={selectProject}
          onNewProject={() => void newProject()}
          onSeedDemo={() => void seedDemo()}
          graphId={graphId}
          artifacts={artifacts}
        />
        <div style={{ display: "flex", flexDirection: "column", minHeight: 0, minWidth: 0 }}>
          <div
            role="tablist"
            style={{
              display: "flex",
              gap: 0,
              borderBottom: "1px solid #30363d",
              background: "#161b22",
            }}
          >
            <div
              style={{
                display: "flex",
                flex: 1,
                alignItems: "stretch",
                minWidth: 0,
                background: mainTab === "graph" ? "#0f1419" : "transparent",
                borderBottom:
                  mainTab === "graph" ? "2px solid #58a6ff" : "2px solid transparent",
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
                active={mainTab === "graph"}
                onClick={() => setMainTab("graph")}
                label="Workspace"
                flex={1}
                suppressBorder
              />
            </div>
            <TabButton
              active={mainTab === "preview"}
              onClick={() => setMainTab("preview")}
              label="3D preview"
            />
            <TabButton
              active={mainTab === "map2d"}
              onClick={() => setMainTab("map2d")}
              label="2D map"
            />
          </div>
          <div style={{ flex: 1, minHeight: 0, position: "relative" }}>
            {mainTab === "graph" ? (
              <GraphErrorBoundary key={graphId ?? "none"}>
                <GraphCanvas
                  graphId={graphId}
                  refreshToken={graphRefreshToken}
                  projectEpsg={4326}
                  artifacts={artifacts}
                  onPipelineQueued={refreshAll}
                  onOpenPlanMapViewer={openPlanMapViewer}
                  onGraphChanged={refreshAll}
                />
              </GraphErrorBoundary>
            ) : mainTab === "preview" ? (
              <ViewportErrorBoundary>
                <Scene trajectoryUrl={trajectoryUrl} />
              </ViewportErrorBoundary>
            ) : (
              <Map2DPanel
                graphId={graphId}
                edges={graphEdges}
                artifacts={artifacts}
                viewerNodeId={mapViewerNodeId}
                onClearViewer={() => setMapViewerNodeId(null)}
              />
            )}
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
  flex,
  suppressBorder,
}: {
  active: boolean;
  onClick: () => void;
  label: string;
  flex?: number;
  /** When nested in a tab group shell (e.g. Play + Workspace). */
  suppressBorder?: boolean;
}) {
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      onClick={onClick}
      style={{
        flex: flex ?? undefined,
        padding: "10px 18px",
        border: "none",
        borderBottom: suppressBorder
          ? "2px solid transparent"
          : active
            ? "2px solid #58a6ff"
            : "2px solid transparent",
        background: suppressBorder ? "transparent" : active ? "#0f1419" : "transparent",
        color: active ? "#e6edf3" : "#8b949e",
        cursor: "pointer",
        fontSize: 14,
        fontWeight: active ? 600 : 400,
      }}
    >
      {label}
    </button>
  );
}
