import { useEffect, useMemo, useState, type CSSProperties } from "react";
import { AgentChat } from "./AgentChat";
import { CrsPicker } from "./CrsPicker";
import type {
  ApiBranch,
  ApiPromotion,
  ApiRevision,
  ApiRevisionDiff,
  ArtifactEntry,
} from "./graphApi";
import type { StoredProject } from "./projectStorage";

type SidebarTab = "chat" | "project";

type Props = {
  projects: StoredProject[];
  activeLocalId: string | null;
  onSelectProject: (p: StoredProject) => void;
  onNewProject: () => void;
  onSeedDemo: () => void;
  onSeedKimberlinaSurfaces: () => void;
  onSeedKimberlinaPreview: () => void;
  graphId: string | null;
  projectEpsg: number;
  workspaceUsedEpsgs?: number[];
  artifacts: ArtifactEntry[];
  onSetProjectCrs: (epsg: number) => void;
  branches: ApiBranch[];
  revisions: ApiRevision[];
  promotions: ApiPromotion[];
  activeBranchId: string | null;
  onActiveBranchId: (id: string | null) => void;
  onCreateBranch: (name: string) => void;
  onCommitCurrentToBranch: (branchId: string, event: string) => void;
  onPromoteBranch: (sourceBranchId: string, targetBranchId: string) => void;
  revisionDiff: ApiRevisionDiff | null;
  onDiffRevisions: (fromRevisionId: string, toRevisionId: string) => void;
  onDeleteProject: (localId: string) => void;
};

export function LeftSidebar({
  projects,
  activeLocalId,
  onSelectProject,
  onNewProject,
  onSeedDemo,
  onSeedKimberlinaSurfaces,
  onSeedKimberlinaPreview,
  graphId,
  projectEpsg,
  workspaceUsedEpsgs = [],
  artifacts,
  onSetProjectCrs,
  branches,
  revisions,
  promotions,
  activeBranchId,
  onActiveBranchId,
  onCreateBranch,
  onCommitCurrentToBranch,
  onPromoteBranch,
  revisionDiff,
  onDiffRevisions,
  onDeleteProject,
}: Props) {
  const [tab, setTab] = useState<SidebarTab>("chat");
  const [newBranchName, setNewBranchName] = useState("");
  const [fromRevisionId, setFromRevisionId] = useState("");
  const [toRevisionId, setToRevisionId] = useState("");
  const [projectCrsValue, setProjectCrsValue] = useState<string>("project");
  useEffect(() => {
    setProjectCrsValue(String(projectEpsg));
  }, [projectEpsg, activeLocalId]);

  const active = useMemo(
    () => projects.find((p) => p.localId === activeLocalId) ?? null,
    [projects, activeLocalId]
  );
  const activeBranch = branches.find((b) => b.id === activeBranchId) ?? null;
  const mainBranch = branches.find((b) => b.name === "main") ?? null;

  return (
    <aside style={aside}>
      <div style={projectCard}>
        <div style={projectCardHeader}>
          <div>
            <div style={sectionLabel}>Project</div>
            <div style={projectCardTitle}>{active?.name ?? "No project selected"}</div>
          </div>
          {active && <div style={projectLiveBadge}>Live</div>}
        </div>
        <select
          style={sel}
          value={activeLocalId ?? ""}
          onChange={(e) => {
            const id = e.target.value;
            const p = projects.find((x) => x.localId === id);
            if (p) onSelectProject(p);
          }}
        >
          <option value="" disabled>
            Select project…
          </option>
          {projects.map((p) => (
            <option key={p.localId} value={p.localId}>
              {p.name}
            </option>
          ))}
        </select>
        <div style={{ display: "flex", gap: 6, marginTop: 8 }}>
          <button type="button" style={{ ...btn, flex: 1 }} onClick={onNewProject}>
            New project…
          </button>
          <button type="button" style={{ ...btn, flex: 1 }} onClick={onSeedDemo}>
            Seed demo
          </button>
        </div>
        <div style={{ display: "flex", gap: 6, marginTop: 8 }}>
          <button type="button" style={{ ...btn, flex: 1 }} onClick={onSeedKimberlinaSurfaces}>
            Geology surfaces
          </button>
          <button type="button" style={{ ...btn, flex: 1 }} onClick={onSeedKimberlinaPreview}>
            Geology + blocks
          </button>
        </div>
        {active ? (
          <>
            <div style={projectMetaRow}>
              <span style={projectMetaChip}>Graph {active.graphId.slice(0, 8)}…</span>
              {graphId ? <span style={projectMetaChip}>EPSG:{projectEpsg}</span> : null}
            </div>
            <button
              type="button"
              style={{
                ...btn,
                marginTop: 10,
                color: "#f85149",
                borderColor: "rgba(248,81,73,0.3)",
                width: "100%",
                background: "rgba(248,81,73,0.06)",
              }}
              onClick={() => {
                if (
                  window.confirm(
                    `Delete "${active.name}" from your local project list?\n\nThis only removes it from your browser — the graph and data on the server are unaffected.`
                  )
                ) {
                  onDeleteProject(active.localId);
                }
              }}
            >
              Delete project…
            </button>
          </>
        ) : (
          <div style={{ ...hint, margin: "10px 0 0" }}>
            Create a project or seed a demo graph to start working.
          </div>
        )}
      </div>

      {/* ── Tab bar ──────────────────────────────────────────────────────── */}
      <div style={tabBar}>
        <button
          type="button"
          style={tab === "chat" ? tabBtnActive : tabBtn}
          onClick={() => setTab("chat")}
        >
          <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden>
            <path
              d="M3 3.5h10a1 1 0 0 1 1 1v6a1 1 0 0 1-1 1H7l-3.5 2v-2H3a1 1 0 0 1-1-1v-6a1 1 0 0 1 1-1Z"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.2"
              strokeLinejoin="round"
            />
          </svg>
          <span>AI Chat</span>
        </button>
        <button
          type="button"
          style={tab === "project" ? tabBtnActive : tabBtn}
          onClick={() => setTab("project")}
        >
          <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden>
            <path
              d="M6.7 1.3h2.6l.4 1.9a5 5 0 0 1 1.2.5l1.7-1 1.8 1.8-1 1.7c.2.4.4.8.5 1.2l1.9.4v2.6l-1.9.4a5 5 0 0 1-.5 1.2l1 1.7-1.8 1.8-1.7-1a5 5 0 0 1-1.2.5l-.4 1.9H6.7l-.4-1.9a5 5 0 0 1-1.2-.5l-1.7 1-1.8-1.8 1-1.7a5 5 0 0 1-.5-1.2l-1.9-.4V7.3l1.9-.4a5 5 0 0 1 .5-1.2l-1-1.7 1.8-1.8 1.7 1a5 5 0 0 1 1.2-.5l.4-1.9Z"
              fill="none"
              stroke="currentColor"
              strokeWidth="1"
              strokeLinejoin="round"
            />
            <circle cx="8" cy="8" r="2.1" fill="none" stroke="currentColor" strokeWidth="1" />
          </svg>
          <span>Project</span>
          {active && (
            <span style={{ marginLeft: "auto", fontSize: 9, color: "#3fb950", fontWeight: 700 }}>
              ●
            </span>
          )}
        </button>
      </div>

      {/* ── Chat tab ─────────────────────────────────────────────────────── */}
      {tab === "chat" && (
        <div style={tabContent}>
          <AgentChat
            projectLocalId={activeLocalId}
            projectName={active?.name ?? ""}
            graphId={graphId}
            activeBranchId={activeBranchId}
          />
        </div>
      )}

      {/* ── Project tab ──────────────────────────────────────────────────── */}
      {tab === "project" && (
        <div style={{ ...tabContent, overflowY: "auto", padding: "10px 12px 16px", gap: 0 }}>
          {/* CRS */}
          {graphId && (
            <>
              <div style={{ ...sectionLabel, marginTop: 16 }}>Coordinate system</div>
              <CrsPicker
                value={projectCrsValue}
                onChange={(v) => {
                  setProjectCrsValue(v);
                  const epsg = v === "project" ? projectEpsg : parseInt(v, 10);
                  if (Number.isFinite(epsg) && epsg > 0) onSetProjectCrs(epsg);
                }}
                projectEpsg={projectEpsg}
                workspaceUsedEpsgs={workspaceUsedEpsgs}
                includeProject
              />
              <div style={{ fontSize: 10, color: "#484f58", marginTop: 4 }}>
                Active: EPSG:{projectEpsg}
              </div>
            </>
          )}

          {/* Branches */}
          <div style={{ ...sectionLabel, marginTop: 18 }}>Branches</div>
          {!graphId ? (
            <p style={hint}>Load a project to manage branches.</p>
          ) : (
            <>
              <select
                style={sel}
                value={activeBranchId ?? ""}
                onChange={(e) => onActiveBranchId(e.target.value || null)}
              >
                <option value="" disabled>
                  Select branch…
                </option>
                {branches.map((b) => (
                  <option key={b.id} value={b.id}>
                    {b.name} ({b.status})
                  </option>
                ))}
              </select>
              <div style={{ display: "flex", gap: 6, marginTop: 7 }}>
                <input
                  value={newBranchName}
                  onChange={(e) => setNewBranchName(e.target.value)}
                  placeholder="new branch name…"
                  style={inp}
                />
                <button
                  type="button"
                  style={btn}
                  onClick={() => {
                    const v = newBranchName.trim();
                    if (!v) return;
                    onCreateBranch(v);
                    setNewBranchName("");
                  }}
                >
                  Create
                </button>
              </div>
              <div style={{ display: "flex", gap: 6, marginTop: 7, flexWrap: "wrap" }}>
                <button
                  type="button"
                  style={btn}
                  disabled={!activeBranch}
                  onClick={() =>
                    activeBranch && onCommitCurrentToBranch(activeBranch.id, "ui_commit_current")
                  }
                >
                  Commit current
                </button>
                <button
                  type="button"
                  style={btn}
                  disabled={!activeBranch || !mainBranch || activeBranch.id === mainBranch.id}
                  onClick={() =>
                    activeBranch &&
                    mainBranch &&
                    onPromoteBranch(activeBranch.id, mainBranch.id)
                  }
                >
                  Promote to main
                </button>
              </div>
              <div style={{ fontSize: 10, color: "#484f58", marginTop: 6 }}>
                {revisions.length} revision{revisions.length !== 1 ? "s" : ""} ·{" "}
                {promotions.length} promotion{promotions.length !== 1 ? "s" : ""}
              </div>

              {/* Recent promotions */}
              {promotions.length > 0 && (
                <div style={{ marginTop: 8 }}>
                  {promotions.slice(0, 4).map((p) => (
                    <div
                      key={p.id}
                      style={{
                        fontSize: 10,
                        color: "#6e7681",
                        padding: "3px 0",
                        borderBottom: "1px solid #161b22",
                        display: "flex",
                        alignItems: "center",
                        gap: 6,
                      }}
                    >
                      <span
                        style={{
                          width: 6,
                          height: 6,
                          borderRadius: "50%",
                          background:
                            p.status === "succeeded"
                              ? "#3fb950"
                              : p.status === "conflict"
                                ? "#f85149"
                                : p.status === "failed"
                                  ? "#f0883e"
                                  : "#8b949e",
                          flexShrink: 0,
                        }}
                      />
                      <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {p.status} · {new Date(p.created_at).toLocaleDateString()}
                      </span>
                      {p.conflict_report && (
                        <button
                          type="button"
                          style={{ ...btn, padding: "1px 5px", fontSize: 9 }}
                          onClick={() =>
                            void navigator.clipboard.writeText(
                              JSON.stringify(p.conflict_report, null, 2)
                            )
                          }
                        >
                          Copy
                        </button>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </>
          )}

          {/* Revision diff */}
          {graphId && revisions.length >= 2 && (
            <>
              <div style={{ ...sectionLabel, marginTop: 18 }}>Revision diff</div>
              <select
                style={sel}
                value={fromRevisionId}
                onChange={(e) => setFromRevisionId(e.target.value)}
              >
                <option value="">From…</option>
                {revisions.map((r) => (
                  <option key={r.id} value={r.id}>
                    {r.id.slice(0, 8)}… ({r.created_by})
                  </option>
                ))}
              </select>
              <select
                style={{ ...sel, marginTop: 5 }}
                value={toRevisionId}
                onChange={(e) => setToRevisionId(e.target.value)}
              >
                <option value="">To…</option>
                {revisions.map((r) => (
                  <option key={r.id} value={r.id}>
                    {r.id.slice(0, 8)}… ({r.created_by})
                  </option>
                ))}
              </select>
              <button
                type="button"
                style={{ ...btn, marginTop: 6 }}
                disabled={!fromRevisionId || !toRevisionId}
                onClick={() =>
                  fromRevisionId &&
                  toRevisionId &&
                  onDiffRevisions(fromRevisionId, toRevisionId)
                }
              >
                Compare
              </button>
              {revisionDiff && (
                <div style={{ marginTop: 6, fontSize: 10, color: "#8b949e" }}>
                  Δ nodes +{revisionDiff.diff.summary.nodes_added}/−
                  {revisionDiff.diff.summary.nodes_removed} ~
                  {revisionDiff.diff.summary.nodes_changed}, edges +
                  {revisionDiff.diff.summary.edges_added}/−
                  {revisionDiff.diff.summary.edges_removed}
                </div>
              )}
            </>
          )}

          {/* Artifacts */}
          <div style={{ ...sectionLabel, marginTop: 18 }}>Artifacts</div>
          {!graphId && <p style={hint}>Load a project to see artifacts.</p>}
          {graphId && artifacts.length === 0 && (
            <p style={hint}>None yet — queue a run and start the worker.</p>
          )}
          {artifacts.length > 0 && (
            <div style={{ marginTop: 4 }}>
              {artifacts.map((a) => (
                <div
                  key={a.key}
                  style={{
                    fontSize: 10,
                    color: "#6e7681",
                    padding: "3px 0",
                    borderBottom: "1px solid #161b22",
                    wordBreak: "break-all",
                    lineHeight: 1.4,
                  }}
                >
                  {a.key}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </aside>
  );
}

/* ── Styles ────────────────────────────────────────────────────────────────── */

const aside: CSSProperties = {
  borderRight: "1px solid #21262d",
  display: "flex",
  flexDirection: "column",
  height: "100%",
  minHeight: 0,
  minWidth: 0,
  width: "100%",
  background: "#0d1117",
  fontSize: 13,
};

const projectCard: CSSProperties = {
  flexShrink: 0,
  margin: "10px 12px 0",
  padding: "12px",
  borderRadius: 12,
  border: "1px solid rgba(56,139,253,0.18)",
  background:
    "linear-gradient(180deg, rgba(22,27,34,0.98) 0%, rgba(13,17,23,0.98) 100%)",
  boxShadow: "0 10px 28px rgba(0,0,0,0.18)",
};

const projectCardHeader: CSSProperties = {
  display: "flex",
  alignItems: "flex-start",
  justifyContent: "space-between",
  gap: 10,
  marginBottom: 8,
};

const projectCardTitle: CSSProperties = {
  fontSize: 15,
  fontWeight: 700,
  color: "#e6edf3",
  lineHeight: 1.2,
};

const projectLiveBadge: CSSProperties = {
  padding: "3px 7px",
  borderRadius: 999,
  background: "rgba(63,185,80,0.12)",
  border: "1px solid rgba(63,185,80,0.28)",
  color: "#3fb950",
  fontSize: 10,
  fontWeight: 700,
  letterSpacing: "0.05em",
  textTransform: "uppercase",
};

const projectMetaRow: CSSProperties = {
  display: "flex",
  flexWrap: "wrap",
  gap: 6,
  marginTop: 10,
};

const projectMetaChip: CSSProperties = {
  padding: "3px 8px",
  borderRadius: 999,
  background: "#11161d",
  border: "1px solid #30363d",
  color: "#8b949e",
  fontSize: 10,
  lineHeight: 1.2,
};

const tabBar: CSSProperties = {
  display: "flex",
  flexShrink: 0,
  borderBottom: "1px solid #21262d",
  background: "#0d1117",
  marginTop: 10,
};

const tabBase: CSSProperties = {
  flex: 1,
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  gap: 6,
  padding: "10px 8px",
  border: "none",
  borderBottom: "2px solid transparent",
  background: "transparent",
  color: "#6e7681",
  cursor: "pointer",
  fontSize: 11,
  fontWeight: 600,
  letterSpacing: "0.04em",
  fontFamily: "inherit",
  transition: "color 0.12s, border-color 0.12s, background 0.12s",
};

const tabBtn: CSSProperties = {
  ...tabBase,
};

const tabBtnActive: CSSProperties = {
  ...tabBase,
  color: "#e6edf3",
  borderBottomColor: "#388bfd",
  background: "rgba(56,139,253,0.04)",
};

const tabContent: CSSProperties = {
  flex: 1,
  display: "flex",
  flexDirection: "column",
  minHeight: 0,
  overflow: "hidden",
};

const sectionLabel: CSSProperties = {
  fontSize: 9.5,
  fontWeight: 700,
  letterSpacing: "0.08em",
  textTransform: "uppercase",
  color: "#484f58",
  marginBottom: 6,
};

const hint: CSSProperties = {
  margin: "0 0 8px",
  color: "#484f58",
  fontSize: 11,
  lineHeight: 1.45,
};

const sel: CSSProperties = {
  width: "100%",
  padding: "6px 8px",
  borderRadius: 6,
  border: "1px solid #30363d",
  background: "#161b22",
  color: "#e6edf3",
  fontSize: 12,
  fontFamily: "inherit",
  cursor: "pointer",
};

const btn: CSSProperties = {
  padding: "5px 10px",
  borderRadius: 6,
  border: "1px solid #30363d",
  background: "#21262d",
  color: "#c9d1d9",
  cursor: "pointer",
  fontSize: 11,
  fontFamily: "inherit",
  transition: "background 0.1s, border-color 0.1s",
};

const inp: CSSProperties = {
  flex: 1,
  minWidth: 0,
  padding: "5px 8px",
  borderRadius: 6,
  border: "1px solid #30363d",
  background: "#161b22",
  color: "#e6edf3",
  fontSize: 12,
  fontFamily: "inherit",
};
