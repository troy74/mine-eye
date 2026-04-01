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

type Props = {
  projects: StoredProject[];
  activeLocalId: string | null;
  onSelectProject: (p: StoredProject) => void;
  onNewProject: () => void;
  onSeedDemo: () => void;
  graphId: string | null;
  projectEpsg: number;
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
};

export function LeftSidebar({
  projects,
  activeLocalId,
  onSelectProject,
  onNewProject,
  onSeedDemo,
  graphId,
  projectEpsg,
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
}: Props) {
  const [artifactsOpen, setArtifactsOpen] = useState(true);
  const [branchesOpen, setBranchesOpen] = useState(true);
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
    <aside
      style={{
        borderRight: "1px solid #30363d",
        display: "flex",
        flexDirection: "column",
        height: "100%",
        minHeight: 0,
        minWidth: 0,
        width: "100%",
        background: "#0d1117",
        fontSize: 13,
      }}
    >
      <section style={section}>
        <div style={sectionTitle}>Project</div>
        <p style={hint}>
          A project is this workspace: graph, node configs, execution cache (artifacts), and local
          agent history.
        </p>
        <select
          style={selectStyle}
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
        <div style={{ display: "flex", gap: 8, marginTop: 10, flexWrap: "wrap" }}>
          <button type="button" style={btnSecondary} onClick={onNewProject}>
            New project…
          </button>
          <button type="button" style={btnSecondary} onClick={onSeedDemo}>
            Seed demo
          </button>
        </div>
        {active && (
          <div style={{ marginTop: 10, fontSize: 11, opacity: 0.55, wordBreak: "break-all" }}>
            Graph <code>{active.graphId.slice(0, 8)}…</code>
          </div>
        )}
        {graphId && (
          <div style={{ marginTop: 10 }}>
            <label style={labTiny}>Project CRS</label>
            <CrsPicker
              value={projectCrsValue}
              onChange={(v) => {
                setProjectCrsValue(v);
                const epsg = v === "project" ? projectEpsg : parseInt(v, 10);
                if (Number.isFinite(epsg) && epsg > 0) onSetProjectCrs(epsg);
              }}
              projectEpsg={projectEpsg}
              includeProject
            />
            <div style={{ fontSize: 10, opacity: 0.6, marginTop: 4 }}>
              Current: EPSG:{projectEpsg}
            </div>
          </div>
        )}
      </section>

      <AgentChat
        projectLocalId={activeLocalId}
        projectName={active?.name ?? ""}
      />

      <section style={{ ...section, flexShrink: 0, maxHeight: "32%", display: "flex", flexDirection: "column", minHeight: 0 }}>
        <button
          type="button"
          onClick={() => setArtifactsOpen((o) => !o)}
          style={collapseHead}
        >
          <span>Artifacts</span>
          <span style={{ opacity: 0.6 }}>{artifactsOpen ? "▼" : "▶"}</span>
        </button>
        {artifactsOpen && (
          <div style={{ overflow: "auto", flex: 1, minHeight: 0, paddingTop: 6 }}>
            {!graphId && (
              <p style={{ ...hint, marginTop: 0 }}>Load a project with a graph to see artifacts.</p>
            )}
            {graphId && artifacts.length === 0 && (
              <p style={{ ...hint, marginTop: 0 }}>None yet — queue a run and start the worker.</p>
            )}
            <ul style={{ margin: 0, paddingLeft: 18, fontSize: 11 }}>
              {artifacts.map((a) => (
                <li key={a.key} style={{ wordBreak: "break-all", marginBottom: 4 }}>
                  {a.key}
                </li>
              ))}
            </ul>
          </div>
        )}
      </section>

      <section style={{ ...section, flexShrink: 0, maxHeight: "34%", display: "flex", flexDirection: "column", minHeight: 0 }}>
        <button
          type="button"
          onClick={() => setBranchesOpen((o) => !o)}
          style={collapseHead}
        >
          <span>Branches & Revisions</span>
          <span style={{ opacity: 0.6 }}>{branchesOpen ? "▼" : "▶"}</span>
        </button>
        {branchesOpen && (
          <div style={{ overflow: "auto", flex: 1, minHeight: 0, paddingTop: 6 }}>
            {!graphId ? (
              <p style={{ ...hint, marginTop: 0 }}>Load a project to manage branches.</p>
            ) : (
              <>
                <label style={labTiny}>Active branch</label>
                <select
                  style={selectStyle}
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
                <div style={{ display: "flex", gap: 6, marginTop: 8 }}>
                  <input
                    value={newBranchName}
                    onChange={(e) => setNewBranchName(e.target.value)}
                    placeholder="new branch name"
                    style={inputMini}
                  />
                  <button
                    type="button"
                    style={btnSecondary}
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
                <div style={{ display: "flex", gap: 6, marginTop: 8, flexWrap: "wrap" }}>
                  <button
                    type="button"
                    style={btnSecondary}
                    disabled={!activeBranch}
                    onClick={() =>
                      activeBranch && onCommitCurrentToBranch(activeBranch.id, "ui_commit_current")
                    }
                  >
                    Commit current
                  </button>
                  <button
                    type="button"
                    style={btnSecondary}
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
                <div style={{ marginTop: 10, fontSize: 11, opacity: 0.75 }}>
                  Revisions: {revisions.length} • Promotions: {promotions.length}
                </div>
                <ul style={{ margin: "8px 0 0", paddingLeft: 18, fontSize: 11 }}>
                  {promotions.slice(0, 6).map((p) => (
                    <li key={p.id} style={{ marginBottom: 4, wordBreak: "break-word" }}>
                      {p.status} • {new Date(p.created_at).toLocaleString()}
                      {p.conflict_report && (
                        <button
                          type="button"
                          style={{ ...btnSecondary, marginLeft: 6, padding: "2px 6px" }}
                          onClick={() =>
                            void navigator.clipboard.writeText(
                              JSON.stringify(p.conflict_report, null, 2)
                            )
                          }
                        >
                          Copy conflict
                        </button>
                      )}
                    </li>
                  ))}
                </ul>
                <div style={{ marginTop: 10, borderTop: "1px solid #30363d", paddingTop: 8 }}>
                  <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 6 }}>
                    Revision Diff
                  </div>
                  <select
                    style={selectStyle}
                    value={fromRevisionId}
                    onChange={(e) => setFromRevisionId(e.target.value)}
                  >
                    <option value="">From revision…</option>
                    {revisions.map((r) => (
                      <option key={r.id} value={r.id}>
                        {r.id.slice(0, 8)}… ({r.created_by})
                      </option>
                    ))}
                  </select>
                  <select
                    style={{ ...selectStyle, marginTop: 6 }}
                    value={toRevisionId}
                    onChange={(e) => setToRevisionId(e.target.value)}
                  >
                    <option value="">To revision…</option>
                    {revisions.map((r) => (
                      <option key={r.id} value={r.id}>
                        {r.id.slice(0, 8)}… ({r.created_by})
                      </option>
                    ))}
                  </select>
                  <button
                    type="button"
                    style={{ ...btnSecondary, marginTop: 6 }}
                    disabled={!fromRevisionId || !toRevisionId}
                    onClick={() =>
                      fromRevisionId && toRevisionId && onDiffRevisions(fromRevisionId, toRevisionId)
                    }
                  >
                    Compare
                  </button>
                  {revisionDiff && (
                    <div style={{ marginTop: 8, fontSize: 11, opacity: 0.85 }}>
                      Δ nodes +{revisionDiff.diff.summary.nodes_added}/-{revisionDiff.diff.summary.nodes_removed} ~
                      {revisionDiff.diff.summary.nodes_changed}, edges +{revisionDiff.diff.summary.edges_added}/-
                      {revisionDiff.diff.summary.edges_removed}
                    </div>
                  )}
                </div>
              </>
            )}
          </div>
        )}
      </section>
    </aside>
  );
}

const section: CSSProperties = {
  padding: "12px 12px 10px",
  flexShrink: 0,
};
const sectionTitle: CSSProperties = {
  fontWeight: 600,
  fontSize: 13,
  marginBottom: 6,
};
const hint: CSSProperties = {
  margin: "0 0 10px",
  opacity: 0.65,
  fontSize: 11,
  lineHeight: 1.45,
};
const selectStyle: CSSProperties = {
  width: "100%",
  padding: "8px 10px",
  borderRadius: 6,
  border: "1px solid #30363d",
  background: "#161b22",
  color: "#e6edf3",
  fontSize: 13,
};
const btnSecondary: CSSProperties = {
  padding: "6px 10px",
  borderRadius: 6,
  border: "1px solid #30363d",
  background: "#21262d",
  color: "#e6edf3",
  cursor: "pointer",
  fontSize: 12,
};
const collapseHead: CSSProperties = {
  width: "100%",
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  padding: "8px 0",
  border: "none",
  borderTop: "1px solid #30363d",
  background: "transparent",
  color: "#e6edf3",
  cursor: "pointer",
  fontWeight: 600,
  fontSize: 13,
};

const labTiny: CSSProperties = {
  fontSize: 10,
  opacity: 0.65,
  marginBottom: 4,
  display: "block",
};

const inputMini: CSSProperties = {
  flex: 1,
  minWidth: 0,
  padding: "6px 8px",
  borderRadius: 6,
  border: "1px solid #30363d",
  background: "#161b22",
  color: "#e6edf3",
  fontSize: 12,
};
