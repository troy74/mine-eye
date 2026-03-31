import { useMemo, useState, type CSSProperties } from "react";
import { AgentChat } from "./AgentChat";
import type { ArtifactEntry } from "./graphApi";
import type { StoredProject } from "./projectStorage";

type Props = {
  projects: StoredProject[];
  activeLocalId: string | null;
  onSelectProject: (p: StoredProject) => void;
  onNewProject: () => void;
  onSeedDemo: () => void;
  graphId: string | null;
  artifacts: ArtifactEntry[];
};

export function LeftSidebar({
  projects,
  activeLocalId,
  onSelectProject,
  onNewProject,
  onSeedDemo,
  graphId,
  artifacts,
}: Props) {
  const [artifactsOpen, setArtifactsOpen] = useState(true);
  const active = useMemo(
    () => projects.find((p) => p.localId === activeLocalId) ?? null,
    [projects, activeLocalId]
  );

  return (
    <aside
      style={{
        borderRight: "1px solid #30363d",
        display: "flex",
        flexDirection: "column",
        height: "100%",
        minHeight: 0,
        minWidth: 260,
        maxWidth: 400,
        width: "28vw",
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
