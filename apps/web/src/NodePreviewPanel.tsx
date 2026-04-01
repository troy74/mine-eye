import { useEffect, useMemo, useState } from "react";
import { api, type ArtifactEntry } from "./graphApi";

type Props = {
  graphId: string | null;
  nodeId: string;
  nodeKind: string;
  artifacts: ArtifactEntry[];
};

function findNodeArtifacts(artifacts: ArtifactEntry[], nodeId: string): ArtifactEntry[] {
  return artifacts
    .filter((a) => a.node_id === nodeId)
    .sort((a, b) => a.key.localeCompare(b.key));
}

function extractRows(root: unknown): Record<string, unknown>[] {
  if (Array.isArray(root)) {
    return root.filter((r): r is Record<string, unknown> => !!r && typeof r === "object");
  }
  if (!root || typeof root !== "object") return [];
  const obj = root as Record<string, unknown>;
  const candidates = ["collars", "surveys", "assays", "points", "assay_points"];
  for (const k of candidates) {
    const v = obj[k];
    if (Array.isArray(v)) {
      return v.filter((r): r is Record<string, unknown> => !!r && typeof r === "object");
    }
  }
  return [];
}

export function NodePreviewPanel({ graphId, nodeId, nodeKind, artifacts }: Props) {
  const nodeArtifacts = useMemo(
    () => findNodeArtifacts(artifacts, nodeId),
    [artifacts, nodeId]
  );
  const [selectedKey, setSelectedKey] = useState<string>("");
  const [rawText, setRawText] = useState<string>("");
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  useEffect(() => {
    const first = nodeArtifacts.find((a) => a.key.endsWith(".json")) ?? nodeArtifacts[0];
    setSelectedKey(first?.key ?? "");
  }, [nodeArtifacts]);

  useEffect(() => {
    if (!selectedKey || !graphId) {
      setRawText("");
      setRows([]);
      return;
    }
    const entry = nodeArtifacts.find((a) => a.key === selectedKey);
    if (!entry) return;
    let cancelled = false;
    setBusy(true);
    setErr(null);
    void (async () => {
      try {
        const r = await fetch(api(entry.url));
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const text = await r.text();
        if (cancelled) return;
        setRawText(text);
        try {
          const root = JSON.parse(text) as unknown;
          setRows(extractRows(root));
        } catch {
          setRows([]);
        }
      } catch (e) {
        if (!cancelled) setErr(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setBusy(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [selectedKey, nodeArtifacts, graphId]);

  const filteredRows = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter((r) => JSON.stringify(r).toLowerCase().includes(q));
  }, [rows, filter]);

  const cols = useMemo(() => {
    const keys = new Set<string>();
    filteredRows.slice(0, 200).forEach((r) => {
      Object.keys(r).forEach((k) => keys.add(k));
    });
    return [...keys];
  }, [filteredRows]);

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", minHeight: 0 }}>
      <div
        style={{
          padding: "10px 12px",
          borderBottom: "1px solid #30363d",
          display: "flex",
          gap: 10,
          alignItems: "center",
          flexWrap: "wrap",
        }}
      >
        <strong>{nodeKind.replace(/_/g, " ")} preview</strong>
        <span style={{ opacity: 0.7, fontSize: 12 }}>node {nodeId.slice(0, 8)}…</span>
        <select
          value={selectedKey}
          onChange={(e) => setSelectedKey(e.target.value)}
          style={{ marginLeft: "auto", minWidth: 240 }}
        >
          {nodeArtifacts.length === 0 ? (
            <option value="">No artifacts</option>
          ) : (
            nodeArtifacts.map((a) => (
              <option key={a.key} value={a.key}>
                {a.key.split("/").pop()}
              </option>
            ))
          )}
        </select>
        <input
          type="text"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter rows…"
        />
      </div>
      {busy && <div style={{ padding: 10, fontSize: 12 }}>Loading preview…</div>}
      {err && <div style={{ padding: 10, color: "#f85149", fontSize: 12 }}>{err}</div>}
      {!busy && !err && (
        <div style={{ flex: 1, minHeight: 0, overflow: "auto", padding: 10 }}>
          {filteredRows.length > 0 ? (
            <table style={{ borderCollapse: "collapse", width: "100%", fontSize: 12 }}>
              <thead>
                <tr>
                  {cols.map((c) => (
                    <th
                      key={c}
                      style={{
                        textAlign: "left",
                        position: "sticky",
                        top: 0,
                        background: "#161b22",
                        borderBottom: "1px solid #30363d",
                        padding: 6,
                      }}
                    >
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filteredRows.slice(0, 500).map((r, i) => (
                  <tr key={i}>
                    {cols.map((c) => (
                      <td
                        key={c}
                        style={{ borderBottom: "1px solid #21262d", padding: 6, maxWidth: 260 }}
                      >
                        {typeof r[c] === "object"
                          ? JSON.stringify(r[c])
                          : String(r[c] ?? "")}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <pre
              style={{
                margin: 0,
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                fontSize: 12,
                lineHeight: 1.4,
              }}
            >
              {rawText || "No data."}
            </pre>
          )}
        </div>
      )}
    </div>
  );
}

