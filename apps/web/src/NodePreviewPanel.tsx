import { useCallback, useEffect, useMemo, useState } from "react";
import { api, type ArtifactEntry } from "./graphApi";

type Props = {
  graphId: string | null;
  nodeId: string;
  nodeKind: string;
  artifacts: ArtifactEntry[];
};

type MdViewDoc = {
  type?: string;
  title?: string;
  markdown?: string;
  html?: string;
  source_artifact_key?: string;
};

type ChartViewDoc = {
  type?: string;
  title?: string;
  html?: string;
};

const MAX_FETCH_BYTES = 1_500_000;
const MAX_PARSE_CHARS = 900_000;
const MAX_RAW_PREVIEW_CHARS = 120_000;

async function readTextCapped(
  resp: Response,
  maxBytes: number
): Promise<{ text: string; truncated: boolean; totalHint?: number }> {
  const contentLen = Number(resp.headers.get("content-length") ?? "");
  const totalHint = Number.isFinite(contentLen) && contentLen > 0 ? contentLen : undefined;
  if (!resp.body) {
    const text = await resp.text();
    if (text.length > maxBytes) return { text: text.slice(0, maxBytes), truncated: true, totalHint };
    return { text, truncated: false, totalHint };
  }
  const reader = resp.body.getReader();
  const chunks: Uint8Array[] = [];
  let read = 0;
  let truncated = false;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (!value) continue;
    if (read + value.byteLength > maxBytes) {
      const remain = Math.max(0, maxBytes - read);
      if (remain > 0) chunks.push(value.slice(0, remain));
      read += remain;
      truncated = true;
      await reader.cancel();
      break;
    }
    chunks.push(value);
    read += value.byteLength;
  }
  const merged = new Uint8Array(read);
  let off = 0;
  for (const c of chunks) {
    merged.set(c, off);
    off += c.byteLength;
  }
  return { text: new TextDecoder().decode(merged), truncated, totalHint };
}

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
  const [mdDoc, setMdDoc] = useState<MdViewDoc | null>(null);
  const [chartDoc, setChartDoc] = useState<ChartViewDoc | null>(null);
  const [shareMsg, setShareMsg] = useState<string | null>(null);
  const [previewNote, setPreviewNote] = useState<string | null>(null);

  const isMdViewer = nodeKind === "md_viewer";
  const isChartViewer = nodeKind === "plot_chart";

  useEffect(() => {
    const first = nodeArtifacts.find((a) => a.key.endsWith(".json")) ?? nodeArtifacts[0];
    setSelectedKey(first?.key ?? "");
  }, [nodeArtifacts]);

  useEffect(() => {
    if (!selectedKey || !graphId) {
      setRawText("");
      setRows([]);
      setMdDoc(null);
      setChartDoc(null);
      setPreviewNote(null);
      return;
    }
    const entry = nodeArtifacts.find((a) => a.key === selectedKey);
    if (!entry) return;
    let cancelled = false;
    setBusy(true);
    setErr(null);
    setPreviewNote(null);
    void (async () => {
      try {
        const r = await fetch(api(entry.url));
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const { text, truncated, totalHint } = await readTextCapped(r, MAX_FETCH_BYTES);
        if (cancelled) return;
        if (truncated) {
          setPreviewNote(
            `Large artifact preview capped at ${MAX_FETCH_BYTES.toLocaleString()} bytes${totalHint ? ` (source ~${totalHint.toLocaleString()} bytes)` : ""}. Use Save/Share for full content.`
          );
        }
        setRawText(
          truncated || text.length > MAX_RAW_PREVIEW_CHARS
            ? `${text.slice(0, MAX_RAW_PREVIEW_CHARS)}\n\n… preview truncated`
            : text
        );
        try {
          if (truncated || text.length > MAX_PARSE_CHARS) {
            setRows([]);
            setMdDoc(null);
            setChartDoc(null);
            return;
          }
          const root = JSON.parse(text) as unknown;
          setRows(extractRows(root).slice(0, 2_000));
          if (
            root &&
            typeof root === "object" &&
            !Array.isArray(root) &&
            ((root as Record<string, unknown>).type === "md_view_doc" ||
              (root as Record<string, unknown>).schema_id === "report.markdown_doc.v1" ||
              (root as Record<string, unknown>).schema_id === "report.markdown_doc.v2")
          ) {
            setMdDoc(root as MdViewDoc);
            setChartDoc(null);
          } else if (
            root &&
            typeof root === "object" &&
            !Array.isArray(root) &&
            ((root as Record<string, unknown>).type === "chart_view_doc" ||
              (root as Record<string, unknown>).schema_id === "report.chart_doc.v1")
          ) {
            setChartDoc(root as ChartViewDoc);
            setMdDoc(null);
          } else {
            setMdDoc(null);
            setChartDoc(null);
          }
        } catch {
          setRows([]);
          setMdDoc(null);
          setChartDoc(null);
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

  const fileBySuffix = useCallback(
    (suffix: string) => nodeArtifacts.find((a) => a.key.endsWith(suffix)) ?? null,
    [nodeArtifacts]
  );

  const downloadText = useCallback((filename: string, text: string, mime: string) => {
    const blob = new Blob([text], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }, []);

  const fetchArtifactText = useCallback(async (entry: ArtifactEntry | null): Promise<string | null> => {
    if (!entry) return null;
    const r = await fetch(api(entry.url));
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const { text } = await readTextCapped(r, 12_000_000);
    return text;
  }, []);

  const saveMd = useCallback(async () => {
    setShareMsg(null);
    const mdEntry = fileBySuffix(".md");
    const text = (await fetchArtifactText(mdEntry)) ?? mdDoc?.markdown ?? rawText;
    if (!text) return;
    downloadText("report.md", text, "text/markdown");
  }, [downloadText, fetchArtifactText, fileBySuffix, mdDoc?.markdown, rawText]);

  const saveHtml = useCallback(async () => {
    setShareMsg(null);
    const htmlEntry = fileBySuffix(".html");
    const text = (await fetchArtifactText(htmlEntry)) ?? mdDoc?.html ?? "<html><body><pre>No HTML</pre></body></html>";
    downloadText("report.html", text, "text/html");
  }, [downloadText, fetchArtifactText, fileBySuffix, mdDoc?.html]);

  const shareArtifact = useCallback(async () => {
    const htmlEntry = fileBySuffix(".html");
    const target = htmlEntry ?? nodeArtifacts.find((a) => a.key === selectedKey) ?? null;
    if (!target) return;
    const absolute = `${window.location.origin}${api(target.url)}`;
    try {
      await navigator.clipboard.writeText(absolute);
      setShareMsg("Share URL copied.");
    } catch {
      setShareMsg(absolute);
    }
  }, [fileBySuffix, nodeArtifacts, selectedKey]);

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
        {isMdViewer || isChartViewer ? (
          <>
            {isMdViewer ? (
              <button type="button" onClick={() => void saveMd()}>
                Save .md
              </button>
            ) : null}
            <button type="button" onClick={() => void saveHtml()}>
              Save .html
            </button>
            <button type="button" onClick={() => void shareArtifact()}>
              Share
            </button>
          </>
        ) : null}
      </div>
      {shareMsg ? (
        <div style={{ padding: "6px 10px", fontSize: 11, color: "#3fb950", borderBottom: "1px solid #30363d" }}>
          {shareMsg}
        </div>
      ) : null}
      {previewNote ? (
        <div style={{ padding: "6px 10px", fontSize: 11, color: "#9fb3c8", borderBottom: "1px solid #30363d" }}>
          {previewNote}
        </div>
      ) : null}
      {busy && <div style={{ padding: 10, fontSize: 12 }}>Loading preview…</div>}
      {err && <div style={{ padding: 10, color: "#f85149", fontSize: 12 }}>{err}</div>}
      {!busy && !err && (
        <div style={{ flex: 1, minHeight: 0, overflow: "auto", padding: 10 }}>
          {(isMdViewer && (mdDoc?.html || selectedKey.endsWith(".html"))) ||
          (isChartViewer && (chartDoc?.html || selectedKey.endsWith(".html"))) ? (
            <iframe
              title={isChartViewer ? "Chart preview" : "Markdown report preview"}
              sandbox="allow-same-origin allow-scripts"
              style={{ width: "100%", minHeight: "70vh", border: "1px solid #30363d", borderRadius: 8, background: "#fff" }}
              srcDoc={chartDoc?.html ?? mdDoc?.html ?? rawText}
            />
          ) : filteredRows.length > 0 ? (
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
