import { useCallback, useEffect, useMemo, useState, type CSSProperties } from "react";
import { api, runGraph, type ArtifactEntry } from "./graphApi";
import {
  defaultArtifactForKind,
  isLikelyBinaryKey,
  isLikelyJsonKey,
} from "./nodeArtifactPreview";

const MAX_TEXT_PREVIEW = 96_000;

type Props = {
  graphId: string;
  nodeId: string;
  kind: string;
  artifacts: ArtifactEntry[];
  onQueued: () => void;
};

export function NodeOutputPanel({
  graphId,
  nodeId,
  kind,
  artifacts,
  onQueued,
}: Props) {
  const sorted = useMemo(
    () => [...artifacts].sort((a, b) => a.key.localeCompare(b.key)),
    [artifacts]
  );

  const defaultArt = useMemo(
    () => defaultArtifactForKind(sorted, kind),
    [sorted, kind]
  );

  const [selectedKey, setSelectedKey] = useState<string | null>(
    () => defaultArt?.key ?? null
  );

  useEffect(() => {
    const d = defaultArtifactForKind(sorted, kind);
    setSelectedKey(d?.key ?? sorted[0]?.key ?? null);
  }, [nodeId, kind, sorted]);

  const selected = sorted.find((a) => a.key === selectedKey) ?? null;

  const [previewText, setPreviewText] = useState<string | null>(null);
  const [previewHtml, setPreviewHtml] = useState<string | null>(null);
  const [previewNote, setPreviewNote] = useState<string | null>(null);
  const [loadErr, setLoadErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!selected) {
      setPreviewText(null);
      setPreviewHtml(null);
      setPreviewNote(null);
      setLoadErr(null);
      return;
    }

    let cancelled = false;
    setLoadErr(null);
    setPreviewNote(null);
    setPreviewHtml(null);

    if (isLikelyBinaryKey(selected.key)) {
      setPreviewText(null);
      setPreviewHtml(null);
      setLoading(true);
      void (async () => {
        try {
          const r = await fetch(api(selected.url), { cache: "no-store" });
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          const buf = await r.arrayBuffer();
          if (cancelled) return;
          const n = buf.byteLength;
          const u8 = new Uint8Array(buf.slice(0, Math.min(256, n)));
          const hex = [...u8].map((b) => b.toString(16).padStart(2, "0")).join(" ");
          setPreviewNote(
            `Binary artifact (${n.toLocaleString()} bytes). First ${u8.length} bytes (hex):\n${hex}`
          );
        } catch (e) {
          if (!cancelled) {
            setLoadErr(e instanceof Error ? e.message : String(e));
          }
        } finally {
          if (!cancelled) setLoading(false);
        }
      })();
      return () => {
        cancelled = true;
      };
    }

    if (!isLikelyJsonKey(selected.key)) {
      setLoading(true);
      setPreviewText(null);
      setPreviewHtml(null);
      if (selected.key.toLowerCase().endsWith(".html")) {
        void (async () => {
          try {
            const r = await fetch(api(selected.url), { cache: "no-store" });
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            const raw = await r.text();
            if (cancelled) return;
            setPreviewHtml(raw);
            setPreviewNote(null);
          } catch (e) {
            if (!cancelled) {
              setLoadErr(e instanceof Error ? e.message : String(e));
            }
          } finally {
            if (!cancelled) setLoading(false);
          }
        })();
      } else {
        setPreviewNote(
          "Preview is optimized for JSON/HTML. Open the path from the Artifacts list or fetch this URL in another tool."
        );
        setLoading(false);
      }
      return;
    }

    setLoading(true);
    setPreviewText(null);
    setPreviewHtml(null);
    void (async () => {
      try {
        const r = await fetch(api(selected.url), { cache: "no-store" });
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const raw = await r.text();
        if (cancelled) return;
        let display = raw;
        if (raw.length > MAX_TEXT_PREVIEW) {
          display =
            raw.slice(0, MAX_TEXT_PREVIEW) +
            `\n\n… truncated (${raw.length.toLocaleString()} chars total)`;
        }
        try {
          const parsed = JSON.parse(raw) as unknown;
          if (
            parsed &&
            typeof parsed === "object" &&
            !Array.isArray(parsed) &&
            (((parsed as Record<string, unknown>).type === "chart_view_doc" ||
              (parsed as Record<string, unknown>).schema_id === "report.chart_doc.v1" ||
              (parsed as Record<string, unknown>).type === "md_view_doc" ||
              (parsed as Record<string, unknown>).schema_id === "report.markdown_doc.v2") &&
              typeof (parsed as Record<string, unknown>).html === "string")
          ) {
            setPreviewHtml(String((parsed as Record<string, unknown>).html));
          }
          display = JSON.stringify(parsed, null, 2);
          if (display.length > MAX_TEXT_PREVIEW) {
            display =
              display.slice(0, MAX_TEXT_PREVIEW) +
              `\n\n… truncated (${display.length.toLocaleString()} chars when formatted)`;
          }
        } catch {
          /* keep raw */
        }
        setPreviewText(display);
      } catch (e) {
        if (!cancelled) {
          setLoadErr(e instanceof Error ? e.message : String(e));
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [selected]);

  const [runErr, setRunErr] = useState<string | null>(null);
  const [runOk, setRunOk] = useState<string | null>(null);
  const [runBusy, setRunBusy] = useState(false);

  const queueFromHere = useCallback(async () => {
    setRunErr(null);
    setRunOk(null);
    setRunBusy(true);
    try {
      const res = await runGraph(graphId, { dirtyRoots: [nodeId], includeManual: true });
      const nq = res.queued?.length ?? 0;
      const ns = res.skipped_manual?.length ?? 0;
      if (nq === 0) {
        setRunOk(
          ns > 0
            ? `No jobs queued (${ns} node(s) use manual recompute).`
            : "No jobs queued (nothing to run for this subgraph)."
        );
      } else {
        setRunOk(
          ns > 0
            ? `Queued ${nq} job(s); ${ns} skipped (manual policy).`
            : `Queued ${nq} job(s).`
        );
      }
      onQueued();
    } catch (e) {
      setRunErr(e instanceof Error ? e.message : String(e));
    } finally {
      setRunBusy(false);
    }
  }, [graphId, nodeId, onQueued]);

  if (sorted.length === 0) {
    return (
      <div style={{ lineHeight: 1.55 }}>
        <p style={{ opacity: 0.8, marginTop: 0 }}>
          No artifacts for this node yet. Queue a run (header or below), run the worker, then
          refresh.
        </p>
        <QueueButton busy={runBusy} onClick={queueFromHere} />
        {runOk && <p style={{ color: "#3fb950", marginTop: 10, fontSize: 11 }}>{runOk}</p>}
        {runErr && <p style={{ color: "#f85149", marginTop: 10 }}>{runErr}</p>}
      </div>
    );
  }

  return (
    <div style={{ lineHeight: 1.5 }}>
      <p style={{ opacity: 0.75, fontSize: 11, marginTop: 0 }}>
        Artifacts written by the worker for <strong>{kind.replace(/_/g, " ")}</strong>. Default
        pick matches the usual output for this node kind.
      </p>

      <label style={lab}>
        <span style={labSpan}>Artifact</span>
        <select
          value={selectedKey ?? ""}
          onChange={(e) => setSelectedKey(e.target.value || null)}
          style={sel}
        >
          {sorted.map((a) => (
            <option key={a.key} value={a.key}>
              {a.key.split("/").pop() ?? a.key}
            </option>
          ))}
        </select>
      </label>

      <div style={{ fontSize: 10, opacity: 0.55, marginBottom: 8, wordBreak: "break-all" }}>
        {selected?.key}
      </div>

      {loading && <p style={{ opacity: 0.7 }}>Loading…</p>}
      {loadErr && <p style={{ color: "#f85149" }}>{loadErr}</p>}
      {previewNote && !previewText && (
        <pre style={preBox}>{previewNote}</pre>
      )}
      {previewHtml ? (
        <iframe
          title="Artifact preview"
          sandbox="allow-same-origin allow-scripts"
          style={{ width: "100%", minHeight: "68vh", border: "1px solid #30363d", borderRadius: 8, background: "#fff", marginBottom: 10 }}
          srcDoc={previewHtml}
        />
      ) : null}
      {previewText && <pre style={preBox}>{previewText}</pre>}

      <div style={{ marginTop: 14, paddingTop: 12, borderTop: "1px solid #30363d" }}>
        <div style={{ fontSize: 11, opacity: 0.75, marginBottom: 8 }}>
          <strong>Queue run from this node</strong> — marks this node and everything downstream
          dirty and enqueues jobs (same as a full run from the header, but scoped).
        </div>
        <QueueButton busy={runBusy} onClick={queueFromHere} />
        {runOk && <p style={{ color: "#3fb950", marginTop: 8, fontSize: 11 }}>{runOk}</p>}
        {runErr && <p style={{ color: "#f85149", marginTop: 8, fontSize: 11 }}>{runErr}</p>}
      </div>
    </div>
  );
}

function QueueButton({
  busy,
  onClick,
}: {
  busy: boolean;
  onClick: () => void | Promise<void>;
}) {
  return (
    <button
      type="button"
      disabled={busy}
      onClick={() => void onClick()}
      style={{
        ...btnRun,
        opacity: busy ? 0.65 : 1,
        cursor: busy ? "not-allowed" : "pointer",
      }}
    >
      {busy ? "Queuing…" : "Queue downstream from this node"}
    </button>
  );
}

const lab: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  gap: 4,
  marginBottom: 10,
  fontSize: 11,
};
const labSpan: CSSProperties = { opacity: 0.75 };
const sel: CSSProperties = {
  background: "#0f1419",
  color: "#e6edf3",
  border: "1px solid #30363d",
  borderRadius: 6,
  padding: "6px 8px",
  fontSize: 12,
  maxWidth: "100%",
};
const preBox: CSSProperties = {
  margin: 0,
  padding: 10,
  background: "#0d1117",
  border: "1px solid #30363d",
  borderRadius: 6,
  fontSize: 10,
  lineHeight: 1.45,
  overflow: "auto",
  maxHeight: 320,
  whiteSpace: "pre-wrap",
  wordBreak: "break-word",
};
const btnRun: CSSProperties = {
  background: "#1f6feb",
  border: "none",
  color: "#fff",
  borderRadius: 6,
  padding: "8px 12px",
  cursor: "pointer",
  fontSize: 12,
  fontWeight: 600,
  width: "100%",
};
