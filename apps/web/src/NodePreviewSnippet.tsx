import { useEffect, useState } from "react";
import { api, type ArtifactEntry } from "./graphApi";
import { defaultArtifactForKind, isLikelyJsonKey } from "./nodeArtifactPreview";

type Props = {
  graphId: string;
  nodeId: string;
  kind: string;
  artifacts: ArtifactEntry[];
};

const MAX = 2200;

export function NodePreviewSnippet({ graphId, nodeId, kind, artifacts }: Props) {
  const [text, setText] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const mine = artifacts.filter((a) => a.node_id === nodeId);
  const art = defaultArtifactForKind(mine, kind);

  useEffect(() => {
    if (!art || !isLikelyJsonKey(art.key)) {
      setText(null);
      setErr(null);
      return;
    }
    let cancelled = false;
    setErr(null);
    void (async () => {
      try {
        const r = await fetch(api(art.url));
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const raw = await r.text();
        if (cancelled) return;
        let display = raw;
        try {
          display = JSON.stringify(JSON.parse(raw) as unknown, null, 2);
        } catch {
          /* raw */
        }
        if (display.length > MAX) {
          display = display.slice(0, MAX) + "\n…";
        }
        setText(display);
      } catch (e) {
        if (!cancelled) {
          setErr(e instanceof Error ? e.message : String(e));
          setText(null);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [graphId, nodeId, kind, art?.key, art?.url]);

  if (!art) {
    return (
      <p style={{ fontSize: 11, opacity: 0.65, marginTop: 12 }}>
        No artifact to preview yet. Use the <strong>Output</strong> tab after the worker has run.
      </p>
    );
  }

  return (
    <div style={{ marginTop: 14 }}>
      <div style={{ fontSize: 11, opacity: 0.75, marginBottom: 6 }}>
        Primary output preview · <code style={{ fontSize: 10 }}>{art.key.split("/").pop()}</code>
      </div>
      {err && <p style={{ color: "#f85149", fontSize: 11 }}>{err}</p>}
      {text && (
        <pre
          style={{
            margin: 0,
            padding: 8,
            background: "#0d1117",
            border: "1px solid #30363d",
            borderRadius: 6,
            fontSize: 9,
            lineHeight: 1.4,
            maxHeight: 160,
            overflow: "auto",
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
          }}
        >
          {text}
        </pre>
      )}
    </div>
  );
}
