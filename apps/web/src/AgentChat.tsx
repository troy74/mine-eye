import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
} from "react";

const CHAT_KEY = (projectLocalId: string) => `mineeye:chat:v1:${projectLocalId}`;

export type ChatAttachment = {
  name: string;
  mime: string;
  size: number;
  dataUrl?: string;
};

export type ChatMessage = {
  id: string;
  role: "user" | "assistant";
  text: string;
  at: number;
  attachments?: ChatAttachment[];
};

function loadMessages(projectLocalId: string | null): ChatMessage[] {
  if (!projectLocalId) return [];
  try {
    const raw = localStorage.getItem(CHAT_KEY(projectLocalId));
    if (!raw) return [];
    const v = JSON.parse(raw) as ChatMessage[];
    return Array.isArray(v) ? v : [];
  } catch {
    return [];
  }
}

function saveMessages(projectLocalId: string, messages: ChatMessage[]) {
  localStorage.setItem(CHAT_KEY(projectLocalId), JSON.stringify(messages));
}

type Props = {
  projectLocalId: string | null;
  projectName: string;
};

export function AgentChat({ projectLocalId, projectName }: Props) {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [draft, setDraft] = useState("");
  const [fileErr, setFileErr] = useState<string | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const fileRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    setMessages(loadMessages(projectLocalId));
  }, [projectLocalId]);

  useEffect(() => {
    const el = scrollRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [messages]);

  const onPickFiles = useCallback(
    async (files: FileList | null) => {
      if (!files?.length || !projectLocalId) return;
      setFileErr(null);
      const list = [...files].slice(0, 6);
      const attachments: ChatAttachment[] = [];
      for (const f of list) {
        if (f.size > 4 * 1024 * 1024) {
          setFileErr(`${f.name} is too large (max 4 MB per file).`);
          return;
        }
        if (f.type.startsWith("image/")) {
          const dataUrl = await new Promise<string>((res, rej) => {
            const r = new FileReader();
            r.onload = () => res(String(r.result ?? ""));
            r.onerror = () => rej(new Error("read"));
            r.readAsDataURL(f);
          });
          attachments.push({
            name: f.name,
            mime: f.type,
            size: f.size,
            dataUrl,
          });
        } else {
          attachments.push({
            name: f.name,
            mime: f.type || "application/octet-stream",
            size: f.size,
          });
        }
      }
      const msg: ChatMessage = {
        id: crypto.randomUUID(),
        role: "user",
        text: "",
        at: Date.now(),
        attachments,
      };
      setMessages((prev) => {
        const next = [...prev, msg];
        saveMessages(projectLocalId, next);
        return next;
      });
    },
    [projectLocalId]
  );

  const sendText = useCallback(() => {
    const t = draft.trim();
    if (!t || !projectLocalId) return;
    const userMsg: ChatMessage = {
      id: crypto.randomUUID(),
      role: "user",
      text: t,
      at: Date.now(),
    };
    setMessages((prev) => {
      const next = [...prev, userMsg];
      saveMessages(projectLocalId, next);
      return next;
    });
    setDraft("");
    const stub: ChatMessage = {
      id: crypto.randomUUID(),
      role: "assistant",
      text:
        "Agent backend is not connected yet — your message is saved on this machine for this project. " +
        "Next step: wire suggestions to POST /graphs/{id}/ai/suggest and stream replies here.",
      at: Date.now(),
    };
    setTimeout(() => {
      setMessages((prev) => {
        const next = [...prev, stub];
        saveMessages(projectLocalId, next);
        return next;
      });
    }, 400);
  }, [draft, projectLocalId]);

  const disabled = !projectLocalId;
  const title = useMemo(
    () => (projectLocalId ? `Project: ${projectName}` : "No project"),
    [projectLocalId, projectName]
  );

  return (
    <div style={wrap}>
      <div style={head}>
        <span style={{ fontWeight: 600, fontSize: 13 }}>Agent</span>
        <span style={{ fontSize: 11, opacity: 0.65 }}>{title}</span>
      </div>
      <div ref={scrollRef} style={history}>
        {disabled && (
          <p style={{ opacity: 0.65, margin: 0, fontSize: 12 }}>
            Select or create a project to use the agent chat. Messages are stored locally in your
            browser.
          </p>
        )}
        {messages.map((m) => (
          <div
            key={m.id}
            style={{
              ...bubble,
              alignSelf: m.role === "user" ? "flex-end" : "flex-start",
              background: m.role === "user" ? "#1f3a5f" : "#21262d",
              borderColor: m.role === "user" ? "#388bfd" : "#30363d",
            }}
          >
            {m.text && <div style={{ fontSize: 12, lineHeight: 1.45 }}>{m.text}</div>}
            {m.attachments?.map((a, i) => (
              <div key={`${m.id}-a-${i}`} style={{ marginTop: 6, fontSize: 11 }}>
                {a.dataUrl ? (
                  <img
                    src={a.dataUrl}
                    alt={a.name}
                    style={{ maxWidth: "100%", borderRadius: 6, maxHeight: 160 }}
                  />
                ) : (
                  <span style={{ opacity: 0.85 }}>
                    File: {a.name} ({Math.round(a.size / 1024)} KB)
                  </span>
                )}
              </div>
            ))}
            <div style={{ fontSize: 9, opacity: 0.45, marginTop: 4 }}>
              {new Date(m.at).toLocaleString()}
            </div>
          </div>
        ))}
      </div>
      {fileErr && <div style={{ color: "#f85149", fontSize: 11, padding: "0 8px" }}>{fileErr}</div>}
      <div style={entryRow}>
        <input
          ref={fileRef}
          type="file"
          multiple
          style={{ display: "none" }}
          onChange={(e) => void onPickFiles(e.target.files)}
        />
        <button
          type="button"
          style={iconBtn}
          disabled={disabled}
          title="Attach files or photos"
          onClick={() => fileRef.current?.click()}
        >
          +
        </button>
        <input
          type="text"
          style={inp}
          placeholder={disabled ? "Select a project…" : "Message…"}
          disabled={disabled}
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              sendText();
            }
          }}
        />
        <button type="button" style={sendBtn} disabled={disabled} onClick={sendText}>
          Send
        </button>
      </div>
    </div>
  );
}

const wrap: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  minHeight: 0,
  flex: 1,
  borderTop: "1px solid #30363d",
  borderBottom: "1px solid #30363d",
};
const head: CSSProperties = {
  padding: "8px 10px",
  display: "flex",
  flexDirection: "column",
  gap: 2,
  flexShrink: 0,
};
const history: CSSProperties = {
  flex: 1,
  minHeight: 120,
  overflowY: "auto",
  padding: 8,
  display: "flex",
  flexDirection: "column",
  gap: 8,
};
const bubble: CSSProperties = {
  maxWidth: "92%",
  padding: "8px 10px",
  borderRadius: 10,
  border: "1px solid #30363d",
};
const entryRow: CSSProperties = {
  display: "flex",
  gap: 6,
  padding: 8,
  flexShrink: 0,
  alignItems: "center",
};
const inp: CSSProperties = {
  flex: 1,
  minWidth: 0,
  padding: "8px 10px",
  borderRadius: 6,
  border: "1px solid #30363d",
  background: "#0d1117",
  color: "#e6edf3",
  fontSize: 13,
};
const sendBtn: CSSProperties = {
  padding: "8px 12px",
  borderRadius: 6,
  border: "none",
  background: "#238636",
  color: "#fff",
  fontWeight: 600,
  cursor: "pointer",
  fontSize: 12,
};
const iconBtn: CSSProperties = {
  width: 36,
  height: 36,
  borderRadius: 8,
  border: "1px solid #30363d",
  background: "#21262d",
  color: "#58a6ff",
  fontSize: 20,
  lineHeight: 1,
  cursor: "pointer",
  flexShrink: 0,
};
