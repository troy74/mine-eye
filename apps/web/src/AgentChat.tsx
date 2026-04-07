import { useUser } from "@clerk/clerk-react";
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
} from "react";
import { aiChat, type AiChatToolEvent } from "./graphApi";

const CHAT_KEY = (projectLocalId: string) => `mineeye:chat:v1:${projectLocalId}`;
const CHAT_APPLY_KEY = (projectLocalId: string) => `mineeye:chat:apply:v1:${projectLocalId}`;
const CHAT_ONBOARD_KEY = (projectLocalId: string) => `mineeye:chat:onboard:v1:${projectLocalId}`;

export type ChatAttachment = {
  name: string;
  mime: string;
  size: number;
  text?: string;
  dataUrl?: string;
};

export type ChatMessage = {
  id: string;
  role: "user" | "assistant";
  text: string;
  at: number;
  attachments?: ChatAttachment[];
  toolEvents?: AiChatToolEvent[];
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
  graphId: string | null;
  activeBranchId?: string | null;
};

export function AgentChat({ projectLocalId, projectName, graphId, activeBranchId = null }: Props) {
  const { user } = useUser();
  const authUserId = user?.id ?? "web-user";
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [pendingAttachments, setPendingAttachments] = useState<ChatAttachment[]>([]);
  const [draft, setDraft] = useState("");
  const [fileErr, setFileErr] = useState<string | null>(null);
  const [sending, setSending] = useState(false);
  const [applyMutations, setApplyMutations] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const fileRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    setMessages(loadMessages(projectLocalId));
  }, [projectLocalId]);

  useEffect(() => {
    if (!projectLocalId || !graphId) return;
    if (messages.length > 0) return;
    let seen = false;
    try {
      seen = localStorage.getItem(CHAT_ONBOARD_KEY(projectLocalId)) === "1";
    } catch {
      seen = false;
    }
    if (seen) return;
    const starter: ChatMessage = {
      id: crypto.randomUUID(),
      role: "assistant",
      text:
        "Project is open. Share your goal and upload files (collar/survey/assay etc) and I’ll map columns, patch ingest nodes, wire the flow, and suggest the next run checks.",
      at: Date.now(),
    };
    setMessages((prev) => {
      const next = [...prev, starter];
      saveMessages(projectLocalId, next);
      return next;
    });
    try {
      localStorage.setItem(CHAT_ONBOARD_KEY(projectLocalId), "1");
    } catch {
      // ignore
    }
  }, [graphId, messages.length, projectLocalId]);

  useEffect(() => {
    if (!projectLocalId) {
      setApplyMutations(false);
      return;
    }
    try {
      const raw = localStorage.getItem(CHAT_APPLY_KEY(projectLocalId));
      setApplyMutations(raw === "1");
    } catch {
      setApplyMutations(false);
    }
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
          let text: string | undefined;
          const lower = f.name.toLowerCase();
          const probablyText =
            f.type.startsWith("text/") ||
            f.type.includes("json") ||
            lower.endsWith(".csv") ||
            lower.endsWith(".tsv") ||
            lower.endsWith(".txt") ||
            lower.endsWith(".json") ||
            lower.endsWith(".geojson");
          if (probablyText) {
            try {
              const raw = await f.text();
              text = raw.slice(0, 180_000);
            } catch {
              text = undefined;
            }
          }
          attachments.push({
            name: f.name,
            mime: f.type || "application/octet-stream",
            size: f.size,
            text,
          });
        }
      }
      setPendingAttachments((prev) => [...prev, ...attachments]);
    },
    [projectLocalId]
  );

  const sendText = useCallback(async () => {
    const t = draft.trim();
    if ((!t && pendingAttachments.length === 0) || !projectLocalId || !graphId || sending) return;
    const userMsg: ChatMessage = {
      id: crypto.randomUUID(),
      role: "user",
      text: t,
      at: Date.now(),
      attachments: pendingAttachments.length > 0 ? pendingAttachments : undefined,
    };
    setMessages((prev) => {
      const next = [...prev, userMsg];
      saveMessages(projectLocalId, next);
      return next;
    });
    setDraft("");
    setPendingAttachments([]);

    setSending(true);
    try {
      const response = await aiChat(graphId, {
        messages: [...messages, userMsg].map((m) => ({
          role: m.role,
          text: m.text ?? "",
          attachments:
            m.attachments?.map((a) => ({
              name: a.name,
              mime: a.mime,
              size: a.size,
              text: a.text,
            })) ?? [],
        })),
        apply_mutations: applyMutations,
        user_id: authUserId,
        branch_id: activeBranchId,
      });
      const assistant: ChatMessage = {
        id: crypto.randomUUID(),
        role: "assistant",
        text: response.assistant_text,
        at: Date.now(),
        toolEvents: response.tool_events,
      };
      setMessages((prev) => {
        const next = [...prev, assistant];
        saveMessages(projectLocalId, next);
        return next;
      });
    } catch (e) {
      const errMsg: ChatMessage = {
        id: crypto.randomUUID(),
        role: "assistant",
        text: `AI request failed: ${e instanceof Error ? e.message : String(e)}`,
        at: Date.now(),
      };
      setMessages((prev) => {
        const next = [...prev, errMsg];
        saveMessages(projectLocalId, next);
        return next;
      });
    } finally {
      setSending(false);
    }
  }, [activeBranchId, applyMutations, authUserId, draft, graphId, messages, pendingAttachments, projectLocalId, sending]);

  const disabled = !projectLocalId || !graphId || sending;
  const title = useMemo(
    () => (projectLocalId ? `Project: ${projectName}` : "No project"),
    [projectLocalId, projectName]
  );

  const resetChat = useCallback(() => {
    if (!projectLocalId) return;
    try {
      localStorage.removeItem(CHAT_KEY(projectLocalId));
      localStorage.removeItem(CHAT_ONBOARD_KEY(projectLocalId));
    } catch {
      // ignore
    }
    setMessages([]);
    setPendingAttachments([]);
    setDraft("");
  }, [projectLocalId]);

  return (
    <div style={wrap}>
      <div style={head}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8 }}>
          <span style={{ fontWeight: 600, fontSize: 12, color: "#e6edf3" }}>
            {projectLocalId ? (projectName || "Agent") : "Agent"}
          </span>
          <button
            type="button"
            onClick={resetChat}
            disabled={!projectLocalId || sending}
            style={{
              border: "1px solid #30363d",
              background: "#161b22",
              color: "#8b949e",
              borderRadius: 6,
              padding: "3px 8px",
              fontSize: 10,
              cursor: "pointer",
            }}
            title="Start fresh chat with default project/org context"
          >
            New Chat
          </button>
        </div>
        <span style={{ fontSize: 10, color: "#484f58" }}>{title}</span>
        {projectLocalId && graphId && (
          <label style={{ marginTop: 4, display: "inline-flex", alignItems: "center", gap: 6, fontSize: 10, color: "#8b949e" }}>
            <input
              type="checkbox"
              checked={applyMutations}
              disabled={sending}
              onChange={(e) => {
                const next = e.target.checked;
                setApplyMutations(next);
                try {
                  localStorage.setItem(CHAT_APPLY_KEY(projectLocalId), next ? "1" : "0");
                } catch {
                  // ignore localStorage failures
                }
              }}
            />
            Apply graph mutations (otherwise plan-only)
          </label>
        )}
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
            {m.text && (
              <div style={{ fontSize: 12, lineHeight: 1.45 }}>
                {(() => {
                  const parsed =
                    m.role === "assistant" ? splitAssistantText(m.text) : { plain: m.text, system: "" };
                  return (
                    <>
                      <div>{parsed.plain}</div>
                      {m.role === "assistant" && parsed.system && (
                        <details style={{ marginTop: 8 }}>
                          <summary style={{ cursor: "pointer", fontSize: 10, color: "#8b949e" }}>
                            System details
                          </summary>
                          <pre
                            style={{
                              margin: "6px 0 0",
                              fontSize: 10,
                              lineHeight: 1.4,
                              color: "#8b949e",
                              whiteSpace: "pre-wrap",
                              wordBreak: "break-word",
                            }}
                          >
                            {parsed.system}
                          </pre>
                        </details>
                      )}
                    </>
                  );
                })()}
              </div>
            )}
            {m.role === "assistant" && m.toolEvents && m.toolEvents.length > 0 && (
              <div style={{ marginTop: 8, display: "grid", gap: 6 }}>
                {m.toolEvents.map((ev, i) => (
                  <details
                    key={`${m.id}-tool-${i}`}
                    style={{
                      border: "1px solid #30363d",
                      borderRadius: 6,
                      background: "rgba(13,17,23,0.45)",
                      padding: "4px 8px",
                    }}
                  >
                    <summary
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: 6,
                        fontSize: 11,
                        cursor: "pointer",
                        listStyle: "none",
                      }}
                    >
                      <span
                        style={{
                          width: 7,
                          height: 7,
                          borderRadius: "50%",
                          background: ev.ok ? "#3fb950" : "#f85149",
                          flexShrink: 0,
                        }}
                      />
                      <strong style={{ color: "#c9d1d9", fontSize: 11 }}>{ev.name}</strong>
                      <span style={{ color: "#8b949e" }}>{ev.summary}</span>
                    </summary>
                      {ev.output_preview && (
                        <div style={{ marginTop: 4, fontSize: 10, color: "#8b949e" }}>
                          {ev.output_preview}
                        </div>
                      )}
                      <pre
                        style={{
                        margin: "6px 0 0",
                        fontSize: 10,
                        lineHeight: 1.35,
                        color: "#8b949e",
                        whiteSpace: "pre-wrap",
                        wordBreak: "break-word",
                      }}
                    >
                      {JSON.stringify(ev.arguments ?? {}, null, 2)}
                    </pre>
                  </details>
                ))}
              </div>
            )}
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
      {pendingAttachments.length > 0 && (
        <div style={{ padding: "0 8px 6px", display: "flex", flexWrap: "wrap", gap: 6 }}>
          {pendingAttachments.map((a, i) => (
            <span
              key={`${a.name}-${i}`}
              style={{
                fontSize: 10,
                border: "1px solid #30363d",
                borderRadius: 999,
                padding: "3px 8px",
                background: "#161b22",
                color: "#8b949e",
              }}
            >
              {a.name} ({Math.round(a.size / 1024)} KB)
            </span>
          ))}
        </div>
      )}
      <div style={entryRow}>
        <input
          ref={fileRef}
          type="file"
          multiple
          style={{ display: "none" }}
          onChange={(e) => {
            void onPickFiles(e.target.files);
            e.currentTarget.value = "";
          }}
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
          placeholder={!projectLocalId || !graphId ? "Select a project…" : (sending ? "Assistant is working…" : "Message…")}
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
        <button type="button" style={sendBtn} disabled={disabled} onClick={() => void sendText()}>
          {sending ? "..." : "Send"}
        </button>
      </div>
    </div>
  );
}

function splitAssistantText(text: string): { plain: string; system: string } {
  const plainMatch = text.match(/<plain>([\s\S]*?)<\/plain>/i);
  const systemMatch = text.match(/<system>([\s\S]*?)<\/system>/i);
  if (plainMatch || systemMatch) {
    return {
      plain: (plainMatch?.[1] ?? "").trim() || text.trim(),
      system: (systemMatch?.[1] ?? "").trim(),
    };
  }
  return { plain: text.trim(), system: "" };
}

const wrap: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  minHeight: 0,
  flex: 1,
};
const head: CSSProperties = {
  padding: "8px 12px 6px",
  display: "flex",
  flexDirection: "column",
  gap: 1,
  flexShrink: 0,
  borderBottom: "1px solid #161b22",
};
const history: CSSProperties = {
  flex: 1,
  minHeight: 80,
  overflowY: "auto",
  padding: "10px 10px 6px",
  display: "flex",
  flexDirection: "column",
  gap: 8,
};
const bubble: CSSProperties = {
  maxWidth: "92%",
  padding: "8px 11px",
  borderRadius: 10,
  border: "1px solid #30363d",
  lineHeight: 1.5,
};
const entryRow: CSSProperties = {
  display: "flex",
  gap: 5,
  padding: "7px 8px 9px",
  flexShrink: 0,
  alignItems: "flex-end",
  borderTop: "1px solid #161b22",
};
const inp: CSSProperties = {
  flex: 1,
  minWidth: 0,
  padding: "7px 10px",
  borderRadius: 8,
  border: "1px solid #30363d",
  background: "#0d1117",
  color: "#e6edf3",
  fontSize: 12,
  fontFamily: "inherit",
  outline: "none",
};
const sendBtn: CSSProperties = {
  padding: "7px 13px",
  borderRadius: 8,
  border: "none",
  background: "#238636",
  color: "#fff",
  fontWeight: 600,
  cursor: "pointer",
  fontSize: 11,
  fontFamily: "inherit",
  flexShrink: 0,
};
const iconBtn: CSSProperties = {
  width: 32,
  height: 32,
  borderRadius: 7,
  border: "1px solid #30363d",
  background: "#21262d",
  color: "#58a6ff",
  fontSize: 18,
  lineHeight: 1,
  cursor: "pointer",
  flexShrink: 0,
  display: "grid",
  placeItems: "center",
};
