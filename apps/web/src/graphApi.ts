export type ApiEdge = {
  id: string;
  from_node: string;
  from_port: string;
  to_node: string;
  to_port: string;
  semantic_type: string;
};

export type ApiNode = {
  id: string;
  graph_id: string;
  config: { kind: string; version: number; params: Record<string, unknown> };
  category: string;
  execution: string;
  cache: string;
  content_hash: string | null;
  /** Worker/orchestrator failure text; cleared after a successful run. */
  last_error: string | null;
};

export type GraphResponse = {
  graph_id: string;
  workspace_id?: string;
  project_crs?: { epsg?: number; wkt?: string | null } | null;
  nodes: ApiNode[];
  edges: ApiEdge[];
};

export const api = (path: string) => `/api${path}`;

/** One row from `GET /graphs/:id/artifacts` (same paths the worker writes under). */
export type ArtifactEntry = {
  node_id: string;
  key: string;
  url: string;
  content_hash: string;
};

export type RunGraphResponse = {
  queued: { queue_row?: unknown; job_id?: string; node_id?: string }[];
  skipped_manual: string[];
};

/**
 * Enqueue jobs for dirty nodes. Omit `dirty_roots` to treat every node as a root (full pipeline).
 * Pass one or more node ids to re-run that node and everything downstream of it.
 */
export async function runGraph(
  graphId: string,
  opts?: { dirtyRoots?: string[] }
): Promise<RunGraphResponse> {
  const body: Record<string, unknown> = {};
  if (opts?.dirtyRoots !== undefined) {
    body.dirty_roots = opts.dirtyRoots;
  }
  const r = await fetch(api(`/graphs/${graphId}/run`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(t || `Run failed: ${r.status}`);
  }
  return r.json() as Promise<RunGraphResponse>;
}

function normalizeNode(raw: unknown): ApiNode {
  const o = raw as Record<string, unknown>;
  const cfg = (o.config ?? {}) as Record<string, unknown>;
  const params = cfg.params;
  const safeParams =
    params !== null &&
    params !== undefined &&
    typeof params === "object" &&
    !Array.isArray(params)
      ? (params as Record<string, unknown>)
      : {};
  return {
    id: String(o.id ?? ""),
    graph_id: String(o.graph_id ?? ""),
    config: {
      kind: String(cfg.kind ?? ""),
      version: Number(cfg.version ?? 1),
      params: safeParams,
    },
    category: String(o.category ?? ""),
    execution: String(o.execution ?? ""),
    cache: String(o.cache ?? ""),
    content_hash:
      o.content_hash === null || o.content_hash === undefined
        ? null
        : String(o.content_hash),
    last_error:
      o.last_error === null || o.last_error === undefined
        ? null
        : String(o.last_error),
  };
}

export async function fetchGraph(graphId: string): Promise<GraphResponse> {
  const r = await fetch(api(`/graphs/${graphId}`));
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const raw = (await r.json()) as Record<string, unknown>;
  const nodesRaw = raw.nodes;
  const edgesRaw = raw.edges;
  const nodes = Array.isArray(nodesRaw)
    ? nodesRaw.map(normalizeNode)
    : [];
  const edges = Array.isArray(edgesRaw)
    ? edgesRaw.map(normalizeEdge)
    : [];
  const pc = raw.project_crs;
  const project_crs =
    pc !== null &&
    pc !== undefined &&
    typeof pc === "object" &&
    !Array.isArray(pc)
      ? (pc as GraphResponse["project_crs"])
      : null;
  return {
    graph_id: String(raw.graph_id ?? graphId),
    workspace_id:
      raw.workspace_id != null ? String(raw.workspace_id) : undefined,
    project_crs,
    nodes,
    edges,
  };
}

export async function createWorkspace(body: {
  name: string;
  owner_user_id: string;
  project_crs?: { epsg?: number; wkt?: string | null };
}): Promise<{ id: string }> {
  const r = await fetch(api("/workspaces"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error((await r.text()) || `Workspace ${r.status}`);
  const j = (await r.json()) as { id: string };
  return { id: String(j.id) };
}

export async function createGraph(
  workspaceId: string,
  body: { name: string; workspace_id: string; owner_user_id: string }
): Promise<{ id: string }> {
  const r = await fetch(api(`/workspaces/${workspaceId}/graphs`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error((await r.text()) || `Graph ${r.status}`);
  const j = (await r.json()) as { id: string };
  return { id: String(j.id) };
}

export async function patchNodeParams(
  graphId: string,
  nodeId: string,
  params: Record<string, unknown>
): Promise<ApiNode> {
  const r = await fetch(api(`/graphs/${graphId}/nodes/${nodeId}`), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ params }),
  });
  if (!r.ok) throw new Error(`PATCH failed: ${r.status}`);
  return normalizeNode(await r.json());
}

function normalizeEdge(raw: unknown): ApiEdge {
  const o = raw as Record<string, unknown>;
  const from = String(o.from_node ?? "");
  const to = String(o.to_node ?? "");
  const fp = String(o.from_port ?? "");
  const tp = String(o.to_port ?? "");
  const st = String(o.semantic_type ?? "table");
  const rawId = o.id;
  const id =
    rawId != null && String(rawId).length > 0
      ? String(rawId)
      : `legacy-${from}-${fp}-${to}-${tp}-${st}`;
  return {
    id,
    from_node: from,
    from_port: fp,
    to_node: to,
    to_port: tp,
    semantic_type: st,
  };
}

export async function addGraphNode(
  graphId: string,
  body: {
    category: string;
    kind: string;
    params?: Record<string, unknown>;
    policy?: Record<string, unknown>;
  }
): Promise<{ id: string }> {
  const r = await fetch(api(`/graphs/${graphId}/nodes`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      category: body.category,
      kind: body.kind,
      params: body.params ?? {},
      policy: body.policy,
    }),
  });
  if (!r.ok) throw new Error((await r.text()) || `Add node ${r.status}`);
  const j = (await r.json()) as { id: string };
  return { id: String(j.id) };
}

export async function deleteGraphNode(
  graphId: string,
  nodeId: string
): Promise<void> {
  const r = await fetch(api(`/graphs/${graphId}/nodes/${nodeId}`), {
    method: "DELETE",
  });
  if (!r.ok) throw new Error((await r.text()) || `Delete node ${r.status}`);
}

export async function createGraphEdge(
  graphId: string,
  body: {
    from_node: string;
    from_port: string;
    to_node: string;
    to_port: string;
    semantic_type: string;
  }
): Promise<{ id: string }> {
  const r = await fetch(api(`/graphs/${graphId}/edges`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error((await r.text()) || `Add edge ${r.status}`);
  const j = (await r.json()) as { id: string };
  return { id: String(j.id) };
}

export async function deleteGraphEdge(
  graphId: string,
  edgeId: string
): Promise<void> {
  const r = await fetch(api(`/graphs/${graphId}/edges/${edgeId}`), {
    method: "DELETE",
  });
  if (!r.ok) throw new Error((await r.text()) || `Delete edge ${r.status}`);
}
