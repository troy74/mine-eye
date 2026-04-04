export type ApiEdge = {
  id: string;
  from_node: string;
  from_port: string;
  to_node: string;
  to_port: string;
  semantic_type: string;
};

export type ApiBranchStatus =
  | "draft"
  | "qa"
  | "approved"
  | "promoted"
  | "archived";

export type ApiBranch = {
  id: string;
  graph_id: string;
  name: string;
  base_revision_id: string | null;
  head_revision_id: string | null;
  status: ApiBranchStatus;
  created_by: string;
  created_at: string;
  updated_at: string;
};

export type ApiRevision = {
  id: string;
  graph_id: string;
  branch_id: string | null;
  parent_revision_id: string | null;
  created_by: string;
  meta: Record<string, unknown>;
  created_at: string;
};

export type ApiPromotionStatus = "pending" | "succeeded" | "conflict" | "failed";

export type ApiPromotion = {
  id: string;
  source_branch_id: string;
  target_branch_id: string;
  source_head_revision_id: string | null;
  promoted_revision_id: string | null;
  status: ApiPromotionStatus;
  conflict_report: Record<string, unknown> | null;
  created_by: string;
  created_at: string;
};

export type ApiRevisionDiff = {
  from_revision_id: string;
  to_revision_id: string;
  diff: {
    summary: {
      nodes_added: number;
      nodes_removed: number;
      nodes_changed: number;
      edges_added: number;
      edges_removed: number;
    };
    nodes: {
      added: string[];
      removed: string[];
      changed: string[];
    };
    edges: {
      added: Array<[string, string, string, string, string]>;
      removed: Array<[string, string, string, string, string]>;
    };
  };
};

export type ApiNode = {
  id: string;
  graph_id: string;
  config: { kind: string; version: number; params: Record<string, unknown> };
  policy: {
    recompute: "auto" | "manual" | string;
    propagation: "eager" | "debounce" | "hold" | string;
    quality: "preview" | "final" | string;
  };
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

export type ViewerManifestLayer = {
  source_node_id: string;
  source_node_kind: string;
  edge_id: string;
  semantic_type: string;
  from_port: string;
  to_port: string;
  artifact_key: string;
  artifact_url: string;
  content_hash: string;
  media_type?: string | null;
  presentation?: Record<string, unknown>;
};

export type ViewerManifestResponse = {
  graph_id: string;
  viewer_node_id: string;
  viewer_node_kind: string;
  manifest_version: number;
  viewer_ui: Record<string, unknown>;
  layers: ViewerManifestLayer[];
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
  opts?: { dirtyRoots?: string[]; includeManual?: boolean }
): Promise<RunGraphResponse> {
  const body: Record<string, unknown> = {};
  if (opts?.dirtyRoots !== undefined) {
    body.dirty_roots = opts.dirtyRoots;
  }
  if (opts?.includeManual !== undefined) {
    body.include_manual = opts.includeManual;
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
    policy: (() => {
      const p = (o.policy ?? {}) as Record<string, unknown>;
      return {
        recompute: String(p.recompute ?? "auto"),
        propagation: String(p.propagation ?? "debounce"),
        quality: String(p.quality ?? "preview"),
      };
    })(),
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
  const r = await fetch(api(`/graphs/${graphId}`), { cache: "no-store" });
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

export async function updateWorkspaceProjectCrs(
  workspaceId: string,
  crs: { epsg: number; wkt?: string | null }
): Promise<void> {
  const r = await fetch(api(`/workspaces/${workspaceId}/project-crs`), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ project_crs: crs }),
  });
  if (!r.ok) throw new Error((await r.text()) || `Update workspace CRS ${r.status}`);
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

export async function listGraphBranches(graphId: string): Promise<ApiBranch[]> {
  const r = await fetch(api(`/graphs/${graphId}/branches`));
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return (await r.json()) as ApiBranch[];
}

export async function createGraphBranch(
  graphId: string,
  body: {
    name: string;
    created_by: string;
    base_revision_id?: string | null;
    status?: ApiBranchStatus;
  }
): Promise<{ id: string }> {
  const r = await fetch(api(`/graphs/${graphId}/branches`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error((await r.text()) || `Branch ${r.status}`);
  const j = (await r.json()) as { id: string };
  return { id: String(j.id) };
}

export async function listGraphRevisions(
  graphId: string,
  opts?: { branchId?: string | null }
): Promise<ApiRevision[]> {
  const params = new URLSearchParams();
  if (opts?.branchId) params.set("branch_id", opts.branchId);
  const suffix = params.toString().length > 0 ? `?${params.toString()}` : "";
  const r = await fetch(api(`/graphs/${graphId}/revisions${suffix}`));
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return (await r.json()) as ApiRevision[];
}

export async function diffGraphRevisions(
  graphId: string,
  fromRevisionId: string,
  toRevisionId: string
): Promise<ApiRevisionDiff> {
  const r = await fetch(
    api(`/graphs/${graphId}/revisions/${fromRevisionId}/diff/${toRevisionId}`)
  );
  if (!r.ok) throw new Error((await r.text()) || `Diff ${r.status}`);
  return (await r.json()) as ApiRevisionDiff;
}

export async function listGraphPromotions(graphId: string): Promise<ApiPromotion[]> {
  const r = await fetch(api(`/graphs/${graphId}/promotions`));
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return (await r.json()) as ApiPromotion[];
}

export async function commitCurrentToBranch(
  graphId: string,
  branchId: string,
  body: {
    created_by: string;
    event?: string;
    details?: Record<string, unknown>;
  }
): Promise<{ id: string }> {
  const r = await fetch(api(`/graphs/${graphId}/branches/${branchId}/commit-current`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error((await r.text()) || `Commit ${r.status}`);
  const j = (await r.json()) as { id: string };
  return { id: String(j.id) };
}

export async function checkoutGraphBranch(
  graphId: string,
  branchId: string,
  body?: { created_by?: string }
): Promise<{ status: string; branch_id: string; head_revision_id: string }> {
  const r = await fetch(api(`/graphs/${graphId}/branches/${branchId}/checkout`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body ?? {}),
  });
  if (!r.ok) throw new Error((await r.text()) || `Checkout ${r.status}`);
  return (await r.json()) as { status: string; branch_id: string; head_revision_id: string };
}

export async function executeGraphPromotion(
  graphId: string,
  body: {
    source_branch_id: string;
    target_branch_id: string;
    created_by: string;
    apply_to_graph?: boolean;
  }
): Promise<{
  promotion_id: string;
  promoted_revision_id?: string;
  status: "succeeded" | "conflict";
  mode: "fast_forward" | "three_way";
  conflict_report?: Record<string, unknown>;
}> {
  const r = await fetch(api(`/graphs/${graphId}/promotions/execute`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error((await r.text()) || `Execute promotion ${r.status}`);
  return (await r.json()) as {
    promotion_id: string;
    promoted_revision_id?: string;
    status: "succeeded" | "conflict";
    mode: "fast_forward" | "three_way";
    conflict_report?: Record<string, unknown>;
  };
}

export async function recordGraphPromotion(
  graphId: string,
  body: {
    source_branch_id: string;
    target_branch_id: string;
    source_head_revision_id?: string | null;
    promoted_revision_id?: string | null;
    status: ApiPromotionStatus;
    conflict_report?: Record<string, unknown> | null;
    created_by: string;
  }
): Promise<{ id: string }> {
  const r = await fetch(api(`/graphs/${graphId}/promotions`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error((await r.text()) || `Promotion ${r.status}`);
  const j = (await r.json()) as { id: string };
  return { id: String(j.id) };
}

export async function patchNodeParams(
  graphId: string,
  nodeId: string,
  params: Record<string, unknown>,
  opts?: {
    branchId?: string | null;
    policy?: {
      recompute?: "auto" | "manual";
      propagation?: "eager" | "debounce" | "hold";
      quality?: "preview" | "final";
    };
  }
): Promise<ApiNode> {
  const r = await fetch(api(`/graphs/${graphId}/nodes/${nodeId}`), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      params,
      policy: opts?.policy,
      branch_id: opts?.branchId ?? null,
    }),
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
    branch_id?: string | null;
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
      branch_id: body.branch_id ?? null,
    }),
  });
  if (!r.ok) throw new Error((await r.text()) || `Add node ${r.status}`);
  const j = (await r.json()) as { id: string };
  return { id: String(j.id) };
}

export async function deleteGraphNode(
  graphId: string,
  nodeId: string,
  opts?: { branchId?: string | null }
): Promise<void> {
  const qs =
    opts?.branchId && opts.branchId.length > 0
      ? `?branch_id=${encodeURIComponent(opts.branchId)}`
      : "";
  const r = await fetch(api(`/graphs/${graphId}/nodes/${nodeId}${qs}`), {
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
    branch_id?: string | null;
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
  edgeId: string,
  opts?: { branchId?: string | null }
): Promise<void> {
  const qs =
    opts?.branchId && opts.branchId.length > 0
      ? `?branch_id=${encodeURIComponent(opts.branchId)}`
      : "";
  const r = await fetch(api(`/graphs/${graphId}/edges/${edgeId}${qs}`), {
    method: "DELETE",
  });
  if (!r.ok) throw new Error((await r.text()) || `Delete edge ${r.status}`);
}

export async function fetchViewerManifest(
  graphId: string,
  viewerNodeId: string
): Promise<ViewerManifestResponse> {
  const r = await fetch(api(`/graphs/${graphId}/viewers/${viewerNodeId}/manifest`), {
    cache: "no-store",
  });
  if (!r.ok) throw new Error((await r.text()) || `Viewer manifest ${r.status}`);
  return (await r.json()) as ViewerManifestResponse;
}
