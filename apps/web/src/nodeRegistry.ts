export type RegistryPortSpec = {
  id: string;
  label: string;
  semantic: string;
  optional?: boolean;
};

export type RegistryNodeSpec = {
  kind: string;
  category: string;
  label: string;
  role: string;
  framework_group?: string;
  submenu?: string;
  plugin_source?: string;
  menu?: { enabled?: boolean; order?: number };
  policy?: Record<string, string>;
  ports?: {
    inputs?: RegistryPortSpec[];
    outputs?: RegistryPortSpec[];
  };
  interaction?: {
    actions?: {
      run?: { allowed?: boolean; tab?: string };
      lock_toggle?: { allowed?: boolean; tab?: string };
      edit?: { allowed?: boolean; tab?: string };
      config?: { allowed?: boolean; tab?: string };
      preview?: { allowed?: boolean; tab?: string };
    };
  };
};

type RegistryDoc = {
  version: number;
  nodes: RegistryNodeSpec[];
};

const EMPTY_REGISTRY: RegistryDoc = { version: 1, nodes: [] };
let runtimeRegistry: RegistryDoc = EMPTY_REGISTRY;
let nodeByKind = new Map(runtimeRegistry.nodes.map((n) => [n.kind, n]));

function setRuntimeRegistry(next: RegistryDoc) {
  runtimeRegistry = next;
  nodeByKind = new Map(runtimeRegistry.nodes.map((n) => [n.kind, n]));
}

export async function loadNodeRegistryFromApi(): Promise<void> {
  const r = await fetch("/api/registry/nodes");
  if (!r.ok) throw new Error(`Node registry HTTP ${r.status}`);
  const raw = (await r.json()) as RegistryDoc;
  if (!raw || !Array.isArray(raw.nodes)) {
    throw new Error("Node registry payload invalid");
  }
  setRuntimeRegistry(raw);
}

export function allNodeSpecs(): RegistryNodeSpec[] {
  return runtimeRegistry.nodes.slice();
}

export function nodeSpec(kind: string): RegistryNodeSpec | undefined {
  return nodeByKind.get(kind);
}

export function nodeRole(kind: string): string | null {
  return nodeSpec(kind)?.role ?? null;
}

export function nodePorts(kind: string, direction: "in" | "out"): RegistryPortSpec[] {
  const spec = nodeSpec(kind);
  if (!spec?.ports) return [];
  return direction === "in"
    ? spec.ports.inputs ?? []
    : spec.ports.outputs ?? [];
}

export function portSemantic(
  kind: string,
  direction: "in" | "out",
  portId: string
): string | null {
  const p = nodePorts(kind, direction).find((x) => x.id === portId);
  return p?.semantic ?? null;
}

export type AddNodePreset = {
  category: string;
  kind: string;
  label: string;
  frameworkGroup: string;
  submenu: string;
  pluginSource: string;
  isGroup?: boolean;
  icon?: string;
  accent?: string;
  params?: Record<string, unknown>;
  policy?: Record<string, string>;
};

export function addNodePresetsFromRegistry(): AddNodePreset[] {
  return runtimeRegistry.nodes
    .filter((n) => n.menu?.enabled !== false)
    .sort((a, b) => (a.menu?.order ?? 9999) - (b.menu?.order ?? 9999))
    .map((n) => ({
      category: n.category,
      kind: n.kind,
      label: n.label,
      frameworkGroup: n.framework_group || n.category || "other",
      submenu: n.submenu || "general",
      pluginSource: n.plugin_source || "core",
      policy: n.policy,
    }));
}
