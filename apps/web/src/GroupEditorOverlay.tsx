import { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import { Background, Controls, ReactFlow, type Edge, type Node } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { ApiNode } from "./graphApi";
import { patchNodeParams } from "./graphApi";
import {
  GROUP_TEMPLATES,
  NODE_GROUP_MAX_DEPTH,
  cloneGroupDefinition,
  defaultGroupNodePosition,
  ensureGroupDefinitionLayout,
  groupDefinitionForInternalNode,
  groupDefinitionFromNode,
  groupTemplateIdFromNode,
  groupVisualMeta,
  maxGroupDefinitionDepth,
  resolveGroupDefinitionAtPath,
  setInternalNodeGroupDefinition,
  updateGroupDefinitionAtPath,
  type GroupDefinition,
  type GroupInputBinding,
  type GroupNodeDef,
  type GroupOutputBinding,
} from "./nodeGroup";
import { allNodeSpecs, nodePorts, nodeSpec, type RegistryPortSpec } from "./nodeRegistry";

type Props = {
  graphId: string;
  activeBranchId?: string | null;
  node: ApiNode | null;
  onClose: () => void;
  onNodeUpdated: (node: ApiNode) => void;
};

type InternalPortOption = {
  nodeId: string;
  nodeKind: string;
  port: RegistryPortSpec;
  portIndex: number;
  key: string;
  label: string;
};

type GroupDiagnostic = {
  level: "error" | "warning";
  message: string;
  path: string[];
  nodeId?: string;
  edgeKey?: string;
};

function getUiParams(node: ApiNode): Record<string, unknown> {
  const raw = node.config.params?.ui;
  return raw && typeof raw === "object" && !Array.isArray(raw)
    ? (raw as Record<string, unknown>)
    : {};
}

function edgeDiagnosticKey(edge: GroupDefinition["internal_edges"][number]): string {
  return `${edge.from_node_id}:${edge.from_port_id}:${edge.to_node_id}:${edge.to_port_id}`;
}

function layoutNodes(def: GroupDefinition, invalidNodeIds: Set<string>): Node[] {
  return def.internal_nodes.map((n, idx) => {
    const label = nodeSpec(n.kind)?.label ?? n.kind.replace(/_/g, " ");
    const pos = n.position ?? defaultGroupNodePosition(idx);
    const nested = n.kind === "node_group" ? groupDefinitionForInternalNode(n) : null;
    const icon = nested?.display?.icon ?? (n.kind === "node_group" ? "▣" : "");
    const badge = n.kind === "node_group" ? "GROUP" : "";
    return {
      id: n.id,
      position: pos,
      data: { label: `${icon ? `${icon} ` : ""}${label}\n${n.id}${badge ? `\n${badge}` : ""}` },
      style: {
        width: 190,
        border: `1px solid ${invalidNodeIds.has(n.id) ? "#f85149" : n.kind === "node_group" ? "#ff9e3d66" : "#30363d"}`,
        borderLeft: `${n.kind === "node_group" ? 5 : 1}px solid ${n.kind === "node_group" ? (nested?.display?.accent ?? "#ff9e3d") : "#30363d"}`,
        borderRadius: 10,
        padding: 10,
        background: n.kind === "node_group"
          ? `linear-gradient(180deg, ${(nested?.display?.accent ?? "#ff9e3d")}16 0%, rgba(22,27,34,0) 38%), #161b22`
          : "#161b22",
        color: "#e6edf3",
        whiteSpace: "pre-line",
        boxShadow: invalidNodeIds.has(n.id) ? "0 0 0 1px rgba(248,81,73,0.45) inset, 0 0 14px rgba(248,81,73,0.18)" : undefined,
      },
    };
  });
}

function toEdges(def: GroupDefinition, invalidEdgeKeys: Set<string>): Edge[] {
  return def.internal_edges.map((e, idx) => ({
    id: `ge-${idx}-${e.from_node_id}-${e.to_node_id}-${e.from_port_id}-${e.to_port_id}`,
    source: e.from_node_id,
    target: e.to_node_id,
    label: `${e.from_port_id} → ${e.to_port_id}`,
    style: {
      stroke: invalidEdgeKeys.has(edgeDiagnosticKey(e)) ? "#f85149" : "#58a6ff",
      strokeWidth: invalidEdgeKeys.has(edgeDiagnosticKey(e)) ? 3 : 2,
      filter: invalidEdgeKeys.has(edgeDiagnosticKey(e)) ? "drop-shadow(0 0 6px rgba(248,81,73,0.35))" : undefined,
    },
    labelStyle: { fill: invalidEdgeKeys.has(edgeDiagnosticKey(e)) ? "#f85149" : "#58a6ff", fontSize: 10 },
  }));
}

function sanitizeId(raw: string): string {
  return raw.replace(/[^a-zA-Z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
}

function buildInternalPortOptions(
  definition: GroupDefinition,
  direction: "in" | "out"
): InternalPortOption[] {
  return definition.internal_nodes.flatMap((n) =>
    nodePorts(n.kind, direction).map((port, idx) => ({
      nodeId: n.id,
      nodeKind: n.kind,
      port,
      portIndex: idx,
      key: `${n.id}:${port.id}:${idx}`,
      label: `${nodeSpec(n.kind)?.label ?? n.kind} · ${n.id} · ${port.label ?? port.id}`,
    }))
  );
}

function compatibleSemantics(fromSemantic: string, toSemantic: string): boolean {
  return fromSemantic === toSemantic;
}

function currentPathLabel(path: string[]): string {
  return path.length > 0 ? path.join(" / ") : "root";
}

function findPort(
  definition: GroupDefinition,
  nodeId: string,
  portId: string,
  direction: "in" | "out"
): RegistryPortSpec | null {
  const internal = definition.internal_nodes.find((item) => item.id === nodeId) ?? null;
  if (!internal) return null;
  return nodePorts(internal.kind, direction).find((port) => port.id === portId) ?? null;
}

function collectDiagnostics(
  definition: GroupDefinition,
  path: string[] = []
): GroupDiagnostic[] {
  const diagnostics: GroupDiagnostic[] = [];
  const pathLabel = currentPathLabel(path);
  const ids = definition.internal_nodes.map((node) => node.id);
  const dupes = ids.filter((id, idx) => ids.indexOf(id) !== idx);
  for (const duplicateId of new Set(dupes)) {
      diagnostics.push({
        level: "error",
        message: `Duplicate internal node id "${duplicateId}" in ${pathLabel}.`,
        path,
        nodeId: duplicateId,
      });
  }

  const indegree = new Map<string, number>();
  const outgoing = new Map<string, string[]>();
  for (const node of definition.internal_nodes) {
    indegree.set(node.id, 0);
    outgoing.set(node.id, []);
  }
  for (const edge of definition.internal_edges) {
    if (!indegree.has(edge.from_node_id) || !indegree.has(edge.to_node_id)) continue;
    indegree.set(edge.to_node_id, (indegree.get(edge.to_node_id) ?? 0) + 1);
    outgoing.set(edge.from_node_id, [...(outgoing.get(edge.from_node_id) ?? []), edge.to_node_id]);
  }
  const queue = [...indegree.entries()].filter(([, v]) => v === 0).map(([id]) => id);
  let visited = 0;
  while (queue.length > 0) {
    const id = queue.shift()!;
    visited += 1;
    for (const next of outgoing.get(id) ?? []) {
      indegree.set(next, (indegree.get(next) ?? 1) - 1);
      if ((indegree.get(next) ?? 0) === 0) queue.push(next);
    }
  }
  if (visited !== definition.internal_nodes.length) {
    diagnostics.push({
      level: "error",
      message: `Internal graph contains a cycle in ${pathLabel}.`,
      path,
    });
  }

  for (const edge of definition.internal_edges) {
    const fromPort = findPort(definition, edge.from_node_id, edge.from_port_id, "out");
    const toPort = findPort(definition, edge.to_node_id, edge.to_port_id, "in");
    if (!fromPort) {
      diagnostics.push({
        level: "error",
        message: `Edge references missing source port ${edge.from_node_id}.${edge.from_port_id} in ${pathLabel}.`,
        path,
        nodeId: edge.from_node_id,
        edgeKey: edgeDiagnosticKey(edge),
      });
      continue;
    }
    if (!toPort) {
      diagnostics.push({
        level: "error",
        message: `Edge references missing target port ${edge.to_node_id}.${edge.to_port_id} in ${pathLabel}.`,
        path,
        nodeId: edge.to_node_id,
        edgeKey: edgeDiagnosticKey(edge),
      });
      continue;
    }
    if (!compatibleSemantics(fromPort.semantic, toPort.semantic)) {
      diagnostics.push({
        level: "error",
        message: `Semantic mismatch on edge ${edge.from_node_id}.${edge.from_port_id} → ${edge.to_node_id}.${edge.to_port_id} in ${pathLabel}: ${fromPort.semantic} vs ${toPort.semantic}.`,
        path,
        nodeId: edge.to_node_id,
        edgeKey: edgeDiagnosticKey(edge),
      });
    }
  }

  for (const binding of definition.input_bindings) {
    const groupInput = definition.inputs.find((item) => item.id === binding.group_input_id) ?? null;
    const targetPort = findPort(definition, binding.internal_node_id, binding.internal_port_id, "in");
    if (!groupInput) {
      diagnostics.push({
        level: "error",
        message: `Input binding references missing wrapper input "${binding.group_input_id}" in ${pathLabel}.`,
        path,
      });
      continue;
    }
    if (!targetPort) {
      diagnostics.push({
        level: "error",
        message: `Input binding references missing internal target ${binding.internal_node_id}.${binding.internal_port_id} in ${pathLabel}.`,
        path,
        nodeId: binding.internal_node_id,
      });
      continue;
    }
    if (!compatibleSemantics(groupInput.semantic, targetPort.semantic)) {
      diagnostics.push({
        level: "error",
        message: `Input binding mismatch in ${pathLabel}: ${groupInput.id} (${groupInput.semantic}) cannot bind to ${binding.internal_node_id}.${binding.internal_port_id} (${targetPort.semantic}).`,
        path,
        nodeId: binding.internal_node_id,
      });
    }
  }

  for (const binding of definition.output_bindings) {
    const groupOutput = definition.outputs.find((item) => item.id === binding.group_output_id) ?? null;
    const sourcePort = findPort(definition, binding.internal_node_id, binding.internal_port_id, "out");
    if (!groupOutput) {
      diagnostics.push({
        level: "error",
        message: `Output binding references missing wrapper output "${binding.group_output_id}" in ${pathLabel}.`,
        path,
      });
      continue;
    }
    if (!sourcePort) {
      diagnostics.push({
        level: "error",
        message: `Output binding references missing internal source ${binding.internal_node_id}.${binding.internal_port_id} in ${pathLabel}.`,
        path,
        nodeId: binding.internal_node_id,
      });
      continue;
    }
    if (!compatibleSemantics(groupOutput.semantic, sourcePort.semantic)) {
      diagnostics.push({
        level: "error",
        message: `Output binding mismatch in ${pathLabel}: ${binding.internal_node_id}.${binding.internal_port_id} (${sourcePort.semantic}) cannot expose to ${groupOutput.id} (${groupOutput.semantic}).`,
        path,
        nodeId: binding.internal_node_id,
      });
    }
  }

  for (const node of definition.internal_nodes) {
    if (node.kind !== "node_group") continue;
    const nested = groupDefinitionForInternalNode(node);
    if (!nested) {
      diagnostics.push({
        level: "error",
        message: `Nested node_group "${node.id}" is missing its internal group definition in ${pathLabel}.`,
        path,
        nodeId: node.id,
      });
      continue;
    }
    diagnostics.push(...collectDiagnostics(nested, [...path, node.id]));
  }

  return diagnostics;
}

function breadcrumbItems(root: GroupDefinition, path: string[]): Array<{ label: string; path: string[] }> {
  const items: Array<{ label: string; path: string[] }> = [{ label: root.label, path: [] }];
  let current = root;
  const runningPath: string[] = [];
  for (const nodeId of path) {
    const internal = current.internal_nodes.find((node) => node.id === nodeId) ?? null;
    const nested = groupDefinitionForInternalNode(internal);
    if (!internal || !nested) break;
    runningPath.push(nodeId);
    items.push({ label: nested.label || internal.id, path: [...runningPath] });
    current = nested;
  }
  return items;
}

export function GroupEditorOverlay({
  graphId,
  activeBranchId,
  node,
  onClose,
  onNodeUpdated,
}: Props) {
  const [groupTemplateId, setGroupTemplateId] = useState("");
  const [rootDefinition, setRootDefinition] = useState<GroupDefinition | null>(null);
  const [path, setPath] = useState<string[]>([]);
  const [selectedNodeKind, setSelectedNodeKind] = useState("");
  const [selectedNestedTemplateId, setSelectedNestedTemplateId] = useState("");
  const [newNodeId, setNewNodeId] = useState("");
  const [selectedOutputKey, setSelectedOutputKey] = useState("");
  const [newOutputId, setNewOutputId] = useState("");
  const [newOutputLabel, setNewOutputLabel] = useState("");
  const [selectedGroupInputId, setSelectedGroupInputId] = useState("");
  const [selectedInputBindingTarget, setSelectedInputBindingTarget] = useState("");
  const [selectedEdgeSource, setSelectedEdgeSource] = useState("");
  const [selectedEdgeTarget, setSelectedEdgeTarget] = useState("");
  const [selectedInternalNodeId, setSelectedInternalNodeId] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saveMsg, setSaveMsg] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);

  useEffect(() => {
    if (!node) {
      setRootDefinition(null);
      setGroupTemplateId("");
      setPath([]);
      setSelectedInternalNodeId(null);
      setDirty(false);
      setError(null);
      setSaveMsg(null);
      return;
    }
    setGroupTemplateId(groupTemplateIdFromNode(node));
    setRootDefinition(groupDefinitionFromNode(node));
    setPath([]);
    setSelectedInternalNodeId(null);
    setDirty(false);
    setError(null);
    setSaveMsg(null);
  }, [node]);

  const currentDefinition = useMemo(
    () => (rootDefinition ? resolveGroupDefinitionAtPath(rootDefinition, path) : null),
    [rootDefinition, path]
  );
  const currentDepth = path.length + 1;
  const addableNodeSpecs = useMemo(
    () => allNodeSpecs().filter((spec) => spec.kind !== "node_group" && spec.menu?.enabled !== false),
    []
  );
  const outputOptions = useMemo(
    () => (currentDefinition ? buildInternalPortOptions(currentDefinition, "out") : []),
    [currentDefinition]
  );
  const inputOptions = useMemo(
    () => (currentDefinition ? buildInternalPortOptions(currentDefinition, "in") : []),
    [currentDefinition]
  );
  const breadcrumb = useMemo(
    () => (rootDefinition ? breadcrumbItems(rootDefinition, path) : []),
    [rootDefinition, path]
  );
  const currentGroupMeta = useMemo(() => {
    if (currentDefinition) {
      return {
        icon: currentDefinition.display?.icon ?? "▣",
        accent: currentDefinition.display?.accent ?? "#ff9e3d",
        badge: currentDefinition.display?.badge ?? "GROUP",
      };
    }
    return groupVisualMeta(node);
  }, [currentDefinition, node]);
  const selectedInternalNode = useMemo(
    () => currentDefinition?.internal_nodes.find((item) => item.id === selectedInternalNodeId) ?? null,
    [currentDefinition, selectedInternalNodeId]
  );
  const diagnostics = useMemo(
    () => (rootDefinition ? collectDiagnostics(rootDefinition) : []),
    [rootDefinition]
  );
  const currentDiagnostics = useMemo(
    () =>
      diagnostics.filter(
        (diagnostic) =>
          diagnostic.path.length === path.length &&
          diagnostic.path.every((segment, idx) => segment === path[idx])
      ),
    [diagnostics, path]
  );
  const invalidNodeIds = useMemo(
    () =>
      new Set(
        currentDiagnostics
          .filter((diagnostic) => diagnostic.level === "error" && diagnostic.nodeId)
          .map((diagnostic) => diagnostic.nodeId as string)
      ),
    [currentDiagnostics]
  );
  const invalidEdgeKeys = useMemo(
    () =>
      new Set(
        currentDiagnostics
          .filter((diagnostic) => diagnostic.level === "error" && diagnostic.edgeKey)
          .map((diagnostic) => diagnostic.edgeKey as string)
      ),
    [currentDiagnostics]
  );
  const rfNodes = useMemo(
    () => (currentDefinition ? layoutNodes(currentDefinition, invalidNodeIds) : []),
    [currentDefinition, invalidNodeIds]
  );
  const rfEdges = useMemo(
    () => (currentDefinition ? toEdges(currentDefinition, invalidEdgeKeys) : []),
    [currentDefinition, invalidEdgeKeys]
  );
  const blockingDiagnostics = useMemo(
    () => diagnostics.filter((diagnostic) => diagnostic.level === "error"),
    [diagnostics]
  );

  if (!node || !rootDefinition || !currentDefinition) return null;

  const setNextRootDefinition = (next: GroupDefinition) => {
    const ensured = ensureGroupDefinitionLayout(next);
    setRootDefinition(ensured);
    setDirty(true);
    setSaveMsg(null);
  };

  const updateCurrentDefinition = (updater: (current: GroupDefinition) => GroupDefinition) => {
    setNextRootDefinition(updateGroupDefinitionAtPath(rootDefinition, path, updater));
  };

  const addInternalNode = () => {
    setError(null);
    const spec = addableNodeSpecs.find((item) => item.kind === selectedNodeKind);
    if (!spec) {
      setError("Choose an internal node kind to add.");
      return;
    }
    const candidateId = sanitizeId(newNodeId.trim()) || sanitizeId(spec.label.toLowerCase()) || spec.kind;
    if (currentDefinition.internal_nodes.some((n) => n.id === candidateId)) {
      setError(`Internal node id "${candidateId}" already exists.`);
      return;
    }
    updateCurrentDefinition((current) => ({
      ...current,
      internal_nodes: [
        ...current.internal_nodes,
        {
          id: candidateId,
          kind: spec.kind,
          category: spec.category,
          params: {},
          position: defaultGroupNodePosition(current.internal_nodes.length),
        },
      ],
    }));
    setSelectedNodeKind("");
    setNewNodeId("");
  };

  const addNestedGroup = () => {
    setError(null);
    if (currentDepth >= NODE_GROUP_MAX_DEPTH) {
      setError(`Max group depth is ${NODE_GROUP_MAX_DEPTH}.`);
      return;
    }
    const template = GROUP_TEMPLATES.find((item) => item.id === selectedNestedTemplateId) ?? null;
    if (!template) {
      setError("Choose a nested group template.");
      return;
    }
    const candidateId =
      sanitizeId(newNodeId.trim()) || sanitizeId(template.label.toLowerCase()) || "group";
    if (currentDefinition.internal_nodes.some((n) => n.id === candidateId)) {
      setError(`Internal node id "${candidateId}" already exists.`);
      return;
    }
    updateCurrentDefinition((current) => ({
      ...current,
      internal_nodes: [
        ...current.internal_nodes,
        setInternalNodeGroupDefinition(
          {
            id: candidateId,
            kind: "node_group",
            category: "model",
            params: {},
            position: defaultGroupNodePosition(current.internal_nodes.length),
          },
          cloneGroupDefinition(template.definition),
          template.id
        ),
      ],
    }));
    setSelectedNestedTemplateId("");
    setNewNodeId("");
  };

  const removeInternalNode = (nodeId: string) => {
    const prunedOutputIds = new Set(
      currentDefinition.output_bindings
        .filter((binding) => binding.internal_node_id === nodeId)
        .map((binding) => binding.group_output_id)
    );
    updateCurrentDefinition((current) => ({
      ...current,
      internal_nodes: current.internal_nodes.filter((n) => n.id !== nodeId),
      internal_edges: current.internal_edges.filter(
        (edge) => edge.from_node_id !== nodeId && edge.to_node_id !== nodeId
      ),
      input_bindings: current.input_bindings.filter((binding) => binding.internal_node_id !== nodeId),
      outputs: current.outputs.filter((out) => !prunedOutputIds.has(out.id)),
      output_bindings: current.output_bindings.filter((binding) => binding.internal_node_id !== nodeId),
    }));
    setSelectedInternalNodeId((prev) => (prev === nodeId ? null : prev));
  };

  const addInputBinding = () => {
    setError(null);
    const wrapperInput = currentDefinition.inputs.find((item) => item.id === selectedGroupInputId);
    const target = inputOptions.find((item) => item.key === selectedInputBindingTarget);
    if (!wrapperInput || !target) {
      setError("Choose a wrapper input and an internal input.");
      return;
    }
    if (!compatibleSemantics(wrapperInput.semantic, target.port.semantic)) {
      setError(`Cannot bind ${wrapperInput.semantic} to ${target.port.semantic}.`);
      return;
    }
    const binding: GroupInputBinding = {
      group_input_id: wrapperInput.id,
      internal_node_id: target.nodeId,
      internal_port_id: target.port.id,
    };
    updateCurrentDefinition((current) => ({
      ...current,
      input_bindings: [
        ...current.input_bindings.filter(
          (item) =>
            !(
              item.group_input_id === binding.group_input_id &&
              item.internal_node_id === binding.internal_node_id &&
              item.internal_port_id === binding.internal_port_id
            )
        ),
        binding,
      ],
    }));
    setSelectedGroupInputId("");
    setSelectedInputBindingTarget("");
  };

  const removeInputBinding = (binding: GroupInputBinding) => {
    updateCurrentDefinition((current) => ({
      ...current,
      input_bindings: current.input_bindings.filter(
        (item) =>
          !(
            item.group_input_id === binding.group_input_id &&
            item.internal_node_id === binding.internal_node_id &&
            item.internal_port_id === binding.internal_port_id
          )
      ),
    }));
  };

  const addInternalEdge = () => {
    setError(null);
    const source = outputOptions.find((item) => item.key === selectedEdgeSource);
    const target = inputOptions.find((item) => item.key === selectedEdgeTarget);
    if (!source || !target) {
      setError("Choose both a source output and target input.");
      return;
    }
    if (source.nodeId === target.nodeId) {
      setError("Self-links are not supported.");
      return;
    }
    if (!compatibleSemantics(source.port.semantic, target.port.semantic)) {
      setError(`Cannot wire ${source.port.semantic} to ${target.port.semantic}.`);
      return;
    }
    updateCurrentDefinition((current) => ({
      ...current,
      internal_edges: [
        ...current.internal_edges.filter(
          (edge) =>
            !(
              edge.from_node_id === source.nodeId &&
              edge.from_port_id === source.port.id &&
              edge.to_node_id === target.nodeId &&
              edge.to_port_id === target.port.id
            )
        ),
        {
          from_node_id: source.nodeId,
          from_port_id: source.port.id,
          from_output_index: source.portIndex,
          to_node_id: target.nodeId,
          to_port_id: target.port.id,
        },
      ],
    }));
    setSelectedEdgeSource("");
    setSelectedEdgeTarget("");
  };

  const removeInternalEdge = (edgeIndex: number) => {
    updateCurrentDefinition((current) => ({
      ...current,
      internal_edges: current.internal_edges.filter((_, idx) => idx !== edgeIndex),
    }));
  };

  const addOutput = () => {
    setError(null);
    const picked = outputOptions.find((o) => o.key === selectedOutputKey);
    if (!picked) {
      setError("Choose an internal output to expose.");
      return;
    }
    const outputId = sanitizeId(newOutputId.trim()) || sanitizeId(`${picked.nodeId}_${picked.port.id}`) || "group_output";
    const outputLabel = newOutputLabel.trim() || picked.label;
    const binding: GroupOutputBinding = {
      group_output_id: outputId,
      internal_node_id: picked.nodeId,
      internal_port_id: picked.port.id,
      internal_output_index: picked.portIndex,
    };
    updateCurrentDefinition((current) => ({
      ...current,
      outputs: [
        ...current.outputs.filter((o) => o.id !== outputId),
        { id: outputId, label: outputLabel, semantic: picked.port.semantic, optional: true },
      ],
      output_bindings: [
        ...current.output_bindings.filter((o) => o.group_output_id !== outputId),
        binding,
      ],
    }));
    setSelectedOutputKey("");
    setNewOutputId("");
    setNewOutputLabel("");
  };

  const removeOutput = (groupOutputId: string) => {
    updateCurrentDefinition((current) => ({
      ...current,
      outputs: current.outputs.filter((o) => o.id !== groupOutputId),
      output_bindings: current.output_bindings.filter((b) => b.group_output_id !== groupOutputId),
    }));
  };

  const saveDefinition = async () => {
    setSaving(true);
    setError(null);
    setSaveMsg(null);
    try {
      const depth = maxGroupDefinitionDepth(rootDefinition);
      if (depth > NODE_GROUP_MAX_DEPTH) {
        throw new Error(`Group nesting depth ${depth} exceeds max supported depth ${NODE_GROUP_MAX_DEPTH}.`);
      }
      if (blockingDiagnostics.length > 0) {
        throw new Error("Resolve editor validation errors before saving.");
      }
      const ui = {
        ...getUiParams(node),
        group_template_id: groupTemplateId || undefined,
        group_definition: ensureGroupDefinitionLayout(rootDefinition),
      };
      const updated = await patchNodeParams(graphId, node.id, { ui }, { branchId: activeBranchId });
      onNodeUpdated(updated);
      setDirty(false);
      setSaveMsg("Saved group layout and definition.");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  };

  const drillIntoSelectedGroup = () => {
    if (!selectedInternalNode || selectedInternalNode.kind !== "node_group") return;
    const nested = groupDefinitionForInternalNode(selectedInternalNode);
    if (!nested) {
      setError("Selected internal group has no nested definition.");
      return;
    }
    if (currentDepth >= NODE_GROUP_MAX_DEPTH) {
      setError(`Max group depth is ${NODE_GROUP_MAX_DEPTH}.`);
      return;
    }
    setPath((prev) => [...prev, selectedInternalNode.id]);
    setSelectedInternalNodeId(null);
  };

  const currentDepthText = `${currentDepth}/${NODE_GROUP_MAX_DEPTH}`;

  return createPortal(
    <div style={{ position: "fixed", inset: 0, background: "#0b1016", zIndex: 10040 }}>
      <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
        <div
          style={{
            height: 58,
            borderBottom: "1px solid #30363d",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "0 18px",
            gap: 12,
            background: "#0f1419",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: 12, minWidth: 0 }}>
            <button
              type="button"
              onClick={() => {
                if (path.length > 0) {
                  setPath((prev) => prev.slice(0, -1));
                  setSelectedInternalNodeId(null);
                } else {
                  onClose();
                }
              }}
              style={ghostButtonStyle}
            >
              {path.length > 0 ? "Up" : "Back"}
            </button>
            <div style={{ display: "flex", alignItems: "center", gap: 6, minWidth: 0, flexWrap: "wrap" }}>
              <span style={{ fontSize: 12, color: "#8b949e" }}>Workspace / Group editor /</span>
              {breadcrumb.map((item, idx) => (
                <button
                  key={`${item.path.join("/") || "root"}:${idx}`}
                  type="button"
                  onClick={() => {
                    setPath(item.path);
                    setSelectedInternalNodeId(null);
                  }}
                  style={{
                    border: "none",
                    background: "transparent",
                    color: idx === breadcrumb.length - 1 ? "#e6edf3" : "#8b949e",
                    cursor: "pointer",
                    padding: 0,
                    fontSize: 12,
                    fontWeight: idx === breadcrumb.length - 1 ? 700 : 500,
                  }}
                >
                  {item.label}
                </button>
              ))}
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span style={{ color: currentGroupMeta.accent }}>{currentGroupMeta.icon}</span>
            <span style={{ ...badgeStyle, borderColor: `${currentGroupMeta.accent}66`, color: currentGroupMeta.accent }}>
              {currentGroupMeta.badge}
            </span>
            <span style={{ fontSize: 12, color: "#8b949e" }}>Depth {currentDepthText}</span>
            <span style={{ fontSize: 12, color: blockingDiagnostics.length > 0 ? "#ffb4ad" : "#8b949e" }}>
              {blockingDiagnostics.length > 0
                ? `${blockingDiagnostics.length} blocking`
                : currentDiagnostics.length > 0
                  ? `${currentDiagnostics.length} notices`
                  : "No issues"}
            </span>
            {saveMsg ? <div style={{ fontSize: 12, color: "#7ee787" }}>{saveMsg}</div> : null}
            {error ? <div style={{ fontSize: 12, color: "#ffb4ad", maxWidth: 460 }}>{error}</div> : null}
            <button type="button" onClick={saveDefinition} disabled={!dirty || saving || blockingDiagnostics.length > 0} style={primaryButtonStyle}>
              {saving ? "Saving…" : dirty ? "Save changes" : "Saved"}
            </button>
          </div>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "1.45fr 0.95fr", minHeight: 0, flex: 1 }}>
          <div style={{ minHeight: 0, borderRight: "1px solid #30363d" }}>
            <ReactFlow
              nodes={rfNodes}
              edges={rfEdges}
              fitView
              onNodeClick={(_, clicked) => setSelectedInternalNodeId(clicked.id)}
              onNodeDoubleClick={(_, clicked) => {
                const internal = currentDefinition.internal_nodes.find((item) => item.id === clicked.id) ?? null;
                if (!internal || internal.kind !== "node_group") return;
                setSelectedInternalNodeId(clicked.id);
                const nested = groupDefinitionForInternalNode(internal);
                if (nested && currentDepth < NODE_GROUP_MAX_DEPTH) {
                  setPath((prev) => [...prev, clicked.id]);
                }
              }}
              onNodeDragStop={(_, dragged) => {
                updateCurrentDefinition((current) => ({
                  ...current,
                  internal_nodes: current.internal_nodes.map((internalNode) =>
                    internalNode.id === dragged.id
                      ? {
                          ...internalNode,
                          position: {
                            x: Math.round(dragged.position.x),
                            y: Math.round(dragged.position.y),
                          },
                        }
                      : internalNode
                  ),
                }));
              }}
            >
              <Background color="#30363d" gap={20} size={1} />
              <Controls />
            </ReactFlow>
          </div>

          <div style={{ minHeight: 0, overflow: "auto", padding: 16, display: "grid", gap: 14, background: "#11161d" }}>
            <section style={sectionStyle}>
              <div style={sectionTitleStyle}>Current group</div>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ color: currentGroupMeta.accent }}>{currentGroupMeta.icon}</span>
                <div style={{ color: "#e6edf3", fontWeight: 700 }}>{currentDefinition.label}</div>
              </div>
              <div style={metaTextStyle}>
                Double-click a nested group on the canvas to drill in. Breadcrumbs let you move back up the stack.
              </div>
              {currentDiagnostics.length > 0 ? (
                <div style={{ display: "grid", gap: 8 }}>
                  {currentDiagnostics.map((diagnostic, idx) => (
                    <div
                      key={`${diagnostic.path.join("/")}:${idx}:${diagnostic.message}`}
                      style={{
                        ...diagnosticStyle,
                        borderColor: diagnostic.level === "error" ? "#f85149" : "#d29922",
                        background: diagnostic.level === "error" ? "rgba(248,81,73,0.10)" : "rgba(210,153,34,0.10)",
                        color: diagnostic.level === "error" ? "#ffb4ad" : "#f2cc60",
                      }}
                    >
                      <strong style={{ marginRight: 6 }}>{diagnostic.level === "error" ? "Error" : "Warning"}</strong>
                      {diagnostic.message}
                    </div>
                  ))}
                </div>
              ) : (
                <div style={{ ...metaTextStyle, color: "#7ee787" }}>No validation issues in this group.</div>
              )}
            </section>

            {path.length === 0 && (
              <section style={sectionStyle}>
                <div style={sectionTitleStyle}>Root template</div>
                <select
                  value={groupTemplateId}
                  onChange={(e) => {
                    const nextTemplateId = e.target.value;
                    setGroupTemplateId(nextTemplateId);
                    const template = GROUP_TEMPLATES.find((t) => t.id === nextTemplateId) ?? null;
                    if (template) {
                      setNextRootDefinition(cloneGroupDefinition(template.definition));
                      setPath([]);
                      setSelectedInternalNodeId(null);
                    }
                  }}
                  style={inputStyle}
                >
                  <option value="">Select template…</option>
                  {GROUP_TEMPLATES.map((template) => (
                    <option key={template.id} value={template.id}>
                      {template.label}
                    </option>
                  ))}
                </select>
                <div style={metaTextStyle}>Changing the root template replaces the top-level group definition.</div>
              </section>
            )}

            <section style={sectionStyle}>
              <div style={sectionTitleStyle}>Internal nodes</div>
              {selectedInternalNode ? (
                <div style={metaTextStyle}>Selected on canvas: {selectedInternalNode.id}</div>
              ) : null}
              {selectedInternalNode?.kind === "node_group" && (
                <button type="button" onClick={drillIntoSelectedGroup} style={ghostButtonStyle}>
                  Open nested group
                </button>
              )}
              <div style={{ display: "grid", gap: 8 }}>
                {currentDefinition.internal_nodes.map((internalNode) => (
                  <div key={internalNode.id} style={cardStyle}>
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                      <div>
                        <div style={{ fontWeight: 600 }}>
                          {internalNode.kind === "node_group" ? `${groupDefinitionForInternalNode(internalNode)?.display?.icon ?? "▣"} ` : ""}
                          {nodeSpec(internalNode.kind)?.label ?? internalNode.kind}
                        </div>
                        <div style={metaTextStyle}>
                          {internalNode.id} · {internalNode.kind}
                          {internalNode.kind === "node_group" ? ` · depth ${Math.min(NODE_GROUP_MAX_DEPTH, currentDepth + 1)}` : ""}
                        </div>
                      </div>
                      <button type="button" onClick={() => removeInternalNode(internalNode.id)} style={ghostButtonStyle}>
                        Remove
                      </button>
                    </div>
                  </div>
                ))}
              </div>
              <div style={{ display: "grid", gap: 8, borderTop: "1px solid #30363d", paddingTop: 12 }}>
                <select value={selectedNodeKind} onChange={(e) => setSelectedNodeKind(e.target.value)} style={inputStyle}>
                  <option value="">Choose internal node kind…</option>
                  {addableNodeSpecs.map((spec) => (
                    <option key={spec.kind} value={spec.kind}>
                      {spec.label}
                    </option>
                  ))}
                </select>
                <input value={newNodeId} onChange={(e) => setNewNodeId(e.target.value)} placeholder="Internal node id" style={inputStyle} />
                <button type="button" onClick={addInternalNode} style={primaryButtonStyle}>
                  Add internal node
                </button>
              </div>
              {currentDepth < NODE_GROUP_MAX_DEPTH ? (
                <div style={{ display: "grid", gap: 8, borderTop: "1px solid #30363d", paddingTop: 12 }}>
                  <div style={{ ...metaTextStyle, color: "#ffb86b" }}>Nested groups</div>
                  <select value={selectedNestedTemplateId} onChange={(e) => setSelectedNestedTemplateId(e.target.value)} style={inputStyle}>
                    <option value="">Choose nested group template…</option>
                    {GROUP_TEMPLATES.map((template) => (
                      <option key={template.id} value={template.id}>
                        {template.label}
                      </option>
                    ))}
                  </select>
                  <button type="button" onClick={addNestedGroup} style={ghostButtonStyle}>
                    Add nested group
                  </button>
                </div>
              ) : (
                <div style={metaTextStyle}>Max nested depth reached for this branch.</div>
              )}
            </section>

            <section style={sectionStyle}>
              <div style={sectionTitleStyle}>Wrapper input bindings</div>
              <div style={{ display: "grid", gap: 8 }}>
                {currentDefinition.input_bindings.map((binding) => {
                  const wrapperInput = currentDefinition.inputs.find((item) => item.id === binding.group_input_id);
                  return (
                    <div key={`${binding.group_input_id}:${binding.internal_node_id}:${binding.internal_port_id}`} style={cardStyle}>
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                        <div>
                          <div style={{ fontWeight: 600 }}>{wrapperInput?.label ?? binding.group_input_id}</div>
                          <div style={metaTextStyle}>
                            {binding.group_input_id}{" → "}{binding.internal_node_id}.{binding.internal_port_id}
                          </div>
                        </div>
                        <button type="button" onClick={() => removeInputBinding(binding)} style={ghostButtonStyle}>
                          Remove
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>
              {currentDefinition.inputs.length > 0 ? (
                <div style={{ display: "grid", gap: 8, borderTop: "1px solid #30363d", paddingTop: 12 }}>
                  <select value={selectedGroupInputId} onChange={(e) => setSelectedGroupInputId(e.target.value)} style={inputStyle}>
                    <option value="">Choose wrapper input…</option>
                    {currentDefinition.inputs.map((input) => (
                      <option key={input.id} value={input.id}>
                        {input.label} · {input.semantic}
                      </option>
                    ))}
                  </select>
                  <select value={selectedInputBindingTarget} onChange={(e) => setSelectedInputBindingTarget(e.target.value)} style={inputStyle}>
                    <option value="">Choose internal input…</option>
                    {inputOptions.map((option) => (
                      <option key={option.key} value={option.key}>
                        {option.label} · {option.port.semantic}
                      </option>
                    ))}
                  </select>
                  <button type="button" onClick={addInputBinding} style={primaryButtonStyle}>
                    Add wrapper input binding
                  </button>
                </div>
              ) : (
                <div style={metaTextStyle}>This group currently has no wrapper inputs.</div>
              )}
            </section>

            <section style={sectionStyle}>
              <div style={sectionTitleStyle}>Internal wiring</div>
              <div style={{ display: "grid", gap: 8 }}>
                {currentDefinition.internal_edges.map((edge, idx) => (
                  <div key={`${idx}:${edge.from_node_id}:${edge.to_node_id}:${edge.from_port_id}:${edge.to_port_id}`} style={cardStyle}>
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                      <div style={{ fontWeight: 600 }}>
                        {edge.from_node_id}.{edge.from_port_id}{" → "}{edge.to_node_id}.{edge.to_port_id}
                      </div>
                      <button type="button" onClick={() => removeInternalEdge(idx)} style={ghostButtonStyle}>
                        Remove
                      </button>
                    </div>
                  </div>
                ))}
              </div>
              <div style={{ display: "grid", gap: 8, borderTop: "1px solid #30363d", paddingTop: 12 }}>
                <select value={selectedEdgeSource} onChange={(e) => setSelectedEdgeSource(e.target.value)} style={inputStyle}>
                  <option value="">Choose source output…</option>
                  {outputOptions.map((option) => (
                    <option key={option.key} value={option.key}>
                      {option.label} · {option.port.semantic}
                    </option>
                  ))}
                </select>
                <select value={selectedEdgeTarget} onChange={(e) => setSelectedEdgeTarget(e.target.value)} style={inputStyle}>
                  <option value="">Choose target input…</option>
                  {inputOptions.map((option) => (
                    <option key={option.key} value={option.key}>
                      {option.label} · {option.port.semantic}
                    </option>
                  ))}
                </select>
                <button type="button" onClick={addInternalEdge} style={primaryButtonStyle}>
                  Add internal wire
                </button>
              </div>
            </section>

            <section style={sectionStyle}>
              <div style={sectionTitleStyle}>Exposed outputs</div>
              <div style={{ display: "grid", gap: 8 }}>
                {currentDefinition.outputs.map((out) => {
                  const binding = currentDefinition.output_bindings.find((b) => b.group_output_id === out.id);
                  return (
                    <div key={out.id} style={cardStyle}>
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                        <div>
                          <div style={{ fontWeight: 600 }}>{out.label}</div>
                          <div style={metaTextStyle}>{out.id} · {out.semantic}</div>
                          {binding ? (
                            <div style={{ ...metaTextStyle, marginTop: 4 }}>
                              {binding.internal_node_id}.{binding.internal_port_id}
                            </div>
                          ) : null}
                        </div>
                        <button type="button" onClick={() => removeOutput(out.id)} style={ghostButtonStyle}>
                          Remove
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>
              <div style={{ display: "grid", gap: 8, borderTop: "1px solid #30363d", paddingTop: 12 }}>
                <select value={selectedOutputKey} onChange={(e) => setSelectedOutputKey(e.target.value)} style={inputStyle}>
                  <option value="">Choose internal output…</option>
                  {outputOptions.map((option) => (
                    <option key={option.key} value={option.key}>
                      {option.label} · {option.port.semantic}
                    </option>
                  ))}
                </select>
                <input value={newOutputId} onChange={(e) => setNewOutputId(e.target.value)} placeholder="Wrapper output id" style={inputStyle} />
                <input value={newOutputLabel} onChange={(e) => setNewOutputLabel(e.target.value)} placeholder="Wrapper output label" style={inputStyle} />
                <button type="button" onClick={addOutput} style={primaryButtonStyle}>
                  Add exposed output
                </button>
              </div>
            </section>
          </div>
        </div>
      </div>
    </div>,
    document.body
  );
}

const sectionStyle = {
  border: "1px solid #30363d",
  borderRadius: 12,
  padding: 12,
  display: "grid",
  gap: 10,
  background: "#11161d",
} as const;

const sectionTitleStyle = {
  color: "#e6edf3",
  fontWeight: 700,
  fontSize: 14,
} as const;

const cardStyle = {
  border: "1px solid #30363d",
  borderRadius: 10,
  padding: 10,
  background: "#161b22",
  color: "#e6edf3",
} as const;

const metaTextStyle = {
  fontSize: 12,
  opacity: 0.8,
  color: "#9da7b3",
} as const;

const inputStyle = {
  background: "#0f1419",
  border: "1px solid #30363d",
  color: "#e6edf3",
  borderRadius: 8,
  padding: "10px 12px",
} as const;

const badgeStyle = {
  display: "inline-flex",
  alignItems: "center",
  height: 20,
  padding: "0 8px",
  borderRadius: 999,
  border: "1px solid #30363d",
  fontSize: 10,
  fontWeight: 700,
  letterSpacing: "0.08em",
  textTransform: "uppercase" as const,
} as const;

const ghostButtonStyle = {
  border: "1px solid #30363d",
  borderRadius: 8,
  background: "#0f1419",
  color: "#e6edf3",
  cursor: "pointer",
  padding: "8px 12px",
} as const;

const primaryButtonStyle = {
  border: "1px solid #2ea043",
  borderRadius: 8,
  background: "#238636",
  color: "#fff",
  cursor: "pointer",
  padding: "10px 12px",
  fontWeight: 600,
} as const;

const diagnosticStyle = {
  border: "1px solid #30363d",
  borderRadius: 10,
  padding: "10px 12px",
  fontSize: 12,
  lineHeight: 1.45,
} as const;
