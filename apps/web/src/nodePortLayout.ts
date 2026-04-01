import type { ApiEdge } from "./graphApi";
import { nodePorts } from "./nodeRegistry";

/** Distinct input port ids (target handles) for this node. */
export function incomingPortIds(
  nodeId: string,
  nodeKind: string,
  edges: ApiEdge[]
): string[] {
  const declared = nodePorts(nodeKind, "in").map((p) => p.id);
  const set = new Set<string>();
  for (const e of edges) {
    if (e.to_node === nodeId) set.add(e.to_port);
  }
  if (declared.length > 0) {
    declared.forEach((p) => set.add(p));
  }
  const arr = [...set];
  return arr.length > 0 ? arr.sort() : ["in"];
}

/** Distinct output port ids (source handles) for this node. */
export function outgoingPortIds(
  nodeId: string,
  nodeKind: string,
  edges: ApiEdge[]
): string[] {
  const declared = nodePorts(nodeKind, "out").map((p) => p.id);
  const set = new Set<string>();
  for (const e of edges) {
    if (e.from_node === nodeId) set.add(e.from_port);
  }
  if (declared.length > 0) {
    declared.forEach((p) => set.add(p));
  }
  const arr = [...set];
  return arr.length > 0 ? arr.sort() : ["out"];
}
