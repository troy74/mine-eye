import type { ApiEdge } from "./graphApi";

/** Distinct input port ids (target handles) for this node. */
export function incomingPortIds(nodeId: string, edges: ApiEdge[]): string[] {
  const set = new Set<string>();
  for (const e of edges) {
    if (e.to_node === nodeId) set.add(e.to_port);
  }
  const arr = [...set];
  return arr.length > 0 ? arr.sort() : ["in"];
}

/** Distinct output port ids (source handles) for this node. */
export function outgoingPortIds(nodeId: string, edges: ApiEdge[]): string[] {
  const set = new Set<string>();
  for (const e of edges) {
    if (e.from_node === nodeId) set.add(e.from_port);
  }
  const arr = [...set];
  return arr.length > 0 ? arr.sort() : ["out"];
}
