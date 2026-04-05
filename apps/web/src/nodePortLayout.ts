import type { ApiEdge } from "./graphApi";
import { nodePorts } from "./nodeRegistry";

const THREEJS_DYNAMIC_INPUT_KIND = "threejs_display_node";

function naturalPortCompare(a: string, b: string): number {
  const am = /^in_(\d+)$/.exec(a);
  const bm = /^in_(\d+)$/.exec(b);
  if (am && bm) return Number(am[1]) - Number(bm[1]);
  if (am) return -1;
  if (bm) return 1;
  return a.localeCompare(b);
}

/** Distinct input port ids (target handles) for this node. */
export function incomingPortIds(
  nodeId: string,
  nodeKind: string,
  edges: ApiEdge[]
): string[] {
  const declared = nodePorts(nodeKind, "in").map((p) => p.id);
  if (nodeKind === THREEJS_DYNAMIC_INPUT_KIND) {
    const genericInUse = new Set<number>();
    const legacyInUse = new Set<string>();
    for (const e of edges) {
      if (e.to_node !== nodeId) continue;
      const m = /^in_(\d+)$/.exec(e.to_port);
      if (m) genericInUse.add(Math.max(1, Number(m[1])));
      else legacyInUse.add(e.to_port);
    }
    let declaredMax = 2;
    for (const id of declared) {
      const m = /^in_(\d+)$/.exec(id);
      if (!m) continue;
      declaredMax = Math.max(declaredMax, Math.max(1, Number(m[1])));
    }
    let usedMax = 0;
    for (const i of genericInUse) usedMax = Math.max(usedMax, i);
    const slotCount = Math.max(declaredMax, usedMax + 1);
    const out: string[] = [];
    for (let i = 1; i <= slotCount; i++) out.push(`in_${i}`);
    const legacy = [...legacyInUse].sort(naturalPortCompare);
    return [...out, ...legacy];
  }
  const set = new Set<string>();
  for (const e of edges) {
    if (e.to_node === nodeId) set.add(e.to_port);
  }
  if (declared.length > 0) {
    declared.forEach((p) => set.add(p));
  }
  const arr = [...set].sort(naturalPortCompare);
  return arr.length > 0 ? arr : ["in"];
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
