import type { ApiEdge } from "./graphApi";
import { nodePorts } from "./nodeRegistry";

// ── Dynamic port group ────────────────────────────────────────────────────────
//
// Some nodes have a variable number of ports of the same conceptual type (e.g.
// "drop any geometry here to seed the AOI"). Rather than declaring 8 static
// blanks in the registry we define a *dynamic group*: the UI always shows
// exactly (max_connected + 1) slots, with a minimum floor.
//
export type DynGroup = {
  direction: "in" | "out";
  /** Generate the port-id for slot n (0-indexed). */
  slotId: (n: number) => string;
  /** Parse a port-id back to its 0-indexed slot number, or null. */
  slotIndex: (portId: string) => number | null;
  /** Always show at least this many slots (floor). */
  minSlots: number;
  /** Human label shown on the card for any slot in this group. */
  label: string;
  /** Semantic used for unconnected slots (for colour). */
  semantic: string;
};

// Ports named: base, base_2, base_3, ...  (e.g. "aoi_in", "aoi_in_2", ...)
function baseNGroup(
  base: string,
  dir: "in" | "out",
  label: string,
  semantic: string,
  minSlots = 2
): DynGroup {
  const esc = base.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const re = new RegExp(`^${esc}_(\\d+)$`);
  return {
    direction: dir,
    slotId: (n) => (n === 0 ? base : `${base}_${n + 1}`),
    slotIndex: (p) => {
      if (p === base) return 0;
      const m = re.exec(p);
      if (!m) return null;
      const i = Number(m[1]) - 1;
      return i >= 1 ? i : null;
    },
    minSlots,
    label,
    semantic,
  };
}

// Ports named: prefix_startN, prefix_(startN+1), ...  (e.g. "in_1", "in_2", ...)
function rangeNGroup(
  prefix: string,
  startN: number,
  label: string,
  semantic: string,
  minSlots = 2
): DynGroup {
  const esc = prefix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const re = new RegExp(`^${esc}_(\\d+)$`);
  return {
    direction: "in",
    slotId: (n) => `${prefix}_${n + startN}`,
    slotIndex: (p) => {
      const m = re.exec(p);
      if (!m) return null;
      const idx = Number(m[1]) - startN;
      return idx >= 0 ? idx : null;
    },
    minSlots,
    label,
    semantic,
  };
}

export const DYNAMIC_PORT_GROUPS: Record<string, DynGroup[]> = {
  threejs_display_node:  [rangeNGroup("in", 1, "Input",    "any",       2)],
  plan_view_2d:          [baseNGroup("in", "in", "Layer",  "any",            2)],
  plan_view_3d:          [rangeNGroup("in", 5, "Layer",    "data_table",     1)],
  cesium_display_node:   [rangeNGroup("in", 5, "Layer",    "data_table",     1)],
  dem_fetch:             [baseNGroup("aoi_in", "in", "Geometry", "point_set", 2)],
  aoi:                   [baseNGroup("seed_in", "in", "Geometry", "point_set", 2)],
  xyz_to_surface:        [baseNGroup("xyz_in", "in", "Geometry", "point_set", 2)],
  data_model_transform:  [baseNGroup("in", "in", "Input",  "data_table",     2)],
};

/** Label/semantic for a port that belongs to a dynamic group (for card display). */
export function dynPortInfo(
  kind: string,
  portId: string
): { label: string; semantic: string } | null {
  for (const g of DYNAMIC_PORT_GROUPS[kind] ?? []) {
    if (g.slotIndex(portId) !== null) return { label: g.label, semantic: g.semantic };
  }
  return null;
}

/** Distinct input port ids (target handles) for this node. */
export function incomingPortIds(
  nodeId: string,
  nodeKind: string,
  edges: ApiEdge[]
): string[] {
  const declared = nodePorts(nodeKind, "in").map((p) => p.id);
  const groups = (DYNAMIC_PORT_GROUPS[nodeKind] ?? []).filter(
    (g) => g.direction === "in"
  );

  if (groups.length === 0) {
    // No dynamic groups — simple union of declared + connected ports
    const set = new Set<string>();
    for (const e of edges) if (e.to_node === nodeId) set.add(e.to_port);
    declared.forEach((p) => set.add(p));
    const arr = [...set].sort(naturalPortCompare);
    return arr.length > 0 ? arr : ["in"];
  }

  // Pre-compute slot counts for each group
  const slotCounts = groups.map((g) => {
    let maxIdx = -1;
    for (const e of edges) {
      if (e.to_node !== nodeId) continue;
      const idx = g.slotIndex(e.to_port);
      if (idx !== null && idx > maxIdx) maxIdx = idx;
    }
    return Math.max(g.minSlots, maxIdx + 2); // always leave one free
  });

  const isInGroup = (p: string) => groups.some((g) => g.slotIndex(p) !== null);
  const expanded = new Set<number>();
  const result: string[] = [];

  // Walk declared ports; expand dynamic groups at their anchor (slot 0)
  for (const p of declared) {
    const gIdx = groups.findIndex((g) => g.slotIndex(p) === 0);
    if (gIdx >= 0 && !expanded.has(gIdx)) {
      for (let i = 0; i < slotCounts[gIdx]; i++) result.push(groups[gIdx].slotId(i));
      expanded.add(gIdx);
    } else if (!isInGroup(p) && !result.includes(p)) {
      result.push(p);
    }
    // non-anchor group port: already covered by expansion — skip
  }

  // Append any groups whose anchor wasn't in the registry (appended at end)
  groups.forEach((g, idx) => {
    if (!expanded.has(idx)) {
      for (let i = 0; i < slotCounts[idx]; i++) result.push(g.slotId(i));
    }
  });

  // Append legacy connected ports not covered by groups or declared
  const seen = new Set(result);
  for (const e of edges) {
    if (e.to_node === nodeId && !isInGroup(e.to_port) && !seen.has(e.to_port)) {
      result.push(e.to_port);
      seen.add(e.to_port);
    }
  }

  return result.length > 0 ? result : ["in"];
}

/** Distinct output port ids (source handles) for this node. */
export function outgoingPortIds(
  nodeId: string,
  nodeKind: string,
  edges: ApiEdge[]
): string[] {
  const declared = nodePorts(nodeKind, "out").map((p) => p.id);
  const set = new Set<string>();
  for (const e of edges) if (e.from_node === nodeId) set.add(e.from_port);
  declared.forEach((p) => set.add(p));
  const arr = [...set];
  return arr.length > 0 ? arr.sort() : ["out"];
}

function naturalPortCompare(a: string, b: string): number {
  const am = /^in_(\d+)$/.exec(a);
  const bm = /^in_(\d+)$/.exec(b);
  if (am && bm) return Number(am[1]) - Number(bm[1]);
  if (am) return -1;
  if (bm) return 1;
  return a.localeCompare(b);
}
