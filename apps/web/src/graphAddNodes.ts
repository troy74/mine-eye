export type { AddNodePreset } from "./nodeRegistry";
import { addNodePresetsFromRegistry } from "./nodeRegistry";

/** Right-click “Add node” presets from central node registry config. */
export function getAddNodePresets(): AddNodePreset[] {
  return addNodePresetsFromRegistry();
}
