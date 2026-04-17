export type { AddNodePreset } from "./nodeRegistry";
import { addNodePresetsFromRegistry } from "./nodeRegistry";
import { GROUP_TEMPLATES, cloneGroupDefinition } from "./nodeGroup";

/** Right-click “Add node” presets from central node registry config. */
export function getAddNodePresets(): AddNodePreset[] {
  const nodePresets = addNodePresetsFromRegistry();
  const groupPresets: AddNodePreset[] = GROUP_TEMPLATES.map((template) => ({
    category: "model",
    kind: "node_group",
    label: template.label,
    frameworkGroup: "groups",
    submenu: "core",
    pluginSource: "core",
    isGroup: true,
    icon: template.definition.display?.icon ?? "▣",
    accent: template.definition.display?.accent ?? "#ff9e3d",
    params: {
      ui: {
        group_template_id: template.id,
        group_definition: cloneGroupDefinition(template.definition),
        _alias: template.label,
      },
    },
  }));
  return [...groupPresets, ...nodePresets];
}
