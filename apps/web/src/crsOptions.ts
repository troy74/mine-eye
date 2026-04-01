/**
 * Curated EPSG entries for acquisition “Source file CRS” pickers (`NodeInspector` → CRS tab).
 *
 * - Add a row here when you need it in the UI.
 * - For **2D map** preview of projected coordinates, also add a matching proj4 string in
 *   `spatialReproject.ts` (`EPSG_DEFS`). Without that, the map will warn and skip reprojection.
 *
 * Full registry: https://epsg.io/ — proj4 strings: `https://epsg.io/<code>.proj4`
 */
export const ACQUISITION_EPSG_OPTIONS: { value: string; label: string }[] = [
  { value: "project", label: "Same as project CRS" },
  { value: "custom", label: "Custom EPSG…" },
  { value: "4326", label: "EPSG:4326 (WGS84 geographic)" },
  { value: "7855", label: "EPSG:7855 (GDA2020 / MGA zone 55)" },
  { value: "7856", label: "EPSG:7856 (GDA2020 / MGA zone 56)" },
  { value: "7850", label: "EPSG:7850 (GDA2020 / MGA zone 50)" },
  { value: "28355", label: "EPSG:28355 (GDA94 / MGA zone 55)" },
  { value: "28356", label: "EPSG:28356 (GDA94 / MGA zone 56)" },
  { value: "28350", label: "EPSG:28350 (GDA94 / MGA zone 50)" },
  { value: "4978", label: "EPSG:4978 (WGS84 geocentric XYZ — avoid for 2D maps)" },
];
