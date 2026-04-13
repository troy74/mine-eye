/**
 * Shared colour-ramp definitions and interpolation used by Map2DPanel and
 * Map3DThreePanel.  A single canonical copy here avoids the four separate
 * palette implementations that previously existed across both files.
 */

export type PaletteStop = { t: number; r: number; g: number; b: number };
export type PaletteName = "mineeye" | "inferno" | "viridis" | "terrain" | "grayscale";

const STOPS: Record<PaletteName, PaletteStop[]> = {
  mineeye: [
    { t: 0.00, r:  44, g: 123, b: 182 },
    { t: 0.25, r:   0, g: 166, b: 202 },
    { t: 0.50, r:   0, g: 204, b: 106 },
    { t: 0.75, r: 249, g: 208, b:  87 },
    { t: 1.00, r: 215, g:  25, b:  28 },
  ],
  inferno: [
    { t: 0.00, r:   0, g:   0, b:   4 },
    { t: 0.20, r:  43, g:  10, b:  90 },
    { t: 0.45, r: 120, g:  28, b: 109 },
    { t: 0.70, r: 209, g:  58, b:  47 },
    { t: 1.00, r: 255, g:  59, b:  47 },
  ],
  viridis: [
    { t: 0.00, r:  68, g:   1, b:  84 },
    { t: 0.25, r:  59, g:  82, b: 139 },
    { t: 0.50, r:  33, g: 144, b: 140 },
    { t: 0.75, r:  93, g: 200, b:  99 },
    { t: 1.00, r: 253, g: 231, b:  37 },
  ],
  terrain: [
    { t: 0.00, r:  43, g: 131, b: 186 },
    { t: 0.35, r: 171, g: 221, b: 164 },
    { t: 0.60, r: 102, g: 189, b:  99 },
    { t: 0.80, r: 253, g: 174, b:  97 },
    { t: 1.00, r: 215, g:  25, b:  28 },
  ],
  grayscale: [
    { t: 0.00, r:   0, g:   0, b:   0 },
    { t: 1.00, r: 255, g: 255, b: 255 },
  ],
};

/** Resolve a palette name, falling back to "mineeye" for unknown names. */
export function resolvePaletteName(name: string): PaletteName {
  if (name in STOPS) return name as PaletteName;
  return "mineeye";
}

/**
 * Interpolate a colour in a named ramp at position t ∈ [0, 1].
 * Returns [r, g, b] as integers 0–255.
 */
export function interpolatePalette(
  name: PaletteName | string,
  t: number
): [number, number, number] {
  const stops = STOPS[resolvePaletteName(name)];
  const tc = Math.max(0, Math.min(1, t));
  for (let i = 1; i < stops.length; i++) {
    const a = stops[i - 1];
    const b = stops[i];
    if (tc <= b.t) {
      const r = (tc - a.t) / Math.max(1e-9, b.t - a.t);
      const lerp = (av: number, bv: number) =>
        Math.round(av + (bv - av) * r);
      return [lerp(a.r, b.r), lerp(a.g, b.g), lerp(a.b, b.b)];
    }
  }
  const last = stops[stops.length - 1];
  return [last.r, last.g, last.b];
}

/**
 * Return an rgba CSS string for use in canvas or CSS.
 */
export function interpolatePaletteRgba(
  name: PaletteName | string,
  t: number,
  alpha = 1
): string {
  const [r, g, b] = interpolatePalette(name, t);
  return `rgba(${r},${g},${b},${alpha})`;
}

/** Convenience: return a hex colour string. */
export function interpolatePaletteHex(
  name: PaletteName | string,
  t: number
): string {
  const [r, g, b] = interpolatePalette(name, t);
  return `#${r.toString(16).padStart(2, "0")}${g.toString(16).padStart(2, "0")}${b.toString(16).padStart(2, "0")}`;
}
