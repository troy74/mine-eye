import L from "leaflet";

import type { RasterLatLngBounds } from "./rasterOverlay";

export type LeafletRasterPane = "mineeye-raster-base" | "mineeye-raster-analytic";

export type LocalExtentTileLayerOptions = L.LayerOptions & {
  bounds: RasterLatLngBounds;
  tileUrlTemplate: string;
  tileMinZoom: number;
  tileMaxZoom: number;
  opacity: number;
  pane?: LeafletRasterPane;
};

function clampInt(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, Math.round(value)));
}

function toLeafletBounds(bounds: RasterLatLngBounds): L.LatLngBounds {
  return L.latLngBounds([bounds.south, bounds.west], [bounds.north, bounds.east]);
}

export function createBoundedImageLayer(
  url: string,
  bounds: RasterLatLngBounds,
  options?: { opacity?: number; pane?: LeafletRasterPane }
): L.ImageOverlay {
  return L.imageOverlay(url, toLeafletBounds(bounds), {
    opacity: options?.opacity ?? 1,
    pane: options?.pane,
    interactive: false,
  });
}

export function createGlobalTileLayer(
  tileUrlTemplate: string,
  options: {
    minZoom: number;
    maxZoom: number;
    opacity: number;
    attribution?: string;
    pane?: LeafletRasterPane;
  }
): L.TileLayer {
  return L.tileLayer(tileUrlTemplate, {
    minZoom: options.minZoom,
    maxZoom: options.maxZoom,
    opacity: options.opacity,
    attribution: options.attribution,
    pane: options.pane,
    updateWhenIdle: true,
    updateWhenZooming: false,
    keepBuffer: 2,
  });
}

class LocalExtentTileLayer extends L.Layer {
  declare options: LocalExtentTileLayerOptions;

  private mapInstance: L.Map | null = null;
  private tileGroup: L.LayerGroup | null = null;

  constructor(options: LocalExtentTileLayerOptions) {
    super(options);
    this.options = options;
  }

  onAdd(map: L.Map): this {
    this.mapInstance = map;
    this.tileGroup = L.layerGroup().addTo(map);
    map.on("moveend zoomend resize viewreset", this.refresh, this);
    this.refresh();
    return this;
  }

  onRemove(map: L.Map): this {
    map.off("moveend zoomend resize viewreset", this.refresh, this);
    if (this.tileGroup) {
      this.tileGroup.clearLayers();
      if (map.hasLayer(this.tileGroup)) {
        map.removeLayer(this.tileGroup);
      }
    }
    this.tileGroup = null;
    this.mapInstance = null;
    return this;
  }

  setOpacity(opacity: number): this {
    this.options.opacity = opacity;
    this.refresh();
    return this;
  }

  private refresh(): void {
    const map = this.mapInstance;
    const group = this.tileGroup;
    if (!map || !group) return;
    group.clearLayers();

    const bounds = this.options.bounds;
    const sourceBounds = toLeafletBounds(bounds);
    const visibleBounds = sourceBounds.intersection(map.getBounds());
    if (!visibleBounds.isValid()) {
      return;
    }

    const zoom = clampInt(
      Math.round(map.getZoom()),
      this.options.tileMinZoom,
      this.options.tileMaxZoom
    );
    const n = 1 << zoom;
    const lonSpan = Math.max(1e-12, bounds.east - bounds.west);
    const latSpan = Math.max(1e-12, bounds.north - bounds.south);

    const fx0 = (visibleBounds.getWest() - bounds.west) / lonSpan;
    const fx1 = (visibleBounds.getEast() - bounds.west) / lonSpan;
    const fy0 = (bounds.north - visibleBounds.getNorth()) / latSpan;
    const fy1 = (bounds.north - visibleBounds.getSouth()) / latSpan;

    const xStart = clampInt(Math.floor(fx0 * n), 0, n - 1);
    const xEnd = clampInt(Math.ceil(fx1 * n) - 1, 0, n - 1);
    const yStart = clampInt(Math.floor(fy0 * n), 0, n - 1);
    const yEnd = clampInt(Math.ceil(fy1 * n) - 1, 0, n - 1);

    for (let ty = yStart; ty <= yEnd; ty++) {
      for (let tx = xStart; tx <= xEnd; tx++) {
        const tileWest = bounds.west + (tx / n) * lonSpan;
        const tileEast = bounds.west + ((tx + 1) / n) * lonSpan;
        const tileNorth = bounds.north - (ty / n) * latSpan;
        const tileSouth = bounds.north - ((ty + 1) / n) * latSpan;
        const url = this.options.tileUrlTemplate
          .replace("{z}", String(zoom))
          .replace("{x}", String(tx))
          .replace("{y}", String(ty));
        createBoundedImageLayer(
          url,
          {
            south: tileSouth,
            west: tileWest,
            north: tileNorth,
            east: tileEast,
          },
          {
            opacity: this.options.opacity,
            pane: this.options.pane,
          }
        ).addTo(group);
      }
    }
  }
}

export function createLocalExtentTileLayer(
  options: LocalExtentTileLayerOptions
): L.Layer {
  return new LocalExtentTileLayer(options);
}
