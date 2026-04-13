import L from "leaflet";

import type { RasterLatLngBounds } from "./rasterOverlay";

export type LeafletRasterPane = "mineeye-raster-base" | "mineeye-raster-analytic";

export type LocalExtentTileLayerOptions = L.LayerOptions & {
  bounds: RasterLatLngBounds;
  tileUrlTemplate: string;
  tileMinZoom: number;
  tileMaxZoom: number;
  tileSize?: number;
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

// Fix #2: Tile cache — keep already-loaded overlays alive across pan/zoom events.
//
// Previously refresh() called group.clearLayers() on every moveend/zoomend,
// destroying every <img> element even for tiles that were already loaded and
// visible.  The browser would immediately re-request them.
//
// Now we maintain a Map<"z/x/y" → ImageOverlay> and on each refresh:
//   • add only tiles not already present in the cache
//   • remove only tiles that are no longer in the visible + zoom set
//   • keep everything else untouched (no URL request, no DOM churn)
//
// The cache is keyed on z/x/y, not zoom+tile+url, so a zoom change
// (which changes z) naturally evicts the old zoom level's tiles.

class LocalExtentTileLayer extends L.Layer {
  declare options: LocalExtentTileLayerOptions;

  private mapInstance: L.Map | null = null;
  private tileGroup: L.LayerGroup | null = null;
  /** Live tile overlays keyed by "z/x/y". */
  private tileCache = new Map<string, L.ImageOverlay>();

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
    this.tileCache.clear();
    return this;
  }

  setOpacity(opacity: number): this {
    this.options.opacity = opacity;
    // Update all cached overlays in place — no need to recreate them.
    this.tileCache.forEach((overlay) => overlay.setOpacity(opacity));
    return this;
  }

  private refresh(): void {
    const map = this.mapInstance;
    const group = this.tileGroup;
    if (!map || !group) return;

    const bounds = this.options.bounds;
    const mapBounds = map.getBounds();

    // Visible intersection of survey bounds and current viewport.
    const west  = Math.max(bounds.west,  mapBounds.getWest());
    const east  = Math.min(bounds.east,  mapBounds.getEast());
    const south = Math.max(bounds.south, mapBounds.getSouth());
    const north = Math.min(bounds.north, mapBounds.getNorth());
    if (west >= east || south >= north) {
      // Survey not in view — clear everything.
      group.clearLayers();
      this.tileCache.clear();
      return;
    }

    // Compute the correct LOCAL tile-pyramid zoom level.
    // The pyramid is defined over the survey extent (not web-mercator), so we
    // cannot use Leaflet's map.getZoom().  Instead we derive the zoom from how
    // large the survey appears on screen relative to one tile's pixel size.
    //
    //   idealZ = floor( log2( surveyScreenPx / tileSize ) )
    //
    // Using the smaller of width/height avoids fetching unnecessarily large tiles.
    const vpSize  = map.getSize();
    const vpLon   = Math.max(1e-12, mapBounds.getEast() - mapBounds.getWest());
    const vpLat   = Math.max(1e-12, mapBounds.getNorth() - mapBounds.getSouth());
    const lonSpan = Math.max(1e-12, bounds.east  - bounds.west);
    const latSpan = Math.max(1e-12, bounds.north - bounds.south);
    const tilePx  = this.options.tileSize ?? 256;

    const surveyPxW  = (lonSpan / vpLon) * vpSize.x;
    const surveyPxH  = (latSpan / vpLat) * vpSize.y;
    const idealZoom  = Math.floor(Math.min(
      Math.log2(surveyPxW / tilePx),
      Math.log2(surveyPxH / tilePx),
    ));
    const zoom = clampInt(idealZoom, this.options.tileMinZoom, this.options.tileMaxZoom);
    const n = 1 << zoom;

    // Tile indices covering the visible intersection.
    const fx0 = (west  - bounds.west) / lonSpan;
    const fx1 = (east  - bounds.west) / lonSpan;
    const fy0 = (bounds.north - north) / latSpan;
    const fy1 = (bounds.north - south) / latSpan;

    const xStart = clampInt(Math.floor(fx0 * n), 0, n - 1);
    const xEnd   = clampInt(Math.ceil(fx1  * n) - 1, 0, n - 1);
    const yStart = clampInt(Math.floor(fy0 * n), 0, n - 1);
    const yEnd   = clampInt(Math.ceil(fy1  * n) - 1, 0, n - 1);

    // Build the set of keys that should be alive after this refresh.
    const wantedKeys = new Set<string>();
    for (let ty = yStart; ty <= yEnd; ty++) {
      for (let tx = xStart; tx <= xEnd; tx++) {
        wantedKeys.add(`${zoom}/${tx}/${ty}`);
      }
    }

    // Remove tiles that are no longer wanted.
    for (const [key, overlay] of this.tileCache) {
      if (!wantedKeys.has(key)) {
        group.removeLayer(overlay);
        this.tileCache.delete(key);
      }
    }

    // Add tiles that are not yet in the cache.
    for (let ty = yStart; ty <= yEnd; ty++) {
      for (let tx = xStart; tx <= xEnd; tx++) {
        const key = `${zoom}/${tx}/${ty}`;
        if (this.tileCache.has(key)) continue;  // already loaded — keep it

        const tileWest  = bounds.west  + (tx / n) * lonSpan;
        const tileEast  = bounds.west  + ((tx + 1) / n) * lonSpan;
        const tileNorth = bounds.north - (ty / n) * latSpan;
        const tileSouth = bounds.north - ((ty + 1) / n) * latSpan;
        const url = this.options.tileUrlTemplate
          .replace("{z}", String(zoom))
          .replace("{x}", String(tx))
          .replace("{y}", String(ty));

        const overlay = createBoundedImageLayer(
          url,
          { south: tileSouth, west: tileWest, north: tileNorth, east: tileEast },
          { opacity: this.options.opacity, pane: this.options.pane },
        );
        overlay.addTo(group);
        this.tileCache.set(key, overlay);
      }
    }
  }
}

export function createLocalExtentTileLayer(
  options: LocalExtentTileLayerOptions
): L.Layer {
  return new LocalExtentTileLayer(options);
}
