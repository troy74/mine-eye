import { lonLatFromProjectedAsync } from "./spatialReproject";

export type RasterBounds = {
  xmin: number;
  xmax: number;
  ymin: number;
  ymax: number;
};

export type RasterLatLngBounds = {
  south: number;
  west: number;
  north: number;
  east: number;
};

export type RasterOverlayContract = {
  schema_id:
    | "scene3d.imagery_drape.v1"
    | "scene3d.tilebroker_response.v1"
    | "raster.tile_cache.v1";
  provider_id?: string;
  provider_label?: string;
  attribution?: string;
  image_url?: string;
  image_url_candidates?: string[];
  tile_url_template?: string;
  tile_scheme?: string;
  tile_min_zoom?: number;
  tile_max_zoom?: number;
  tile_size?: number;
  bounds?: RasterBounds;
  source_crs?: { epsg?: number };
  z_mode?: "drape_on_surface" | "flat";
  quality_flags?: string[];
  fingerprint?: string;
};

export type RasterServingMode = "single_image" | "global_xyz_tiles" | "local_extent_tiles";

export type RasterOverlaySource = {
  mode: RasterServingMode;
  bounds: RasterLatLngBounds | null;
  imageUrl: string | null;
  tileUrlTemplate: string | null;
  tileMinZoom: number;
  tileMaxZoom: number;
  tileSize: number;
  attribution?: string;
  providerLabel?: string;
};

function finiteNumber(v: unknown): number | undefined {
  return typeof v === "number" && Number.isFinite(v) ? v : undefined;
}

function parseBounds(v: unknown): RasterBounds | undefined {
  if (!v || typeof v !== "object" || Array.isArray(v)) return undefined;
  const obj = v as Record<string, unknown>;
  const xmin = finiteNumber(obj.xmin);
  const xmax = finiteNumber(obj.xmax);
  const ymin = finiteNumber(obj.ymin);
  const ymax = finiteNumber(obj.ymax);
  if (
    xmin === undefined ||
    xmax === undefined ||
    ymin === undefined ||
    ymax === undefined
  ) {
    return undefined;
  }
  return { xmin, xmax, ymin, ymax };
}

export function parseRasterOverlayContract(v: unknown): RasterOverlayContract | null {
  if (!v || typeof v !== "object" || Array.isArray(v)) return null;
  const obj = v as Record<string, unknown>;
  if (obj.schema_id === "raster.tile_cache.v1") {
    const tiles =
      obj.tiles && typeof obj.tiles === "object" && !Array.isArray(obj.tiles)
        ? (obj.tiles as Record<string, unknown>)
        : null;
    return {
      schema_id: "raster.tile_cache.v1",
      provider_id:
        typeof obj.provider_id === "string" ? obj.provider_id : "heatmap_raster_tile_cache",
      provider_label:
        typeof obj.provider_label === "string"
          ? obj.provider_label
          : "Heatmap raster cache",
      attribution: typeof obj.attribution === "string" ? obj.attribution : undefined,
      image_url: typeof obj.image_url === "string" ? obj.image_url : undefined,
      image_url_candidates:
        Array.isArray(obj.image_url_candidates) &&
        obj.image_url_candidates.some((x) => typeof x === "string")
          ? obj.image_url_candidates.filter((x): x is string => typeof x === "string")
          : typeof obj.image_url === "string" && obj.image_url.trim().length > 0
            ? [obj.image_url]
            : undefined,
      tile_url_template:
        typeof tiles?.tile_url_template === "string"
          ? String(tiles.tile_url_template)
          : undefined,
      tile_scheme: typeof tiles?.scheme === "string" ? String(tiles.scheme) : undefined,
      tile_min_zoom: finiteNumber(tiles?.min_zoom),
      tile_max_zoom: finiteNumber(tiles?.max_zoom),
      tile_size: finiteNumber(tiles?.tile_size),
      bounds: parseBounds(obj.bounds),
      source_crs:
        obj.source_crs && typeof obj.source_crs === "object" && !Array.isArray(obj.source_crs)
          ? (obj.source_crs as { epsg?: number })
          : undefined,
      fingerprint: typeof obj.fingerprint === "string" ? obj.fingerprint : undefined,
    };
  }
  if (
    obj.schema_id !== "scene3d.imagery_drape.v1" &&
    obj.schema_id !== "scene3d.tilebroker_response.v1"
  ) {
    return null;
  }
  return obj as RasterOverlayContract;
}

export function rasterContractPriority(contract: RasterOverlayContract | null): number {
  if (!contract) return -1;
  if (contract.schema_id === "raster.tile_cache.v1") return 3;
  if (contract.tile_scheme === "xyz_local") return 2;
  return 1;
}

export function imageryUrlCandidates(url: string): string[] {
  const out: string[] = [url];
  if (url.includes("/World_Imagery/MapServer/export")) {
    if (url.includes("services.arcgisonline.com")) {
      out.push(url.replace("services.arcgisonline.com", "server.arcgisonline.com"));
    } else if (url.includes("server.arcgisonline.com")) {
      out.push(url.replace("server.arcgisonline.com", "services.arcgisonline.com"));
    }
  }
  return [...new Set(out)];
}

export function rasterImageCandidates(contract: RasterOverlayContract | null): string[] {
  if (!contract) return [];
  const raw = [
    ...(typeof contract.image_url === "string" ? [contract.image_url] : []),
    ...(Array.isArray(contract.image_url_candidates) ? contract.image_url_candidates : []),
  ]
    .map((u) => u.trim())
    .filter((u, i, arr) => u.length > 0 && arr.indexOf(u) === i);
  return raw.flatMap((u) => imageryUrlCandidates(u)).filter((u, i, arr) => arr.indexOf(u) === i);
}

export function rasterTileUrl(contract: RasterOverlayContract | null): string | null {
  if (!contract || typeof contract.tile_url_template !== "string") return null;
  const trimmed = contract.tile_url_template.trim();
  return trimmed.length > 0 ? trimmed : null;
}

export function rasterHasRenderableSource(contract: RasterOverlayContract | null): boolean {
  return Boolean(rasterTileUrl(contract) || rasterImageCandidates(contract).length > 0);
}

export function rasterServingMode(contract: RasterOverlayContract | null): RasterServingMode | null {
  if (!contract) return null;
  if (contract.tile_scheme === "xyz_local" && rasterTileUrl(contract)) return "local_extent_tiles";
  if (rasterTileUrl(contract)) return "global_xyz_tiles";
  if (rasterImageCandidates(contract).length > 0) return "single_image";
  return null;
}

export async function rasterBoundsToLatLng(
  contract: RasterOverlayContract | null
): Promise<RasterLatLngBounds | null> {
  if (!contract?.bounds) return null;
  const epsg =
    typeof contract.source_crs?.epsg === "number" ? contract.source_crs.epsg : 4326;
  if (epsg === 4326) {
    return {
      south: contract.bounds.ymin,
      west: contract.bounds.xmin,
      north: contract.bounds.ymax,
      east: contract.bounds.xmax,
    };
  }
  const sw = await lonLatFromProjectedAsync(epsg, contract.bounds.xmin, contract.bounds.ymin);
  const ne = await lonLatFromProjectedAsync(epsg, contract.bounds.xmax, contract.bounds.ymax);
  if (!sw || !ne) return null;
  return {
    south: sw[1],
    west: sw[0],
    north: ne[1],
    east: ne[0],
  };
}

export async function resolveRasterOverlaySource(
  contract: RasterOverlayContract | null
): Promise<RasterOverlaySource | null> {
  if (!contract) return null;
  const bounds = await rasterBoundsToLatLng(contract);
  const imageUrl = rasterImageCandidates(contract)[0] ?? null;
  const tileUrlTemplate = rasterTileUrl(contract);
  const mode = rasterServingMode(contract);
  if (!mode) return null;
  return {
    mode,
    bounds,
    imageUrl,
    tileUrlTemplate,
    tileMinZoom: typeof contract.tile_min_zoom === "number" ? contract.tile_min_zoom : 0,
    tileMaxZoom: typeof contract.tile_max_zoom === "number" ? contract.tile_max_zoom : 22,
    tileSize:
      typeof contract.tile_size === "number" && contract.tile_size > 0
        ? contract.tile_size
        : 256,
    attribution: contract.attribution,
    providerLabel: contract.provider_label,
  };
}
