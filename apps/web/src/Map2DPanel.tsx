import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { useEffect, useMemo, useRef, useState } from "react";
import { api, type ApiEdge, type ArtifactEntry } from "./graphApi";
import { isPlanViewInputSemantic } from "./portTaxonomy";
import { lonLatFromProjected } from "./spatialReproject";
import {
  epsgFromCollarJson,
  extractPlanViewPointsFromJson,
} from "./spatialExtract";

type Props = {
  graphId: string | null;
  edges: ApiEdge[];
  artifacts: ArtifactEntry[];
  /** `plan_view_2d` node id — map shows only data from wired upstream inputs. */
  viewerNodeId: string | null;
  onClearViewer: () => void;
};

function upstreamSourcesForViewer(
  edges: ApiEdge[],
  viewerId: string
): { fromNode: string; semantic: string; port: string }[] {
  return edges
    .filter((e) => e.to_node === viewerId && isPlanViewInputSemantic(e.semantic_type))
    .map((e) => ({
      fromNode: e.from_node,
      semantic: e.semantic_type,
      port: `${e.from_port}→${e.to_port}`,
    }));
}

function jsonArtifactsForNodes(
  artifacts: ArtifactEntry[],
  nodeIds: Set<string>
): ArtifactEntry[] {
  return artifacts.filter(
    (a) => nodeIds.has(a.node_id) && a.key.toLowerCase().endsWith(".json")
  );
}

export function Map2DPanel({
  graphId,
  edges,
  artifacts,
  viewerNodeId,
  onClearViewer,
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const groupRef = useRef<L.LayerGroup | null>(null);
  const [status, setStatus] = useState<string>("");

  const sources = useMemo(() => {
    if (!viewerNodeId) return [];
    return upstreamSourcesForViewer(edges, viewerNodeId);
  }, [edges, viewerNodeId]);

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;
    const map = L.map(containerRef.current, {
      zoomControl: true,
      attributionControl: true,
    }).setView([20, 0], 2);
    L.tileLayer(
      "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
      {
        maxZoom: 19,
        attribution:
          '&copy; <a href="https://www.esri.com/">Esri</a> (World Imagery)',
      }
    ).addTo(map);
    const g = L.layerGroup().addTo(map);
    mapRef.current = map;
    groupRef.current = g;
    return () => {
      g.clearLayers();
      map.remove();
      mapRef.current = null;
      groupRef.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    const group = groupRef.current;
    if (!map || !group) return;

    group.clearLayers();

    if (!graphId || !viewerNodeId) {
      setStatus(
        "Select a plan view node on the graph and click “Open 2D map”. The map only uses nodes wired into that viewer’s inputs."
      );
      return;
    }

    if (sources.length === 0) {
      setStatus(
        "No compatible inputs wired into this plan view node (expects point_set, table, interval_set, or trajectory_set on an incoming edge)."
      );
      return;
    }

    const upstreamIds = new Set(sources.map((s) => s.fromNode));
    const arts = jsonArtifactsForNodes(artifacts, upstreamIds);
    if (arts.length === 0) {
      setStatus(
        "Upstream nodes have no JSON artifacts yet. Use “Queue pipeline run” in the header, run the worker process (same DATABASE_URL / ARTIFACT_ROOT as the API), then “Refresh graph + artifacts”. If collar ingest shows Failed, fix mapping and save, then queue again."
      );
      return;
    }

    let cancelled = false;
    setStatus("Loading upstream artifacts…");

    void (async () => {
      const latlngs: L.LatLngExpression[] = [];
      const notes: string[] = [];
      let totalPoints = 0;

      try {
        for (const art of arts) {
          if (cancelled) return;
          const shortName = art.key.split("/").pop() ?? art.key;
          const r = await fetch(api(art.url));
          if (!r.ok) {
            notes.push(`${shortName}: HTTP ${r.status}`);
            continue;
          }
          const text = await r.text();
          const pts = extractPlanViewPointsFromJson(text, shortName);
          if (pts.length === 0) {
            notes.push(`${shortName}: no x,y points parsed`);
            continue;
          }
          const epsg = epsgFromCollarJson(text);
          for (const p of pts) {
            let lon = p.x;
            let lat = p.y;
            if (epsg !== null && epsg !== 4326) {
              const ll = lonLatFromProjected(epsg, p.x, p.y);
              if (ll) {
                lon = ll[0];
                lat = ll[1];
              } else {
                notes.push(
                  `${shortName}: EPSG:${epsg} — add a proj4 def in spatialReproject.ts or set collar output to WGS84`
                );
              }
            }
            const c = L.circleMarker([lat, lon], {
              radius: 5,
              color: "#38bdf8",
              fillColor: "#38bdf8",
              fillOpacity: 0.85,
              weight: 2,
            });
            c.bindTooltip(p.label);
            c.addTo(group);
            latlngs.push([lat, lon]);
          }
          totalPoints += pts.length;
        }

        if (cancelled) return;

        if (latlngs.length === 0) {
          setStatus(
            `No drawable points from ${arts.length} artifact(s). ${notes.join(" · ")}`
          );
          return;
        }

        setStatus(
          `${totalPoints} point(s) from ${arts.length} artifact(s) (${sources.length} input link(s)). ${notes.length ? notes.join(" · ") : "EPSG:4326 assumed where unspecified."}`
        );
        const b = L.latLngBounds(latlngs);
        map.fitBounds(b.pad(0.25));
      } catch (e) {
        if (!cancelled) {
          setStatus(e instanceof Error ? e.message : String(e));
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [graphId, edges, artifacts, viewerNodeId, sources]);

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", minHeight: 0 }}>
      <div
        style={{
          padding: "8px 12px",
          borderBottom: "1px solid #30363d",
          fontSize: 12,
          display: "flex",
          alignItems: "center",
          gap: 10,
          flexWrap: "wrap",
          background: "#161b22",
        }}
      >
        <strong style={{ color: "#e6edf3" }}>2D map</strong>
        {viewerNodeId && (
          <>
            <span style={{ opacity: 0.75 }}>
              Viewer <code style={{ fontSize: 11 }}>{viewerNodeId.slice(0, 8)}…</code>
              {sources.length > 0 && (
                <span style={{ opacity: 0.65 }}>
                  {" "}
                  · {sources.length} input link(s)
                </span>
              )}
            </span>
            <button
              type="button"
              onClick={onClearViewer}
              style={{
                marginLeft: "auto",
                fontSize: 11,
                padding: "4px 10px",
                borderRadius: 6,
                border: "1px solid #30363d",
                background: "#21262d",
                color: "#e6edf3",
                cursor: "pointer",
              }}
            >
              Clear viewer
            </button>
          </>
        )}
      </div>
      <div style={{ fontSize: 11, opacity: 0.7, padding: "6px 12px", lineHeight: 1.4 }}>
        {status}
      </div>
      <div ref={containerRef} style={{ flex: 1, minHeight: 200, zIndex: 0 }} />
    </div>
  );
}
