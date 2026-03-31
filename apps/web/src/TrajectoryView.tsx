import { useEffect, useState } from "react";
import * as THREE from "three";

type Segment = {
  x_from: number;
  y_from: number;
  z_from: number;
  x_to: number;
  y_to: number;
  z_to: number;
  hole_id: string;
};

function isSegment(x: unknown): x is Segment {
  if (!x || typeof x !== "object") return false;
  const o = x as Record<string, unknown>;
  return (
    typeof o.x_from === "number" &&
    typeof o.y_from === "number" &&
    typeof o.z_from === "number" &&
    typeof o.x_to === "number" &&
    typeof o.y_to === "number" &&
    typeof o.z_to === "number"
  );
}

export function TrajectoryView({ url }: { url: string }) {
  const [geom, setGeom] = useState<THREE.BufferGeometry | null>(null);

  useEffect(() => {
    let cancelled = false;
    let created: THREE.BufferGeometry | null = null;

    (async () => {
      try {
        const r = await fetch(url);
        if (!r.ok || cancelled) return;
        const raw: unknown = await r.json();
        if (!Array.isArray(raw) || !raw.every(isSegment)) {
          if (!cancelled) setGeom(null);
          return;
        }
        const segs = raw;
        const positions: number[] = [];
        for (const s of segs) {
          positions.push(s.x_from, s.z_from, s.y_from, s.x_to, s.z_to, s.y_to);
        }
        if (positions.length === 0) {
          if (!cancelled) setGeom(null);
          return;
        }
        const g = new THREE.BufferGeometry();
        created = g;
        g.setAttribute("position", new THREE.Float32BufferAttribute(positions, 3));
        if (cancelled) {
          g.dispose();
          return;
        }
        setGeom(g);
      } catch (e) {
        console.warn("TrajectoryView fetch/parse failed:", e);
        if (!cancelled) setGeom(null);
      }
    })();

    return () => {
      cancelled = true;
      created?.dispose();
      setGeom((prev) => {
        prev?.dispose();
        return null;
      });
    };
  }, [url]);

  if (!geom) return null;

  return (
    <lineSegments geometry={geom}>
      <lineBasicMaterial color="#58a6ff" />
    </lineSegments>
  );
}
