#!/usr/bin/env python3

import csv
import json
import math
import random
from pathlib import Path


ROOT = Path(__file__).resolve().parent
GHANA_COLLARS = ROOT.parent / "ghana_drill_dataset" / "collar.csv"
EPSG = 32630
SPACING_M = 25.0
LINES = 3
ELECTRODES_PER_LINE = 28
N_LEVELS = range(1, 7)
SEED = 20260413


def read_collars():
    with GHANA_COLLARS.open("r", newline="") as f:
        rows = list(csv.DictReader(f))
    xs = [float(r["easting"]) for r in rows]
    ys = [float(r["northing"]) for r in rows]
    zs = [float(r["elevation"]) for r in rows]
    return {
        "xmin": min(xs),
        "xmax": max(xs),
        "ymin": min(ys),
        "ymax": max(ys),
        "zmean": sum(zs) / len(zs),
        "xmid": 0.5 * (min(xs) + max(xs)),
        "ymid": 0.5 * (min(ys) + max(ys)),
    }


def terrain_z(base_z, x, y, x0, y0):
    return base_z + 0.008 * (x - x0) + 0.018 * (y - y0)


def anomaly_strength(x, y, pseudo_depth, cx, cy, cz):
    dx = (x - cx) / 130.0
    dy = (y - cy) / 55.0
    dz = (pseudo_depth - cz) / 40.0
    return math.exp(-(dx * dx + dy * dy + dz * dz) * 0.5)


def main():
    random.seed(SEED)
    bounds = read_collars()
    x0 = bounds["xmid"] - (ELECTRODES_PER_LINE - 1) * SPACING_M / 2.0
    base_y = bounds["ymid"] - SPACING_M
    base_z = bounds["zmean"] - 3.0

    cx = bounds["xmid"] + 35.0
    cy = bounds["ymid"] + 8.0
    cz = 70.0

    electrodes = []
    by_id = {}
    for line_idx in range(LINES):
        line_id = f"L{line_idx + 1:02d}"
        y = base_y + line_idx * SPACING_M
        for i in range(ELECTRODES_PER_LINE):
            eid = f"{line_id}_E{i + 1:02d}"
            x = x0 + i * SPACING_M
            z = terrain_z(base_z, x, y, bounds["xmid"], bounds["ymid"])
            row = {
                "electrode_id": eid,
                "line_id": line_id,
                "x": round(x, 3),
                "y": round(y, 3),
                "z": round(z, 3),
            }
            electrodes.append(row)
            by_id[eid] = row

    measurements = []
    measurement_id = 1
    for line_idx in range(LINES):
        line_id = f"L{line_idx + 1:02d}"
        line_eids = [f"{line_id}_E{i + 1:02d}" for i in range(ELECTRODES_PER_LINE)]
        for a_idx in range(ELECTRODES_PER_LINE - 3):
            for n_level in N_LEVELS:
                b_idx = a_idx + 1
                m_idx = b_idx + n_level
                n_idx = m_idx + 1
                if n_idx >= ELECTRODES_PER_LINE:
                    continue

                a = by_id[line_eids[a_idx]]
                b = by_id[line_eids[b_idx]]
                m = by_id[line_eids[m_idx]]
                n = by_id[line_eids[n_idx]]
                mx = 0.25 * (a["x"] + b["x"] + m["x"] + n["x"])
                my = 0.25 * (a["y"] + b["y"] + m["y"] + n["y"])
                pseudo_depth = max(12.0, n_level * SPACING_M * 0.75)
                strength = anomaly_strength(mx, my, pseudo_depth, cx, cy, cz)

                rho = 620.0 - 210.0 * strength + random.gauss(0.0, 12.0)
                rho = max(85.0, rho)
                chargeability = 4.5 + 18.0 * strength + random.gauss(0.0, 0.9)
                current_ma = 1650.0 + random.gauss(0.0, 45.0)
                voltage_mv = max(0.12, (rho / 520.0) * 12.0 + random.gauss(0.0, 0.15))
                recip = abs(random.gauss(2.2, 1.1))
                stack_count = max(3, int(round(random.gauss(8.0, 1.5))))

                if random.random() < 0.035:
                    recip = 18.0 + random.random() * 8.0
                if random.random() < 0.02:
                    chargeability = -1.5 - random.random() * 2.0

                measurements.append({
                    "measurement_id": f"GHIP_{measurement_id:04d}",
                    "line_id": line_id,
                    "survey_mode": "tdip",
                    "array_type": "dipole_dipole",
                    "a_id": a["electrode_id"],
                    "a_x": a["x"],
                    "a_y": a["y"],
                    "a_z": a["z"],
                    "b_id": b["electrode_id"],
                    "b_x": b["x"],
                    "b_y": b["y"],
                    "b_z": b["z"],
                    "m_id": m["electrode_id"],
                    "m_x": m["x"],
                    "m_y": m["y"],
                    "m_z": m["z"],
                    "n_id": n["electrode_id"],
                    "n_x": n["x"],
                    "n_y": n["y"],
                    "n_z": n["z"],
                    "n_level": n_level,
                    "current_ma": round(current_ma, 3),
                    "voltage_mv": round(voltage_mv, 4),
                    "apparent_resistivity_ohm_m": round(rho, 4),
                    "chargeability_mv_v": round(chargeability, 4),
                    "gate_start_ms": 120.0,
                    "gate_end_ms": 780.0,
                    "stack_count": stack_count,
                    "reciprocity_error_pct": round(recip, 4),
                    "synthetic_pseudo_depth_m": round(pseudo_depth, 3),
                    "synthetic_anomaly_strength": round(strength, 6),
                })
                measurement_id += 1

    payload = {
        "dataset_name": "ghana_tdip_synthetic_v1",
        "crs": {"epsg": EPSG, "wkt": None},
        "survey_mode": "tdip",
        "electrodes": [
            {
                "electrode_id": e["electrode_id"],
                "line_id": e["line_id"],
                "x": e["x"],
                "y": e["y"],
                "z": e["z"],
            }
            for e in electrodes
        ],
        "rows": measurements,
    }

    truth = {
        "schema_id": "synthetic.ip_truth.v1",
        "dataset_name": "ghana_tdip_synthetic_v1",
        "crs": {"epsg": EPSG, "wkt": None},
        "footprint": {
            "xmin": min(e["x"] for e in electrodes),
            "xmax": max(e["x"] for e in electrodes),
            "ymin": min(e["y"] for e in electrodes),
            "ymax": max(e["y"] for e in electrodes),
        },
        "anomaly_model": {
            "type": "buried_chargeability_resistivity_body",
            "center_x": round(cx, 3),
            "center_y": round(cy, 3),
            "center_depth_m": cz,
            "sigma_x_m": 130.0,
            "sigma_y_m": 55.0,
            "sigma_z_m": 40.0,
            "background_resistivity_ohm_m": 620.0,
            "resistivity_drop_ohm_m": 210.0,
            "background_chargeability_mv_v": 4.5,
            "chargeability_peak_mv_v": 22.5,
        },
        "generation": {
            "seed": SEED,
            "electrode_spacing_m": SPACING_M,
            "line_count": LINES,
            "electrodes_per_line": ELECTRODES_PER_LINE,
            "measurement_count": len(measurements),
        },
    }

    ROOT.mkdir(parents=True, exist_ok=True)
    with (ROOT / "ghana_tdip_measurements.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(measurements[0].keys()))
        writer.writeheader()
        writer.writerows(measurements)
    with (ROOT / "ghana_tdip_electrodes.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(electrodes[0].keys()))
        writer.writeheader()
        writer.writerows(electrodes)
    with (ROOT / "ghana_tdip_payload.json").open("w") as f:
        json.dump(payload, f, indent=2)
    with (ROOT / "ghana_tdip_truth.json").open("w") as f:
        json.dump(truth, f, indent=2)


if __name__ == "__main__":
    main()
