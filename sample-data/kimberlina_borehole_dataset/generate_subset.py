#!/usr/bin/env python3

"""Fetch and materialize a compact Kimberlina borehole fixture set.

The source table is the public ``kim_ready.csv`` fixture used in GemPy's
Kimberlina example.  We keep a small, reproducible subset here so Rust-side
middleware tests can exercise borehole stratigraphy handling without depending
on the full upstream table at runtime.
"""

from __future__ import annotations

import csv
import io
import urllib.request
from collections import defaultdict
from pathlib import Path


SOURCE_URL = (
    "https://raw.githubusercontent.com/softwareunderground/subsurface/main/"
    "tests/data/borehole/kim_ready.csv"
)

# Chosen to preserve:
# - spatial spread across the field
# - rare formations (`fruitvale`, `eocene`)
# - both clean and messy interval stacks (tiny overlap/contact rows included)
SELECTED_HOLES = [
    "Arkelian23_26",
    "Wiedman55_26",
    "Gow1",
    "Sharples_Marathon_BillingtonXX",
    "S_&_D_Killingwoth_EPM1",
    "SUPERIOR_TENNECO_GAUTXX",
]


def parse_float(raw: str) -> float:
    return float(raw) if raw else 0.0


def fetch_source_rows() -> list[dict[str, str]]:
    with urllib.request.urlopen(SOURCE_URL) as response:
        text = response.read().decode("utf-8")
    return list(csv.DictReader(io.StringIO(text)))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    root = Path(__file__).resolve().parent
    rows = fetch_source_rows()
    subset_rows = [row for row in rows if row["name"] in SELECTED_HOLES]

    write_csv(
        root / "kim_ready_subset.csv",
        list(subset_rows[0].keys()),
        subset_rows,
    )

    collar_by_hole: dict[str, dict[str, object]] = {}
    intervals_by_hole: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
    interfaces: list[dict[str, object]] = []

    for row in subset_rows:
        hole_id = row["name"]
        altitude = parse_float(row["altitude"])
        top = parse_float(row["top"])
        base = parse_float(row["base"])
        formation = row["formation"].strip()

        collar_by_hole[hole_id] = {
            "hole_id": hole_id,
            "easting": parse_float(row["x"]),
            "northing": parse_float(row["y"]),
            "elevation": altitude,
            "total_depth_m": max(
                base,
                parse_float(collar_by_hole.get(hole_id, {}).get("total_depth_m", 0.0)),
            ),
            "trajectory_hint": "vertical_from_contacts",
        }

        if not formation:
            continue

        interval = {
            "hole_id": hole_id,
            "from_m": top,
            "to_m": base,
            "formation": formation,
            "easting": parse_float(row["x"]),
            "northing": parse_float(row["y"]),
            "elevation": altitude,
            "top_elevation_m": altitude - top,
            "base_elevation_m": altitude - base,
            "source_top_abs_m": parse_float(row["_top_abs"]),
        }
        intervals_by_hole[hole_id].append(interval)

        if formation != "topo":
            interfaces.append(
                {
                    "hole_id": hole_id,
                    "formation": formation,
                    "contact_depth_m": top,
                    "easting": parse_float(row["x"]),
                    "northing": parse_float(row["y"]),
                    "elevation": altitude - top,
                    "contact_kind": "top",
                }
            )

    collars = sorted(collar_by_hole.values(), key=lambda row: str(row["hole_id"]))
    intervals: list[dict[str, object]] = []
    for hole_id in sorted(intervals_by_hole):
        intervals.extend(
            sorted(intervals_by_hole[hole_id], key=lambda row: float(row["from_m"]))
        )

    interfaces.sort(key=lambda row: (str(row["hole_id"]), float(row["contact_depth_m"])))

    write_csv(
        root / "collar.csv",
        [
            "hole_id",
            "easting",
            "northing",
            "elevation",
            "total_depth_m",
            "trajectory_hint",
        ],
        collars,
    )
    write_csv(
        root / "lithology_intervals.csv",
        [
            "hole_id",
            "from_m",
            "to_m",
            "formation",
            "easting",
            "northing",
            "elevation",
            "top_elevation_m",
            "base_elevation_m",
            "source_top_abs_m",
        ],
        intervals,
    )
    write_csv(
        root / "interface_points.csv",
        [
            "hole_id",
            "formation",
            "contact_depth_m",
            "easting",
            "northing",
            "elevation",
            "contact_kind",
        ],
        interfaces,
    )

    print(
        "Wrote Kimberlina subset:",
        len(SELECTED_HOLES),
        "holes,",
        len(subset_rows),
        "raw rows,",
        len(intervals),
        "formation intervals,",
        len(interfaces),
        "interface points",
    )


if __name__ == "__main__":
    main()
