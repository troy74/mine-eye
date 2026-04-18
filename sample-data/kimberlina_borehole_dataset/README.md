# Kimberlina Borehole Test Subset

This directory contains a compact, reproducible subset of the public
`kim_ready.csv` borehole fixture used by GemPy's Kimberlina example:

- source example: <https://docs.gempy.org/examples/real/mik.html>
- public source CSV:
  <https://raw.githubusercontent.com/softwareunderground/subsurface/main/tests/data/borehole/kim_ready.csv>

## Why this subset exists

We want a small fixture that is good enough for Rust middleware tests while we
add GemPy-inspired formation/modelling functionality to `mine-eye`.

The subset is designed to preserve:

- a realistic borehole contact table shape
- good spatial spread across the Kimberlina field
- rare formations such as `fruitvale` and `eocene`
- a few thin or nearly coincident contacts that are useful for edge-case tests

## Files

- `kim_ready_subset.csv` — raw-source-compatible subset with the original
  column layout
- `collar.csv` — one collar row per borehole with total depth inferred from the
  deepest interval
- `lithology_intervals.csv` — normalized formation intervals for direct Rust
  ingestion/tests
- `interface_points.csv` — derived top-of-formation contact points suitable for
  future stratigraphy/surface modelling nodes
- `generate_subset.py` — reproducibly rebuilds all of the above from the public
  source table

## Notes for middleware work

- The source fixture does not include azimuth/dip survey stations, so this
  subset should be treated as vertical-contact boreholes for now.
- That still makes it useful for the near-term pipeline:
  `collars -> vertical traces / trajectories -> formation interface points`.
- Blank-formation terminator rows from the source file are kept in
  `kim_ready_subset.csv` for provenance, but they are intentionally omitted from
  the normalized `lithology_intervals.csv` and `interface_points.csv` outputs.

## Regenerate

```bash
python3 sample-data/kimberlina_borehole_dataset/generate_subset.py
```
