# Ghana Synthetic TDIP Dataset

Synthetic time-domain induced polarization dataset tied to the Ghana sample drill footprint.

Files:

- `ghana_tdip_measurements.csv` — flat quadrupole rows with inline A/B/M/N coordinates
- `ghana_tdip_electrodes.csv` — unique electrode positions
- `ghana_tdip_payload.json` — ready-to-ingest payload for `ip_survey_ingest`
- `ghana_tdip_truth.json` — hidden anomaly specification for validation
- `generate_tdip.py` — deterministic generator

Design notes:

- CRS is `EPSG:32630`
- Array type is `dipole_dipole`
- Three parallel lines are laid across the existing Ghana collar footprint
- Measurements include mild noise plus a few deliberately bad rows
- The truth model is a buried conductive / chargeable body used only for testing

Recommended workflow:

1. `ip_survey_ingest` with `ghana_tdip_payload.json`
2. `ip_qc_normalize`
3. next stage: pseudosection / inversion nodes
