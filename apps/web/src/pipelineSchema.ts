/**
 * Port / geometry intent for V1 drillhole pipeline (collar → survey → desurvey → assay).
 * Workers and validators will eventually enforce these; UI uses them for mapping hints.
 *
 * Collar (PointSet): collar id + (x,y,z) in source CRS; z may be absolute RL or relative to
 *   a reference datum; optional azimuth/dip at collar if measured; ids pass through.
 *
 * Survey (TrajectorySet segments): from collar reference, directed segment by azimuth, dip,
 *   and length (or cumulative depth); segment id for trace topology.
 *
 * Desurvey: builds 3D trace / tube (cylinders) from collar + survey segments (current worker:
 *   polyline stub; future: proper tube mesh + interval solids).
 *
 * Assay (IntervalSet / multi-measure): intervals keyed to hole id + from–to depth; values land
 *   at interval centres and join desurvey solids by matching hole + depth range (centre of
 *   matching cylinder).
 */
export const PIPELINE_GEOMETRY_NOTES = `Collar → point (x,y,z) + optional az/dip + id · Survey → segment (az, dip, length/depth) + id · Desurvey → 3D trace/tubes · Assay → multi-measure at interval centres matched to hole+depth.`;

export const ACQUISITION_CSV_KINDS = [
  "artifact_ingest",
  "collar_ingest",
  "survey_ingest",
  "surface_sample_ingest",
  "assay_ingest",
  "magnetic_mapper",
] as const;

export type AcquisitionCsvKind = (typeof ACQUISITION_CSV_KINDS)[number];

export function isAcquisitionCsvKind(kind: string): kind is AcquisitionCsvKind {
  return (ACQUISITION_CSV_KINDS as readonly string[]).includes(kind);
}

export type CollarMappingFields = {
  hole_id: string;
  x: string;
  y: string;
  z: string;
  azimuth_deg?: string;
  dip_deg?: string;
  z_is_relative?: boolean;
};

export type SurveyMappingFields = {
  hole_id: string;
  azimuth_deg: string;
  dip_deg: string;
  /** Depth along hole or segment length — UI label depends on file convention */
  depth_or_length_m: string;
  segment_id?: string;
};

export type AssayMappingFields = {
  hole_id: string;
  from_m: string;
  to_m: string;
};
