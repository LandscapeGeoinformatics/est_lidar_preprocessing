-- Table: lidar_processing.mapsheets_mapping

-- DROP TABLE IF EXISTS lidar_processing.mapsheets_mapping;

CREATE TABLE IF NOT EXISTS lidar_processing.mapsheets_mapping
(
    "nr" integer NOT NULL,
    "nr10000" integer NOT NULL,
    CONSTRAINT mapsheets_mapping_pkey PRIMARY KEY ("nr")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS lidar_processing.mapsheets_mapping
    OWNER to waiti84;
