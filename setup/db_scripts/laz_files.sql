-- Table: lidar_processing.laz_files

-- DROP TABLE IF EXISTS lidar_processing.laz_files;

CREATE TABLE IF NOT EXISTS lidar_processing.laz_files
(
    filename text COLLATE pg_catalog."default" NOT NULL,
    year integer NOT NULL,
    laz_type text COLLATE pg_catalog."default" NOT NULL,
    bucket text COLLATE pg_catalog."default",
    path text COLLATE pg_catalog."default",
    processing_time timestamp with time zone,
    laz_map_sheet integer NOT NULL,
    download_time timestamp with time zone,
    state smallint NOT NULL DEFAULT 0,
    to_crs text COLLATE pg_catalog."default",
    etak_path text COLLATE pg_catalog."default",
    identifier text COLLATE pg_catalog."default" NOT NULL,
    download_url text COLLATE pg_catalog."default",
    reclassify_path text COLLATE pg_catalog."default",
    dem_path text COLLATE pg_catalog."default",
    ndvi_path text COLLATE pg_catalog."default",
    CONSTRAINT laz_files_pkey PRIMARY KEY (filename)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS lidar_processing.laz_files
    OWNER to waiti84;
