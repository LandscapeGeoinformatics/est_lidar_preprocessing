-- Table: lidar_processing.dem_files

-- DROP TABLE IF EXISTS lidar_processing.dem_files;

CREATE TABLE IF NOT EXISTS lidar_processing.dem_files
(
    filename text COLLATE pg_catalog."default" NOT NULL,
    year integer NOT NULL,
    download_time timestamp with time zone,
    bucket text COLLATE pg_catalog."default",
    path text COLLATE pg_catalog."default",
    state smallint NOT NULL DEFAULT 0,
    dem_map_sheet integer NOT NULL,
    identifier text COLLATE pg_catalog."default" NOT NULL,
    download_url text COLLATE pg_catalog."default",
    CONSTRAINT dem_files_pkey PRIMARY KEY (filename)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS lidar_processing.dem_files
    OWNER to waiti84;
