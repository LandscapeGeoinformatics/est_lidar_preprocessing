import json
from typing import List
import os
import argparse
from datetime import datetime, timezone
import logging

import pdal
from osgeo import ogr

ogr.UseExceptions()


class ReclassificationPipeline:

    def __init__(
            self,
            input_file: str,
            output_file: str,
            dem_file: str,
            etak_file: str,
            ndvi_file: str
        ) -> None:

        # Store input arguments as instance attributes
        self.input_file = input_file
        self.output_file = output_file
        if (dem_file.startswith("gs://")):
            dem_file = dem_file.replace("gs://", "/vsigs/")
        self.dem_file = dem_file
        self.etak_file = etak_file
        if (ndvi_file.startswith("gs://")):
            ndvi_file = ndvi_file.replace("gs://", "/vsigs/")
        if (ndvi_file.startswith("s3://")):
            ndvi_file = ndvi_file.replace("s3://", "/vsis3/")
        self.ndvi_file = ndvi_file

        # Default pipeline structure
        self.pipeline = {
            "pipeline": [
                # Read input file
                {
                    "type": "readers.las",
                    "filename": input_file
                },
                # Create sea mask attribute
                {
                    "type": "filters.ferry",
                    "dimensions": "=>WithinSea"
                },
                # Create powerline mask attribute
                {
                    "type": "filters.ferry",
                    "dimensions": "=>WithinPowerline"
                },
                # Create water body mask attribute
                {
                    "type": "filters.ferry",
                    "dimensions": "=>WithinWaterBody"
                },
                # Create building mask attribute
                {
                    "type": "filters.ferry",
                    "dimensions": "=>WithinBuilding"
                },
                # Assing binary values to overlay attributes
                {
                    "type": "filters.assign",
                    "value": [
                        "WithinSea = 1 WHERE WithinSea != 0",
                        "WithinPowerline = 1 WHERE WithinPowerline != 0",
                        "WithinWaterBody = 1 WHERE WithinWaterBody != 0",
                        "WithinBuilding = 1 WHERE WithinBuilding != 0"
                    ]
                },
                # Subtract Z from NDVI values as temporary HAG
                {
                    "type": "filters.hag_dem",
                    "raster": ndvi_file,
                    "zero_ground": False
                },
                # Copy temporary HAG to new NDVI attribute
                {
                    "type": "filters.ferry",
                    "dimensions": "HeightAboveGround=>NDVI"
                },
                # Extract actual NDVI from temporary HAG based on Z
                {
                    "type": "filters.assign",
                    "value" : "NDVI = Z - NDVI"
                },
                # Calculate actual HAG from elevation raster
                {
                    "type": "filters.hag_dem",
                    "raster": dem_file,
                    "zero_ground": False
                },
                # Use Z as HAG for points in sea or outside elevation raster
                {
                    "type": "filters.assign",
                    "value" : "HeightAboveGround = 0 WHERE (WithinSea == 1) || (HeightAboveGround > 9999)"
                },
                # Save original classification values as new attribute
                {
                    "type": "filters.ferry",
                    "dimensions": "Classification=>OriginalClassification"
                },
                # Assign new classification values based on conditions
                {
                    "type": "filters.assign",
                    "value": [
                        "Classification = 6 WHERE ((WithinSea == 0) && (OriginalClassification == 1) && (WithinBuilding == 1))",
                        "Classification = 5 WHERE ((WithinSea == 0) && (OriginalClassification == 1) && (WithinBuilding == 0) && (HeightAboveGround > 0.2) && (NDVI > 0.33) && ((WithinPowerline == 0) || ((WithinPowerline == 1) && (HeightAboveGround < 6))))",
                        "Classification = 2 WHERE ((WithinSea == 0) && (OriginalClassification == 1) && (WithinBuilding == 0) && (HeightAboveGround < 0.2))",
                        "Classification = 9 WHERE ((WithinSea == 0) && (OriginalClassification == 1) && (WithinWaterBody == 1) && (HeightAboveGround < 0.2))",
                        "Classification = 1 WHERE ((WithinSea == 0) && (OriginalClassification == 5) && (WithinBuilding == 0) && (WithinPowerline == 1) && (HeightAboveGround > 6))",
                        "Classification = 2 WHERE ((WithinSea == 0) && (OriginalClassification == 9) && (WithinWaterBody == 0))"
                    ]
                },
                # Write output file
                {
                    "type": "writers.las",
                    "filename": output_file,
                    "minor_version": 4,
                    "dataformat_id": 8,
                    "extra_dims": "all"
                }
            ]
        }
        self.update_pipeline(input_file, etak_file)

    # Get bounds of LAZ file
    def get_laz_bounds(self, input_file: str) -> List[float]:
        pipeline_dict = {
            "pipeline": [
                {
                    "type": "readers.las",
                    "filename": input_file
                }
            ]
        }
        pipeline_json = json.dumps(pipeline_dict)
        pipeline_obj = pdal.Pipeline(pipeline_json)
        pipeline_obj.execute()
        metadata = pipeline_obj.metadata
        minx = metadata["metadata"]["readers.las"]["minx"]
        miny = metadata["metadata"]["readers.las"]["miny"]
        maxx = metadata["metadata"]["readers.las"]["maxx"]
        maxy = metadata["metadata"]["readers.las"]["maxy"]
        return [minx, miny, maxx, maxy]


    # Check if features exist within given bounds
    def features_exist(self, etak_file: str, query: str) -> bool:

        # Open ETAK file
        ds = ogr.Open(etak_file)

        # Execute the provided query
        result = ds.ExecuteSQL(query)

        # Check if any features exist
        has_features = result is not None and result.GetFeatureCount() > 0

        # Clean up result set
        ds.ReleaseResultSet(result)

        return has_features

    # Update overlay bounding box based on LAZ file bounds
    def update_overlay_bbox(self, laz_bounds: List[float], query: str) -> str:
        minx = laz_bounds[0]
        miny = laz_bounds[1]
        maxx = laz_bounds[2]
        maxy = laz_bounds[3]
        query = (
            query
            .replace("minx", str(minx))
            .replace("miny", str(miny))
            .replace("maxx", str(maxx))
            .replace("maxy", str(maxy))
        )
        return query

    # Get year from input filename
    def get_input_file_year(self, input_file: str):
        return int(os.path.basename(input_file).split(".")[0].split("_")[1])

    # Get season from input filename
    def get_input_file_season(self, input_file: str):
        return os.path.basename(input_file).split(".")[0].split("_")[2]

    # Update pipeline with overlay filters
    def update_pipeline(
            self,
            input_file: str,
            etak_file: str
        ) -> None:

        # Get input LAZ file bounds
        laz_bounds = self.get_laz_bounds(input_file)

        # List of overlay filters to insert dynamically
        overlay_steps = []

        # Check for sea
        sea_layers = [
            "E_201_meri_a"
        ]
        sea_query = self.update_overlay_bbox(
            laz_bounds,
            (
                f"SELECT geom, kood FROM {sea_layers[0]} "
                "WHERE ST_Intersects(geom, ST_GeomFromText("
                "'POLYGON((minx miny, maxx miny, maxx maxy, minx maxy, minx miny))', 3301))"
            )
        )
        if self.features_exist(etak_file, sea_query):
            overlay_steps.append({
                "type": "filters.overlay",
                "dimension": "WithinSea",
                "datasource": etak_file,
                "column": "kood",
                "query": sea_query
            })

        # Check for powerlines
        powerline_layers = [
            "E_601_elektriliin_j"
        ]
        powerline_query = self.update_overlay_bbox(
            laz_bounds,
            (
                f"SELECT ST_Buffer(geom, 13) AS geom, nimipinge FROM {powerline_layers[0]} "
                "WHERE nimipinge IN (110, 220, 330) "
                "AND ST_Intersects(geom, ST_GeomFromText('POLYGON((minx miny, maxx miny, maxx maxy, minx maxy, minx miny))', 3301))"
            )
        )
        if self.features_exist(etak_file, powerline_query):
            overlay_steps.append({
                "type": "filters.overlay",
                "dimension": "WithinPowerline",
                "datasource": etak_file,
                "column": "nimipinge",
                "query": powerline_query
            })

        # Check for water bodies
        water_layers = [
            "E_202_seisuveekogu_a",
            "E_203_vooluveekogu_a"
        ]
        water_query = self.update_overlay_bbox(
            laz_bounds,
            (
                "SELECT * FROM ("
                f"SELECT geom, kood FROM {water_layers[0]} "
                "UNION ALL "
                f"SELECT geom, kood FROM {water_layers[1]} "
                ") AS combined_layers "
                "WHERE ST_Intersects(geom, ST_GeomFromText('POLYGON((minx miny, maxx miny, maxx maxy, minx maxy, minx miny))', 3301))"
            )
        )
        if self.features_exist(etak_file, water_query):
            overlay_steps.append({
                "type": "filters.overlay",
                "dimension": "WithinWaterBody",
                "datasource": etak_file,
                "column": "kood",
                "query": water_query
            })

        # Check for buildings
        building_layers = [
            "E_401_hoone_ka",
            "E_403_muu_rajatis_ka"
        ]
        building_query = self.update_overlay_bbox(
            laz_bounds,
            (
                "SELECT * FROM ("
                f"SELECT geom, kood FROM {building_layers[0]} "
                "UNION ALL "
                f"SELECT geom, kood FROM {building_layers[1]} "
                ") AS combined_layers "
                "WHERE ST_Intersects(geom, ST_GeomFromText('POLYGON((minx miny, maxx miny, maxx maxy, minx maxy, minx miny))', 3301))"
            )
        )
        if self.features_exist(etak_file, building_query):
            overlay_steps.append({
                "type": "filters.overlay",
                "dimension": "WithinBuilding",
                "datasource": etak_file,
                "column": "kood",
                "query": building_query
            })

        # Insert overlay steps
        insert_index = 5
        self.pipeline["pipeline"][insert_index:insert_index] = overlay_steps

        # Extract year from filename
        year = self.get_input_file_year(input_file)

        # Extract season from filename
        season = self.get_input_file_season(input_file)

        # Add additional conditions depending on year and season
        if (
            (year == 2018 and season in ["mets", "tava"]) or 
            (year == 2019 and season in ["mets", "tava"]) or 
            (year == 2020 and season == "tava")
        ):
            self.pipeline["pipeline"][-2]["value"].append(
                "Classification = 5 WHERE ((WithinSea == 0) && (OriginalClassification == 6) && (WithinBuilding == 0) && (NDVI > 0.4))"
            )
        if season == "mets":
            self.pipeline["pipeline"][-2]["value"].append(
                "Classification = 5 WHERE ((WithinSea == 0) && (OriginalClassification == 2) && (HeightAboveGround > 0.2) && (NDVI > 0.33))"
            )

    def run(self) -> None:
        pipeline_json = json.dumps(self.pipeline)
        pipeline_obj = pdal.Pipeline(pipeline_json)
        pipeline_obj.execute()

    def print_pipeline(self) -> None:
        print(json.dumps(self.pipeline, indent=4))


def main(
        input_file: str, output_file: str, dem_file: str, etak_file: str, ndvi_file: str, print_pipeline=True,
    ) -> int:
    try:
        # Create reclassification pipeline based on input files
        pipeline = ReclassificationPipeline(
            input_file, output_file, dem_file, etak_file, ndvi_file
        )

        # Print pipeline
        if (print_pipeline):
            pipeline.print_pipeline()

        # Run pipeline
        pipeline.run()
        return (3, datetime.now(timezone.utc))
    except Exception as e:
        logging.error(f"reclassify : {input_file.split('/')[-1]} failed {e}")
        return (-3, datetime.now(timezone.utc))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
    description=(
            "Calculate HeightAboveGround (HAG) and reclassify points "
            "based on HAG, ReturnNumber, NumberOfReturns and "
            "relation to building footprints."
        )
    )
    parser.add_argument("input_file", help="name of input LAZ file")
    parser.add_argument(
        "output_file", help="name of reclassified output LAZ file"
    )
    parser.add_argument(
        "dem_file", help="name of DEM file for HAG calculation"
    )
    parser.add_argument(
        "etak_file",
        help="name of ETAK file to read building footprints from"
    )
    parser.add_argument(
        "ndvi_file",
        help="name of NDVI file for summer season"
    )

    # Parse the arguments
    args = parser.parse_args()
    input_file = args.input_file
    output_file = args.output_file
    dem_file = args.dem_file
    etak_file = args.etak_file
    ndvi_file = args.ndvi_file

    # Run main function
    main(input_file, output_file, dem_file, etak_file, ndvi_file)
