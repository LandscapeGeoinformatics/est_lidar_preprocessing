import argparse

import numpy as np
import pyproj
import laspy
import gcsfs
import io
import tempfile
import logging
import tempfile
from datetime import datetime, timezone
from google.cloud import storage

# Remove points flagged as overlaps
def remove_overlapping_points(laz_points: laspy.LasData) -> laspy.LasData:

    # Create array of overlap flags
    overlap_points = np.array(laz_points.overlap)

    # Get indices of points without overlap
    no_overlap_ids = np.where(overlap_points == 0)

    # Overwrite input array with only the points without overlap
    laz_points.points.array = laz_points.points.array[no_overlap_ids]

    # Update header with new point count
    laz_points.header.point_count = len(no_overlap_ids[0])

    return laz_points


# Add CRS to points
def add_crs(laz_points: laspy.LasData, out_crs: str) -> laspy.LasData:
    laz_points.header.add_crs(pyproj.CRS.from_string(out_crs))
    return laz_points


def main(input_file: str, output_file: str, out_crs: str) -> int:

    # Read points
    # To support gcp access
    try:
        if (input_file.lower().startswith('gs://')):
            temp = tempfile.NamedTemporaryFile('wb', suffix='.laz')
            path = input_file.split('/')
            client = storage.Client()
            obj = client.get_bucket(path[2])
            obj = obj.get_blob('/'.join(path[3:]))
            with open(temp.name, 'wb') as f:
                obj.download_to_file(f)

            input_file = temp.name
        with open(input_file, 'rb') as f:
            laz_points = laspy.read(f)

        # Remove overlapping points
        laz_points = remove_overlapping_points(laz_points)

        # Add CRS
        laz_points = add_crs(laz_points, out_crs)

        # Write fixed output file
        if (output_file.lower().startswith('gs://')):
            # have to give the .laz suffix , otherwise, the laz file will be very large.
            # Seems like lazpy does the compression depends on the file extension
            tmp = tempfile.NamedTemporaryFile(suffix='.laz')
            laz_points.write(tmp.name)
            fs = gcsfs.GCSFileSystem() if (output_file.lower().startswith('gs://')) else io
            fs.put_file(tmp.name, output_file)
        else:
            laz_points.write(output_file)
        return (2, datetime.now(timezone.utc))
    except Exception as e:
        logging.error(f'fix laz: {input_file.split("/")[-1]} failed: {e}')
        return (-2, datetime.now(timezone.utc))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
    description=(
            "Remove overlapping points, assign CRS and "
            "write out as LAZ 1.4 file."
        )
    )
    parser.add_argument("input_file", help="name of input LAZ file")
    parser.add_argument("output_file", help="name of output LAZ file")
    parser.add_argument(
        "--out_crs",
        help="output CRS (default: %(default)s)",
        default="EPSG:3301"
    )

    # Parse the arguments
    args = parser.parse_args()
    input_file = args.input_file
    output_file = args.output_file

    if args.out_crs:
        out_crs = args.out_crs

    try:

        # Run main function
        main(input_file, output_file, out_crs)

    except Exception as e:
        print(f"Error: {str(e)}")
