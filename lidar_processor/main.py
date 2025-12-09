import yaml
import argparse
import logging
from typing import List
from psycopg import Error as dbError
import time
import os
import uuid

from lidar_processor.dependencies.db import Database
from lidar_processor.schemas.config import DBConfig, StorageConfig, LidarConfig
from lidar_processor.model.state_processing.records_creation import laz_files_creation, dem_files_creation
from lidar_processor.model.state_processing.download_files import download_files
from lidar_processor.model.state_processing.fix_lidar import fix_lidar
from lidar_processor.model.state_processing.reclassify import reclassify
from lidar_processor.model.state_processing.recovery import recovery


loglevel = {'info': logging.INFO,
            'debug': logging.DEBUG,
            'error': logging.ERROR,
            'warning': logging.WARNING}


def parse_args(arg_list: List[str] | None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="configuration path", default='./config.yaml')
    parser.add_argument("-i", "--id", help="identifier", default=str(uuid.uuid4().hex.upper()))
    parser.add_argument("-r", "--recovery", help="identifier to recover", default=None)
    parser.add_argument("-log", "--loglevel", help="configuration path", default='info')
    args = parser.parse_args(arg_list)
    return args


def main(arg_list: List[str] | None = None):
    args = parse_args(arg_list)
    # get config yaml file path from args
    configpath = args.config
    recovery_mode = args.recovery
    suffix = "" if (recovery_mode is None) else "_recovery"
    id_ = args.id if (recovery_mode is None) else args.recovery
    logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)7s {%(module)s} [%(funcName)s] %(message)s',
                        datefmt='%Y-%m-%d,%H:%M:%S', level=loglevel[args.loglevel.lower()])
    config = None
    try:
        with open(configpath) as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        logging.error(f"[{id_}] lidar_processor{suffix}: load {configpath} failed: {e}")
        os.sys.exit(-1)
    except OSError as e:
        logging.error(f"[{id_}] lidar_processor{suffix}: load {configpath} failed: {e}")
        os.sys.exit(-1)

    # pre-checking
    db, dbconfig, storageconfig, lidarconfig = None, None, None, None
    try:
        dbconfig = DBConfig(**config['db'])
        storageconfig = StorageConfig(**config['storage'])
        lidarconfig = LidarConfig(**config['lidar'])
    except KeyError as e:
        logging.error(f"[{id_}] config file missing section: {e}")
        os.sys.exit(-1)

    try:
        db = Database(**dbconfig.__dict__)
    except dbError as e:
        logging.error(f'[{id_}] lidar_processor{suffix}: db initialization failed {e}')
        os.sys.exit(-1)
    # we should filter out those laz mapsheet that doesn't belongs to Estonia
    filtered_range = db.execute_sql("""select nr from mapsheets_mapping where nr = ANY (%(laz_mapsheets)s)""",
                                    {'laz_mapsheets': lidarconfig.laz_mapsheets})
    if (len(filtered_range) > 0):
        try:
            start = time.time()
            logging.info(f'[{id_}] lidar_processor{suffix}: identifier {id_}')
            laz_filename = [f'{mapsheet[0]}_{lidarconfig.laz_year}_{lidarconfig.laz_type}.laz' for mapsheet in filtered_range]
            if (recovery_mode is not None):
                id_, laz_list, dem_list = recovery(db, id_)
                laz_filename = [i[0] for i in laz_list]
                laz_recovery_map_sheets = [f.split('_')[0] for f in laz_filename]
                logging.info(f'[{id_}] lidar_processor{suffix}: {len(laz_list)} laz files for processing.')
                #new_dem_list = dem_files_creation(db, laz_recovery_map_sheets, lidarconfig.dem_year, id_)
                #dem_list += new_dem_list
                logging.info(f'[{id_}] lidar_processor{suffix}: {len(dem_list)} new dem files for download.')
            if (recovery_mode is None):
                logging.info(f'[{id_}] lidar_processor{suffix}: {len(laz_filename)} laz files for processing.')
                # enter state 0 (laz_files_creation), return new laz(s) need to download (laz filename, laz map sheet, state)
                laz_list = laz_files_creation(db, laz_filename, lidarconfig.laz_to_crs, storageconfig.etak_path, id_)
                laz_mapsheets = [r[1] for r in laz_list]
                logging.info(f'[{id_}] lidar_processor{suffix}: {len(laz_mapsheets)} new laz files for download.')
                # enter state 0 (dem_files_creation) return new dem(s) need to download (dem filename, state)
                #dem_list = dem_files_creation(db, laz_mapsheets, lidarconfig.dem_year, id_)
                #logging.info(f'[{id_}] lidar_processor{suffix}: {len(dem_list)} new dem files for download.')
            # enter state 1 or -1 , return tuple of list that download successfully,  the first is laz filename and second is dem filename
            download_result = download_files(db, [r[0] for r in laz_list], storageconfig.bucket + '/' + storageconfig.laz_path, 'laz_files')
            logging.info(f'[{id_}] lidar_processor{suffix}: {len(download_result)} laz files downloaded')
            # enter state 2 or -2 , return tuple of list , (fixed , fix_failed , not_found, fix_no_need)
            fix_result = fix_lidar(db, laz_filename, storageconfig.bucket + '/' + storageconfig.fix_path,
                                   lidarconfig.laz_to_crs)
            fixed_laz = fix_result[0] + fix_result[3]
            print('here')
            # enter state 3 or -3, return tuple of list , (reclassified , reclasify_failed , not_found)
            reclassify_result = reclassify(db, fixed_laz, lidarconfig.laz_year, lidarconfig.laz_type, lidarconfig.dem_year,
                                           storageconfig.bucket + '/' + storageconfig.fix_path,
                                           storageconfig.bucket + '/' + storageconfig.reclassify_path,
                                           storageconfig.etak_path, storageconfig.ndvi_path)
            end = time.time()
            logging.info(f'[{id_}] lidar_processor{suffix}: completed {(end-start)/60} mins.')
            state = [0, 1, 2,  -1, -2, -3]
            rerun = []
            for s in state:
                result = db.execute_sql('select filename from laz_files where state=%(state)s and filename=ANY(%(filename)s)', {'state': s, 'filename': laz_filename})
                result = [i[0] for i in result]
                rerun += result
                logging.info(f'[{id_}] lidar_processor{suffix}: state {s} files : {result}')
            if (len(rerun) > 0):
                logging.warning(f'[{id_}] lidar_processor{suffix}: {id_} need recover')
                os.sys.exit(-1)
            else:
                result = db.execute_sql('select count(filename) from laz_files where state=3 and filename=ANY(%(filename)s)', {'filename': laz_filename})
                logging.info(f'[{id_}] lidar_processor{suffix}: completed successfully, reclassified {result[0][0]} laz files.')
                os.sys.exit(0)
        except dbError as e:
            logging.error(f'[{id_}] {e}')
            logging.warning(f'[{id_}] lidar_processor{suffix}: {id_} need recover')
            os.sys.exit(-1)
        except ValueError as e:
            if ("nothing to recovery" in e.args[0]):
                os.sys.exit(-1)
            logging.error(f'[{id_}] {e}')
            logging.warning(f'[{id_}] lidar_processor{suffix}: {id_} need recover')
            os.sys.exit(-1)
    else:
        logging.error(f'[{id_}] lidar_processor{suffix}: laz map sheets range is incorrect.')
        os.sys.exit(-1)


if __name__ == "__main__":
    main()
