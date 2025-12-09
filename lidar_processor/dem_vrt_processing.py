import yaml
import argparse
import logging
from typing import List
from psycopg import Error as dbError
import time
import os
import uuid
from osgeo import gdal

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
    suffix = "" if (recovery_mode is None) else "recovery"
    id_ = args.id if (recovery_mode is None) else args.recovery
    logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)7s {%(module)s} [%(funcName)s] %(message)s',
                        datefmt='%Y-%m-%d,%H:%M:%S', level=loglevel[args.loglevel.lower()])
    config = None
    try:
        with open(configpath) as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        logging.error(f"{__name__}: [{id_} {suffix}] load {configpath} failed: {e}")
        os.sys.exit(-1)
    except OSError as e:
        logging.error(f"{__name__}: [{id_} {suffix}] load {configpath} failed: {e}")
        os.sys.exit(-1)

    # pre-checking
    db, dbconfig, storageconfig, lidarconfig = None, None, None, None
    try:
        dbconfig = DBConfig(**config['db'])
        storageconfig = StorageConfig(**config['storage'])
        lidarconfig = LidarConfig(**config['lidar'])
    except KeyError as e:
        logging.error(f"{__name__}: [{id_} {suffix}] config file missing section: {e}")
        os.sys.exit(-1)

    try:
        db = Database(**dbconfig.__dict__)
    except dbError as e:
        logging.error(f'{__name__}: [{id_} {suffix}] db initialization failed {e}')
        os.sys.exit(-1)
    # we should filter out those laz mapsheet that doesn't belongs to Estonia
    try:
        start = time.time()
        logging.info(f'{__name__} [{id_} {suffix}] Download DEM and create VRT ')
        if (recovery_mode is not None):
            id_, laz_list, dem_list = recovery(db, id_)
            logging.info(f'{__name__} [{id_} {suffix}] {len(dem_list)} new dem files for download.')
        if (recovery_mode is None):
            # enter state 0 (dem_files_creation) return new dem(s) need to download (dem filename, state)
            dem_list = dem_files_creation(db, lidarconfig.dem_year, id_)
            logging.info(f'{__name__} [{id_} {suffix}] {len(dem_list)} new dem files for download.')
        # enter state 1 or -1 , return tuple of list that download successfully,  the first is laz filename and second is dem filename
        download_result = download_files(db, [r[0] for r in dem_list], storageconfig.bucket + '/' + storageconfig.dem_path, 'dem_files')
        logging.info(f'{__name__} [{id_} {suffix}] {len(download_result)} laz files downloaded')
        end = time.time()
        logging.info(f'{__name__} [{id_} {suffix}] completed {(end-start)/60} mins.')
        state = [0]
        dem_filenames = [r[0] for r in dem_list]
        rerun = []
        for s in state:
            result = db.execute_sql('select filename from dem_files where state=%(state)s and \
                                     filename=ANY(%(filename)s)', {'state': s, 'filename': dem_filenames})
            result = [i[0] for i in result]
            rerun += result
            logging.info(f'{__name__} [{id_} {suffix}] state {s} files : {result}')
        if (len(rerun) > 0):
            logging.warning(f'{__name__} [{id_} {suffix}] need recover')
            os.sys.exit(-1)
        cur = db.conn.cursor()
        with db.conn.transaction():
            cur.execute(f"select filename from dem_files where state=1 and \
                                     identifier like '%{id_.replace('_R', '')}%' for update nowait;")
            dem_filenames = [i[0] for i in cur.fetchall()]
            dem_filepaths = [storageconfig.bucket.replace("gs://", "/vsigs/") + '/' + storageconfig.dem_path + '/' + d for d in dem_filenames]
            vrt_filepath = storageconfig.bucket.replace("gs://", "/vsigs/") + '/' + storageconfig.dem_path + '/' + f'dem_{lidarconfig.dem_year}.vrt'
            gdal.BuildVRT(vrt_filepath, dem_filepaths)
            logging.info(f'{__name__} [{id_} {suffix}] exported vrt : {vrt_filepath}')
            data = [(storageconfig.bucket + '/' + storageconfig.dem_path + '/' + f'dem_{lidarconfig.dem_year}.vrt', d) for d in dem_filenames]
            statement = 'update dem_files set vrt_path=%s where filename=%s'
            cur.executemany(statement, data)
        os.sys.exit(0)
    except dbError as e:
        logging.error(f'{__name__} [{id_} {suffix}] {e}')
        logging.warning(f'{__name__} [{id_} {suffix}] need recover')
        os.sys.exit(-1)
    except ValueError as e:
        if ("nothing to recovery" in e.args[0]):
            os.sys.exit(-1)
        logging.error(f'{__name__} [{id_} {suffix}] {e}')
        logging.warning(f'{__name__} [{id_} {suffix}] need recover')
        os.sys.exit(-1)


if __name__ == "__main__":
    main()
