from typing import List, Tuple
from lidar_processor.dependencies.db import Database
from lidar_processor.dependencies.threading import ReturnValueThread
from lidar_processor.model.state_processing.records_creation import dem_file_naming
from lidar_processor.model.processing_script.reclassify_laz_file import main as reclassify_process

import concurrent.futures
from multiprocessing import cpu_count
import os
import logging
from tqdm import tqdm
from datetime import datetime, timezone
from psycopg import Error as dbError
from psycopg import errors as stateError

etak_filename = "ETAK_EESTI_GPKG.gpkg"
etak_mapping = {2017: "ETAK_EESTI_GPKG_2017_12_15",
                2018: "ETAK_EESTI_GPKG_2019_01_01",
                2019: "ETAK_EESTI_GPKG_2020_01_04",
                2020: "ETAK_EESTI_GPKG_2021_01_02",
                2021: "ETAK_EESTI_GPKG_2022_01_01",
                2022: "ETAK_EESTI_GPKG_2023_01_01",
                2023: "ETAK_EESTI_GPKG_2024_01_01",
                2024: "ETAK_EESTI_GPKG_2025_01_01"}

ndvi_mapping = {'mets': '{year}/est_s2_ndvi_{year}-06-01_{year}-08-31_cog.tif',
                'tava': '{year}/est_s2_ndvi_{year}-04-01_{year}-05-31_cog.tif'}


def reclassify(db: Database, laz_list: List[str], laz_year: int, laz_type: str, dem_year: int, laz_fixed_filepath: str,
               reclassify_path: str, etak_path: str, ndvi_path: str):
    # determinate the file name of dem by year
    etak_folder = etak_mapping.get(laz_year)
    statement = 'update laz_files set (state, processing_time, etak_path, reclassify_path, dem_path, ndvi_path) = (%s,%s,%s,%s,%s,%s) where filename=%s'
    if (etak_folder is None):
        logging.error('reclassify: etak mapping failed.')
        raise ValueError('reclassify: etak mapping failed.')
    cur = db.conn.cursor()
    try:
        with db.conn.transaction():
            # lock laz_files rows with state=2 (fixed) for update.
            cur.execute('select filename,laz_map_sheet,bucket from laz_files where filename = ANY(%(laz_filenames)s) and state=2 for update nowait;',
                        {'laz_filenames': laz_list})
            laz_set = cur.fetchall()
            if (len(laz_set) > 0):
                # select corresponding dem_files by joining mapsheets_mapping
                # (nr = laz_files.laz_map_sheet , nr10000 = dem_files.dem_map_sheet)
                # where dem_state = 1 (downloaded) and laz_state = 2 (fixed)
                filtered_laz_list = [i[0] for i in laz_set]
                merged_statement = "select laz_files.filename, laz_files.bucket, laz_files.state, laz_files.laz_map_sheet, \
                                    dem_filename, dem_vrt_path, dem_state, nr, nr10000 \
                             from laz_files LEFT join \
                             (select  dem_files.filename as dem_filename, dem_files.vrt_path as dem_vrt_path, dem_files.state as dem_state,\
                             dem_files.year as dem_year, nr, nr10000 from dem_files\
                             LEFT join mapsheets_mapping on dem_files.dem_map_sheet = mapsheets_mapping.nr10000) as tmp \
                             on laz_files.laz_map_sheet = tmp.nr  \
                             where tmp.dem_state=1 and laz_files.state = 2 and laz_files.filename = ANY (%(laz_filenames)s)"
                if (dem_year > 2020):
                    merged_statement += ' and tmp.dem_year = %(dem_year)s'
                    cur.execute(merged_statement, {'laz_filenames': filtered_laz_list, 'dem_year': dem_year})
                else:
                    merged_statement += "and tmp.dem_filename like '%%dem%%'"
                    cur.execute(merged_statement, {'laz_filenames': filtered_laz_list})
                merged_set = cur.fetchall()
                etak_full_path = etak_path + '/' + etak_folder + '/' + etak_filename
                ndvi_full_path = ndvi_path + '/' + ndvi_mapping[laz_type].format(year=laz_year)
                if (len(merged_set) > 0):
                    mp = int(os.environ.get('SLURM_CPUS_PER_TASK', cpu_count() - 1))
                    logging.info(f'reclassify: parallel process {mp}')
                    with concurrent.futures.ProcessPoolExecutor(mp) as executor:
                        params = [(laz_fixed_filepath + '/' + m[0].replace('.laz', '_fixed.laz'),
                                  reclassify_path + '/' + m[0].replace('.laz', '_reclassified.laz'),
                                  m[5],
                                  etak_full_path, ndvi_full_path, False) for m in merged_set]
                        reclassify_result = list(tqdm(executor.map(reclassify_process, *zip(*params)), total=len(params)))
                    data = [(result[0], result[1], etak_full_path, reclassify_path + '/' + merged_set[i][0].replace('.laz', '_reclassified.laz'),
                            merged_set[i][5], ndvi_full_path,
                            merged_set[i][0])
                            for i, result in enumerate(reclassify_result)]
                    cur.executemany(statement, data)
                    reclassify_failed = [merged_set[i][0] for i, r in enumerate(reclassify_result) if (r[0] == -3)]
                    reclassified = [merged_set[i][0] for i, r in enumerate(reclassify_result) if (r[0] == 3)]
                    not_found = list(set(laz_list) - set(reclassified) - set(reclassify_failed))
                    if (len(not_found) > 0):
                        data = [(-3, datetime.now(timezone.utc), etak_path, None, None, None, i) for i in not_found]
                        cur.executemany(statement, data)
                    logging.info(f'reclassify: all threads completed , fixed : {len(reclassified)}, fix failed: {len(reclassify_failed)}, not found: {len(not_found)}')
                    return (reclassified, reclassify_failed, not_found)
                else:
                    logging.error('reclassify: get dem files failed, please check if those dem files are downloaded and laz files are fixed')
                    raise ValueError('reclassify: get dem files failed, please check if those dem files are downloaded and laz files are fixed')
            else:
                logging.error('reclassify: all laz_files are not ready, please check if those are fixed.')
                raise ValueError('reclassify: all laz_files are not ready, please check if those are fixed.')
    except stateError.LockNotAvailable as e:
        logging.error(f'reclassify: db lock error {e}')
        raise
    except dbError as e:
        data = [(-3, datetime.now(timezone.utc), etak_path, None, None, None, i) for i in laz_list]
        cur.executemany(statement, data)
        logging.error(f'reclassify: db error {e}')
        raise




