from typing import List, Tuple
from lidar_processor.dependencies.db import Database
from lidar_processor.dependencies.threading import ReturnValueThread
from lidar_processor.model.processing_script.fix_laz_file import main as fix_process

import logging
import os
from tqdm import tqdm
import concurrent.futures
from multiprocessing import cpu_count
from datetime import datetime, timezone
from psycopg import Error as dbError
from psycopg import errors as stateError


def fix_lidar(db: Database, laz_list: List[str], fixed_filepath: str, to_crs: str) -> Tuple[List[str], List[str], List[str]] | None:
    cur = db.conn.cursor()
    statement = 'update laz_files set (state, processing_time, to_crs) = (%s,%s,%s) where filename=%s'
    try:
        with db.conn.transaction():
            # lock laz_files rows with state=1 (downloaded) for update.
            cur.execute('select filename,laz_map_sheet,bucket,path from laz_files where filename = ANY(%(laz_filenames)s) and state=1 for update nowait;',
                        {'laz_filenames': laz_list})
            laz_set = cur.fetchall()
            cur.execute('select filename from laz_files where filename = ANY(%(laz_filenames)s) and state=2 for update nowait;',
                        {'laz_filenames': laz_list})
            excluded_laz_set = [r[0] for r in cur.fetchall()]
            if (len(excluded_laz_set) == len(laz_list)):
                logging.info(f'fix_lidar: nothing to fix : excluded: {len(excluded_laz_set)}')
                return ([], [], [], excluded_laz_set)

            if (len(laz_set) > 0):
                # mp = int(os.environ.get('SLURM_CPUS_PER_TASK', cpu_count() - 1))
                # logging.info(f'fix_lidar: parallel process {mp}')
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    params = [(r[2] + '/' + r[3] + '/' + r[0], fixed_filepath + '/' + r[0].replace('.laz', '_fixed.laz'), to_crs)
                              for r in laz_set]
                    fix_result = list(tqdm(executor.map(fix_process, *zip(*params)), total=len(params)))
                fix_failed = [laz_set[i][0] for i, r in enumerate(fix_result) if (r[0] == -2)]
                fixed = [laz_set[i][0] for i, r in enumerate(fix_result) if (r[0] == 2)]
                not_found = list(set(laz_list) - set(fixed) - set(fix_failed) - set(excluded_laz_set))
                data = [(result[0], result[1], to_crs, laz_set[i][0]) for i, result in enumerate(fix_result)]
                logging.info(f'fix_lidar: all threads completed , fixed : {len(fixed)}, fix failed: {len(fix_failed)}, not found: {len(not_found)}, excluded: {len(excluded_laz_set)}')
                cur.executemany(statement, data)
                if (len(not_found) > 0):
                    data = [(-2, datetime.now(timezone.utc), to_crs, i) for i in not_found]
                    cur.executemany(statement, data)
                return (fixed, fix_failed, not_found, excluded_laz_set)
            else:
                logging.error('fix_lidar: all laz_files are not ready, please check if those are downloaded.')
                raise ValueError('fix_lidar: all laz_files are not ready, please check if those are downloaded.')
    except stateError.LockNotAvailable as e:
        logging.error(f'fix_lidar: db lock error {e}')
        raise
    except dbError as e:
        logging.error(f'fix_lidar: db error {e}')
        data = [(-2, datetime.now(timezone.utc), to_crs, i) for i in laz_set]
        cur.executemany(statement, data)
        raise




