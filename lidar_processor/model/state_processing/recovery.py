from typing import List, Tuple
from lidar_processor.dependencies.db import Database
from psycopg import Error as dbError
from psycopg import errors as stateError
import logging
from datetime import datetime, timezone


# reset laz, dem files state and return a list of laz filename for recovery
def recovery(db: Database, id_: str) -> Tuple[str, List[Tuple[str,]], List[Tuple[str,]]]:
    cur = db.conn.cursor()
    try:
        with db.conn.transaction():
            # lock laz_files rows with state=1 (downloaded) for update.
            recovery_statement = "select filename, laz_map_sheet, state\
                                  from laz_files where (state <> 3 and identifier = %(id_)s)  \
                                  for update nowait;"
            cur.execute(recovery_statement, {'id_': id_})
            laz_recovery_set = cur.fetchall()
            laz_reset_state = []
            laz_recovery_filenames = []
            dem_reset_state = {}
            recovery_statement = "select filename,state from dem_files where identifier=%(id_)s and state<>1 for update nowait;"
            cur.execute(recovery_statement, {'id_': id_})
            dem_recovery_set = cur.fetchall()
            logging.info(f'recovery: {len(laz_recovery_set)} laz_files to recover, {len(dem_recovery_set)} dem_files to recover')
            if ((len(dem_recovery_set) > 0) or (len(laz_recovery_set) > 0)):
                for i in dem_recovery_set:
                    if (i[1] <= 0):
                        if (dem_reset_state.get(i[0]) is None):
                            dem_reset_state[i[0]] = ((0, f'{id_}_R', None, i[0]))
                for i in laz_recovery_set:
                    laz_state = i[2] if (i[2] >= 0) else abs(i[2]) - 1
                    laz_reset_state.append((laz_state, f'{id_}_R', datetime.now(timezone.utc), i[0]))
                    laz_recovery_filenames.append((i[0],))
                reset_statement = 'update laz_files set (state, identifier, processing_time) = (%s,%s,%s) where filename=%s'
                logging.debug(f'recovery: {laz_reset_state}')
                cur.executemany(reset_statement, laz_reset_state)
                reset_statement = 'update dem_files set (state, identifier, download_time) = (%s,%s,%s) where filename=%s'
                logging.debug(f'recovery: {dem_reset_state}')
                cur.executemany(reset_statement, [v for k, v in dem_reset_state.items()])
                logging.info('recovery: reset state completed.')
                return (f'{id_}_R', laz_recovery_filenames, [(k, ) for k in dem_reset_state.keys()])
            else:
                logging.warning('recovery_lidar: nothing to recovery.')
                raise ValueError('recovery_lidar: nothing to recovery.')
    except stateError.LockNotAvailable as e:
        logging.error(f'recoery_lidar: db lock error {e}')
        raise
    except dbError as e:
        logging.error(f'recovery_lidar: db error {e}')
        raise

