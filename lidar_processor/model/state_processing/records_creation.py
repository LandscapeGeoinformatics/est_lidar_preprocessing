from typing import List, Tuple
from lidar_processor.dependencies.db import Database
from psycopg import Error as dbError
import logging


dem_file_naming= {2017: "{mapsheet}_dem_1m_2017-2020.tif", 2021: "{mapsheet}_dtm_1m_{year}.tif"}


def laz_files_creation(db: Database, file_name: List[str], to_crs: str, etak_path: str, id_: str) -> List[Tuple[str, int, int]]:
    # entry action
    try:
        statement = 'select filename from laz_files where filename = ANY(%(file_name)s)'
        result = db.execute_sql(statement, {'file_name': file_name})
    except dbError as e:
        logging.error(f'laz_files_creation: select failed {e}')
        raise
    logging.info(f'create laz_files records: {len(file_name)}')
    logging.debug(f'create laz_files records: {file_name}')
    if (len(result) == 0):
        # do action
        statement = "insert into laz_files (filename, laz_map_sheet, year, laz_type, to_crs, etak_path, identifier) \
                     values (%s,%s,%s,%s,%s,%s,%s) returning filename, laz_map_sheet, state"
        data = [(f, f.split('.')[0].split('_')[0],
                 f.split('.')[0].split('_')[1],
                 f.split('.')[0].split('_')[2], to_crs, etak_path, id_)
                for f in file_name]
        try:
            result = db.execute_many(statement, data)
            logging.info(f'create laz_files records success.')
            logging.debug(f'create laz_files records: {result}')
            return result
        except dbError as e:
            # exist fail
            logging.error(f'laz_files_creation: insert failed {e}')
            raise
    else:
        # exist fail
        logging.error(f'laz_files_creation: filename exists {result}')
        raise ValueError('laz_files_creation: filename exists {result}')


def dem_files_creation(db: Database, dem_year: int, id_: str) -> List[Tuple[str, int]]:
    # entry action
    try:
        statement = 'select nr10000 from mapsheets_mapping'
        result = db.execute_sql(statement)
    except dbError as e:
        logging.error(f'dem_files_creation: select mapsheet mapping failed {e}')
        raise
    if (len(result) > 0):
        # do action
        dem_filename = [dem_file_naming[k] for k in sorted(dem_file_naming.keys()) if (k <= dem_year)]
        if len(dem_filename) == 0:
            logging.error('dem_files_creation: couldn\'t determinate the file name.')
            raise ValueError('dem_files_creation: couldn\'t determinate the file name.')
        dem_filenames = [dem_filename[-1].format(mapsheet=dem_mapsheet[0], year=dem_year) for dem_mapsheet in result]
        # find non-existing dem files
        while (True):
            statement = 'select filename from dem_files where filename = ANY(%(dem_filenames)s)'
            try:
                result = db.execute_sql(statement, {'dem_filenames': dem_filenames})
                result = [r[0] for r in result]
            except dbError as e:
                # exist fail
                logging.error(f'dem_files_creation: filter non-exists records failed: {e}')
                raise
            non_exits_dem_filenames = list(set(dem_filenames) - set(result))
            non_exits_dem_mapsheet = [d.split('_')[0] for d in non_exits_dem_filenames]
            logging.info(f'create dem_files records: {len(non_exits_dem_filenames)}')
            logging.debug(f'create dem_files records: {non_exits_dem_filenames}')
            # create records for non-existing dem files
            if (len(non_exits_dem_mapsheet) > 0):
                statement = "insert into dem_files (filename, year, dem_map_sheet, identifier) values (%s,%s,%s,%s) returning filename,state"
                data = [(f, dem_year, non_exits_dem_mapsheet[i], id_) for i, f in enumerate(non_exits_dem_filenames)]
                try:
                    result = db.execute_many(statement, data)
                    logging.info('create dem_files records success.')
                    return result
                except dbError as e:
                    # exist fail
                    logging.error(f'dem_files_creation: insert failed {e}')
                    pass
            # nothing to create
            else:
                return []
    else:
        # exist fail
        logging.error(f'dem_files_creation: select mapsheet mapping failed, no records found {laz_map_sheets}')
        raise ValueError(f'dem_files_creation: select mapsheet mapping failed, no records found {laz_map_sheets}')

