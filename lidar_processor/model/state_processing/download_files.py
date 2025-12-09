from typing import List, Tuple
from lidar_processor.dependencies.db import Database
# from lidar_processor.dependencies.threading import ReturnValueThread

from concurrent.futures import ThreadPoolExecutor
import logging
import tempfile
import requests
import urllib3
import gcsfs
import io
from urllib.parse import quote
from tqdm import tqdm
from datetime import datetime, timezone
from psycopg import Error as dbError
from psycopg import errors as stateError


laz_url = "https://geoportaal.maaamet.ee/index.php?lang_id=1&plugin_act=otsing&kaardiruut={mapsheet}&andmetyyp=lidar_laz_{type}&dl=1&f={mapsheet}_{year}_{type}.laz&page_id=614"
dem_url = "https://geoportaal.maaamet.ee/index.php?lang_id=1&plugin_act=otsing&kaardiruut={mapsheet}&andmetyyp=dem_1m_geotiff&dl=1&f={mapsheet}_{type}_1m{year}.tif&page_id=614"


def download_worker(url: str, filepath: str) -> int:
    try:
        #  logging.info(f'download_worker: downloading {filepath.split("/")[-1]}')
        logging.debug(f'download_worker: {url}')
        http = urllib3.PoolManager()
        r = http.requests("GET", url, retries=15)
        if (r.headers['content-type'].lower() == 'application/octet-stream'):
            if (filepath.startswith('gs://')):
                fs = gcsfs.GCSFileSystem()
                temp = tempfile.NamedTemporaryFile('wb')
                temp.write(r.data)
                fs.put_file(temp.name, filepath)
            else:
                with open(filepath, 'wb') as f:
                    f.write(r.content)
            #  logging.info(f'download_worker: download {filepath.split("/")[-1]} done')
            return (1, datetime.now(timezone.utc))
        else:
            logging.error(f'download_worker: {filepath.split("/")[-1]} repsones is not in correct format. {url}')
            return (-1, datetime.now(timezone.utc))

    except requests.exceptions.RequestException as e:
        logging.error(f'download_worker: requests failed: {e}')
        return (-1, datetime.now(timezone.utc))
    except urllib3.exceptions.RequestError as e:
        logging.error(f'download_worker: requests failed: {e}')
        return (-1, datetime.now(timezone.utc))
    except urllib3.exceptions.MaxRetryError as e:
        logging.error(f'download_worker: requests failed: {e}')
        return (-1, datetime.now(timezone.utc))
    except OSError as e:
        logging.error(f'download_worker: upload file failed: {e}')
        return (-1, datetime.now(timezone.utc))


def download_files(db: Database, filename_list: List[str],
                   filepath: str, table='laz_files') -> List[str]:
    # entry action null
    # do action
    pos = 2 if filepath.startswith('gs://') else 1
    bucket = 'gs://' + filepath.split('/')[pos] if (filepath.startswith('gs://')) else '/'.join(filepath.split('/')[: pos + 1])
    download_path = '/'.join(filepath.split('/')[pos + 1:])
    statement = f'update {table} set (state, bucket, path, download_time) = (%s,%s,%s,%s) where filename=%s'
    try:
        cur = db.conn.cursor()
        with db.conn.transaction():
            # lock laz_files, dem_files rows with state=0 (created) for update.
            cur.execute(f'select filename from {table} where filename = ANY(%(laz_filenames)s) and state=0 for update nowait;',
                        {'laz_filenames': filename_list})
            file_list = [i[0] for i in cur.fetchall()]
            if (table == 'laz_files'):
                downloadurls = [laz_url.format(mapsheet=name.split('.')[0].split('_')[0],
                                               type=name.split('.')[0].split('_')[2],
                                               year=name.split('.')[0].split('_')[1]) for name in file_list]
            else:
                # dem file got two file name format : {mapsheet}_dem_1m_2017-2020.tif , {mapsheet}_dtm_1m_{year}.tif (>=2021)
                downloadurls = [dem_url.format(mapsheet=name.split('.')[0].split('_')[0], type='dem', year='_2017-2020')
                                if ('dem' in name) else dem_url.format(mapsheet=name.split('.')[0].split('_')[0], type='dtm', year='')
                                for name in file_list]
            logging.info(f'download_files: {table}: {len(downloadurls)}')
            download_paths = [filepath + '/' + name for name in file_list]
            download_result = []
            logging.info('download_files: threads')
            # to avoid too many concurrent download which may get blocked.
            download_batch = 10
            with ThreadPoolExecutor(max_workers=download_batch) as executor:
                download_result = list(tqdm(executor.map(download_worker, downloadurls, download_paths), total=len(downloadurls)))

            failed = [filename_list[i] for i, r in enumerate(download_result) if (r[0] == -1)]
            logging.info(f'download_files: all threads completed , failed: {len(failed)}')
            # update download state to laz_files and dem_files
            data = [(result[0], bucket, download_path, result[1], quote(downloadurls[i]), filename_list[i]) for i, result in enumerate(download_result)]
            statement = f'update {table} set (state, bucket, path, download_time, download_url) = (%s,%s,%s,%s,%s) where filename=%s'
            cur.executemany(statement, data)
            logging.info('download_files: update state completed.')
            return list(set(filename_list) - set(failed))
    except stateError.LockNotAvailable as e:
        logging.error(f'download_files: db lock error {e}')
        raise
    except dbError as e:
        data = [(-1, bucket, download_path, datetime.now(timezone.utc), i) for i in filename_list]
        statement = f'update {table} set (state, bucket, path, download_time) = (%s,%s,%s,%s) where filename=%s'
        cur.executemany(statement, data)
        logging.error(f'download_files: db error {e}')
        raise
    except Exception as e:
        data = [(-1, bucket, download_path, datetime.now(timezone.utc), i) for i in filename_list]
        statement = f'update {table} set (state, bucket, path, download_time) = (%s,%s,%s,%s) where filename=%s'
        cur.executemany(statement, data)
        logging.error(f'download_files: error {e}')
        raise










