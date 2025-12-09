import psycopg
from typing import Dict, Any, Tuple, Union, List
import logging


class Database():

    def __init__(self, **kwargs):
        try:
            schema = kwargs['db_schema']
            del kwargs['db_schema']
            self.conn = psycopg.connect(**kwargs, options=f'-c search_path={schema}')
        except psycopg.Error as e:
            logging.error(f'DB connection failed: {e}')
            raise

    def execute_sql(self, statement: str, data: Union[Dict[Any, Any], Tuple[Any], List[Any]] = None):
        cur = self.conn.cursor()
        with self.conn.transaction():
            try:
                cur.execute(statement, data)
                result = cur.fetchall()
                return result
            except psycopg.Error as e:
                logging.error(f'DB execute failed: {e}')
        self.conn.commit()

    def execute_many(self, statement: str, data: Union[Dict[Any, Any], Tuple[Any], List[Any]] = None):
        cur = self.conn.cursor()
        with self.conn.transaction():
            try:
                cur.executemany(statement, data, returning=True)
                result = []
                while True:
                    result.append(cur.fetchone())
                    if not cur.nextset():
                        break
                return result
            except psycopg.Error as e:
                logging.error(f'DB execute failed: {e}')
                raise
        self.conn.commit()

