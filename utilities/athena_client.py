import pandas as pd
from configparser import ConfigParser
import os
import pyathena
import multiprocessing as mp
import hashlib

class athena_API():
    def _get_connection_keys(self):
        config = ConfigParser()
        config.read(os.path.expanduser('~/.dpcfg.ini'))
        return dict(
                aws_access_key_id = config.get('athena','AccessKeyId'),
                aws_secret_access_key = config.get('athena','SecretAccessKey'),
                s3_staging_dir = 's3://aws-athena-query-results-724810233589-us-east-1',
                region_name = 'us-east-1'
                )

    def _get_db_connection(self):
        keys = self._get_connection_keys()
        return pyathena.connect(**keys)

    def get_pandas_df(self, query):
        connection = self._get_db_connection()   
        return pd.read_sql(query, connection)

    def get_SQL_query(self):
        # Retrieve SQL query
        with open('main_query.sql', 'r') as f:
            sql_query = f.read()
        return sql_query
