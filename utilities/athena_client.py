import pandas as pd
from configparser import ConfigParser
import os
import pyathena
import multiprocessing as mp
import hashlib

class athena_API():
    def _get_connection_keys(self, region_name):
        config = ConfigParser()
        config.read(os.path.expanduser('~/.dpcfg.ini'))
        return dict(
                aws_access_key_id = config.get('athena','AccessKeyId'),
                aws_secret_access_key = config.get('athena','SecretAccessKey'),
                s3_staging_dir = f's3://aws-athena-query-results-724810233589-{region_name}',
                region_name = region_name
                )

    def _get_db_connection(self, region_name):
        keys = self._get_connection_keys(region_name)
        return pyathena.connect(**keys)

    def get_pandas_df(self, query, region_name='us-east-1'):
        connection = self._get_db_connection(region_name)   
        return pd.read_sql(query, connection)

    def get_SQL_query(self):
        # Retrieve SQL query
        with open('main_query.sql', 'r') as f:
            sql_query = f.read()
        return sql_query
