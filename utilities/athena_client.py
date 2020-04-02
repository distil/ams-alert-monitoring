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
    
    # provided a dataframe coming from Google Sheet with the correct fields
    # Create a list of all queries using those fields
    def _tw_query(self, row):
        row_dict = row[1]
        return f'''SELECT COUNT(*)
                    FROM bon_log_prod.access
                    WHERE account_id = '{row_dict['account_id']}'
                      and site_id = '{row_dict['site_id']}'
                      and access_time > date_add('minute', - 60, NOW())  -- 60*24*7 + 60
                      and ds >= CAST(DATE(date_add('day', -1, NOW())) as VARCHAR) -- efficency, reduces the search scope
                      and regexp_like(lower(request_path), '{row_dict['url']}') = true
                      {row_dict['and_condition']}
                      '''
    def _lw_query(self, row):
        row_dict = row[1]
        return f'''SELECT COUNT(*)
                    FROM bon_log_prod.access
                    WHERE account_id = '{row_dict['account_id']}'
                      and site_id = '{row_dict['site_id']}'
                      and access_time > date_add('minute', - 10140, NOW())  -- 60*24*7 + 60
                      and access_time <= date_add('minute', - 10080, NOW()) -- 60*24*7
                      and ds >= CAST(DATE(date_add('day', -8, NOW())) as VARCHAR) -- efficency, reduces the search scope
                      and ds <= CAST(DATE(date_add('day', -6, NOW())) as VARCHAR) -- efficency, reduces the search scope
                      and regexp_like(lower(request_path), '{row_dict['url']}') = true
                      {row_dict['and_condition']}
                      '''

    def add_row_queries(self, gsheet_df):
        gsheet_df['tw_query'] = [self._tw_query(row) for row in gsheet_df.iterrows()]
        gsheet_df['lw_query'] = [self._lw_query(row) for row in gsheet_df.iterrows()]
        return gsheet_df
