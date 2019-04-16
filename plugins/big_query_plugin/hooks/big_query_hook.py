import json
from airflow.hooks.base_hook import BaseHook

LOCAL_DIR = '/tmp/'


class BigQueryHook(BaseHook):

    # Hook to connect with BigQueryHook
    def __init__(self, big_query_conn_id='big_query_default'):

        # Link to the connection setting
        conn = self.get_connection(big_query_conn_id)

        # Getting Acces ID from connection
        credentials = conn.extra_dejson

        self.project_id = conn.extra_dejson['project_id']

        with open(LOCAL_DIR + 'big_query.json', 'w') as fp:
            json.dump(credentials, fp)
