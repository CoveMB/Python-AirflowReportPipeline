import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.hooks.base_hook import BaseHook

LOCAL_DIR = '/tmp/'


class GoogleHook(BaseHook):

    # Hook to connect with BigQueryHook
    def __init__(self, google_conn_id='big_query_default',
                 type='client', campus_name=None):

        self.google_conn_id = google_conn_id
        self.type = type
        self.campus_name = campus_name

        # Link to the right connection setting to get the right json
        conn = self.get_connection(google_conn_id)

        # Getting Acces ID json from connection
        json_credentials = conn.extra_dejson

        self.project_id = conn.extra_dejson['project_id']

        with open(
                LOCAL_DIR + google_conn_id + '.json', 'w') as fp:
            json.dump(json_credentials, fp)

        if google_conn_id == 'google_analytics_default':
            # Select the right view is connecting for GA
            self.view_id = conn.extra_dejson[campus_name + '_view']

        if self.type == 'client':
            # Setting client if required
            self.client = bigquery.Client.from_service_account_json(
                LOCAL_DIR + google_conn_id + '.json')

        if self.type == 'credentials':
            # Setting credentials if required
            self.credentials = service_account.Credentials.from_service_account_file(
                LOCAL_DIR + google_conn_id + '.json',)

        os.remove(LOCAL_DIR + google_conn_id + '.json')
