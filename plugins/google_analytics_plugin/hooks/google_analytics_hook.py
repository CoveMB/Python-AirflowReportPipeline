import os
import json
import argparse

from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from airflow.hooks.base_hook import BaseHook

LOCAL_DIR = '/tmp/'


class GoogleAnalyticsHook(BaseHook):

    # Hook to connect with BigQueryHook
    def __init__(self, google_analytics_conn_id='google_analytics_default', campus_name=None):

        # Link to the connection setting
        conn = self.get_connection(google_analytics_conn_id)

        self.VIEW_ID = conn.extra_dejson[campus_name + '_account'

        SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

        DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')

        # Parse command-line arguments
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            parents=[tools.argparser])

        flags = parser.parse_args([])

        # Getting Acces ID from connection
        credentials = conn.extra_dejson

        with open(LOCAL_DIR + 'google_analytics.json', 'w') as fp:
            json.dump(credentials, fp)
