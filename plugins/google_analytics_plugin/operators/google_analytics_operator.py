import json
import argparse
from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from airflow.models import BaseOperator
from google_plugin.hooks.google_hook import GoogleHook

LOCAL_DIR = '/tmp/'


class GAOperator(BaseOperator):

    # Hook to connect with BigQueryHook
    def __init__(self,
                 google_conn_id='google_analytics_default',
                 campus_name=None):

        self. google_conn_id = google_conn_id
        self.campus_name = campus_name
        # Get credentials
        hook = GoogleHook(type='credentials',
                                 google_conn_id=self. google_conn_id)

        credentials = hook.credentials

        VIEW_ID = hook.view_id

        SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

        DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')

        # Parse command-line arguments
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            parents=[tools.argparser])

        flags = parser.parse_args([])
