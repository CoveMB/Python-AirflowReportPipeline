from airflow.hooks.base_hook import BaseHook
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount


class FacebookAdsHook(BaseHook):

    # Hook to connect with FacebookAdsHook
    def __init__(self,
                 facebook_ads_conn_id='facebook_ads_default',
                 campus_name=None):

        # Link to the connection setting
        conn = self.get_connection(facebook_ads_conn_id)

        # Getting Acces ID from connection
        my_app_id = conn.extra_dejson['app_id']
        my_app_secret = conn.extra_dejson['fb_secret']
        my_access_token = conn.extra_dejson['access_token']
        self.campus_id = conn.extra_dejson[campus_name + '_account']

        # Initialitiong connection with Facebook
        FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

    def get_campus_insights(self, params, fields):

        # Connecting to accounts
        campus_account = AdAccount(self.campus_id)

        campus_insights = campus_account.get_insights(
            params=params,
            fields=fields)

        return campus_insights
