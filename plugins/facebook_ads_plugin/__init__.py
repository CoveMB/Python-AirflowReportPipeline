from airflow.plugins_manager import AirflowPlugin
from facebook_ads_plugin.hooks.facebook_ads_hook import FacebookAdsHook


class FacebookAdsPlugin(AirflowPlugin):
    name = "facebook_ads_plugin"
    hooks = [FacebookAdsHook]
    operators = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
