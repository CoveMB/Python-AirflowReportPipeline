from airflow.plugins_manager import AirflowPlugin
from google_analytics_plugin.hooks.google_analytics_hook import GoogleAnalyticsHook


class GoogleAnalyticsPlugin(AirflowPlugin):
    name = "google_analytics_plugin"
    hooks = [GoogleAnalyticsHook]
    operators = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
