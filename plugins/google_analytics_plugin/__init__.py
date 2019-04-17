from airflow.plugins_manager import AirflowPlugin
from google_analytics_plugin.operators.google_analytics_operator import GAOperator


class GoogleAnalyticsPlugin(AirflowPlugin):
    name = "google_analytics_plugin"
    hooks = []
    operators = [GAOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
