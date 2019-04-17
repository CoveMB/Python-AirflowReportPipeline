from airflow.plugins_manager import AirflowPlugin
from google_plugin.hooks.google_hook import GoogleHook


class GooglePlugin(AirflowPlugin):
    name = "google_plugin"
    hooks = [GoogleHook]
    operators = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
