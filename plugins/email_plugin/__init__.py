from airflow.plugins_manager import AirflowPlugin
from email_plugin.hooks.email_hook import EmailHook


class EmailPlugin(AirflowPlugin):
    name = "email_plugin"
    hooks = [EmailHook]
    operators = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
