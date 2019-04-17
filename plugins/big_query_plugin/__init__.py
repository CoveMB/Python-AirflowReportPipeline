from airflow.plugins_manager import AirflowPlugin
from big_query_plugin.operators.big_query_operator import BigQueryOperatorLoadCSV


class BigQueryPlugin(AirflowPlugin):
    name = "big_query_plugin"
    hooks = []
    operators = [BigQueryOperatorLoadCSV]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
