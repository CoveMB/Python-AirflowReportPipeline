from airflow.plugins_manager import AirflowPlugin
from aws_s3_plugin.hooks.aws_s3_hook import AwsS3Hook


class AwsS3Plugin(AirflowPlugin):
    name = "aws_s3_plugin"
    hooks = [AwsS3Hook]
    operators = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
