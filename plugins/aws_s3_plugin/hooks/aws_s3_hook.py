import boto3
from airflow.hooks.base_hook import BaseHook


class AwsS3Hook(BaseHook):

    # Hook to connect with FacebookAdsHook
    def __init__(self, aws_s3_conn_id='aws_s3_default'):

        # Link to the connection setting
        conn = self.get_connection(aws_s3_conn_id)

        # Getting Acces ID from connection
        self.aws_id = conn.extra_dejson['AWSAccessKeyId']
        self.aws_secret = conn.extra_dejson['AWSSecretKey']

    def get_object(self, bucket_name, object_key):

        # Get S3 client
        client = boto3.client('s3', aws_access_key_id=self.aws_id,
                              aws_secret_access_key=self.aws_secret)

        # Connecting to accounts
        object = client.get_object(Bucket=bucket_name, Key=object_key)

        return object
