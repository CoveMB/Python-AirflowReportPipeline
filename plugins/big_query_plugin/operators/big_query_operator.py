from google.cloud import bigquery
from big_query_plugin.hooks.big_query_hook import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

LOCAL_DIR = '/tmp/'


class BigQueryOperatorLoadCSV(BaseOperator):
    # Define load to csv BigQueryOperator
    @apply_defaults
    def __init__(self,
                 dataset_id=None,
                 table_id=None,
                 autodetect=True,
                 write_disposition='WRITE_EMPTY',
                 big_query_conn_id='big_query_default',
                 file_name=None,
                 skip_leading_rows=1,
                 directory='data/',
                 * args,
                 **kwargs):
        super(BigQueryOperatorLoadCSV, self).__init__(*args, **kwargs)
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.autodetect = autodetect
        self.write_disposition = write_disposition
        self.big_query_conn_id = big_query_conn_id
        self.file_name = file_name
        self.skip_leading_rows = skip_leading_rows
        self.directory = directory

    # Do the thing
    def execute(self, context):

        # Create client
        hook = BigQueryHook(type='client')

        dataset_ref = hook.client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_id)
        job_config = bigquery.LoadJobConfig()

        # The source format is CSV, but just in case
        job_config.source_format = bigquery.SourceFormat.CSV

        # Config how the data will be inserted into Big Query
        job_config.skip_leading_rows = self.skip_leading_rows
        job_config.autodetect = self.autodetect
        job_config.write_disposition = self.write_disposition

        # Start writting into BigQuery
        with open(
                self.directory + self.file_name + '.csv',
                'rb') as source_file:
            job = hook.client.load_table_from_file(
                source_file,
                table_ref,
                location='US',  # Must match the destination dataset location.
                job_config=job_config)  # API request

        job.result()  # Waits for table load to complete.

        print('Loaded {} rows into {}:{}.'.format(
            job.output_rows, self.dataset_id, self.table_id))
