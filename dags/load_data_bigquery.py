import os
from google.cloud import bigquery
from airflow.hooks.big_query_plugin import BigQueryHook

LOCAL_DIR = '/tmp/'


def main(**kwargs):

    # Retrieve acampus from Xcom
    ti = kwargs["ti"]
    source = ti.xcom_pull(
        task_ids="report_init_task")

    campus_name = source["campus"]

    # Select the right dataset and table
    dataset_id = "FacebookAirflowPipelineWeekly"
    table_id = campus_name + "FacebookData"

    # Establish connection
    client = BigQueryHook()

    client = bigquery.Client.from_service_account_json(
        LOCAL_DIR + 'big_query.json')

    os.remove(LOCAL_DIR + 'big_query.json')

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()

    # The source format is CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV

    # Config how the data will be inserted into Big Query
    job_config.skip_leading_rows = 1
    job_config.autodetect = True
    job_config.write_disposition = "WRITE_TRUNCATE"

    # Start writting into BigQuery
    with open(
            LOCAL_DIR + campus_name + '_facebook_data_cleaned.csv',
            'rb') as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location='US',  # Must match the destination dataset location.
            job_config=job_config)  # API request

    job.result()  # Waits for table load to complete.

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_id, table_id))


if __name__ == '__main___':
    main()
