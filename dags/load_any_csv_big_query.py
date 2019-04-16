# load the dependencies
from airflow import DAG
from airflow.operators.big_query_plugin import BigQueryOperatorLoadCSV
from datetime import timedelta, datetime


DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('load_any_csv_big_query',
         start_date=datetime.today(),
         schedule_interval="@daily",
         default_args=DAG_DEFAULT_ARGS,
         catchup=False) as dag:

    load_any_csv_big_query_task = BigQueryOperatorLoadCSV(
        task_id="load_any_csv_big_query_task",
        dataset_id='LastWeekLeads',
        table_id="LastWeekMtlLeads",
        write_disposition='WRITE_TRUNCATE',
        file_name='last_week_mtl_leads',
    )
