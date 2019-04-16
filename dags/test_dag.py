# load the dependencies
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

import test_script as test


DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('test_dag', start_date=datetime.today(), schedule_interval="@daily", default_args=DAG_DEFAULT_ARGS, catchup=False) as dag:

    test_task = PythonOperator(
        task_id="test_task",
        python_callable=test.main,
        provide_context=True)
