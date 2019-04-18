# load the dependencies
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.big_query_plugin import BigQueryOperatorLoadCSV
from datetime import timedelta, datetime

import fetching_facebook_data as fetching
import cleanning_facebook_data as cleanning
import calculating_facebook_data as calculating_facebook
import query_bq_google_ads as quering
import calculating_google_ads_network as calculating_network
import calculating_google_ads_search as calculating_search
import getting_leads_data_local as getting_leads
import calculating_last_week_leads as calculating_leads
import calculating_event_leads as calculating_event
import report_email as email

campus_name = "InterDec"
LOCAL_DIR = '/tmp/'


# Init workflow passing campus name via xcoms
def report_init():
    campus = {
        "campus": "InterDec",
        "image": "https://www.collegeinterdec.com/~/media/images/responsive/interdec/homepage/visuel-page-accueil-2024x777-a.ashx"
    }

    return campus


DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('inter_dec_report', start_date=datetime.today(), schedule_interval="@daily", default_args=DAG_DEFAULT_ARGS, catchup=False) as dag:
    report_init_task = PythonOperator(
        task_id="report_init_task", python_callable=report_init)

    fetching_facebook_data_task = PythonOperator(
        task_id="fetching_facebook_data_task",
        python_callable=fetching.main,
        provide_context=True)

    cleanning_facebook_data_task = PythonOperator(
        task_id="cleanning_facebook_data_task",
        python_callable=cleanning.main,
        provide_context=True)

    load_data_bigquery_task = BigQueryOperatorLoadCSV(
        task_id="load_data_bigquery_task",
        dataset_id="FacebookAirflowPipelineWeekly",
        table_id=campus_name + "FacebookData",
        write_disposition='WRITE_TRUNCATE',
        file_name=campus_name + '_facebook_data_cleaned',
        directory=LOCAL_DIR,
        )

    calculatinfg_facebook_data_for_report_task = PythonOperator(
        task_id="calculatinfg_facebook_data_for_report_task",
        python_callable=calculating_facebook.main,
        provide_context=True)

    query_bq_google_ads_task = PythonOperator(
        task_id="query_bq_google_ads_task",
        python_callable=quering.main,
        provide_context=True)

    calculating_google_ads_network_task = PythonOperator(
        task_id="calculating_google_ads_network_task",
        python_callable=calculating_network.main,
        provide_context=True)

    calculating_google_ads_search_task = PythonOperator(
        task_id="calculating_google_ads_search_task",
        python_callable=calculating_search.main,
        provide_context=True)

    getting_leads_data_task = PythonOperator(
        task_id="getting_leads_data_task",
        python_callable=getting_leads.main,
        provide_context=True)

    calculating_leads_last_week_task = PythonOperator(
        task_id="calculating_leads_last_week_task",
        python_callable=calculating_leads.main,
        provide_context=True)

    calculating_event_leads_task = PythonOperator(
        task_id="calculating_event_leads_task",
        python_callable=calculating_event.main,
        provide_context=True)

    send_report_email_task = PythonOperator(
        task_id="send_report_email_task",
        python_callable=email.main,
        provide_context=True)

    delete_xcom_task = PostgresOperator(
        task_id='delete_xcom_task',
        postgres_conn_id='airflow_db',
        sql="delete from xcom where dag_id='inter_dec_report' and task_id='report_init_task'",
        dag=dag)

    report_init_task >> fetching_facebook_data_task
    report_init_task >> query_bq_google_ads_task
    report_init_task >> getting_leads_data_task

    fetching_facebook_data_task >> cleanning_facebook_data_task >> load_data_bigquery_task >> calculatinfg_facebook_data_for_report_task >> send_report_email_task

    query_bq_google_ads_task >> calculating_google_ads_network_task >> send_report_email_task
    query_bq_google_ads_task >> calculating_google_ads_search_task >> send_report_email_task

    getting_leads_data_task >> calculating_leads_last_week_task >> calculating_event_leads_task >> send_report_email_task

    send_report_email_task >> delete_xcom_task
