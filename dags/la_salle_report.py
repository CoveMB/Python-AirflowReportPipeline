# load the dependencies
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import timedelta, datetime

import fetching_facebook_data as fetching
import cleanning_facebook_data as cleanning
import load_data_bigquery as loading
import calculating_facebook_data as calculating_facebook
import query_bq_google_ads as quering
import calculating_google_ads_network as calculating_network
import calculating_google_ads_search as calculating_search
import getting_leads_data_local as getting_leads
import calculating_last_week_leads as calculating_leads
import report_email as email


# Init workflow passing campus name via xcoms
def report_init():
    campus = {
        "campus": "LaSalle",
        "image": "https://www.lasallecollege.com/~/media/images/responsive/collegelasalle_montreal/home/visuel-page-accueil-2024x777_v3_en.ashx"
    }

    return campus


DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('la_salle_report', start_date=datetime.today(), schedule_interval="@daily", default_args=DAG_DEFAULT_ARGS, catchup=False) as dag:
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

    load_data_bigquery_task = PythonOperator(
        task_id="load_data_bigquery_task",
        python_callable=loading.main,
        provide_context=True)

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

    send_report_email_task = PythonOperator(
        task_id="send_report_email_task",
        python_callable=email.main,
        provide_context=True)

    delete_xcom_task = PostgresOperator(
        task_id='delete_xcom_task',
        postgres_conn_id='airflow_db',
        sql="delete from xcom where dag_id='la_salle_report' and task_id='report_init_task'",
        dag=dag)

    report_init_task >> fetching_facebook_data_task
    report_init_task >> query_bq_google_ads_task
    report_init_task >> getting_leads_data_task

    fetching_facebook_data_task >> cleanning_facebook_data_task >> load_data_bigquery_task >> calculatinfg_facebook_data_for_report_task >> send_report_email_task

    query_bq_google_ads_task >> calculating_google_ads_network_task >> send_report_email_task
    query_bq_google_ads_task >> calculating_google_ads_search_task >> send_report_email_task

    getting_leads_data_task >> calculating_leads_last_week_task >> send_report_email_task

    send_report_email_task >> delete_xcom_task
