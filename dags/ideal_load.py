from airflow.operators.big_query_plugin import BigQueryOperatorLoadCSV

LOCAL_DIR = '/tmp/'


def main(**kwargs):

    # Retrieve acampus from Xcom
    ti = kwargs["ti"]
    source = ti.xcom_pull(
        task_ids="report_init_task")

    campus_name = source["campus"]

    # Use Big Query opérator
    BigQueryOperatorLoadCSV(
        task_id="data_loading",
        dataset_id="FacebookAirflowPipelineWeekly",
        table_id=campus_name + "FacebookData",
        write_disposition='WRITE_TRUNCATE',
        file_name=campus_name + '_facebook_data_cleaned',
        directory=LOCAL_DIR,
    )


if __name__ == '__main___':
    main()
