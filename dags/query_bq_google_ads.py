
from google.oauth2 import service_account
import pandas_gbq as pd_gbq
import pandas as pd
from airflow.hooks.big_query_plugin import BigQueryHook

LOCAL_DIR = '/tmp/'


def main(**kwargs):

    # Retrieve acampus from Xcom
    ti = kwargs["ti"]
    source = ti.xcom_pull(
        task_ids="report_init_task")

    campus_name = source["campus"]

    # Connect to Big Query
    client = BigQueryHook()

    credentials = service_account.Credentials.from_service_account_file(
        LOCAL_DIR + 'big_query.json',)

    bq_project_id = client.project_id

    # SQL query
    sql = (
        "select * from GA" + campus_name +
        "CampaignReport.CAMPAIGN_PERFORMANCE_REPORT WHERE Date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 WEEK) AND Date <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(SUNDAY)), INTERVAL 0 WEEK);"
    )

    # Make it a pandas dataframe
    df = pd_gbq.read_gbq(sql,
                         project_id=bq_project_id,
                         credentials=credentials)

    # Create a school column
    df["school"] = campus_name

    # If campus is LaSalle query big query again to get eLearning data
    if campus_name == "LaSalle":
        sql2 = (
            "select * FROM GAeLearningCampaignReport.CAMPAIGN_PERFORMANCE_REPORT WHERE Date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 WEEK) AND Date <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(SUNDAY)), INTERVAL 0 WEEK);"
        )

        # Make it a pandas dataframe
        df2 = pd_gbq.read_gbq(sql2,
                              project_id=bq_project_id,
                              credentials=credentials)

        # Create a school column
        df2["school"] = "Elearning"

        # Join the 2 results
        frames = [df, df2]

        df = pd.concat(frames)

    # Save the result
    df.to_csv(LOCAL_DIR + campus_name + '_google_ads_data_cleaned.csv',
              index=False)


if __name__ == '__main___':
    main()
