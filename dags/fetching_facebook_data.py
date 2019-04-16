import pandas as pd
from airflow.hooks.facebook_ads_plugin import FacebookAdsHook

LOCAL_DIR = '/tmp/'


def main(**kwargs):

    # Seting-up the resquest
    # Retrieve account id from Xcom
    ti = kwargs["ti"]
    source = ti.xcom_pull(
        task_ids="report_init_task")

    campus_name = source["campus"]

    # Initiate connection
    campus = FacebookAdsHook(campus_name=campus_name)

    # Seting-up the params
    params = {
        'date_preset': "last_week_mon_sun",
        "level": "campaign",
        'time_increment': "1"
    }

    fields = ["campaign_name", "reach", "clicks", "spend"]

    # Requesting insights
    campus_insights = campus.get_campus_insights(
        params,
        fields)

    # Transform insights into a list of dict
    campus_insights = [dict(x) for x in campus_insights]

    # Write insights into file to export data

    # Create data frame
    df = pd.DataFrame.from_dict(campus_insights)

    # Format time
    df = df.assign(Time=pd.to_datetime(df.date_stop))

    # Remove unecessary columns
    df = df.drop(
        'date_start', axis='columns').drop('date_stop', axis='columns')

    # Save data into csv file
    df.to_csv(LOCAL_DIR + campus_name + '_facebook_data.csv', index=False)


if __name__ == '__main___':
    main()
