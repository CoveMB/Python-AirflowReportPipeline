import pandas as pd
from airflow.hooks.aws_s3_plugin import AwsS3Hook

LOCAL_DIR = '/tmp/'


def main(**kwargs):

    # Retrieve account id from Xcom
    ti = kwargs["ti"]
    source = ti.xcom_pull(
        task_ids="report_init_task")

    campus_name = source["campus"]

    # Establish S3 connection
    s3 = AwsS3Hook()

    # Get object from S3
    object = s3.get_object(object_key='last_week_leads.csv',
                           bucket_name='lcieducationleads')

    # Make it a dataframe
    df = pd.read_csv(object['Body'])

    # Isolating campuses if multiple campus in one report
    if campus_name == "LaSalle" or campus_name == "InterDec":
        if campus_name == "LaSalle":
            campus_name = ["LaSalle College", "LaSalle College Laval"]

        elif campus_name == "InterDec":
            campus_name = ["Inter-Dec College Montréal",
                           "Inter-Dec College Laval"]

        # Isoalating paid media source
        leads = df.loc[df['Latest Campus of Interest'].isin(campus_name)]

    # Isolate campus's leads
    else:
        leads = df.loc[df['Latest Campus of Interest'] == campus_name]

    # Isoalating paid media source
    leads = leads.loc[leads['GA Source'].isin(['facebook', 'google'])]

    # Original campus name
    campus_name = source["campus"]

    leads.to_csv(LOCAL_DIR + campus_name + '_last_week_leads.csv', index=False)


if __name__ == '__main__':
    main()
