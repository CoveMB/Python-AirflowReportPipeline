import pandas as pd

LOCAL_DIR = '/tmp/'


def main(**kwargs):

    # Retrieve account id from Xcom
    ti = kwargs["ti"]
    source = ti.xcom_pull(
        task_ids="report_init_task")

    campus_name = source["campus"]

    # Create dataframe
    df = pd.read_csv(
        LOCAL_DIR + campus_name + '_last_week_leads.csv', index_col=False)

    if len(df.event.unique()) < 2:
        return False


    event.to_csv(
        LOCAL_DIR + campus_name + '_last_week_event_leads.csv',
        index=False)


if __name__ == '__main__':
main()
