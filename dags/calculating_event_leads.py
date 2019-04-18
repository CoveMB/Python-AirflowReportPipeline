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

    event = pd.pivot_table(
        df,
        values='E-mail 1',
        index=['event'],
        columns=['GA Source'],
        aggfunc=lambda x: len(
                x.unique()), fill_value=0, margins=True).reset_index()

    event = event[event.event != 'All']

    event = event.drop('All', axis='columns')

    event.to_csv(
        LOCAL_DIR + campus_name + '_last_week_event_leads.csv',
        index=False)

    return True


if __name__ == '__main__':
    main()
