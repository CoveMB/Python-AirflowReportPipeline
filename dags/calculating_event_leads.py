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

    # Determine if there is an event
    if len(df.event.unique()) < 2:

        # If not return no event status
        event = {
            "event": "false"
            }

        return event

    # If yes create pivot table to get number of registered
    event = pd.pivot_table(
        df,
        values='E-mail 1',
        index=['event'],
        columns=['GA Source'],
        aggfunc=lambda x: len(
                x.unique()), fill_value=0, margins=True).reset_index()

    event = event[event.event != 'All']

    event = event.drop('All', axis='columns')

    # Save it
    event.to_csv(
        LOCAL_DIR + campus_name + '_last_week_event_leads.csv',
        index=False)

    # return event status
    event = {
        "event": "true"
        }

    return event


if __name__ == '__main__':
    main()
