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

    # Get leads per source
    pivot = pd.pivot_table(
        df,
        values='E-mail 1',
        index=['Program Choice 1', 'Latest Campus of Interest'],
        columns=['GA Source'],
        aggfunc=lambda x: len(x.unique()), fill_value=0,
        margins=True).reset_index()

    # Create pivot
    pivot = pivot.sort_values(by=['Latest Campus of Interest',
                                  'Program Choice 1'], ascending=False)

    # Format columns name
    pivot.columns = ['Program Choice', 'Campus', 'Facebook',
                     'Google', "All"]

    # Save result
    pivot.to_csv(
        LOCAL_DIR + campus_name + '_last_week_leads_calculated.csv',
        index=False)


if __name__ == '__main__':
    main()
