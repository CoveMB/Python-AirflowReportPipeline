import pandas as pd
import numpy as np

LOCAL_DIR = '/tmp/'


def main(**kwargs):

    # Retrieve acampus from Xcom
    ti = kwargs["ti"]
    source = ti.xcom_pull(
        task_ids="report_init_task")

    campus_name = source["campus"]

    # Read data file to create a data frame
    df = pd.read_csv(LOCAL_DIR + campus_name + '_google_ads_data_cleaned.csv')

    # Make sure Cost is the right data type
    df.Cost = df.Cost.astype(int)

    # Create a pivo table depending of network type per school
    pivot = pd.pivot_table(
        df, values='Cost', index=['school'], columns=['AdNetworkType1'],
        aggfunc=np.sum, fill_value=0, margins=True).reset_index()

    # Create network array to loop throught
    networks = df["AdNetworkType1"].unique()

    # Format
    for i in networks:
        pivot[i] = pivot[i].astype(str) + " $"

    pivot['All'] = pivot['All'].astype(str) + " $"

    pivot = pivot[pivot.school != 'All']

    # Drop columns containing no data
    pivot = pivot.loc[:, (pivot != '0 $').any(axis=0)]

    # Save in new file
    pivot.to_csv(LOCAL_DIR + campus_name + '_google_spent_per_network.csv',
                 header=True, index=False, index_label=False)


if __name__ == '__main__':
    main()
