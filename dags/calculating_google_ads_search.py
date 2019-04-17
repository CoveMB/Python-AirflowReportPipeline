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

    # Seaparate search campaigns
    search_result = df.loc[df['AdNetworkType1'] == "Search Network"]

    # Replace nampagne name per brand or not brand
    search_result.CampaignName = search_result.CampaignName.apply(
        lambda x: '| Brand Search' if 'brand' in x else "| Not Brand")

    # Create a pivot table to get search cost breakdown
    pivot = pd.pivot_table(
        search_result,
        values='Cost',
        index=['school'],
        columns=['CampaignName'],
        aggfunc=np.sum, fill_value=0, margins=True).reset_index()

    # Drop unecessary columns
    pivot = pivot[pivot.school != 'All']

    pivot = pivot.drop('All', axis='columns')

    # Format
    pivot['| Brand Search'] = '| ' + pivot['| Brand Search'].astype(str) + "$"
    pivot['| Not Brand'] = '| ' + pivot['| Not Brand'].astype(str) + "$"

    pivot.to_csv(LOCAL_DIR + campus_name + '_google_spent_search.csv',
                 header=True, index=False, index_label=False)


if __name__ == '__main__':
    main()
