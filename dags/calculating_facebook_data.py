import os
import pandas as pd
import numpy as np

LOCAL_DIR = '/tmp/'


def main(**kwargs):

    # Retrieve account name from Xcom
    ti = kwargs["ti"]
    source = ti.xcom_pull(
        task_ids="report_init_task")

    campus_name = source["campus"]

    # Get some metrics
    # Create new dataframe
    df = pd.read_csv(LOCAL_DIR + campus_name + '_facebook_data_cleaned.csv')

    # Make sure spend is the right data type
    df.spend = df.spend.astype(int)

    # Create pivot table to extract some KPI
    pivot = pd.pivot_table(
        df, values='spend', index=['school'],
        aggfunc=np.sum, fill_value=0,).reset_index()

    # Format the spend column
    pivot['spend'] = ": " + pivot['spend'].astype(str) + " $"

    pivot.to_csv(LOCAL_DIR + campus_name + '_facebook_data_calculated.csv',
                 header=True, index=False, index_label=False)

    # Calculate spent for Mtl campus
    if campus_name == "InterDec" or campus_name == "LaSalle":
        #
        # # Create separator
        # separator = pd.DataFrame({
        #     "school": ["Last week spent per PO"],
        #     "spend": ["---------"]})

        # Create a new pivot table for per PO breakdown
        po_pivot = pd.pivot_table(df, values='spend',
                                  index=['po'],
                                  aggfunc=np.sum, fill_value=0).reset_index()

        po_pivot.columns = ["school", "spend"]

        # Format it
        po_pivot['spend'] = ": " + po_pivot['spend'].astype(str) + " $"

        # Create final output
        # spent = pivot.append(separator, ignore_index=True).append(
        #     po_pivot, ignore_index=True).reset_index()

        # spent = spent.drop(columns=['index'])

        # Save the result
        po_pivot.to_csv(LOCAL_DIR + campus_name +
                        '_facebook_data_po_calculated.csv',
                        header=True, index=False, index_label=False)

    os.remove(LOCAL_DIR + campus_name + '_facebook_data_cleaned.csv')


if __name__ == '__main__':
    main()
