import os
import pandas as pd

LOCAL_DIR = '/tmp/'


def main(**kwargs):

    # Retrieve acampus from Xcom
    ti = kwargs["ti"]
    source = ti.xcom_pull(
        task_ids="report_init_task")

    campus_name = source["campus"]

    # Read data file to create a data frame
    df = pd.read_csv(LOCAL_DIR + campus_name + '_facebook_data.csv')

    # Identify schools and boost
    if campus_name == "LaSalle":
        df['campaign_name'] = df['campaign_name'].str.replace(
            "CLM", "LaSalle").str.replace(
                "CL", "Boost").str.replace(
                    "eLearning ", "eLearning")

    elif campus_name == "InterDec":
        df['campaign_name'] = df['campaign_name'].str.replace(
            "CID", "InterDec")

    else:
        df['campaign_name'] = campus_name + "-" + df['campaign_name'].astype(
            str)

    # Create school column
    df['school'] = df["campaign_name"].str.split("-", n=1, expand=True)[0]
    df['school'] = df['school'].str.replace(" ", "")

    # Intermediate state to get a clean campaign column
    df["campaign_rename"] = df["campaign_name"].str.split(
        "-", n=1,
        expand=True)[1]

    if campus_name == "LaSalle" or campus_name == "InterDec":
        # Create PO column
        df["po"] = df["campaign_rename"].str.rsplit("-", n=1, expand=True)[1]
        df["po"] = df["po"].str.replace(" ", "")

    # Create a clean campaign column
    df["campaign"] = df["campaign_rename"].str.rsplit("-", n=1, expand=True)[0]

    # Get rid of dirty columns
    df = df.drop(
        'campaign_name', axis='columns').drop(
        'campaign_rename', axis='columns')

    # Reorder columns
    if campus_name == "LaSalle" or campus_name == "InterDec":
        clean = df[["campaign",
                    "school",
                    "po",
                    "Time",
                    "spend",
                    "reach",
                    "clicks"]]
    else:
        clean = df[["campaign",
                    "school",
                    "Time",
                    "spend",
                    "reach",
                    "clicks"]]

    # Save in new file
    clean.to_csv(LOCAL_DIR + campus_name + '_facebook_data_cleaned.csv',
                 header=True, index=False, index_label=False)

    # os.remove(LOCAL_DIR + campus_name + '_facebook_data.csv')


if __name__ == '__main__':
    main()
