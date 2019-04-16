import pandas as pd

LOCAL_DIR = '/tmp/'
DATA_DIR = 'data/'


def main(**kwargs):

    # Retrieve account id from Xcom
    ti = kwargs["ti"]
    source = ti.xcom_pull(
        task_ids="report_init_task")

    campus_name = source["campus"]

    # Make it a dataframe
    df = pd.read_csv(DATA_DIR + 'last_week_mtl_leads.csv')

    # Isolating campuses if multiple campus in one report
    if campus_name == "LaSalle" or campus_name == "InterDec":
        if campus_name == "LaSalle":
            # Format schools
            df["Latest Campus of Interest"] = df["Latest Campus of Interest"].str.replace(
                "LaSalle College", "CL Montreal").str.replace(
                    "LaSalle College Laval", "CL Laval")

            campus_name = ["CL Montreal", "CL Laval", "E-learning"]

        elif campus_name == "InterDec":
            # Format schools
            df["Latest Campus of Interest"] = df["Latest Campus of Interest"].str.replace(
                "Inter-Dec College Montréal", "CID Montreal").str.replace(
                    "Inter-Dec College Laval", "CID Laval")

            campus_name = ["CID Montreal", "CID Laval"]

        # Isoalating paid media source
        leads = df.loc[df['Latest Campus of Interest'].isin(campus_name)]

    # Isolate campus's leads
    else:
        leads = df.loc[df['Latest Campus of Interest'] == campus_name]

    # Isoalating paid media source
    leads = leads.loc[leads['GA Source'].isin(['facebook', 'google'])]

    # Original campus name
    campus_name = source["campus"]

    # Save it
    leads.to_csv(LOCAL_DIR + campus_name + '_last_week_leads.csv', index=False)


if __name__ == '__main__':
    main()
