import awswrangler as wr
import pandas as pd
import paramiko
from airflow.models import Variable


def get_cj_conversions_from_sftp():
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # Get the connection details from Airflow Variables
    username = Variable.get("cj_user_sftp")
    password = Variable.get("cj_password_sftp")
    endpoint = Variable.get("cj_endpoint_sftp")
    ssh_client.connect(endpoint,
                       port=22,
                       username=username,
                       password=password)
    sftp = ssh_client.open_sftp()

    outgoing_directory = '/outgoing/'  # Specify the path to the "outgoing" directory

    files = sftp.listdir_attr(outgoing_directory)

    files.sort(key=lambda f: f.filename)

    # Get the last file after sorting
    if files:
        last_file = files[-1].filename
        print("Last file:", last_file)

        with sftp.open(outgoing_directory + last_file, 'r') as file:
            df = pd.read_csv(file, usecols=['Posting_Date', 'Event_DateXXX', 'Order_ID',
                                            'Publisher_Name', 'Website_Name'], index_col=False)
            print(df)  # Display the DataFrame

    else:
        print("No files found in the directory")

    if not df.empty:
        wr.s3.to_csv(
            df=df,
            path='s3://grover-eu-central-1-production-data-bi-curated/affiliate' +
            '/cj/fetch/conversions.csv',
            sep=';',
            columns=[
                'Posting_Date', 'Event_DateXXX', 'Order_ID', 'Publisher_Name', 'Website_Name'
            ],
            index=False)
    else:
        print("The Conversions Dataframe is empty!")

    sftp.close()
    ssh_client.close()
