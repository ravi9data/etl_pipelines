from airflow.models import Variable

from plugins.utils.gsheet_utils import get_dataframe, pull_sheet_data_links
from plugins.utils.s3_utils import current_path, write_csv_file_s3


def extract_links(conn_id, spreadsheet_id, data_to_pull, info):
    """
    This function is used to extract the hyperlinks from the main file
    """
    links = pull_sheet_data_links(conn_id, spreadsheet_id, data_to_pull, info)
    reduced_data = []
    for link in links:
        link_data = [link["userEnteredValue"]["stringValue"], link["hyperlink"]]
        if "Report" in link_data:
            continue
        if "Link" in link_data:
            continue
        reduced_data.append(link_data)
    return reduced_data


def load_campaign_data(
    conn_id, bucket_name, campaign_data_path, data_to_pull, info
):
    """
    This function is used to load the campaign data
    """
    spreadsheet_id = Variable.get("price_elasticity_sheet_id")
    reduced_data = extract_links(conn_id, spreadsheet_id, data_to_pull, info)
    dataframes = {}

    for d in reduced_data:
        sheet_id = d[1].split("/d/")[1].split("/")[0]
        try:
            dataframes[d[0]] = get_dataframe(
                conn_id, sheet_id, "Product List!A2:N", d[0]
            )
        except Exception as ex:
            print(
                "Cannot access this {sheet_id} {sheet_name} due to {reason}".format(
                    sheet_id=sheet_id, sheet_name=d[0], reason=str(ex)
                )
            )

    for df in dataframes:
        campaign_name = df.replace(" ", "_")
        campaign_name = campaign_name.replace("/", "_")
        campaign_data_full_path = (
            f"s3://{bucket_name}{campaign_data_path}".format(
                bucket_name=bucket_name, campaign_data_path=campaign_data_path
            )
            + campaign_name
            + ".csv"
        )
        current_campaign_data_full_path = current_path(campaign_data_full_path)
        write_csv_file_s3(dataframes[df], current_campaign_data_full_path)
