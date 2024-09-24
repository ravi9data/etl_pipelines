import logging

import pandas

from plugins.utils.s3_utils import (current_path, list_files_s3,
                                    read_csv_file_s3, write_csv_file_s3)

logger = logging.getLogger(__name__)


def processed_campaign_data(
    bucket_name, campaign_data_path, campaign_sku_wo_campaign_id_path
):
    """
    This function is used to process the campaign data
    """
    campaign_data_path = f"s3://{bucket_name}{campaign_data_path}".format(
        bucket_name=bucket_name, campaign_data_path=campaign_data_path
    )
    # List all CSV files in the S3 folder
    current_campaign_data_path = current_path(campaign_data_path)
    campaign_data_files = list_files_s3(current_campaign_data_path, ".csv")

    final_df = output()
    for campaign_file in campaign_data_files:
        print("file_path**********", campaign_file)
        campaign_name = campaign_file.split("/")[1].split(".")[0]
        print("campaign_name**********", campaign_name)
        df = read_csv_file_s3(campaign_file)
        processed_df = process_dataframe(df, campaign_name)
        if processed_df.empty:
            continue
        else:
            final_df = pandas.concat(
                [
                    final_df,
                    processed_df[
                        ["sku", "campaign_date", "collection", "campaign_name"]
                    ],
                ],
                ignore_index=True,
            )
    # Now add a flag to the SKU
    final_df["is_campaign"] = 1
    # Remove timestamp
    final_df["campaign_date"] = pandas.to_datetime(
        final_df["campaign_date"], format="%d/%m/%Y"
    )
    campaign_sku_wo_campaign_id_path = (
        f"s3://{bucket_name}{campaign_sku_wo_campaign_id_path}".format(
            bucket_name=bucket_name,
            campaign_sku_wo_campaign_id=campaign_sku_wo_campaign_id_path,
        )
    )
    current_campaign_sku_wo_campaign_id_path = current_path(
        campaign_sku_wo_campaign_id_path
    )
    write_csv_file_s3(
        final_df[["sku", "campaign_date", "is_campaign"]],
        current_campaign_sku_wo_campaign_id_path,
    )


def output():
    """
    This function creates the output df with required columns
    """
    df = pandas.DataFrame(
        columns=["sku", "campaign_date", "collection", "campaign_name"]
    )
    return df


def fill_dates(df):
    """
    This function is used to fill the dates
    """
    dates_df = pandas.concat(
        [
            g.set_index("start")
            .reindex(
                pandas.date_range(g["start"].min(), g["end"].max(), freq="d"),
                method="ffill",
            )
            .reset_index()
            .rename({"index": "campaign_date"}, axis=1)
            for _, g in df.groupby(["sku-id", "collection"])
        ],
        axis=0,
    )
    return dates_df


def process_dataframe(df, campaign_name):
    """
    This function is used to process the dataframe
    """
    # Skip processing if we don't have the right info
    try:
        if "start" not in df.columns:
            print(f"Missing data in {campaign_name}")
            return pandas.DataFrame()

        if "end" not in df.columns:
            print(f"Missing data in {campaign_name}")
            return pandas.DataFrame()

        # Skip if there was any bug processing the sheets
        if df["pdp"].str.contains("Loading...").any():
            return pandas.DataFrame()

        # Slice and remove entries where ? is present in date
        df = df[~df["start"].str.contains(r"\?", na=False)]
        df = df[~df["end"].str.contains(r"\?", na=False)]

        # Drop rows where the sku-id is not unique because missing a number
        df = df[df["sku-id"].str.contains(r"\S+-\S+", regex=True, na=False)]

        # Drop rows where the collection is NaN or empty string (could add OOS)

        df = df[~df["collection"].isna()]
        df = df[df["collection"] != ""]

        # Rename column for US ones where sku is named different
        df.columns = df.columns.str.replace(r"\(us\)", "", regex=True)
        df.columns = df.columns.str.strip()

        # Add year to string
        df["start"] = df["start"] + "/2022"
        df["end"] = df["end"] + "/2022"
        # Convert to datetime
        df["start"] = pandas.to_datetime(df["start"], format="%d/%m/%Y")
        df["end"] = pandas.to_datetime(df["end"], format="%d/%m/%Y")
        # Add campaign name
        df["campaign_name"] = campaign_name

        # Fill dates
        df = fill_dates(df)

        return df

    except Exception as ex:
        logger.info("The dataframe has following issue {}".format(ex))
        return pandas.DataFrame()
