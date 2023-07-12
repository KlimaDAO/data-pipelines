""" Raw Verra data flow """
from prefect import task
import pandas as pd
import utils


SLUG = "verra_data_v2"


@task()
def fetch_verra_data_v2_task():
    """Builds Verra data"""
    df = utils.get_latest_dataframe("raw_verra_data")
    df_bridged_mco2 = utils.get_latest_dataframe("raw_eth_moss_bridged_offsets")

    # Dates
    df["Vintage"] = (
        pd.to_datetime(df["Vintage Start"]).dt.tz_localize(None).dt.year
    )
    df = df.rename(columns={"Retirement/Cancellation Date": "Retirement Date"})
    df["Retirement Date"] = pd.to_datetime(
        df["Retirement Date"]
    )
    df["Issuance Date"] = pd.to_datetime(df["Issuance Date"])
    df["Days to Retirement"] = (
        df["Retirement Date"] - df["Issuance Date"]
    ).dt.days

    # Status
    df.loc[df["Days to Retirement"] > 0, "Status"] = "Retired"
    df["Status"] = df["Status"].fillna("Available")

    # Offset type
    df.loc[
        df["Retirement Details"].str.contains("TOUCAN").fillna(False), "Toucan"
    ] = True
    df["Toucan"] = df["Toucan"].fillna(False)
    df.loc[
        df["Retirement Details"].str.contains("C3T").fillna(False), "C3"
    ] = True
    df["C3"] = df["C3"].fillna(False)

    # Serial Number
    lst_sn = list(df_bridged_mco2["Serial Number"])
    df.loc[df["Serial Number"].isin(lst_sn), "Moss"] = True

    # Other stuff
    df["Quantity"] = df["Quantity Issued"]
    df["Moss"] = df["Moss"].fillna(False)

    return utils.auto_rename_columns(df)


@task()
def validate_verra_data_v2_task(df):
    """Validates Verra data"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def verra_data_v2_flow(result_storage):
    """Fetches Verra data and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_verra_data_v2_task,
        validate_data_task=validate_verra_data_v2_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    verra_data_v2_flow()
