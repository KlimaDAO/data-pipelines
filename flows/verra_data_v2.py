""" Raw Verra data flow """
from prefect import task
from functools import lru_cache
import pycountry
import pandas as pd
import utils


SLUG = "verra_data_v2"


@lru_cache
def get_country(country):
    return pycountry.countries.search_fuzzy(country)[0].alpha_3


@task()
def fetch_verra_data_v2_task():
    """Builds Verra data"""
    df = utils.get_latest_dataframe("raw_verra_data")
    df_bridged_mco2 = utils.get_latest_dataframe("raw_eth_moss_bridged_offsets")
    # Dates
    df["Vintage"] = (
        pd.to_datetime(df["Vintage Start"]).dt.tz_localize(None).dt.year.astype(int)
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

    # Credit type
    lst_sn = list(df_bridged_mco2["Serial Number"])
    ToucanRows = df["Retirement Details"].str.contains("TOUCAN").fillna(False)
    C3Rows = df["Retirement Details"].str.contains("C3T").fillna(False)
    MossRows = df["Serial Number"].isin(lst_sn)
    df.loc[ToucanRows, "Toucan"] = True
    df.loc[ToucanRows, "Bridge"] = "Toucan"

    df.loc[C3Rows, "C3"] = True
    df.loc[C3Rows, "Bridge"] = "C3"

    df.loc[MossRows, "Moss"] = True
    df.loc[MossRows, "Bridge"] = "Moss"

    df["Toucan"] = df["Toucan"].fillna(False)
    df["C3"] = df["C3"].fillna(False)
    df["Moss"] = df["Moss"].fillna(False)
    df["Bridge"] = df["Bridge"].fillna(pd.NA)

    # Country info
    df["Country code"] = [
        get_country(country) for country in df["Country"]
    ]

    # Other stuff
    df["Quantity"] = df["Quantity Issued"]
    df["Moss"] = df["Moss"].fillna(False)

    df["Project Id"] = "VCS-" + df["ID"]

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
