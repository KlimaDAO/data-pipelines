""" Raw Verra data flow """
from prefect import task
import requests
import pandas as pd
import utils

RENAME_MAP = {
    "issuanceDate": "Issuance Date",
    "programObjectives": "Sustainable Development Goals",
    "instrumentType": "Credit Type",
    "vintageStart": "Vintage Start",
    "vintageEnd": "Vintage End",
    "reportingPeriodStart": "Reporting Period Start",
    "reportingPeriodEnd": "Reporting Period End",
    "resourceIdentifier": "ID",
    "resourceName": "Name",
    "region": "Region",
    "country": "Country",
    "protocolCategory": "Project Type",
    "protocol": "Methodology",
    "totalVintageQuantity": "Total Vintage Quantity",
    "quantity": "Quantity Issued",
    "serialNumbers": "Serial Number",
    "additionalCertifications": "Additional Certifications",
    "retiredCancelled": "Is Cancelled",
    "retireOrCancelDate": "Retirement/Cancellation Date",
    "retirementBeneficiary": "Retirement Beneficiary",
    "retirementReason": "Retirement Reason",
    "retirementDetails": "Retirement Details",
    "inputTypes": "Input Type",
    "holdingIdentifier": "Holding ID",
}

SEARCH_API_URL = "https://registry.verra.org/uiapi/asset/asset/search"
SLUG = "raw_verra_data"


def get_search_api_params():
    """Returns parameters for the Verra data query"""
    return {
        "$maxResults": utils.get_param_as_int("MAX_RECORDS", 20000),
        "$count": "true",
        "$skip": 0,
        "format": "csv"
    }


@task()
def fetch_verra_data_task():
    """Fetches Verra data"""
    if utils.get_param("DRY_RUN"):
        data = [{"issuanceDate": "something"}]
    else:
        r = requests.post(SEARCH_API_URL,
                          params=get_search_api_params(),
                          json={"program": "VCS",
                                "issuanceTypeCodes": ["ISSUE"]
                                },
                          timeout=20 * 60
                          )
        data = r.json()["value"]
    df = pd.DataFrame(data).rename(columns=RENAME_MAP)

    # df["Vintage"] = df["Vintage Start"]
    df["Vintage"] = (
        pd.to_datetime(df["Vintage Start"]).dt.tz_localize(None).dt.year
    )
    # FIXME: Remove Vintage start column?

    df["Quantity"] = df["Quantity Issued"]
    # FIXME: Remove Quantity issued column?

    df["Retirement/Cancellation Date"] = pd.to_datetime(
        df["Retirement/Cancellation Date"]
    )
    df["Date"] = df["Retirement/Cancellation Date"]
    # FIXME: Remove Quantity issued column?

    df.loc[
        df["Retirement Details"].str.contains("TOUCAN").fillna(False), "Toucan"
    ] = True
    df["Toucan"] = df["Toucan"].fillna(False)
    df.loc[
        df["Retirement Details"].str.contains("C3T").fillna(False), "C3"
    ] = True
    df["C3"] = df["C3"].fillna(False)

    return df


@task()
def validate_verra_data_task(df):
    """Validates Verra data"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_verra_data_flow(result_storage):
    """Fetches Verra data and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_verra_data_task,
        validate_data_task=validate_verra_data_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_verra_data_flow()
