""" Raw Verra data flow """
from prefect import flow, task
import requests
import pandas as pd
import utils

VERRA_RENAME_MAP = {
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
        "$maxResults": utils.get_param("MAX_RECORDS", 20000),
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
    df = pd.DataFrame(data).rename(columns=VERRA_RENAME_MAP)
    return df


@task()
def validate_verra_data_task(df):
    """Validates Verra data

    Arguments:
    df: the dataframe to be validated
    """
    utils.validate_against_latest_dataset(SLUG, df)


@task(persist_result=True,
      result_storage_key=f"{SLUG}-{{parameters[suffix]}}",
      result_serializer=utils.DfSerializer())
def store_verra_data_task(df, suffix):
    """Stores Verra data

    Arguments:
    df: the dataframe
    suffix: a date or 'live'
    """
    return df


@flow()
def raw_verra_data():
    """Fetches Verra data and stores it"""
    df = fetch_verra_data_task()
    validate_verra_data_task(df)
    store_verra_data_task(df, utils.now())
    store_verra_data_task(df, "latest")


@flow()
def raw_verra_data_flow(result_storage):
    """Fetches Verra data and stores it"""
    raw_verra_data.with_options(result_storage=result_storage)()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_verra_data()
