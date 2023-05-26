""" Raw Verra data flows """
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

MAX_RESULTS = 20000
SEARCH_API_URL = "https://registry.verra.org/uiapi/asset/asset/search"
SEARCH_API_PARAMS = {
    "$maxResults": 20000,
    "$count": "true",
    "$skip": 0,
    "format": "csv"
}
SLUG = "raw_verra_data"


@task()
def fetch_verra_data_task():
    """Fetches Verra data and returns them in Json format

    Arguments:
    dry_run: if true, this will return placeholder data
    """
    if utils.get_param("DRY_RUN"):
        data = [{"issuanceDate": "something"}]
    else:
        r = requests.post(SEARCH_API_URL,
                          params=SEARCH_API_PARAMS,
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
    """Validates verra data

    Arguments:
    df: the dataframe to be validated
    """
    latest_df = None
    try:
        latest_df = utils.read_df(f"{SLUG}-latest")
    except Exception as err:
        print(err)
        pass

    if latest_df is not None:
        assert df.shape[0] >= latest_df.shape[0], "New dataset has a lower number of rows"
        assert df.shape[1] == latest_df.shape[1], "New dataset does not have the same number of colums"
    else:
        print("Live dataframe cannot be found. Skipping validation")


@task(persist_result=True,
      result_storage_key=f"{SLUG}-{{parameters[suffix]}}",
      result_serializer=utils.DfSerializer())
def store_verra_data_task(df, suffix):
    """Stores verra data

    Arguments:
    df: the dataframe
    suffix: a date or 'live'
    """
    return df


@flow()
def raw_verra_data():
    """Fetches Verra data and stores them"""
    df = fetch_verra_data_task()
    validate_verra_data_task(df)
    store_verra_data_task(df, utils.now())
    store_verra_data_task(df, "latest")


@flow()
def raw_verra_data_flow(result_storage):
    """Fetches Verra data and stores them"""
    raw_verra_data.with_options(result_storage=result_storage)()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_verra_data()
