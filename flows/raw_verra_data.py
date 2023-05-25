""" Raw Verra data flows """
from prefect import flow, task
import requests
import pandas as pd
import utils
from prefect.context import FlowRunContext

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
SEARCH_API_URL = f"https://registry.verra.org/uiapi/asset/asset/search?$maxResults={MAX_RESULTS}&$count=true&$skip=0&format=csv"
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
    data: the data to be validated
    """
    #old_df = None
    ctx = FlowRunContext.get()
    print(ctx.result_factory.storage_block)
    """
    try:
        old_df = utils.read_df(SLUG)
    except ValueError as err:
        print(err)
        pass

    if old_df is not None:
        assert df.shape[0] >= old_df.shape[0], "New dataset has a lower number of rows"
        assert df.shape[1] == old_df.shape[1], "New dataset does not have the same number of colums"
    """

@task(persist_result=True, result_storage_key="raw_verra_data-{parameters[date]}.json")
def store_verra_data_task(df, date):
    """Stores verra data

    Arguments:
    df: the dataframe
    """
    return df


@flow(name="raw_verra_data")
def raw_verra_data():
    """Fetches Verra data and stores them

    Arguments:
    storage: a Prefect block name or "local"
    dry_run: if true, this will store placeholder data
    """
    df = fetch_verra_data_task()
    validate_verra_data_task(df)
    #store_verra_data_task(df, utils.now())
    #store_verra_data_task(df, "live")


if __name__ == "__main__":
    raw_verra_data()
