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

VERRA_SEARCH_API_URL = "https://registry.verra.org/uiapi/asset/asset/search?$maxResults=2&$count=true&$skip=0&format=csv"


@task()
def raw_verra_data_task(dry_run=False):
    """Fetches Verra data and returns them in Json format

    Arguments:
    dry_run: if true, this will return placeholder data
    """
    if dry_run:
        data = [{"issuanceDate": "something"}]
    else:
        r = requests.post(VERRA_SEARCH_API_URL,
                          json={"program": "VCS",
                                "issuanceTypeCodes": ["ISSUE"]
                                },
                          )
        data = r.json()["value"]
    df_verra = pd.DataFrame(data).rename(columns=VERRA_RENAME_MAP)
    return df_verra.to_json()


@flow(name="raw_verra_data")
def raw_verra_data(storage="local", dry_run=True):
    """Fetches Verra data and stores them

    Arguments:
    storage: a Prefect block name or "local"
    dry_run: if true, this will store placeholder data
    """
    data = raw_verra_data_task(dry_run)
    utils.write_file(storage, "raw_verra_data.json", data.encode("utf-8"))


if __name__ == "main":
    raw_verra_data()
