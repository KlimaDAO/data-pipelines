from prefect import flow, task
import requests
import pandas as pd
import utils
import json

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
    if dry_run:
        return json.dumps({"a": "b"})
    r = requests.post(VERRA_SEARCH_API_URL,
                json={"program": "VCS", "issuanceTypeCodes": ["ISSUE"]},
            )
    data = r.json()["value"]
    df_verra = pd.DataFrame(data).rename(columns=VERRA_RENAME_MAP)
    return df_verra

@flow(name="raw_verra_data")
def raw_verra_data(storage="local", dry_run=True):
    data = raw_verra_data_task(dry_run)
    utils.write_file(storage, "raw_verra_data", data.encode("utf-8"))



raw_verra_data()