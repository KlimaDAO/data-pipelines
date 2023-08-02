""" Raw Celo carbon metrics flow """
import pandas as pd
from prefect import task
import utils

SLUG = "celo_carbon_metrics"
RENAME_MAP = {
    "carbonMetrics_id": "ID",
    "carbonMetrics_timestamp": "Timestamp",
    "carbonMetrics_datetime": "Date",
    "carbonMetrics_bctSupply": "BCT supply",
    "carbonMetrics_nctSupply": "NCT supply",
    "carbonMetrics_mco2Supply": "MCO2 supply",
    "carbonMetrics_totalCarbonSupply": "Total carbon supply",
    "carbonMetrics_mco2Retired": "MCO2 retired",
    "carbonMetrics_totalRetirements": "Total retirements"
}


@task()
def fetch_celo_carbon_metrics_task():
    """Fetches Celo carbon metrics"""
    df = utils.get_latest_dataframe("raw_celo_carbon_metrics")
    df = df.rename(columns=RENAME_MAP)
    df = df.drop(columns=["ID"])
    df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d %H:%M:%S").dt.date
    return utils.auto_rename_columns(df)


@task()
def validate_celo_carbon_metrics_task(df):
    """Validates Celo carbon metrics"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def celo_carbon_metrics_flow(result_storage=None):
    """Fetches Celo carbon metrics and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_celo_carbon_metrics_task,
        validate_data_task=validate_celo_carbon_metrics_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    celo_carbon_metrics_flow()
