""" Raw Ethereum carbon metrics flow """
import pandas as pd
from prefect import task
import utils


SLUG = "eth_carbon_metrics"
RENAME_MAP = {
    "carbonMetrics_id": "ID",
    "carbonMetrics_timestamp": "Timestamp",
    "carbonMetrics_datetime": "Date",
    "carbonMetrics_mco2Supply": "MCO2 supply",
    "carbonMetrics_totalCarbonSupply": "Total carbon supply",
    "carbonMetrics_mco2Retired": "MCO2 retired",
    "carbonMetrics_totalRetirements": "Total retirements"
}


@task()
def fetch_eth_carbon_metrics_task():
    """Fetches Ethereum carbon metrics"""
    df = utils.get_latest_dataframe("raw_eth_carbon_metrics")
    df = df.rename(columns=RENAME_MAP)
    df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d %H:%M:%S").dt.date
    return utils.auto_rename_columns(df)


@task()
def validate_eth_carbon_metrics_task(df):
    """Validates Ethereum carbon metrics"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def eth_carbon_metrics_flow(result_storage=None):
    """Fetches Ethereum carbon metrics and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_eth_carbon_metrics_task,
        validate_data_task=validate_eth_carbon_metrics_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    eth_carbon_metrics_flow()
