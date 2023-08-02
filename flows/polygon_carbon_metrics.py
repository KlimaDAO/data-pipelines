""" Raw Polygon carbon metrics flow """
import pandas as pd
from prefect import task
import utils


SLUG = "polygon_carbon_metrics"
RENAME_MAP = {
    "carbonMetrics_id": "ID",
    "carbonMetrics_timestamp": "Timestamp",
    "carbonMetrics_datetime": "Date",
    "carbonMetrics_bctSupply": "BCT supply",
    "carbonMetrics_nctSupply": "NCT supply",
    "carbonMetrics_mco2Supply": "MCO2 supply",
    "carbonMetrics_uboSupply": "UBO supply",
    "carbonMetrics_nboSupply": "NBO supply",
    "carbonMetrics_bctRedeemed": "BCT redeemed",
    "carbonMetrics_nctRedeemed": "NCT redeemed",
    "carbonMetrics_uboRedeemed": "UBO redeemed",
    "carbonMetrics_nboRedeemed": "NBO redeemed",
    "carbonMetrics_totalCarbonSupply": "Total carbon supply",
    "carbonMetrics_mco2Retired": "MCO2 retired",
    "carbonMetrics_tco2Retired": "TCO2 retired",
    "carbonMetrics_c3tRetired": "C3T retired",
    "carbonMetrics_totalRetirements": "Total retirements",
    "carbonMetrics_bctKlimaRetired": "BCT Klima retired",
    "carbonMetrics_nctKlimaRetired": "NCT Klima retired",
    "carbonMetrics_mco2KlimaRetired": "MCO2 Klima retired",
    "carbonMetrics_uboKlimaRetired": "UBO Klima retired",
    "carbonMetrics_nboKlimaRetired": "NCO Klima retired",
    "carbonMetrics_totalKlimaRetirements": "Total Klima retirements",
    "carbonMetrics_tco2KlimaRetired": "TCO2 Klima retired",
    "carbonMetrics_c3tKlimaRetired": "C3T Klima retired",
    "carbonMetrics_not_klima_retired": "Not Klima retired"
}


@task()
def fetch_polygon_carbon_metrics_task():
    """Fetches Polygon carbon metrics"""
    df = utils.get_latest_dataframe("raw_polygon_carbon_metrics")
    df = df.rename(columns=RENAME_MAP)
    df = df.drop(columns=["ID"])
    df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d %H:%M:%S").dt.date
    return utils.auto_rename_columns(df)


@task()
def validate_polygon_carbon_metrics_task(df):
    """Validates Polygon carbon metrics"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def polygon_carbon_metrics_flow(result_storage=None):
    """Fetches Polygon carbon metrics and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_polygon_carbon_metrics_task,
        validate_data_task=validate_polygon_carbon_metrics_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    polygon_carbon_metrics_flow()
