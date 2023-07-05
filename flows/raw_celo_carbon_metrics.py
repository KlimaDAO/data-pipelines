""" Raw Celo carbon metrics flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
from subgrounds.subgraph import SyntheticField
import utils
import constants


SLUG = "raw_celo_carbon_metrics"
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
def fetch_raw_celo_carbon_metrics_task():
    """Fetches Celo carbon metrics"""

    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_CELO_SUBGRAPH_URL)

    carbon_data.CarbonMetric.datetime = SyntheticField(
        utils.format_timestamp,
        SyntheticField.STRING,
        carbon_data.CarbonMetric.timestamp,
    )

    carbon_metrics = carbon_data.Query.carbonMetrics(
        orderBy=carbon_data.CarbonMetric.timestamp,
        orderDirection="desc",
        first=utils.get_max_records(),
        where=[carbon_data.CarbonMetric.timestamp > 0],
    )

    return sg.query_df(
        [
            carbon_metrics.id,
            carbon_metrics.timestamp,
            carbon_metrics.datetime,
            carbon_metrics.bctSupply,
            carbon_metrics.nctSupply,
            carbon_metrics.mco2Supply,
            carbon_metrics.totalCarbonSupply,
            carbon_metrics.mco2Retired,
            carbon_metrics.totalRetirements,
        ]
    ).rename(columns=RENAME_MAP)


@task()
def validate_raw_celo_carbon_metrics_task(df):
    """Validates Celo carbon metrics"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_celo_carbon_metrics_flow(result_storage=None):
    """Fetches Celo carbon metrics and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_raw_celo_carbon_metrics_task,
        validate_data_task=validate_raw_celo_carbon_metrics_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_celo_carbon_metrics_flow()
