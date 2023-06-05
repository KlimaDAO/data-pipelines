""" Raw Ethereum carbon metrics flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
from subgrounds.subgraph import SyntheticField
import utils
import constants


SLUG = "raw_eth_carbon_metrics"


@task()
def fetch_eth_carbon_metrics_task():
    """Fetches Ethereum carbon metrics"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_ETH_SUBGRAPH_URL)
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

    df = sg.query_df(
        [
            carbon_metrics.id,
            carbon_metrics.timestamp,
            carbon_metrics.datetime,
            carbon_metrics.timestamp,
            carbon_metrics.mco2Supply,
            carbon_metrics.totalCarbonSupply,
            carbon_metrics.mco2Retired,
            carbon_metrics.totalRetirements,
        ]
    )

    zero_timestamp_index = df[(df["carbonMetrics_timestamp"] == "0")].index
    df.drop(zero_timestamp_index, inplace=True)

    return df


@task()
def validate_eth_carbon_metrics_task(df):
    """Validates Ethereum carbon metrics"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_eth_carbon_metrics_flow(result_storage=None):
    """Fetches Ethereum carbon metrics and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_eth_carbon_metrics_task,
        validate_data_task=validate_eth_carbon_metrics_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_eth_carbon_metrics_flow()
