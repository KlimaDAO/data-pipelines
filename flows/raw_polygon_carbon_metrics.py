""" Raw Polygon carbon metrics flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
from subgrounds.subgraph import SyntheticField
import utils
import constants


SLUG = "raw_polygon_carbon_metrics"


@utils.task_with_backoff
def fetch_raw_polygon_carbon_metrics_task():
    """Fetches Polygon carbon metrics"""

    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_LEGACY_SUBGRAPH_URL)

    carbon_data.CarbonMetric.datetime = SyntheticField(
        utils.format_timestamp,
        SyntheticField.STRING,
        carbon_data.CarbonMetric.timestamp,
    )

    carbon_data.CarbonMetric.not_klima_retired = SyntheticField(
        lambda totalRetirements, totalKlimaRetirements: totalRetirements
        - totalKlimaRetirements,
        SyntheticField.FLOAT,
        [
            carbon_data.CarbonMetric.totalRetirements,
            carbon_data.CarbonMetric.totalKlimaRetirements,
        ],
    )

    carbon_data.CarbonMetric.tco2KlimaRetired = SyntheticField(
        lambda bctKlimaRetired, nctKlimaRetired: bctKlimaRetired
        + nctKlimaRetired,
        SyntheticField.FLOAT,
        [
            carbon_data.CarbonMetric.bctKlimaRetired,
            carbon_data.CarbonMetric.nctKlimaRetired,
        ],
    )

    carbon_data.CarbonMetric.c3tKlimaRetired = SyntheticField(
        lambda uboKlimaRetired, nboKlimaRetired: uboKlimaRetired
        + nboKlimaRetired,
        SyntheticField.FLOAT,
        [
            carbon_data.CarbonMetric.uboKlimaRetired,
            carbon_data.CarbonMetric.nboKlimaRetired,
        ],
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
            carbon_metrics.uboSupply,
            carbon_metrics.nboSupply,
            carbon_metrics.bctRedeemed,
            carbon_metrics.nctRedeemed,
            carbon_metrics.uboRedeemed,
            carbon_metrics.nboRedeemed,
            carbon_metrics.totalCarbonSupply,
            carbon_metrics.mco2Retired,
            carbon_metrics.tco2Retired,
            carbon_metrics.c3tRetired,
            carbon_metrics.totalRetirements,
            carbon_metrics.bctKlimaRetired,
            carbon_metrics.nctKlimaRetired,
            carbon_metrics.mco2KlimaRetired,
            carbon_metrics.uboKlimaRetired,
            carbon_metrics.nboKlimaRetired,
            carbon_metrics.totalKlimaRetirements,
            carbon_metrics.tco2KlimaRetired,
            carbon_metrics.c3tKlimaRetired,
            carbon_metrics.not_klima_retired,
        ]
    )


@task()
def validate_raw_polygon_carbon_metrics_task(df):
    """Validates Polygon carbon metrics"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_polygon_carbon_metrics_flow(result_storage=None):
    """Fetches Polygon carbon metrics and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_raw_polygon_carbon_metrics_task,
        validate_data_task=validate_raw_polygon_carbon_metrics_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_polygon_carbon_metrics_flow()
