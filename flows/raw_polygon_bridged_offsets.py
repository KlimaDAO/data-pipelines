""" Raw Polygon bridged offsets flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_polygon_bridged_offsets"


@task()
def fetch_polygon_bridged_offsets_task():
    """Fetches Polygon bridged offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)
    carbon_offsets = carbon_data.Query.carbonOffsets(
        orderBy=carbon_data.CarbonOffset.lastUpdate,
        orderDirection="desc",
        first=utils.get_max_records(),
    )

    return sg.query_df(
        [
            carbon_offsets.tokenAddress,
            carbon_offsets.bridge,
            carbon_offsets.region,
            carbon_offsets.vintage,
            carbon_offsets.projectID,
            carbon_offsets.standard,
            carbon_offsets.methodology,
            carbon_offsets.country,
            carbon_offsets.category,
            carbon_offsets.name,
            carbon_offsets.balanceBCT,
            carbon_offsets.balanceNCT,
            carbon_offsets.balanceUBO,
            carbon_offsets.balanceNBO,
            carbon_offsets.totalBridged,
            carbon_offsets.bridges.value,
            carbon_offsets.bridges.timestamp,
        ]
    )


@task()
def validate_polygon_bridged_offsets_task(df):
    """Validates Polygon bridged offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_polygon_bridged_offsets_flow(result_storage=None):
    """Fetches Polygon bridged offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_polygon_bridged_offsets_task,
        validate_data_task=validate_polygon_bridged_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_polygon_bridged_offsets_flow()
