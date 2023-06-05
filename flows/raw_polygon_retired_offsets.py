""" Raw Polygon retired offsets flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_polygon_retired_offsets"


@task()
def fetch_polygon_retired_offsets_task():
    """Fetches Polygon retired offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)
    carbon_offsets = carbon_data.Query.retires(first=utils.get_max_records())

    return sg.query_df(
        [
            carbon_offsets.value,
            carbon_offsets.timestamp,
            carbon_offsets.retiree,
            carbon_offsets.offset.tokenAddress,
            carbon_offsets.offset.bridge,
            carbon_offsets.offset.region,
            carbon_offsets.offset.vintage,
            carbon_offsets.offset.projectID,
            carbon_offsets.offset.standard,
            carbon_offsets.offset.methodology,
            carbon_offsets.offset.standard,
            carbon_offsets.offset.country,
            carbon_offsets.offset.category,
            carbon_offsets.offset.name,
            carbon_offsets.offset.totalRetired,
            carbon_offsets.transaction.id,
            carbon_offsets.transaction._select("from"),
        ]
    )


@task()
def validate_polygon_retired_offsets_task(df):
    """Validates Polygon retired offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_polygon_retired_offsets_flow(result_storage=None):
    """Fetches Polygon retired offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_polygon_retired_offsets_task,
        validate_data_task=validate_polygon_retired_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_polygon_retired_offsets_flow()
