""" Raw Polygon pools retired offsets flow """
from prefect import flow, task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_polygon_pools_retired_offsets"


@task()
def fetch_polygon_pools_retired_offsets_task():
    """Fetches Polygon pools retired offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)
    klimaretires = carbon_data.Query.klimaRetires(first=utils.get_max_records())

    return sg.query_df(
        [
            klimaretires.timestamp,
            klimaretires.pool,
            klimaretires.amount,
            klimaretires.retiringAddress,
            klimaretires.beneficiary,
            klimaretires.beneficiaryAddress,
            klimaretires.retirementMessage,
            klimaretires.transaction.id,
        ]
    )


@task()
def validate_polygon_pools_retired_offsets_task(df):
    """Validates Polygon pools retired offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@flow()
def raw_polygon_pools_retired_offsets():
    """Fetches Polygon pools retired offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_polygon_pools_retired_offsets_task,
        validate_data_task=validate_polygon_pools_retired_offsets_task,
    )


@flow()
def raw_polygon_pools_retired_offsets_flow(result_storage):
    """Fetches Polygon pools retired offsets and stores it"""
    raw_polygon_pools_retired_offsets.with_options(result_storage=result_storage)()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_polygon_pools_retired_offsets()