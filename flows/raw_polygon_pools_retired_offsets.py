""" Raw Polygon pools retired offsets flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_polygon_pools_retired_offsets"


RENAME_MAP = {
    "klimaRetires_amount": "Quantity",
    "klimaRetires_timestamp": "Date",
    "klimaRetires_pool": "Pool",
    "klimaRetires_retiringAddress": "Retiring Address",
    "klimaRetires_beneficiary": "Beneficiary",
    "klimaRetires_beneficiaryAddress": "Beneficiary Address",
    "klimaRetires_retirementMessage": "Retirement Message",
    "klimaRetires_transaction_id": "Tx ID",
}


@task()
def fetch_raw_polygon_pools_retired_offsets_task():
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
    ).rename(columns=RENAME_MAP)


@task()
def validate_raw_polygon_pools_retired_offsets_task(df):
    """Validates Polygon pools retired offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_polygon_pools_retired_offsets_flow(result_storage=None):
    """Fetches Polygon pools retired offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_raw_polygon_pools_retired_offsets_task,
        validate_data_task=validate_raw_polygon_pools_retired_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_polygon_pools_retired_offsets_flow()
