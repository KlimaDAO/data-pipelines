""" Raw Polygon pools retired offsets flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_polygon_pools_retired_offsets"


RENAME_MAP = {
    "klimaRetires_retire_amount": "Quantity",
    "klimaRetires_retire_timestamp": "Date",
    "klimaRetires_retire_pool_id": "Pool",
    "klimaRetires_retire_retiringAddress": "Retiring Address",
    "klimaRetires_retire_beneficiaryName": "Beneficiary",
    "klimaRetires_retire_beneficiaryAddress": "Beneficiary Address",
    "klimaRetires_retire_retirementMessage": "Retirement Message",
    "klimaRetires_retire_hash": "Tx ID",
}

# TODO: This fetches almost the same data as the raw_polygon_klima_retirements task
# We should merge those scripts


@utils.task_with_backoff
def fetch_raw_polygon_pools_retired_offsets_task():
    """Fetches Polygon pools retired offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)
    klimaretires = carbon_data.Query.klimaRetires(first=utils.get_max_records())

    df = sg.query_df(
        [
            klimaretires.retire.timestamp,
            klimaretires.retire.pool.id,
            klimaretires.retire.hash,
            klimaretires.retire.amount,
            klimaretires.retire.retiringAddress,
            klimaretires.retire.beneficiaryName,
            klimaretires.retire.beneficiaryAddress,
            klimaretires.retire.retirementMessage,
        ]
    ).rename(columns=RENAME_MAP)
    utils.convert_tons(df, ["Quantity"])
    return df


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
