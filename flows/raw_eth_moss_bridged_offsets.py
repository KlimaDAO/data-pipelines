""" Raw Ethereum Moss bridged offsets flow """
from prefect import flow, task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_eth_moss_bridged_offsets"

RENAME_MAP = {
    "batches_id": "ID",
    "batches_serialNumber": "Serial Number",
    "batches_timestamp": "Date",
    "batches_tokenAddress": "Token Address",
    "batches_vintage": "Vintage",
    "batches_projectID": "Project ID",
    "batches_value": "Quantity",
    "batches_originaltx": "Original Tx Address",
}


@task()
def fetch_eth_moss_bridged_offsets_task():
    """Fetches Ethereum Moss bridged offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_MOSS_ETH_SUBGRAPH_URL)
    carbon_offsets = carbon_data.Query.batches(first=utils.get_max_records())
    return sg.query_df(
        [
            carbon_offsets.id,
            carbon_offsets.serialNumber,
            carbon_offsets.timestamp,
            carbon_offsets.tokenAddress,
            carbon_offsets.vintage,
            carbon_offsets.projectID,
            carbon_offsets.value,
            carbon_offsets.originaltx,
        ]
    ).rename(columns=RENAME_MAP)


@task()
def validate_eth_moss_bridged_offsets_task(df):
    """Validates Ethereum Moss bridged offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.with_result_storage
@flow()
def raw_eth_moss_bridged_offsets_flow(result_storage=None):
    """Fetches Ethereum Moss bridged offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_eth_moss_bridged_offsets_task,
        validate_data_task=validate_eth_moss_bridged_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_eth_moss_bridged_offsets_flow()
