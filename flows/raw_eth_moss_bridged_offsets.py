""" Raw Ethereum Moss bridged offsets flow """
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


@utils.task_with_backoff
def fetch_raw_eth_moss_bridged_offsets_task():
    """Fetches Ethereum Moss bridged offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_ETH_SUBGRAPH_URL)
    carbon_offsets = carbon_data.Query.batches(first=utils.get_max_records())
    df = sg.query_df(
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
    # Fix project ID
    df["Project ID"] = "VCS-" + df["Project ID"].astype(str)
    return df


def validate_raw_eth_moss_bridged_offsets_task(df):
    """Validates Ethereum Moss bridged offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_eth_moss_bridged_offsets_flow(result_storage=None):
    """Fetches Ethereum Moss bridged offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_raw_eth_moss_bridged_offsets_task,
        validate_data_task=validate_raw_eth_moss_bridged_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_eth_moss_bridged_offsets_flow()
