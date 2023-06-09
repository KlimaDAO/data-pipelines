""" Raw Ethereum moss retired offsets flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_eth_moss_retired_offsets"

RENAME_MAP = {
    "mossOffsets_value": "Quantity",
    "mossOffsets_timestamp": "Date",
    "mossOffsets_retiree": "Retiree",
    "mossOffsets_receiptId": "Receipt ID",
    "mossOffsets_onBehalfOf": "OnBehalf Of",
    "mossOffsets_transaction_id": "Tx ID",
    "mossOffsets_transaction_from": "Tx From Address",
}


@task()
def fetch_raw_eth_moss_retired_offsets_task():
    """Fetches Ethereum moss retired offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_MOSS_ETH_TEST_SUBGRAPH_URL)
    carbon_offsets = carbon_data.Query.mossOffsets(first=utils.get_max_records())

    return sg.query_df(
        [
            carbon_offsets.value,
            carbon_offsets.timestamp,
            carbon_offsets.retiree,
            carbon_offsets.receiptId,
            carbon_offsets.onBehalfOf,
            carbon_offsets.transaction.id,
            carbon_offsets.transaction._select("from"),
        ]
    ).rename(columns=RENAME_MAP)


@task()
def validate_raw_eth_moss_retired_offsets_task(df):
    """Validates Ethereum moss retired offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_eth_moss_retired_offsets_flow(result_storage=None):
    """Fetches Ethereum moss retired offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_raw_eth_moss_retired_offsets_task,
        validate_data_task=validate_raw_eth_moss_retired_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_eth_moss_retired_offsets_flow()
