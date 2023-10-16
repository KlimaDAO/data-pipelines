""" Raw Ethereum Moss bridged offsets transactions flow """
from subgrounds.subgrounds import Subgrounds
import utils
import constants

SLUG = "raw_eth_bridged_offsets_transactions"

RENAME_MAP = {
    "bridges_value": "Quantity",
    "bridges_timestamp": "Date",
    "bridges_transaction_id": "Tx Address",
}


@utils.task_with_backoff
def fetch_raw_eth_bridged_offsets_transactions_task():
    """Fetches Ethereum Moss bridged offsets transactions"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_ETH_SUBGRAPH_URL)
    carbon_offsets = carbon_data.Query.bridges(first=utils.get_max_records())
    return sg.query_df(
        [
            carbon_offsets.value,
            carbon_offsets.timestamp,
            carbon_offsets.transaction.id,
        ]
    ).rename(columns=RENAME_MAP)


def validate_raw_eth_bridged_offsets_transactions_task(df):
    """Validates Ethereum Moss bridged offsets transactions"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_eth_bridged_offsets_transactions_flow(result_storage=None):
    """Fetches Ethereum Moss bridged offsets transactions and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_raw_eth_bridged_offsets_transactions_task,
        validate_data_task=validate_raw_eth_bridged_offsets_transactions_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_eth_bridged_offsets_transactions_flow()
