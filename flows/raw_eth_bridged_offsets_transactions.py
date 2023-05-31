""" Raw Ethereum Moss bridged offsets transactions flow """
from prefect import flow, task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_eth_bridged_offsets_transactions"


@task()
def fetch_eth_bridged_offsets_transactions_task():
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
    )


@task()
def validate_eth_bridged_offsets_transactions_task(df):
    """Validates Ethereum Moss bridged offsets transactions"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.with_result_storage
@flow()
def raw_eth_bridged_offsets_transactions_flow(result_storage=None):
    """Fetches Ethereum Moss bridged offsets transactions and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_eth_bridged_offsets_transactions_task,
        validate_data_task=validate_eth_bridged_offsets_transactions_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_eth_bridged_offsets_transactions_flow()
