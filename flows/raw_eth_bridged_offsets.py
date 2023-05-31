""" Raw Ethereum bridged offsets flow """
from prefect import flow, task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_eth_bridged_offsets"


@task()
def fetch_eth_bridged_offsets_task():
    """Fetches Ethereum bridged offsets"""
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
    )


@task()
def validate_eth_bridged_offsets_task(df):
    """Validates Ethereum bridged offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@flow()
def raw_eth_bridged_offsets():
    """Fetches Ethereum bridged offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_eth_bridged_offsets_task,
        validate_data_task=validate_eth_bridged_offsets_task,
    )


@flow()
def raw_eth_bridged_offsets_flow(result_storage):
    """Fetches Ethereum bridged offsets and stores it"""
    raw_eth_bridged_offsets.with_options(result_storage=result_storage)()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_eth_bridged_offsets()