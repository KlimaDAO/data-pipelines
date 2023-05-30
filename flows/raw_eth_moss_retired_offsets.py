""" Raw Ethereum Moss retired offsets flow """
from prefect import flow, task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_eth_moss_retired_offsets"


@task()
def fetch_eth_moss_retired_offsets_task():
    """Fetches Ethereum Moss retired offsets"""
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
def validate_eth_moss_retired_offsets_task(df):
    """Validates Ethereum Moss retired offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@flow()
def raw_eth_moss_retired_offsets():
    """Fetches Ethereum Moss retired offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_eth_moss_retired_offsets_task,
        validate_data_task=validate_eth_moss_retired_offsets_task,
    )


@flow()
def raw_eth_moss_retired_offsets_flow(result_storage):
    """Fetches Ethereum Moss retired offsets and stores it"""
    raw_eth_moss_retired_offsets.with_options(result_storage=result_storage)()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_eth_moss_retired_offsets()
