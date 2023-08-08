""" Raw Ethereum moss retired offsets flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_eth_moss_retired_offsets"

RENAME_MAP = {
    "retires_id": "Id",
    "retires_value": "Quantity",
    "retires_timestamp": "Date",
    "retires_retiree": "Retiree",
    "retires_beneficiary": "Beneficiary",
    "retires_transaction_id": "Tx ID",
    "retires_transaction_from": "Tx From Address",
}


@utils.task_with_backoff
def fetch_raw_eth_moss_retired_offsets_task():
    """Fetches Ethereum moss retired offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph("https://api.thegraph.com/subgraphs/name/klimadao/staging-ethereum-bridged-carbon")
    carbon_offsets = carbon_data.Query.retires(
        first=utils.get_max_records(),
        orderBy=carbon_data.Retire.timestamp
    )

    df = sg.query_df(
        [
            carbon_offsets.id,
            carbon_offsets.value,
            carbon_offsets.timestamp,
            carbon_offsets.retiree,
            carbon_offsets.beneficiary,
            carbon_offsets.transaction.id,
            carbon_offsets.transaction._select("from"),
        ]
    )
    df = df.rename(columns=RENAME_MAP)
    return df


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
