""" Raw Ethereum retired offsets flow """
from prefect import flow, task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_eth_retired_offsets"

RENAME_MAP = {
    "retires_value": "Quantity",
    "retires_timestamp": "Date",
    "retires_retiree": "Retiree",
    "retires_offset_bridge": "Bridge",
    "retires_offset_region": "Region",
    "retires_offset_vintage": "Vintage",
    "retires_offset_projectID": "Project ID",
    "retires_offset_standard": "Standard",
    "retires_offset_methodology": "Methodology",
    "retires_offset_country": "Country",
    "retires_offset_category": "Project Type",
    "retires_offset_name": "Name",
    "retires_offset_tokenAddress": "Token Address",
    "retires_offset_totalRetired": "Total Quantity",
    "retires_transaction_id": "Tx ID",
    "retires_transaction_from": "Tx From Address",
}


@task()
def fetch_eth_retired_offsets_task():
    """Fetches Ethereum retired offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_ETH_SUBGRAPH_URL)
    carbon_offsets = carbon_data.Query.retires(first=utils.get_max_records())
    return sg.query_df(
        [
            carbon_offsets.value,
            carbon_offsets.timestamp,
            carbon_offsets.retiree,
            carbon_offsets.offset.tokenAddress,
            carbon_offsets.offset.bridge,
            carbon_offsets.offset.region,
            carbon_offsets.offset.vintage,
            carbon_offsets.offset.projectID,
            carbon_offsets.offset.standard,
            carbon_offsets.offset.methodology,
            carbon_offsets.offset.standard,
            carbon_offsets.offset.country,
            carbon_offsets.offset.category,
            carbon_offsets.offset.name,
            carbon_offsets.offset.totalRetired,
            carbon_offsets.transaction.id,
            carbon_offsets.transaction._select("from"),
        ]
    ).rename(columns=RENAME_MAP)


@task()
def validate_eth_retired_offsets_task(df):
    """Validates Ethereum retired offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.with_result_storage
@flow()
def raw_eth_retired_offsets_flow(result_storage=None):
    """Fetches Ethereum retired offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_eth_retired_offsets_task,
        validate_data_task=validate_eth_retired_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_eth_retired_offsets_flow()
