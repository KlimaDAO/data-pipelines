""" Raw Polygon retired offsets flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_polygon_retired_offsets"


RENMAE_MAP = {
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
def fetch_polygon_retired_offsets_task():
    """Fetches Polygon retired offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)
    carbon_offsets = carbon_data.Query.retires(first=utils.get_max_records())

    df = sg.query_df(
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
    ).rename(columns=RENMAE_MAP)

    # Remove DAO MultiSig Address
    df = df[
        df["Tx From Address"] != "0x693ad12dba5f6e07de86faa21098b691f60a1bea"
    ]

    return df.reset_index()


@task()
def validate_polygon_retired_offsets_task(df):
    """Validates Polygon retired offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_polygon_retired_offsets_flow(result_storage=None):
    """Fetches Polygon retired offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_polygon_retired_offsets_task,
        validate_data_task=validate_polygon_retired_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_polygon_retired_offsets_flow()
