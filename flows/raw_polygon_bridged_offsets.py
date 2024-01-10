""" Raw Polygon bridged offsets flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
import utils
import constants

DEPENDENCIES = []

SLUG = "raw_polygon_bridged_offsets"

RENAME_MAP = {
    "carbonCredits_bridged": "Total Quantity",
    "carbonCredits_currentSupply": "Quantity",
    "carbonCredits_bridgeProtocol": "Bridge",
    "carbonCredits_project_region": "Region",
    "carbonCredits_vintage": "Vintage",
    "carbonCredits_project_id": "Project ID",
    "carbonCredits_project_methodology": "Methodology",
    "carbonCredits_project_country": "Country",
    "carbonCredits_project_category": "Project Type",
    "carbonCredits_project_name": "Name",
    "carbonCredits_id": "Token Address",
    "carbonCredits_poolBalances_pool_id": "pool_id",
    "carbonCredits_poolBalances_balance": "pool_balance",
    "carbonCredits_poolBalances_crossChainSupply": "pool_cross_chain_supply",
    "carbonCredits_poolBalances_deposited": "pool_deposited",
    "carbonCredits_poolBalances_redeemed": "pool_redeemed",
    "carbonCredits_poolBalances_lastSnapshotDayID": "pool_last_snapshot_day",
    "carbonCredits_poolBalances_nextSnapshotDayID": "pool_next_snapshot_day",
    "carbonCredits_bridges_timestamp": "Date",
}


@utils.task_with_backoff
def fetch_raw_polygon_bridged_offsets_task():
    """Fetches Polygon bridged offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)
    credits = carbon_data.Query.carbonCredits(
        orderBy=carbon_data.CarbonCredit.lastBatchId,
        orderDirection="desc",
        first=utils.get_max_records(),
    )

    df = sg.query_df(
        [
            credits.id,
            credits.currentSupply,
            credits.bridged,
            credits.bridgeProtocol,
            credits.project.region,
            credits.vintage,
            credits.project.id,
            credits.project.methodologies,
            credits.project.country,
            credits.project.category,
            credits.project.name,
            credits.poolBalances,
            credits.bridges(
                orderBy=carbon_data.Bridge.timestamp,
                orderDirection="asc",
            ),
        ]
    )
    credits_df = df[0].rename(columns=RENAME_MAP)
    bridges_df = df[1].rename(columns=RENAME_MAP)

    credits_df = utils.flatten_pool_balances(credits_df)

    # Adds timestamp of the first bridging event
    def get_timestamp(token_address):
        row = bridges_df.loc[bridges_df['Token Address'] == token_address]
        return row["Date"].iloc[0] if not row.empty else 0

    credits_df["Date"] = credits_df["Token Address"].apply(get_timestamp)

    return credits_df.reset_index()


@task()
def validate_raw_polygon_bridged_offsets_task(df):
    """Validates Polygon bridged offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_polygon_bridged_offsets_flow(result_storage=None):
    """Fetches Polygon bridged offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_raw_polygon_bridged_offsets_task,
        validate_data_task=validate_raw_polygon_bridged_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_polygon_bridged_offsets_flow()
