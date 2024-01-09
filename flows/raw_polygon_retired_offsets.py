""" Raw Polygon retired offsets flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
from subgrounds.pagination import ShallowStrategy
import utils
import constants


SLUG = "raw_polygon_retired_offsets"


RENAME_MAP = {
    "retires_id": "ID",
    "retires_amount": "Quantity",
    "retires_timestamp": "Date",
    "retires_retiringAddress_id": "Retiree",
    "retires_credit_bridgeProtocol": "Bridge",
    "retires_credit_project_region": "Region",
    "retires_credit_vintage": "Vintage",
    "retires_credit_project_projectID": "Project ID",
    "retires_credit_project_methodologies": "Methodology",
    "retires_credit_project_country": "Country",
    "retires_credit_project_category": "Project Type",
    "retires_credit_project_name": "Name",
    "retires_credit_id": "Token Address",
    "retires_credit_retired": "Total Quantity",
    "retires_hash": "Tx ID",
    "retires_credit_poolBalances_id": "pool_id",
    "retires_credit_poolBalances_balance": "pool_balance",
    "retires_credit_poolBalances_crossChainSupply": "pool_cross_chain_supply",
    "retires_credit_poolBalances_deposited": "pool_deposited",
    "retires_credit_poolBalances_redeemed": "pool_redeemed",
    "retires_credit_poolBalances_lastSnapshotDayID": "pool_last_snapshot_day",
    "retires_credit_poolBalances_nextSnapshotDayID": "pool_next_snapshot_day",
}


@utils.task_with_backoff
def fetch_raw_polygon_retired_offsets_task():
    """Fetches Polygon retired offsets"""
    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)
    retires = carbon_data.Query.retires(first=utils.get_max_records())

    df = sg.query_df(
        [
            retires.id,
            retires.amount,
            retires.timestamp,
            retires.retiringAddress.id,
            retires.credit.id,
            retires.credit.bridgeProtocol,
            retires.credit.project.region,
            retires.credit.vintage,
            retires.credit.project.id,
            retires.credit.project.methodologies,
            retires.credit.project.country,
            retires.credit.project.category,
            retires.credit.project.name,
            retires.credit.retired,
            retires.credit.poolBalances,
            retires.hash,
        ], pagination_strategy=ShallowStrategy
    ).rename(columns=RENAME_MAP)

    df = utils.flatten_pool_balances(df)

    df = df[
        df["Retiree"] != "0x693ad12dba5f6e07de86faa21098b691f60a1bea"
    ]

    return df.reset_index(drop=True)


@task()
def validate_raw_polygon_retired_offsets_task(df):
    """Validates Polygon retired offsets"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_polygon_retired_offsets_flow(result_storage=None):
    """Fetches Polygon retired offsets and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_raw_polygon_retired_offsets_task,
        validate_data_task=validate_raw_polygon_retired_offsets_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_polygon_retired_offsets_flow()
