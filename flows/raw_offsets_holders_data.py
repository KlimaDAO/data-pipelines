""" Raw offsets holders data flow """
from prefect import flow, task
from subgrounds.subgrounds import Subgrounds
import utils
import constants


SLUG = "raw_offsets_holders_data"


@task()
def fetch_offsets_holders_data_task():
    """Fetches offsets holders data"""

    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_HOLDERS_SUBGRAPH_URL)
    holdings = carbon_data.Query.holdings(
        orderBy=carbon_data.Holding.timestamp,
        orderDirection="desc",
        first=utils.get_max_records()
    )

    return sg.query_df(
        [
            holdings.id,
            holdings.token,
            holdings.timestamp,
            holdings.tokenAmount,
            holdings.carbonValue,
            holdings.klimate.id,
        ]
    )


@task()
def validate_offsets_holders_data_task(df):
    """Validates offsets holders data"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.with_result_storage
@flow()
def raw_offsets_holders_data_flow(result_storage):
    """Fetches offsets holders data and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_offsets_holders_data_task,
        validate_data_task=validate_offsets_holders_data_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_offsets_holders_data_flow()
