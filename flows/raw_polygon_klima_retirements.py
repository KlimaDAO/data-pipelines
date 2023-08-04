""" Raw Polygon Klima retirements flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
from subgrounds.subgraph import SyntheticField
import utils
import constants


SLUG = "raw_polygon_klima_retirements"


@utils.task_with_backoff
def fetch_raw_polygon_klima_retirements_task():
    """Fetches Polygon Klima retirements"""

    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)

    carbon_data.KlimaRetire.datetime = SyntheticField(
        utils.format_timestamp,
        SyntheticField.STRING,
        carbon_data.KlimaRetire.timestamp,
    )

    carbon_data.KlimaRetire.proof = SyntheticField(
        lambda tx_id: f'https://polygonscan.com/tx/{tx_id}',
        SyntheticField.STRING,
        carbon_data.KlimaRetire.transaction.id,
    )

    klima_retirees = carbon_data.Query.klimaRetires(
        orderBy=carbon_data.KlimaRetire.timestamp,
        orderDirection="desc",
        first=utils.get_max_records()
    )

    return sg.query_df(
        [
            klima_retirees.transaction.id,
            klima_retirees.beneficiaryAddress,
            klima_retirees.offset.projectID,
            klima_retirees.offset.bridge,
            klima_retirees.token,
            klima_retirees.datetime,
            klima_retirees.amount,
            klima_retirees.proof
        ]
    )


@task()
def validate_raw_polygon_klima_retirements_task(df):
    """Validates Polygon Klima retirements"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_polygon_klima_retirements_flow(result_storage=None):
    """Fetches Polygon Klima retirements and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_raw_polygon_klima_retirements_task,
        validate_data_task=validate_raw_polygon_klima_retirements_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_polygon_klima_retirements_flow()
