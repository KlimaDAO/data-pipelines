""" Raw Polygon Klima retirements flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
from subgrounds.subgraph import SyntheticField
import utils
import constants


RENAME_MAP = {
    "klimaRetires_retire_hash": "Transaction ID",
    "klimaRetires_retire_beneficiaryAddress_id": "Beneficiary",
    "klimaRetires_retire_credit_project_projectID": "Project ID",
    "klimaRetires_retire_credit_bridgeProtocol": "Bridge",
    "klimaRetires_retire_pool_id": "Token",
    "klimaRetires_retire_amount": "Quantity",
    "klimaRetires_datetime": "Retirement Date",
    "klimaRetires_proof": "Proof"
}

SLUG = "raw_polygon_klima_retirements"


@utils.task_with_backoff
def fetch_raw_polygon_klima_retirements_task():
    """Fetches Polygon Klima retirements"""

    sg = Subgrounds()
    carbon_data = sg.load_subgraph(constants.CARBON_SUBGRAPH_URL)

    carbon_data.KlimaRetire.datetime = SyntheticField(
        utils.format_timestamp,
        SyntheticField.STRING,
        carbon_data.KlimaRetire.retire.timestamp,
    )
    carbon_data.KlimaRetire.proof = SyntheticField(
        lambda tx_id: f'https://polygonscan.com/tx/{tx_id}',
        SyntheticField.STRING,
        carbon_data.KlimaRetire.retire.hash,
    )

    klima_retires = carbon_data.Query.klimaRetires(
        orderBy=carbon_data.KlimaRetire.retire.timestamp,
        orderDirection="desc",
        first=utils.get_max_records()
    )

    df = sg.query_df(
        [
            klima_retires.retire.hash,
            klima_retires.retire.beneficiaryAddress.id,
            klima_retires.retire.credit.project.projectID,
            klima_retires.retire.credit.bridgeProtocol,
            klima_retires.retire.pool.id,
            klima_retires.datetime,
            klima_retires.retire.amount,
            klima_retires.proof
        ]
    ).rename(columns=RENAME_MAP)

    df = utils.convert_tons(df, [
                    "Quantity",
        ])

    return df


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
