""" Raw Verra data flow """
from prefect import task
import pandas as pd
import utils


SLUG = "all_retirements"


@task()
def fetch_all_retirements_task():
    """Builds Merged Retirements data"""
    verra_df = utils.get_latest_dataframe("verra_retirements")
    klima_df = utils.get_latest_dataframe("polygon_klima_retirements")

    # Verra manipulation
    verra_df = verra_df[['retirement_beneficiary',
                         'retirement_date',
                         'serial_number',
                         'bridge',
                         'project_id',
                         'quantity']].copy()
    verra_df = verra_df.rename(
        columns={
            'retirement_beneficiary': 'beneficiary'
            })
    verra_df["origin"] = "Offchain"
    verra_df["transaction_id"] = None
    verra_df["token"] = None

    # Klima manipulation
    klima_df["serial_number"] = None
    klima_df = klima_df[[
        "beneficiary",
        "retirement_date",
        "serial_number",
        "bridge",
        "quantity",
        "origin",
        "transaction_id",
        "token",
        "project_id"
    ]]
    klima_df["serial_number"] = None

    df = pd.concat([verra_df, klima_df])
    df = df.sort_values(by="retirement_date", ascending=False).reset_index(drop=True)
    return utils.auto_rename_columns(df)


@task()
def validate_all_retirements_task(df):
    """Validates Verra data"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def all_retirements_flow(result_storage):
    """Fetches Verra data and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_all_retirements_task,
        validate_data_task=validate_all_retirements_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    all_retirements_flow()
