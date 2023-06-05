""" Raw assets prices flow """
from prefect import task
from subgrounds.subgrounds import Subgrounds
import pandas as pd
import utils
import constants


SLUG = "raw_assets_prices"


@task()
def fetch_assets_prices_task():
    """Fetches assets prices"""

    df_prices = pd.DataFrame()
    sg = Subgrounds()
    tokens_dict = constants.TOKENS

    price_sg = sg.load_subgraph(constants.PAIRS_SUBGRAPH_URL)
    for i in tokens_dict.keys():
        swaps = price_sg.Query.swaps(
            first=utils.get_max_records(),
            orderBy=price_sg.Swap.timestamp,
            orderDirection="desc",
            where=[
                price_sg.Swap.pair == tokens_dict[i]["Pair Address"]
            ],
        )

        # Pull swap ID for NCT
        fields = [swaps.pair.id, swaps.close, swaps.timestamp]
        if i == 'NCT':
            fields.append(swaps.id)

        df = sg.query_df(fields)

        # Filter out mispriced NCT swap
        if i == 'NCT':
            df = df[df.swaps_id != constants.MISPRICED_NCT_SWAP_ID]
            df = df.drop('swaps_id', axis=1)

        # Rename and format fields
        rename_prices_map = {
            "swaps_pair_id": f"{i}_Address",
            "swaps_close": f"{i}_Price",
            "swaps_timestamp": "Date",
        }
        df = df.rename(columns=rename_prices_map)
        df["Date"] = (
            pd.to_datetime(df["Date"], unit="s")
            .dt.tz_localize("UTC")
            .dt.floor("D")
            .dt.date
        )
        df = df.drop_duplicates(keep="first", subset=[f"{i}_Address", "Date"])
        df = df[df[f"{i}_Price"] != 0]
        if df_prices.empty:
            df_prices = df
        else:
            df_prices = df_prices.merge(df, how="outer", on="Date")
        df_prices = df_prices.sort_values(by="Date", ascending=False)

    return df_prices


@task()
def validate_assets_prices_task(df):
    """Validates assets prices"""
    utils.validate_against_latest_dataframe(SLUG, df)


@utils.flow_with_result_storage
def raw_assets_prices_flow(result_storage=None):
    """Fetches assets prices and stores it"""
    utils.raw_data_flow(
        slug=SLUG,
        fetch_data_task=fetch_assets_prices_task,
        validate_data_task=validate_assets_prices_task,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    raw_assets_prices_flow()
