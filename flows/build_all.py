""" Raw Verra data flow """
import utils
from raw_verra_data import raw_verra_data_flow
from verra_data import verra_data_flow

from raw_polygon_retired_offsets import raw_polygon_retired_offsets_flow
from raw_polygon_pools_retired_offsets import raw_polygon_pools_retired_offsets_flow
from raw_polygon_pools_redeemed_offsets import raw_polygon_pools_redeemed_offsets_flow
from raw_polygon_pools_deposited_offsets import raw_polygon_pools_deposited_offsets_flow
from raw_polygon_klima_retirements import raw_polygon_klima_retirements_flow
from raw_polygon_klima_retirements_daily import raw_polygon_klima_retirements_daily_flow
from raw_polygon_bridged_offsets import raw_polygon_bridged_offsets_flow
from raw_offsets_holders_data import raw_offsets_holders_data_flow
from raw_eth_retired_offsets import raw_eth_retired_offsets_flow
from raw_eth_moss_retired_offsets import raw_eth_moss_retired_offsets_flow
from raw_eth_moss_bridged_offsets import raw_eth_moss_bridged_offsets_flow
from raw_eth_bridged_offsets_transactions import raw_eth_bridged_offsets_transactions_flow

from raw_eth_carbon_metrics import raw_eth_carbon_metrics_flow
from raw_celo_carbon_metrics import raw_celo_carbon_metrics_flow
from raw_polygon_carbon_metrics import raw_polygon_carbon_metrics_flow

from raw_assets_prices import raw_assets_prices_flow


from polygon_retired_offsets import polygon_retired_offsets_flow
from polygon_bridged_offsets import polygon_bridged_offsets_flow
from eth_moss_bridged_offsets import eth_moss_bridged_offsets_flow
from eth_retired_offsets import eth_retired_offsets_flow

from tokens_data import tokens_data_flow


@utils.flow_with_result_storage
def build_all_flow(result_storage=None):
    raw_verra_data_flow()
    raw_polygon_retired_offsets_flow()
    raw_polygon_pools_retired_offsets_flow()
    raw_polygon_pools_redeemed_offsets_flow()
    raw_polygon_pools_deposited_offsets_flow()
    raw_polygon_klima_retirements_flow()
    raw_polygon_klima_retirements_daily_flow()
    raw_polygon_bridged_offsets_flow()
    raw_offsets_holders_data_flow()
    raw_eth_retired_offsets_flow()
    raw_eth_moss_retired_offsets_flow()
    raw_eth_moss_bridged_offsets_flow()
    raw_eth_bridged_offsets_transactions_flow()
    raw_eth_carbon_metrics_flow()
    raw_celo_carbon_metrics_flow()
    raw_polygon_carbon_metrics_flow()
    raw_assets_prices_flow()

    verra_data_flow()
    polygon_retired_offsets_flow()
    polygon_bridged_offsets_flow()
    eth_moss_bridged_offsets_flow()
    eth_retired_offsets_flow()

    tokens_data_flow()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    build_all_flow()
