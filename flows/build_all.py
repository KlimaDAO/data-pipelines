""" Raw Verra data flow """
import utils
from utils import run
from raw_verra_data import raw_verra_data_flow

from verra_data import verra_data_flow
from verra_data_v2 import verra_data_v2_flow
from verra_retirements import verra_retirements_flow

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
from eth_carbon_metrics import eth_carbon_metrics_flow
from celo_carbon_metrics import celo_carbon_metrics_flow
from polygon_carbon_metrics import polygon_carbon_metrics_flow

from raw_assets_prices import raw_assets_prices_flow
from flows.current_assets_prices import current_assets_prices_flow
from assets_prices import assets_prices_flow


from polygon_retired_offsets import polygon_retired_offsets_flow
from polygon_retired_offsets_v2 import polygon_retired_offsets_v2_flow
from polygon_bridged_offsets import polygon_bridged_offsets_flow
from polygon_bridged_offsets_v2 import polygon_bridged_offsets_v2_flow
from polygon_pools_retired_offsets import polygon_pools_retired_offsets_flow
from polygon_pools_redeemed_offsets import polygon_pools_redeemed_offsets_flow
from polygon_pools_deposited_offsets import polygon_pools_deposited_offsets_flow
from eth_moss_bridged_offsets import eth_moss_bridged_offsets_flow
from eth_moss_bridged_offsets_v2 import eth_moss_bridged_offsets_v2_flow
from eth_retired_offsets import eth_retired_offsets_flow
from eth_retired_offsets_v2 import eth_retired_offsets_v2_flow

from polygon_klima_retirements import polygon_klima_retirements_flow
from polygon_klima_retirements_daily import polygon_klima_retirements_daily_flow
from all_retirements import all_retirements_flow

from offsets_holders_data import offsets_holders_data_flow

from tokens_data import tokens_data_flow
from tokens_data_v2 import tokens_data_v2_flow


@utils.flow_with_result_storage
def build_all_flow(result_storage=None):
    """Run all the build flows"""
    run(raw_verra_data_flow)
    run(raw_polygon_retired_offsets_flow)
    run(raw_polygon_pools_retired_offsets_flow)
    run(raw_polygon_pools_redeemed_offsets_flow)
    run(raw_polygon_pools_deposited_offsets_flow)
    run(raw_polygon_klima_retirements_flow)
    run(raw_polygon_klima_retirements_daily_flow)
    run(raw_polygon_bridged_offsets_flow)
    run(raw_offsets_holders_data_flow)
    run(raw_eth_retired_offsets_flow)
    run(raw_eth_moss_retired_offsets_flow)
    run(raw_eth_moss_bridged_offsets_flow)
    run(raw_eth_bridged_offsets_transactions_flow)
    run(raw_eth_carbon_metrics_flow)
    run(raw_celo_carbon_metrics_flow)
    run(raw_polygon_carbon_metrics_flow)
    run(raw_assets_prices_flow)

    run(current_assets_prices_flow)
    run(assets_prices_flow)

    run(verra_data_flow)
    run(polygon_retired_offsets_flow)
    run(polygon_bridged_offsets_flow)
    run(polygon_pools_retired_offsets_flow)
    run(polygon_pools_redeemed_offsets_flow)
    run(polygon_pools_deposited_offsets_flow)

    run(eth_moss_bridged_offsets_flow)
    run(eth_retired_offsets_flow)

    run(verra_data_v2_flow)
    run(verra_retirements_flow)
    run(polygon_retired_offsets_v2_flow)
    run(polygon_bridged_offsets_v2_flow)
    run(eth_moss_bridged_offsets_v2_flow)
    run(eth_retired_offsets_v2_flow)

    run(tokens_data_flow)
    run(offsets_holders_data_flow)

    run(polygon_klima_retirements_flow)
    run(polygon_klima_retirements_daily_flow)
    run(all_retirements_flow)

    run(eth_carbon_metrics_flow)
    run(celo_carbon_metrics_flow)
    run(polygon_carbon_metrics_flow)

    run(tokens_data_v2_flow)


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    build_all_flow()
