""" Raw Verra data flow """
import utils
from prefect.logging import get_run_logger
from raw_verra_data import raw_verra_data_flow

from verra_data import verra_data_flow
from verra_data_v2 import verra_data_v2_flow

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

from offsets_holders_data import offsets_holders_data_flow

from tokens_data import tokens_data_flow


def run(flow):
    """Runs a flow and log errors"""
    state = flow(return_state=True)
    maybe_result = state.result(raise_on_failure=False)
    if isinstance(maybe_result, ValueError):
        logger = get_run_logger()
        logger.warn(f"flow {flow.__name__} failed")


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

    run(verra_data_flow)
    run(polygon_retired_offsets_flow)
    run(polygon_bridged_offsets_flow)
    run(polygon_pools_retired_offsets_flow)
    run(polygon_pools_redeemed_offsets_flow)
    run(polygon_pools_deposited_offsets_flow)

    run(eth_moss_bridged_offsets_flow)
    run(eth_retired_offsets_flow)

    run(verra_data_v2_flow)
    run(polygon_retired_offsets_v2_flow)
    run(polygon_bridged_offsets_v2_flow)
    run(eth_moss_bridged_offsets_v2_flow)
    run(eth_retired_offsets_v2_flow)

    run(tokens_data_flow)
    run(offsets_holders_data_flow)


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    build_all_flow()
