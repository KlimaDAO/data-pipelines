""" Raw Verra data flow """
from utils import run, flow_with_result_storage
import os


@flow_with_result_storage
def build_all_flow(result_storage=None):
    """Run all the build flows"""
    # set up environement for subflows
    if result_storage:
        os.environ["DATA_PIPELINES_RESULT_STORAGE"] = result_storage

    # run subflows
    run("raw_verra_data")
    run("raw_polygon_retired_offsets")
    run("raw_polygon_pools_retired_offsets")
    run("raw_polygon_pools_redeemed_offsets")
    run("raw_polygon_pools_deposited_offsets")
    run("raw_polygon_klima_retirements")
    run("raw_polygon_bridged_offsets")
    run("raw_offsets_holders_data")
    run("raw_eth_retired_offsets")
    run("raw_eth_moss_retired_offsets")
    run("raw_eth_moss_bridged_offsets")
    run("raw_eth_bridged_offsets_transactions")
    run("raw_eth_carbon_metrics")
    run("raw_celo_carbon_metrics")
    run("raw_polygon_carbon_metrics")
    run("raw_assets_prices")

    run("current_assets_prices")
    run("assets_prices")
    run("verra_data")
    run("polygon_retired_offsets")
    run("polygon_bridged_offsets")
    run("polygon_pools_retired_offsets")
    run("polygon_pools_redeemed_offsets")
    run("polygon_pools_deposited_offsets")

    run("eth_moss_bridged_offsets")
    run("eth_retired_offsets")

    run("verra_data_v2")
    run("verra_retirements")
    run("polygon_retired_offsets_v2")
    run("polygon_bridged_offsets_v2")
    run("eth_moss_bridged_offsets_v2")
    run("eth_retired_offsets_v2")

    run("tokens_data")
    run("offsets_holders_data")

    run("polygon_klima_retirements")
    run("all_retirements")

    run("eth_carbon_metrics")
    run("celo_carbon_metrics")
    run("polygon_carbon_metrics")

    run("tokens_data_v2")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    build_all_flow()
