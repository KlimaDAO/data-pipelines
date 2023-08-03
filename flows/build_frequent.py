""" Raw Verra data flow """
import utils
from utils import run

from flows.current_assets_prices import current_assets_prices_flow


@utils.flow_with_result_storage
def build_frequent_flow(result_storage=None):
    """Run flows that need to be run frequently"""
    run(current_assets_prices_flow)


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    build_frequent_flow()
