import os
from dcp.storage.database.utils import get_tmp_sqlite_db_url


from loguru import logger
from snapflow import Environment, DataspaceCfg, GraphCfg

logger.enable("snapflow")


def ensure_api_key() -> str:
    api_key = os.environ.get("SHOPIFY_ADMIN_URL")
    if api_key is not None:
        return api_key
    api_key = input("Enter Shopify Admin URL: ")
    return api_key


def test_shopify():
    from snapflow_shopify import module as shopify

    api_key = ensure_api_key()
    storage = get_tmp_sqlite_db_url()
    env = Environment(DataspaceCfg(metadata_storage="sqlite://", storages=[storage]))

    # Initial graph
    orders = GraphCfg(
        key="import_orders",
        function="shopify.import_orders",
        params={"admin_url": api_key},
    )
    g = GraphCfg(nodes=[orders])
    results = env.produce(
        orders.key, g, target_storage=storage, execution_timelimit_seconds=1
    )
    records = results[0].stdout().as_records()
    assert len(records) > 0


if __name__ == "__main__":
    test_shopify()
