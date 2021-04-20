import os

from snapflow import Environment, graph, produce

from loguru import logger

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
    env = Environment(metadata_storage="sqlite://")

    g = graph()

    # Initial graph
    orders = g.create_node(
        "shopify.import_orders",
        params={"admin_url": api_key},
    )
    blocks = produce(orders, env=env, modules=[shopify])
    records = blocks[0].as_records()
    assert len(records) > 0


if __name__ == "__main__":
    test_shopify()
