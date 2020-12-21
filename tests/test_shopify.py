import os

from snapflow import Environment, graph, produce


def ensure_api_key() -> str:
    api_key = os.environ.get("SHOPIFY_ADMIN_URL")
    if api_key is not None:
        return api_key
    api_key = input("Enter Shopify Admin URL: ")
    return api_key


def test_shopify():
    import snapflow_shopify as shopify

    api_key = ensure_api_key()
    env = Environment(metadata_storage="sqlite://")

    g = graph()

    # Initial graph
    orders = g.create_node(
        "shopify.extract_orders",
        config={"shopify_admin_url": api_key},
    )
    output = produce(orders, env=env, modules=[shopify])
    records = output.as_records()
    assert len(records) > 0


if __name__ == "__main__":
    test_shopify()