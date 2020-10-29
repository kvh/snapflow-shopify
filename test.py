from pprint import pprint

from dags import Environment
from dags.core.graph import Graph
from dags.testing.utils import get_tmp_sqlite_db_url


def test_shopify(api_key: str):
    import dags_shopify

    env = Environment(metadata_storage="sqlite://")
    g = Graph(env)
    s = env.add_storage(get_tmp_sqlite_db_url())
    env.add_module(dags_shopify)
    # Initial graph
    g.add_node(
        "shopify_orders",
        "shopify.extract_orders",
        config={"shopify_admin_url": api_key},
    )
    output = env.produce(g, "shopify_orders", target_storage=s)
    records = output.as_records_list()
    pprint(records)


if __name__ == "__main__":
    api_key = input("Enter Shopify Admin URL: ")
    if not len(api_key) > 30:
        raise Exception(f"Invalid admin url {api_key}")
    test_shopify(api_key)